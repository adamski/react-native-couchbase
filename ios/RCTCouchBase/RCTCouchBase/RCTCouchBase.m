//
//  RCTCouchBase.m
//  RCTCouchBase
//
//  Created by Alberto Martinez de Murga on 16/02/2016.
//  Copyright © 2016 Upinion. All rights reserved.
//

#import "RCTCouchBase.h"
#import <React/RCTConvert.h>

@implementation RCTCouchBase

RCT_EXPORT_MODULE(CouchBase)

NSString* const PUSH = @"couchBasePushEvent";
NSString* const PULL = @"couchBasePullEvent";
NSString* const DB_CHANGED = @"couchBaseDBEvent";
NSString* const AUTH_ERROR = @"couchbBaseAuthError";
NSString* const NOT_FOUND = @"couchBaseNotFound";
NSString* const OFFLINE_KEY = @"couchBaseOffline";
NSString* const ONLINE_KEY = @"couchBaseOnline";

- (NSArray<NSString *> *)supportedEvents {
    return @[@"couchBasePushEvent",
             @"couchBasePullEvent",
             @"couchBaseDBEvent",
             @"couchbBaseAuthError",
             @"couchBaseNotFound",
             @"couchBaseOffline",
             @"couchBaseOnline"
             ];
}

- (id)init
{
    self = [super init];
    
    if (self) {
        CBLRegisterJSViewCompiler();
        manager = [[CBLManager alloc] init];
        databases = [[NSMutableDictionary alloc] init];
        pulls = [[NSMutableDictionary alloc] init];
        pushes = [[NSMutableDictionary alloc] init];
        timeout = 0;
        skippedEvents = 0;
        skipReplicationEvents = 0;
        if (!manager) {
            NSLog(@"Cannot create Manager instance");
            return self;
        }
        //[CBLManager enableLogging:@"Sync"];
        //[CBLManager enableLogging:@"CBLDatabase"];


    }
    return self;
}


- (NSString*)getName
{
    return @"CouchBase";
}


- (NSDictionary*)constantsToExport
{
    return @{
             @"PUSH" : PUSH,
             @"PULL" : PULL,
             @"DBChanged": DB_CHANGED,
             @"AuthError": AUTH_ERROR,
             @"NotFound": NOT_FOUND,
             @"Offline": OFFLINE_KEY,
             @"Online": ONLINE_KEY
             };
}

- (void) startServer: (int) port
            withUser: (NSString *) user
        withPassword: (NSString * ) pass
        withCallback: (RCTResponseSenderBlock) onEnd
{
    
    
    // Set up the listener.
    listener = [[CBLListener alloc] initWithManager:manager port:port];
    if (user != nil && pass != nil){
        NSDictionary *auth = @{user:pass};
        [listener setPasswords: auth];
    }

    // Init the listener.
    @try {
        NSError *err = nil;
        BOOL success = [listener start: &err];
        
        // Error handler
        if (success) {
            NSLog(@"CouchBase running on %@", listener.URL);
            // Callback handler
            if (onEnd != nil) {
                onEnd(@[[NSNumber numberWithInt:listener.port]]);
            }
            
        } else {
            NSLog(@"%@", err);
            //Close old databases
            for (CBLDatabase *db in [databases allValues]) {
                [db close:nil];
            }
            [self startServer:port+1 withUser:user withPassword:pass withCallback:onEnd];
        }
        
        // Exception handler
    } @catch (NSException *e) {
        NSLog(@"%@",e);
    }
}

- (void) startSync: (NSString*) databaseLocal
     withRemoteUrl: (NSString*) remoteUrl
    withRemoteUser: (NSString*) remoteUser
withRemotePassword: (NSString*) remotePassword
        withEvents: (BOOL) events // TODO: Convert to options
      withCallback: (RCTResponseSenderBlock) onEnd
{
    CBLDatabase* db = [manager existingDatabaseNamed:databaseLocal error:nil];
    if (!db) {
        NSLog(@"Database %@: could not be found", databaseLocal);
    } else {
        // Establish the connection.
        NSURL *url = [NSURL URLWithString:remoteUrl];
        id<CBLAuthenticator> auth = [CBLAuthenticator
                                     basicAuthenticatorWithName:remoteUser
                                     password:remotePassword];
        CBLReplication* push = [db createPushReplication: url];
        CBLReplication* pull = [db createPullReplication: url];
        
        push.continuous = YES;
        pull.continuous = YES;
        
        push.authenticator = auth;
        pull.authenticator = auth;
        
        if (timeout > 0) {
            push.customProperties = @{
                                      @"poll": [NSNumber numberWithInteger:timeout],
                                      @"websocket": @false
                                      };
            
            pull.customProperties = @{
                                      @"poll": [NSNumber numberWithInteger:timeout],
                                      @"websocket": @false
                                      };
        }
        [pushes setObject:push forKey:databaseLocal];
        [pulls  setObject:pull forKey:databaseLocal];
        // Add the events handler.
        if (events) {
            [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(handleReplicationEvent:) name:kCBLReplicationChangeNotification object:push];
            [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(handleReplicationEvent:) name:kCBLReplicationChangeNotification object:pull];
            [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(handleDatabaseEvent:) name:kCBLDatabaseChangeNotification object:nil];
        }
        
        [push start];
        [pull start];
    }
    // Callback handler
    if (onEnd != nil) {
        NSArray *cb = @[];
        onEnd(@[[NSNull null], cb]);
    }
}

- (void) startDatabaseChangeEvents: (NSString*) databaseLocal
                          resolver: (RCTPromiseResolveBlock) resolve
                          rejecter: (RCTPromiseRejectBlock) reject
{
    CBLDatabase* db = [manager existingDatabaseNamed:databaseLocal error:nil];
    if (!db) {
        reject(@"not_opened", [NSString stringWithFormat:@"Database %@: could not be found", databaseLocal], nil);
    } else {
        [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(handleDatabaseEvent:) name:kCBLDatabaseChangeNotification object:nil];
        resolve(@{});
    }
}

- (void) startPush: (NSString*) databaseLocal
     withRemoteUrl: (NSString*) remoteUrl
    withRemoteUser: (NSString*) remoteUser
withRemotePassword: (NSString*) remotePassword
        withEvents: (BOOL) events // TODO: Convert to options
          resolver: (RCTPromiseResolveBlock) resolve
          rejecter: (RCTPromiseRejectBlock) reject
{
    CBLDatabase* db = [manager existingDatabaseNamed:databaseLocal error:nil];
    if (!db) {
        reject(@"not_opened", [NSString stringWithFormat:@"Database %@: could not be found", databaseLocal], nil);
    } else {
        // Establish the connection.
        NSURL *url = [NSURL URLWithString:remoteUrl];
        id<CBLAuthenticator> auth = [CBLAuthenticator
                                     basicAuthenticatorWithName:remoteUser
                                     password:remotePassword];
        CBLReplication* push = [db createPushReplication: url];
        push.continuous = YES;
        push.authenticator = auth;
        
        if (timeout > 0) {
            push.customProperties = @{
                                      @"poll": [NSNumber numberWithInteger:timeout],
                                      @"websocket": @false
                                      };
        }
        [pushes setObject:push forKey:databaseLocal];
        // Add the events handler.
        if (events) {
            [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(handleReplicationEvent:) name:kCBLReplicationChangeNotification object:push];
        }
        
        [push start];
    }
    // Callback handler
    resolve(@{});
}

- (void) startPull: (NSString*) databaseLocal
     withRemoteUrl: (NSString*) remoteUrl
    withRemoteUser: (NSString*) remoteUser
withRemotePassword: (NSString*) remotePassword
        withOptions: (NSDictionary*) options
          resolver: (RCTPromiseResolveBlock) resolve
          rejecter: (RCTPromiseRejectBlock) reject
{
    CBLDatabase* db = [manager existingDatabaseNamed:databaseLocal error:nil];
    if (!db) {
        reject(@"not_opened", [NSString stringWithFormat:@"Database %@: could not be found", databaseLocal], nil);
    } else {
        // Establish the connection.
        NSURL *url = [NSURL URLWithString:remoteUrl];
        CBLReplication* pull = [db createPullReplication: url];

        if (remoteUser && remotePassword) {
            id <CBLAuthenticator> auth = [CBLAuthenticator
                    basicAuthenticatorWithName:remoteUser
                                      password:remotePassword];
            pull.authenticator = auth;
        }

        pull.continuous = [RCTConvert BOOL:options[@"continuous"]];

        if (options[@"channels"] != NULL) {
            pull.channels = [RCTConvert NSStringArray:options[@"channels"]];
            NSLog (@"Set replication channels: %@", pull.channels);
        }
        else if (options[@"filter"] != NULL) {
            pull.filter = [RCTConvert NSString:options[@"filter"]];
            pull.filterParams = [RCTConvert NSDictionary:options[@"filterParams"]];
            NSLog (@"Set filter '%@' with params '%@'", pull.filter, pull.filterParams);
        }
        else if (options[@"documentIDs"] != NULL) {
            pull.documentIDs = [RCTConvert NSStringArray:options[@"documentIDs"]];
        }

        if (timeout > 0) {
            pull.customProperties = @{
                                      @"poll": [NSNumber numberWithInteger:timeout],
                                      @"websocket": @false
                                      };
        }
        [pulls  setObject:pull forKey:databaseLocal];
        // Add the events handler.
        // TODO: Put "withEvents" into constant for reuse
        if ([RCTConvert BOOL:options[@"withEvents"]]) {
            [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(handleReplicationEvent:) name:kCBLReplicationChangeNotification object:pull];
        }
        
        [pull start];
    }
    resolve(@{});
}

- (void) handleDatabaseEvent: (NSNotification*) notification
{
    CBLDatabase* database = notification.object;
    NSArray* changes = notification.userInfo[@"changes"];
    
    for (CBLDatabaseChange* change in changes) {
        NSDictionary* map = @{
                              @"databaseName": database.name,
                              @"id": change.documentID
                              };
        [self sendEventWithName:DB_CHANGED body:map];
    }
}

- (void) handleReplicationEvent: (NSNotification*) notification
{
    CBLReplication* repl = notification.object;
    NSString* nameEvent = repl.pull? PULL : PUSH;
    if (repl.status == kCBLReplicationOffline) {
        NSDictionary* mapError = @{
                                   @"databaseName": repl.localDatabase.name,
                                   };
        [self sendEventWithName:OFFLINE_KEY body:mapError];
    } else {
        NSDictionary* mapSuccess = @{
                                   @"databaseName": repl.localDatabase.name,
                                   };
        [self sendEventWithName:ONLINE_KEY body:mapSuccess];
    }
    if (repl.status == kCBLReplicationActive) 
    {
        if (skippedEvents == 0 || (repl.completedChangesCount == repl.changesCount))
        {
            NSDictionary* map = @{
                    @"databaseName": repl.localDatabase.name,
                    @"stopped": [NSNumber numberWithBool:false],
                    @"changesCount": [NSString stringWithFormat:@"%u", repl.completedChangesCount],
                    @"totalChanges": [NSString stringWithFormat:@"%u", repl.changesCount]
            };
            [self sendEventWithName:nameEvent body:map];
        }
        if (skipReplicationEvents > 0)
        {
            skippedEvents = (skippedEvents + 1) % skipReplicationEvents;
        }

    } else if(repl.status == kCBLReplicationStopped) {
        NSDictionary* map = @{
                @"databaseName": repl.localDatabase.name,
                @"stopped": [NSNumber numberWithBool:true],
                @"changesCount": [NSString stringWithFormat:@"%u", repl.completedChangesCount],
                @"totalChanges": [NSString stringWithFormat:@"%u", repl.changesCount]
        };
        [self sendEventWithName:nameEvent body:map];
    }
    NSError *error = repl.lastError;
    if (error != nil && error.code == 401) {
        NSDictionary* mapError = @{
                @"databaseName": repl.localDatabase.name,
        };
        [self sendEventWithName:AUTH_ERROR body:mapError];
    } else if (error != nil && error.code == 404) {
        NSDictionary* mapError = @{
                @"databaseName": repl.localDatabase.name,
        };
        [self sendEventWithName:NOT_FOUND body:mapError];
    }
}

RCT_EXPORT_METHOD(serverManager: (NSDictionary*) options
                   withCallback: (RCTResponseSenderBlock) onEnd)
{
    if (!manager) {
        NSLog(@"Couchbase manager does not exist.");
    }

    NSLog (@"Default directory for CBLManager: %@", [CBLManager defaultDirectory]);

    if (options && options[@"skipReplicationEvents"] != NULL) {
        skipReplicationEvents = [RCTConvert NSInteger:options[@"skipReplicationEvents"]];
    }

    //[CBLManager enableLogging:@"Database"];
    //[CBLManager enableLogging:@"Router"];
    //[CBLManager enableLogging:@"Listener"];

    // Callback handler
    if (onEnd != nil) {
        onEnd(@[]);
    }
}

RCT_EXPORT_METHOD(serverLocal: (int) listenPort
                              withUserLocal: (NSString*) userLocal
                              withPasswordLocal: (NSString*) passwordLocal
                              withCallback: (RCTResponseSenderBlock) onEnd)
{
    // Init server.
    [self startServer: listenPort
             withUser: userLocal
         withPassword: passwordLocal
         withCallback: onEnd];
}


RCT_EXPORT_METHOD(serverLocalRemote: (int) listenPort
                              withUserLocal: (NSString*) userLocal
                              withPasswordLocal: (NSString*) passwordLocal
                              withDatabaseLocal: (NSString*) databaseLocal
                              withRemoteUrl: (NSString*) remoteUrl
                              withRemoteUser: (NSString*) remoteUser
                              withRemotePassword: (NSString*) remotePassword
                              withEvents: (BOOL) events
                              withCallback: (RCTResponseSenderBlock) onEnd)
{
    // Init the server.
    [self startServer:listenPort
             withUser:userLocal
         withPassword:passwordLocal
         withCallback:onEnd];

    [manager doAsync:^(void) {
        NSError* err;
        CBLDatabase* database = [manager databaseNamed:databaseLocal error:&err];
        if (!database) {
            NSLog(@"Database %@: could not be created. %@", databaseLocal, err);
        } else {
            [databases setObject:database forKey:databaseLocal];
        }
        // Init sync.
        [self startSync:databaseLocal
          withRemoteUrl:remoteUrl
         withRemoteUser:remoteUser
     withRemotePassword:remotePassword
             withEvents:events
           withCallback:nil];
    }];
}

RCT_EXPORT_METHOD(serverRemote: (NSString*) databaseLocal
                              withRemoteUrl: (NSString*) remoteUrl
                              withRemoteUser: (NSString*) remoteUser
                              withRemotePassword: (NSString*) remotePassword
                              withEvents: (BOOL) events
                              withCallback: (RCTResponseSenderBlock) onEnd)
{
    [manager doAsync:^(void) {
        // Init sync.
        [self startSync:databaseLocal
          withRemoteUrl:remoteUrl
         withRemoteUser:remoteUser
     withRemotePassword:remotePassword
             withEvents:events
           withCallback:onEnd];
    }];
}


RCT_EXPORT_METHOD(databaseChangeEvents: (NSString*) databaseLocal
                              resolver: (RCTPromiseResolveBlock) resolve
                              rejecter: (RCTPromiseRejectBlock) reject)
{
    [manager doAsync:^(void) {
        // Init sync.
        [self startDatabaseChangeEvents:databaseLocal
                               resolver:resolve
                               rejecter:reject];
    }];
}

RCT_EXPORT_METHOD(serverRemotePush: (NSString*) databaseLocal
                              withRemoteUrl: (NSString*) remoteUrl
                              withRemoteUser: (NSString*) remoteUser
                              withRemotePassword: (NSString*) remotePassword
                              withEvents: (BOOL) events
                              resolver: (RCTPromiseResolveBlock) resolve
                              rejecter: (RCTPromiseRejectBlock) reject)
{
    [manager doAsync:^(void) {
        // Init sync.
        [self startPush:databaseLocal
          withRemoteUrl:remoteUrl
         withRemoteUser:remoteUser
     withRemotePassword:remotePassword
             withEvents:events // TODO: Convert to options
               resolver:resolve
               rejecter:reject];
    }];
}

RCT_EXPORT_METHOD(serverRemotePull: (NSString*) databaseLocal
                              withRemoteUrl: (NSString*) remoteUrl
                              withRemoteUser: (NSString*) remoteUser
                              withRemotePassword: (NSString*) remotePassword
                              withOptions: (NSDictionary*) options
                              resolver: (RCTPromiseResolveBlock) resolve
                              rejecter: (RCTPromiseRejectBlock) reject)
{

    [manager doAsync:^(void) {
        // Init sync.
        [self startPull:databaseLocal
          withRemoteUrl:remoteUrl
         withRemoteUser:remoteUser
     withRemotePassword:remotePassword
            withOptions:options
               resolver:resolve
               rejecter:reject];
    }];
}

RCT_EXPORT_METHOD(compact: (NSString*) databaseLocal)
{
    if (![manager databaseExistsNamed: databaseLocal]) {
        NSLog(@"Database %@: could not be found", databaseLocal);
    } else {
        [manager doAsync:^(void) {
            NSError* err;
            CBLDatabase* database = [manager existingDatabaseNamed:databaseLocal error:&err];
            bool compact = [database compact:&err];
            if (!compact) {
                NSLog(@"Database %@: could not compact. %@", databaseLocal, err);
            }
        }];
    }
}

RCT_EXPORT_METHOD(setTimeout: (NSInteger) newtimeout)
{
    timeout = newtimeout;
}

RCT_EXPORT_METHOD(createDatabase: (NSString*) databaseName
                              resolver:(RCTPromiseResolveBlock)resolve
                              rejecter:(RCTPromiseRejectBlock)reject)
{
    [manager doAsync:^(void) {
        NSError* err;
        CBLDatabase* database = [manager databaseNamed:databaseName error:&err];
        if (!database) {
            reject(@"not_opened", [NSString stringWithFormat:@"Database %@: could not be created", databaseName], err);
        } else {
            [databases setObject:database forKey:databaseName];
            resolve(@{});
        }
    }];
}

RCT_EXPORT_METHOD(destroyDatabase: (NSString*) databaseName
                              resolver:(RCTPromiseResolveBlock)resolve
                              rejecter:(RCTPromiseRejectBlock)reject)
{
    __block NSError* err;
    if (![manager databaseExistsNamed:databaseName]) {
        reject(@"not_opened", [NSString stringWithFormat:@"Database %@: could not be opened", databaseName], nil);
        return;
    } else {
        [databases removeObjectForKey:databaseName];
        [pushes removeObjectForKey:databaseName];
        [pulls removeObjectForKey:databaseName];

        [manager doAsync:^(void) {
            CBLDatabase* database = [manager existingDatabaseNamed:databaseName error:nil];
            bool deleted = [database deleteDatabase:&err];
            if (!deleted) {
                reject(@"not_opened", [NSString stringWithFormat:@"Database %@: could not be destroyed", databaseName], err);
                return;
            } else {
                resolve(@{});
            }
        }];
    }
}

RCT_EXPORT_METHOD(closeDatabase: (NSString*) databaseName withCallback: (RCTResponseSenderBlock) onEnd)
{
    __block NSError* err;
    if (![manager databaseExistsNamed:databaseName]) {
        NSLog(@"Database %@: could not be found", databaseName);
        // Callback handler
        if (onEnd != nil) {
            NSArray *cb = @[];
            onEnd(@[[NSNull null], cb]);
        }
    } else {
        [manager doAsync:^(void) {
            CBLDatabase* database = [manager existingDatabaseNamed:databaseName error:nil];
            bool closed = [database close:&err];
            if (!closed) {
                NSLog(@"Database %@: could not be destroyed. %@", databaseName, err);
            }
            // Callback handler
            if (onEnd != nil) {
                NSArray *cb = @[];
                onEnd(@[[NSNull null], cb]);
            }
        }];

    }
}

RCT_EXPORT_METHOD(putDocument: (NSString*) db
                              withId:(NSString*) docId
                              withObject:(NSDictionary*) dict
                              resolver:(RCTPromiseResolveBlock)resolve
                              rejecter:(RCTPromiseRejectBlock)reject)
{
    if (![manager databaseExistsNamed: db]) {
        reject(@"not_opened", [NSString stringWithFormat:@"Database %@: could not be opened", db], nil);
        return;
    }
    [manager doAsync:^(void) {
        NSError* err;

        CBLDatabase* database = [manager existingDatabaseNamed:db error:&err];
        // We need to check if it is a _local document or a normal document.
        NSRegularExpression* regex = [NSRegularExpression regularExpressionWithPattern:@"^_local\/(.+)"
                                                                               options:0
                                                                                 error:&err];
        NSTextCheckingResult* match = [regex firstMatchInString:docId
                                                        options:0
                                                          range:NSMakeRange(0, [docId length])];
        if (match) {
            NSString* localDocId = [docId substringWithRange:[match rangeAtIndex:1]];
            bool success = [database putLocalDocument:dict withID:localDocId error:&err];
            if (success) {
                resolve(@{});
            } else {
                reject(@"missing_document", [NSString stringWithFormat:@"could not create/update document: %@", docId], nil);
            }
        } else {
            CBLDocument* doc = [database documentWithID:docId];
            if (doc != nil) {
                CBLSavedRevision* revision = [doc putProperties:dict error:&err];
                if (revision) {
                    resolve(@{});
                } else {
                    reject(@"missing_document", [NSString stringWithFormat:@"could not create/update document: %@", docId], nil);
                }
            } else {
                reject(@"missing_document", [NSString stringWithFormat:@"could not create/update document: %@", docId], nil);
            }
        }
    }];
}

RCT_EXPORT_METHOD(getDocument: (NSString*) db
                          withId:(NSString*) docId
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)
{
    if (![manager databaseExistsNamed: db]) {
        reject(@"not_opened", [NSString stringWithFormat:@"Database %@: could not be opened", db], nil);
        return;
    }
    [manager doAsync:^(void) {
        NSLog(@"getDocument id: @%", docId);
        NSError *err;
        
        CBLDatabase* database = [manager existingDatabaseNamed:db error:&err];
        // We need to check if it is a _local document or a normal document.
        NSRegularExpression* regex = [NSRegularExpression regularExpressionWithPattern:@"^_local\/(.+)"
                                                                               options:0
                                                                                 error:&err];
        NSTextCheckingResult* match = [regex firstMatchInString:docId
                                                        options:0
                                                          range:NSMakeRange(0, [docId length])];
        
        if (match) {
            NSString* localDocId = [docId substringWithRange:[match rangeAtIndex:1]];
            CBLJSONDict* doc = [database existingLocalDocumentWithID:localDocId];
            if (doc != nil) {
                resolve(doc);
            } else {
                reject(@"not_opened", [NSString stringWithFormat:@"document not found %@", docId], nil);
            }
        } else {
            CBLDocument* doc = [database existingDocumentWithID:docId];
            if (doc != nil && doc.properties != nil) {
                resolve(doc.properties);
            } else {
                reject(@"not_opened", [NSString stringWithFormat:@"document not found %@", docId], nil);
            }
        }
    }];
}


RCT_EXPORT_METHOD(getAllDocuments: (NSString*) db
                  withIds:(nullable NSArray*) ids
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)
{
    if (![manager databaseExistsNamed: db]) {
        reject(@"not_opened", [NSString stringWithFormat:@"Database %@: could not be opened", db], nil);
        return;
    }
    [manager doAsync:^(void) {
        NSError* err;
        
        CBLDatabase* database = [manager existingDatabaseNamed:db error:&err];
        NSMutableArray* results = [[NSMutableArray alloc] init];
        if (ids == NULL || [ids count] == 0) {
            CBLQuery* query = [database createAllDocumentsQuery];
            query.allDocsMode = kCBLAllDocs;
            
            CBLQueryEnumerator* qResults = [query run:&err];
            
            if (qResults == nil) {
                reject(@"query_failed", @"The query could not be completed", err);
                return;
            }
            
            for (CBLQueryRow* row in qResults) {
                if (row.document.properties != nil) {
                    [results addObject:@{@"doc": row.document.properties,
                                         @"_id": row.document.documentID,
                                         @"key": row.document.documentID,
                                         @"value": @{@"rev": row.document.currentRevisionID}
                                         }];
                }
            }
        } else {
            
            for(NSString* docId in ids) {
                // We need to check if it is a _local document or a normal document.
                NSRegularExpression* regex = [NSRegularExpression regularExpressionWithPattern:@"^_local\/(.+)"
                                                                                       options:0
                                                                                         error:&err];
                NSTextCheckingResult* match = [regex firstMatchInString:docId
                                                                options:0
                                                                  range:NSMakeRange(0, [docId length])];
                
                if (match) {
                    NSString* localDocId = [docId substringWithRange:[match rangeAtIndex:1]];
                    CBLJSONDict* doc = [database existingLocalDocumentWithID:localDocId];
                    if (doc != nil) {
                        [results addObject: @{@"doc": doc,
                                              @"_id": localDocId,
                                              @"key": localDocId
                                              }];
                    }
                } else {
                    CBLDocument* doc = [database existingDocumentWithID:docId];
                    if (doc != nil && doc.properties != nil) {
                        NSMutableDictionary* values = [NSMutableDictionary dictionaryWithObjects: [doc.properties allValues] forKeys:[doc.properties allKeys]];
                        [values setValue: doc.documentID forKey:@"_id"];
                        [results addObject: @{@"doc": values,
                                              @"_id": doc.documentID,
                                              @"key": doc.documentID,
                                              @"value": @{@"rev": doc.currentRevisionID}
                                              }];
                    }
                }
            }
        }
        resolve(@{
                  @"rows": results,
                  @"total_rows": [NSNumber numberWithUnsignedInteger: results.count]
                  });
    }];
}

RCT_EXPORT_METHOD(addView: (NSString*) db
                              withName: (NSString*) name
                              withView: (NSDictionary*) viewDict
                              resolver:(RCTPromiseResolveBlock)resolve
                              rejecter:(RCTPromiseRejectBlock)reject)
{
    NSLog(@"Add view: %@", name);
    // Create a view and register its map function
    if (![manager databaseExistsNamed: db]) {
        reject(@"not_opened", [NSString stringWithFormat:@"Database %@: could not be opened", db], nil);
        return;
    }

    if (!viewDict[@"map"]) {
        reject (@"missing_map", @"Missing map function", nil);
        return;
    }

    [manager doAsync:^(void) {
        NSError *err;

        CBLDatabase *database = [manager existingDatabaseNamed:db error:&err];
        CBLView *view = [database viewNamed:name];

        NSString* mapFunction = [RCTConvert NSString:viewDict[@"map"]];

        NSString* version;
        if (viewDict[@"version"] != NULL) version = [RCTConvert NSString:viewDict[@"version"]];
        else version = @"1";

        CBLMapBlock mapBlock = [[CBLView compiler]compileMapFunction:mapFunction language:@"javascript"];

        if (viewDict[@"reduce"]) {
            NSString* reduceFunction = [RCTConvert NSString:viewDict[@"reduce"]];
            CBLReduceBlock reduceBlock = [[CBLView compiler]compileReduceFunction:reduceFunction language:@"javascript"];
            [view setMapBlock:mapBlock reduceBlock:reduceBlock version:version];

            if ([view reduceBlock] == nil) {
                reject(@"invalid_reduce", @"Invalid reduce function", nil);
                return;
            }
        } else {
            [view setMapBlock:mapBlock version:version];
        }

        if (![database existingViewNamed:name]) {
            reject(@"invalid_view", [NSString stringWithFormat:@"View %@: was not added to database", name], nil);
            return;
        }

        if ([view mapBlock] == nil) {
            reject(@"invalid_map", @"Invalid map function", nil);
            return;
        }

    }];
}

RCT_EXPORT_METHOD(getView: (NSString*) db
                  withDesign: (NSString*) design
                  withView: (NSString*) viewName
                  withParams: (NSDictionary*) params
                  withKeys: (NSArray*) keys
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)
{
    if (![manager databaseExistsNamed: db]) {
        reject(@"not_opened", [NSString stringWithFormat:@"Database %@: could not be opened", db], nil);
        return;
    }
    [manager doAsync:^(void) {
        NSError* err;
        //NSLog(@"Getting View named %@", viewName);
        CBLDatabase* database = [manager existingDatabaseNamed:db error:&err];
        CBLView* view = [database existingViewNamed:viewName];
        if (view == nil || (view && [view mapBlock] == nil)) {
            if (view && [view mapBlock] == nil)
                NSLog(@"MapBlock for %@ nil", viewName);
            else NSLog(@"View %@ nil", viewName);

            view = [database viewNamed:viewName];

            CBLDocument* viewsDoc = [database existingDocumentWithID:[NSString stringWithFormat:@"_design/%@", design]];
            if (viewsDoc == nil || viewsDoc.properties == nil || [viewsDoc.properties objectForKey:@"views"] == nil) {
                reject(@"not_found", [NSString stringWithFormat:@"Database %@: design file could not be opened", db], nil);
                return;
            }
            
            NSDictionary* views = [viewsDoc.properties objectForKey:@"views"];
            if ([views objectForKey:viewName] == nil || [[views objectForKey:viewName] objectForKey:@"map"] == nil) {
                reject(@"not_found", [NSString stringWithFormat:@"Database %@: view not found", db], nil);
                return;
            }
            
            NSDictionary* viewDefinition = [views objectForKey:viewName];
            CBLMapBlock mapBlock = [[CBLView compiler]compileMapFunction:[viewDefinition objectForKey:@"map"] language:@"javascript"];
            NSString* version = [viewDefinition objectForKey:@"version"];
            
            if (mapBlock == nil) {
                reject(@"invalid_map", @"Invalid map function", nil);
                return;
            }
            
            if([viewDefinition objectForKey:@"reduce"] != nil) {
                CBLReduceBlock reduceBlock = [[CBLView compiler]compileReduceFunction:[viewDefinition objectForKey:@"reduce"] language:@"javascript"];
                if (reduceBlock == nil) {
                    reject(@"invalid_reduce", @"Invalid reduce function", nil);
                    return;
                }
                [view setMapBlock:mapBlock reduceBlock:reduceBlock version: version != nil ? [NSString stringWithString: version] : @"1.1"];
            } else {
                [view setMapBlock:mapBlock version: version != nil ? [NSString stringWithString: version] : @"1.1"];
            }
        } else {
            [view updateIndex];
        }
        
        CBLQuery* query = [view createQuery];

        //NSLog(@"Limit input: %@", params[@"limit"]);

        BOOL includeDocs = true;
        if (params[@"include_docs"] != NULL)
            includeDocs = [RCTConvert BOOL:params[@"include_docs"]];

        NSArray* paramKeys = [params allKeys];
        if ([paramKeys containsObject:@"startkey"]) query.startKey = params[@"startkey"];
        if ([paramKeys containsObject:@"endkey"]) query.endKey = params[@"endkey"];
        if ([paramKeys containsObject:@"descending"]) query.descending = [RCTConvert BOOL:params[@"descending"]];
        if ([paramKeys containsObject:@"limit"]) query.limit = [RCTConvert NSUInteger:params[@"limit"]];
        if ([paramKeys containsObject:@"skip"]) query.skip = [RCTConvert NSUInteger:params[@"skip"]];
        if ([paramKeys containsObject:@"group"]) query.groupLevel = [RCTConvert NSUInteger:params[@"group"]];

        if (keys != nil && [keys count] > 0) query.keys = keys;

        //if (query.limit != NULL)
        //    NSLog(@"Query limit: %d", query.limit);

        CBLQueryEnumerator* qResults = [query run: &err];
        if (err != nil) {
            reject(@"query_error", @"The query failed", err);
            return;
        }
        
        NSMutableArray* results = [[NSMutableArray alloc] init];
        for (CBLQueryRow* row in qResults) {
            if (row.document.properties != nil) {
                NSLog(@"CBLQueryRow key: %@", row.key);
                NSLog(@"CBLQueryRow document: %@", row.document);
                NSMutableDictionary* values = [NSMutableDictionary dictionaryWithObjects: [row.document.properties allValues] forKeys:[row.document.properties allKeys]];
                [values setValue: row.document.documentID forKey:@"_id"];
                [results addObject: @{@"value": (includeDocs ? values : row.value),
                                      @"_id": row.document.documentID,
                                      @"key": row.key? row.key : row.document.documentID,
                                      }];
            // The reduced views have a different format.
            } else if([view reduceBlock] != nil) {
                NSDictionary* resultEntry = @{@"key": row.key? row.key : nil, @"value": row.value};
                [results addObject:resultEntry];
            }
        }

        if ([paramKeys containsObject:@"update_seq"] && [[params objectForKey:@"update_seq"]  isEqual: @YES]) {
            resolve(@{@"rows": results,
                      @"offset": [NSNumber numberWithUnsignedInteger: query.skip ? query.skip : 0],
                      @"total_rows": [NSNumber numberWithLongLong: qResults.count],
                      @"update_seq": [NSNumber numberWithLongLong: qResults.sequenceNumber]
                      });
        } else {
            resolve(@{@"rows": results,
                      @"offset": [NSNumber numberWithUnsignedInteger: query.skip ? query.skip : 0],
                      @"total_rows": [NSNumber numberWithLongLong: qResults.count]
                      });
        }
    }];
}

/**
 * Install pre-built database
 */
RCT_EXPORT_METHOD(installPrebuiltDatabase:(NSString *) databaseName
                              resolver:(RCTPromiseResolveBlock)resolve
                              rejecter:(RCTPromiseRejectBlock)reject)
{
    CBLDatabase* db = [manager existingDatabaseNamed:databaseName error:nil];
    if (db == nil) {
        NSLog(@"Database not found, installing...");
        NSError* err;
        NSString* dbPath = [[NSBundle mainBundle] pathForResource:databaseName ofType:@"cblite2"];
        NSLog(@"Found database file at %@", dbPath);
        [manager replaceDatabaseNamed:databaseName withDatabaseDir:dbPath error:&err];
        if (err != nil)
        {
            reject (@"not_opened", @"Failed to install prebuilt database", err);
            return;
        }

        resolve(@{@"installed": @YES});
        return;
    }

    NSLog(@"Database found, not installed");
    resolve(@{@"installed": @NO});
}

@end
