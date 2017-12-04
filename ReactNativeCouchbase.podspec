require 'json'

package = JSON.parse(File.read(File.join(__dir__, 'package.json')))

Pod::Spec.new do |s|
  s.name                = "ReactNativeCouchbase"
  s.version             = package['version']
  s.summary             = package['description']
  s.homepage            = "https://github.com/Upinion/react-native-couchbase"
  s.license             = package['license']
  s.author              = package['author']
  s.platform            = :ios, "8.0"
  s.source              = { :path => "." }
  s.source_files        = "ios", "ios/**/*.{h,m}"
end
