Gem::Specification.new do |s|
  s.name = 'logstash-output-doradus'
  s.version         = "0.1.3"
  s.licenses = ["Apache License (2.0)"]
  s.summary = "This example output does nothing."
  s.description = "This gem is a doradus logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors = ["Piyush"]
  s.email = "piyush.mattoo@software.dell.com"
  s.homepage = "https://github.com/PiyushMattoo/logstash-output-doradus"
  s.require_paths = ["lib"]

  # Files
  s.files = `git ls-files`.split($\)
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core", ">= 1.4.0", "< 2.0.0"
  s.add_runtime_dependency "logstash-codec-plain"
  s.add_development_dependency "logstash-devutils"
end
