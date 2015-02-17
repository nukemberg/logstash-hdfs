Gem::Specification.new do |s|

  s.name            = 'logstash-output-hdfs'
  s.version         = '0.2.1'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "$summary"
  s.description     = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["Avishai Ish-Shalom"]
  s.email           = 'avishai@fewbytes.com'
  s.homepage        = "https://github.com/avishai-ish-shalom/logstash-hdfs"
  s.require_paths = ["lib"]

  # Files
  s.files = `git ls-files`.split($\)+::Dir.glob('vendor/*')

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency 'logstash', '>= 1.4.0', '< 2.0.0'

  s.add_development_dependency 'logstash-devutils'
end
