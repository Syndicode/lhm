require 'rake/testtask'
require 'bundler'
require 'bundler/gem_tasks'

Bundler::GemHelper.install_tasks

Rake::TestTask.new('unit') do |t|
  t.libs << 'lib'
  t.libs << 'spec'
  t.test_files = FileList['spec/unit/**/*_spec.rb']
  t.verbose = true
end

Rake::TestTask.new('integration') do |t|
  t.libs << 'lib'
  t.libs << 'spec'
  t.test_files = FileList['spec/integration/**/*_spec.rb']
  t.verbose = true
  end

Rake::TestTask.new('dev') do |t|
  t.libs << 'lib'
  t.libs << 'spec'
  t.test_files = FileList[
    'spec/test_helper.rb',
  #  Add file to test individually
  ]
  t.verbose = true
end

# `rake dev` allows to test a single file without the need to run the entire integration test suite
Rake::TestTask.new('dev') do |t|
  t.libs << 'lib'
  t.libs << 'spec'
  t.test_files = FileList[
    'spec/test_helper.rb',
    # Add path to file to test
  ]
  t.verbose = true
end

task :specs => [:unit, :integration]
task :default => :specs
