Gem::Specification.new do |spec|
  spec.name              = 'ruby-binlog'
  spec.version           = '0.1.9'
  spec.summary           = 'Ruby binding for MySQL Binary log API.'
  spec.description       = 'Ruby binding for MySQL Binary log API.'
  spec.files             = Dir.glob('ext/{*.cpp,*.h,extconf.rb}') + %w(README.md)
  spec.author            = 'winebarrel'
  spec.email             = 'sgwr_dts@yahoo.co.jp'
  spec.homepage          = 'https://bitbucket.org/winebarrel/ruby-binlog'
  spec.extensions        = 'ext/extconf.rb'
  spec.has_rdoc          = true
  spec.rdoc_options      << '--title' << 'ruby-binlog - Ruby binding for MySQL Binary log API.'
  spec.extra_rdoc_files  = %w(README.md)
end
