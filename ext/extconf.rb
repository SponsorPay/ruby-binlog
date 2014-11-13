require 'mkmf'

if have_library('stdc++') and have_library('boost_thread-mt') and have_library('boost_system-mt') and have_library('replication')
  CONFIG['warnflags'].slice!(/ -Wdeclaration-after-statement/)
  CONFIG['warnflags'].slice!(/ -Wimplicit-function-declaration/)
  create_makefile('binlog')
end
