require 'mkmf'

if have_library('stdc++') and have_library('replication')
  create_makefile('binlog')
end
