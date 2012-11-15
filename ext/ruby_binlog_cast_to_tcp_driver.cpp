#include "ruby_binlog.h"

namespace ruby {
namespace binlog {

mysql::system::Binlog_tcp_driver *cast_to_tcp_driver(mysql::system::Binary_log_driver *driver) {
  return dynamic_cast<mysql::system::Binlog_tcp_driver *>(driver);
}

} // namespace binlog
} // namespace ruby
