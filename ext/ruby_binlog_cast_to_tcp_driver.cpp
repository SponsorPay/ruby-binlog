#include "ruby_binlog.h"

namespace ruby {
namespace binlog {

mysql::system::Binlog_tcp_driver *cast_to_tcp_driver(mysql::system::Binary_log_driver *driver) {
  const char *cname = typeid(*driver).name();
  std::string name = cname;

  if(name.find("tcp_driver") == std::string::npos) {
    return 0;
  }

  return static_cast<mysql::system::Binlog_tcp_driver *>(driver);
}

} // namespace binlog
} // namespace ruby
