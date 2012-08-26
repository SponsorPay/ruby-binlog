#include "ruby_binlog.h"

namespace ruby {
namespace binlog {

const char* get_field_type_str(mysql::system::enum_field_types type) {
  switch(type) {
  case mysql::system::MYSQL_TYPE_DECIMAL:     return "DECIMAL";     break;
  case mysql::system::MYSQL_TYPE_TINY:        return "TINY";        break;
  case mysql::system::MYSQL_TYPE_SHORT:       return "SHORT";       break;
  case mysql::system::MYSQL_TYPE_LONG:        return "LONG";        break;
  case mysql::system::MYSQL_TYPE_FLOAT:       return "FLOAT";       break;
  case mysql::system::MYSQL_TYPE_DOUBLE:      return "DOUBLE";      break;
  case mysql::system::MYSQL_TYPE_NULL:        return "NULL";        break;
  case mysql::system::MYSQL_TYPE_TIMESTAMP:   return "TIMESTAMP";   break;
  case mysql::system::MYSQL_TYPE_LONGLONG:    return "LONGLONG";    break;
  case mysql::system::MYSQL_TYPE_INT24:       return "INT24";       break;
  case mysql::system::MYSQL_TYPE_DATE:        return "DATE";        break;
  case mysql::system::MYSQL_TYPE_TIME:        return "TIME";        break;
  case mysql::system::MYSQL_TYPE_DATETIME:    return "DATETIME";    break;
  case mysql::system::MYSQL_TYPE_YEAR:        return "YEAR";        break;
  case mysql::system::MYSQL_TYPE_NEWDATE:     return "NEWDATE";     break;
  case mysql::system::MYSQL_TYPE_VARCHAR:     return "VARCHAR";     break;
  case mysql::system::MYSQL_TYPE_BIT:         return "BIT";         break;
  case mysql::system::MYSQL_TYPE_NEWDECIMAL:  return "NEWDECIMAL";  break;
  case mysql::system::MYSQL_TYPE_ENUM:        return "ENUM";        break;
  case mysql::system::MYSQL_TYPE_SET:         return "SET";         break;
  case mysql::system::MYSQL_TYPE_TINY_BLOB:   return "TINY_BLOB";   break;
  case mysql::system::MYSQL_TYPE_MEDIUM_BLOB: return "MEDIUM_BLOB"; break;
  case mysql::system::MYSQL_TYPE_LONG_BLOB:   return "LONG_BLOB";   break;
  case mysql::system::MYSQL_TYPE_BLOB:        return "BLOB";        break;
  case mysql::system::MYSQL_TYPE_VAR_STRING:  return "VAR_STRING";  break;
  case mysql::system::MYSQL_TYPE_STRING:      return "STRING";      break;
  case mysql::system::MYSQL_TYPE_GEOMETRY:    return "GEOMETRY";    break;
  }

  return 0;
}

} // namespace binlog
} // namespace ruby
