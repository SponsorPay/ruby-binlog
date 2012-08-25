#ifndef __RUBY_BINLOG_EVENT_H__
#define __RUBY_BINLOG_EVENT_H__

#include "ruby_binlog.h"

namespace ruby {
namespace binlog {

struct QueryEvent {
  mysql::Query_event *m_event;

  static void free(QueryEvent *p);
  static VALUE alloc(VALUE klass);
  static void set_event(VALUE self, mysql::Binary_log_event *event);
  static void init();
  static VALUE query(VALUE self);
};

} // namespace binlog
} // namespace ruby

#endif // __RUBY_BINLOG_EVENT_H__
