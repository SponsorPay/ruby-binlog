#ifndef __RUBY_BINLOG_EVENT_H__
#define __RUBY_BINLOG_EVENT_H__

#include "ruby_binlog.h"

extern VALUE rb_cBinlogEvent;

namespace ruby {
namespace binlog {

struct Event {
  mysql::Log_event_header *m_event_header;

  static VALUE get_marker(VALUE self);
  static VALUE get_timestamp(VALUE self);
  static VALUE get_type_code(VALUE self);
  static VALUE get_server_id(VALUE self);
  static VALUE get_event_length(VALUE self);
  static VALUE get_next_position(VALUE self);
  static VALUE get_flags(VALUE self);
  static VALUE get_event_type(VALUE self);
  static void init(VALUE clazz);
};

struct QueryEvent : public Event {
  mysql::Query_event *m_event;

  static void free(QueryEvent *p);
  static VALUE alloc(VALUE klass);
  static void set_event(VALUE self, mysql::Binary_log_event *event);
  static void init();
  static VALUE query(VALUE self);
};

struct UnimplementedEvent : public Event {
  mysql::Binary_log_event *m_event;

  static void free(QueryEvent *p);
  static VALUE alloc(VALUE klass);
  static void set_event(VALUE self, mysql::Binary_log_event *event);
  static void init();
};

} // namespace binlog
} // namespace ruby

#endif // __RUBY_BINLOG_EVENT_H__
