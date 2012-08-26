#include "ruby_binlog.h"

VALUE rb_cBinlogQueryEvent;

namespace ruby {
namespace binlog {

void QueryEvent::free(QueryEvent *p) {
  if (p->m_event) {
    delete p->m_event;
    p->m_event = 0;
  }

  p->m_event_header = 0;

  delete p;
}

VALUE QueryEvent::alloc(VALUE klass) {
  QueryEvent *p;

  p = new QueryEvent();
  p->m_event = 0;
  p->m_event_header = 0;

  return Data_Wrap_Struct(klass, 0, &free, p);
}

void QueryEvent::set_event(VALUE self, mysql::Binary_log_event *event) {
  QueryEvent *p;

  Data_Get_Struct(self, QueryEvent, p);
  p->m_event = static_cast<Query_event*>(event);
  p->m_event_header = event->header();
}

void QueryEvent::init() {
  rb_cBinlogQueryEvent = rb_define_class_under(rb_mBinlog, "QueryEvent", rb_cBinlogEvent);
  rb_define_alloc_func(rb_cBinlogQueryEvent, &alloc);

  Event::init(rb_cBinlogQueryEvent);

  rb_define_method(rb_cBinlogQueryEvent, "thread_id",  __F(&get_thread_id),  0);
  rb_define_method(rb_cBinlogQueryEvent, "exec_time",  __F(&get_exec_time),  0);
  rb_define_method(rb_cBinlogQueryEvent, "error_code", __F(&get_error_code), 0);
  rb_define_method(rb_cBinlogQueryEvent, "variables", __F(&get_variables), 0);
  rb_define_method(rb_cBinlogQueryEvent, "db_name",    __F(&get_db_name),    0);
  rb_define_method(rb_cBinlogQueryEvent, "query",      __F(&get_query),      0);
}

VALUE QueryEvent::get_thread_id(VALUE self) {
  QueryEvent *p;
  Data_Get_Struct(self, QueryEvent, p);
  return ULONG2NUM(p->m_event->thread_id);
}

VALUE QueryEvent::get_exec_time(VALUE self) {
  QueryEvent *p;
  Data_Get_Struct(self, QueryEvent, p);
  return ULONG2NUM(p->m_event->exec_time);
}

VALUE QueryEvent::get_error_code(VALUE self) {
  QueryEvent *p;
  Data_Get_Struct(self, QueryEvent, p);
  return ULONG2NUM(p->m_event->error_code);
}

VALUE QueryEvent::get_variables(VALUE self) {
  QueryEvent *p;
  VALUE retval = rb_ary_new();

  Data_Get_Struct(self, QueryEvent, p);

  for (std::vector<uint8_t>::iterator itor = p->m_event->variables.begin();
       itor != p->m_event->variables.end(); itor++) {
    rb_ary_push(retval, UINT2NUM(*itor));
  }

  return retval;
}

VALUE QueryEvent::get_db_name(VALUE self) {
  QueryEvent *p;
  Data_Get_Struct(self, QueryEvent, p);
  return rb_str_new2(p->m_event->db_name.c_str());
}

VALUE QueryEvent::get_query(VALUE self) {
  QueryEvent *p;
  Data_Get_Struct(self, QueryEvent, p);
  return rb_str_new2(p->m_event->query.c_str());
}

} // namespace binlog
} // namespace ruby
