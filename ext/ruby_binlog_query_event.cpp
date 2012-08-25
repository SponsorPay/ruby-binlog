#include "ruby_binlog.h"

VALUE rb_cBinlogQueryEvent;

namespace ruby {
namespace binlog {

void QueryEvent::free(QueryEvent *p) {
  if (p->m_event) {
    delete p->m_event;
    p->m_event = 0;
    p->m_event_header = 0;
  }

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

  rb_define_method(rb_cBinlogQueryEvent, "query", __F(&query), 0);
}

VALUE QueryEvent::query(VALUE self) {
  QueryEvent *p;
  Data_Get_Struct(self, QueryEvent, p);
  return rb_str_new2(p->m_event->query.c_str());
}

} // namespace binlog
} // namespace ruby
