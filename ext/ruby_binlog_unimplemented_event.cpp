#include "ruby_binlog.h"

VALUE rb_cBinlogUnimplementedEvent;

namespace ruby {
namespace binlog {

void UnimplementedEvent::free(QueryEvent *p) {
  if (p->m_event) {
    delete p->m_event;
    p->m_event = 0;
  }

  p->m_event_header = 0;

  delete p;
}

VALUE UnimplementedEvent::alloc(VALUE klass) {
  UnimplementedEvent *p;

  p = new UnimplementedEvent();
  p->m_event = 0;
  p->m_event_header = 0;

  return Data_Wrap_Struct(klass, 0, &free, p);
}

void UnimplementedEvent::set_event(VALUE self, mysql::Binary_log_event *event) {
  UnimplementedEvent *p;

  Data_Get_Struct(self, UnimplementedEvent, p);
  p->m_event = event;
  p->m_event_header = event->header();
}

void UnimplementedEvent::init() {
  rb_cBinlogUnimplementedEvent = rb_define_class_under(rb_mBinlog, "UnimplementedEvent", rb_cBinlogEvent);
  rb_define_alloc_func(rb_cBinlogUnimplementedEvent, &alloc);

  Event::init(rb_cBinlogUnimplementedEvent);
}

} // namespace binlog
} // namespace ruby
