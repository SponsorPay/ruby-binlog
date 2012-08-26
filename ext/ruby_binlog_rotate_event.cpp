#include "ruby_binlog.h"

VALUE rb_cBinlogRotateEvent;

namespace ruby {
namespace binlog {

void RotateEvent::free(RotateEvent *p) {
  if (p->m_event) {
    delete p->m_event;
    p->m_event = 0;
  }

  p->m_event_header = 0;

  delete p;
}

VALUE RotateEvent::alloc(VALUE klass) {
  RotateEvent *p;

  p = new RotateEvent();
  p->m_event = 0;
  p->m_event_header = 0;

  return Data_Wrap_Struct(klass, 0, &free, p);
}

void RotateEvent::set_event(VALUE self, mysql::Binary_log_event *event) {
  RotateEvent *p;

  Data_Get_Struct(self, RotateEvent, p);
  p->m_event = static_cast<Rotate_event*>(event);
  p->m_event_header = event->header();
}

void RotateEvent::init() {
  rb_cBinlogRotateEvent = rb_define_class_under(rb_mBinlog, "RotateEvent", rb_cBinlogEvent);
  rb_define_alloc_func(rb_cBinlogRotateEvent, &alloc);

  Event::init(rb_cBinlogRotateEvent);

  rb_define_method(rb_cBinlogRotateEvent, "binlog_file", __F(&get_binlog_file), 0);
  rb_define_method(rb_cBinlogRotateEvent, "binlog_pos",  __F(&get_binlog_pos),  0);
}

VALUE RotateEvent::get_binlog_file(VALUE self) {
  RotateEvent *p;
  Data_Get_Struct(self, RotateEvent, p);
  return rb_str_new2(p->m_event->binlog_file.c_str());
}

VALUE RotateEvent::get_binlog_pos(VALUE self) {
  RotateEvent *p;
  Data_Get_Struct(self, RotateEvent, p);
  return ULL2NUM(p->m_event->binlog_pos);
}

} // namespace binlog
} // namespace ruby
