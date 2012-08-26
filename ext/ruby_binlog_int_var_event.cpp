#include "ruby_binlog.h"

VALUE rb_cBinlogIntVarEvent;

namespace ruby {
namespace binlog {

void IntVarEvent::free(IntVarEvent *p) {
  if (p->m_event) {
    delete p->m_event;
    p->m_event = 0;
  }

  p->m_event_header = 0;

  delete p;
}

VALUE IntVarEvent::alloc(VALUE klass) {
  IntVarEvent *p;

  p = new IntVarEvent();
  p->m_event = 0;
  p->m_event_header = 0;

  return Data_Wrap_Struct(klass, 0, &free, p);
}

void IntVarEvent::set_event(VALUE self, mysql::Binary_log_event *event) {
  IntVarEvent *p;

  Data_Get_Struct(self, IntVarEvent, p);
  p->m_event = static_cast<Int_var_event*>(event);
  p->m_event_header = event->header();
}

void IntVarEvent::init() {
  rb_cBinlogIntVarEvent = rb_define_class_under(rb_mBinlog, "IntVarEvent", rb_cBinlogEvent);
  rb_define_alloc_func(rb_cBinlogIntVarEvent, &alloc);

  Event::init(rb_cBinlogIntVarEvent);

  rb_define_method(rb_cBinlogIntVarEvent, "var_type", __F(&get_type),  0);
  rb_define_method(rb_cBinlogIntVarEvent, "value",    __F(&get_value), 0);
}

VALUE IntVarEvent::get_type(VALUE self) {
  IntVarEvent *p;
  Data_Get_Struct(self, IntVarEvent, p);
  return UINT2NUM(p->m_event->type);
}

VALUE IntVarEvent::get_value(VALUE self) {
  IntVarEvent *p;
  Data_Get_Struct(self, IntVarEvent, p);
  return ULL2NUM(p->m_event->value);
}

} // namespace binlog
} // namespace ruby
