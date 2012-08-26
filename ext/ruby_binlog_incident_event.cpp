#include "ruby_binlog.h"

VALUE rb_cBinlogIncidentEvent;

namespace ruby {
namespace binlog {

void IncidentEvent::free(IncidentEvent *p) {
  if (p->m_event) {
    delete p->m_event;
    p->m_event = 0;
  }

  p->m_event_header = 0;

  delete p;
}

VALUE IncidentEvent::alloc(VALUE klass) {
  IncidentEvent *p;

  p = new IncidentEvent();
  p->m_event = 0;
  p->m_event_header = 0;

  return Data_Wrap_Struct(klass, 0, &free, p);
}

void IncidentEvent::set_event(VALUE self, mysql::Binary_log_event *event) {
  IncidentEvent *p;

  Data_Get_Struct(self, IncidentEvent, p);
  p->m_event = static_cast<Incident_event*>(event);
  p->m_event_header = event->header();
}

void IncidentEvent::init() {
  rb_cBinlogIncidentEvent = rb_define_class_under(rb_mBinlog, "IncidentEvent", rb_cBinlogEvent);
  rb_define_alloc_func(rb_cBinlogIncidentEvent, &alloc);

  Event::init(rb_cBinlogIncidentEvent);

  rb_define_method(rb_cBinlogIncidentEvent, "incident_type", __F(&get_type),    0);
  rb_define_method(rb_cBinlogIncidentEvent, "message",       __F(&get_message), 0);
}

VALUE IncidentEvent::get_type(VALUE self) {
  IncidentEvent *p;
  Data_Get_Struct(self, IncidentEvent, p);
  return UINT2NUM(p->m_event->type);
}

VALUE IncidentEvent::get_message(VALUE self) {
  IncidentEvent *p;
  Data_Get_Struct(self, IncidentEvent, p);
  return rb_str_new2(p->m_event->message.c_str());
}

} // namespace binlog
} // namespace ruby
