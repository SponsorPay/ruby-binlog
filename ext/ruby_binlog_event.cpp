#include "ruby_binlog.h"

namespace ruby {
namespace binlog {

VALUE Event::get_marker(VALUE self) {
  Event *p;
  Data_Get_Struct(self, Event, p);
  return UINT2NUM(p->m_event_header->marker);
}

VALUE Event::get_timestamp(VALUE self) {
  Event *p;
  Data_Get_Struct(self, Event, p);
  return ULONG2NUM(p->m_event_header->timestamp);
}

VALUE Event::get_type_code(VALUE self) {
  Event *p;
  Data_Get_Struct(self, Event, p);
  return UINT2NUM(p->m_event_header->type_code);
}

VALUE Event::get_server_id(VALUE self) {
  Event *p;
  Data_Get_Struct(self, Event, p);
  return ULONG2NUM(p->m_event_header->server_id);
}

VALUE Event::get_event_length(VALUE self) {
  Event *p;
  Data_Get_Struct(self, Event, p);
  return ULONG2NUM(p->m_event_header->event_length);
}

VALUE Event::get_next_position(VALUE self) {
  Event *p;
  Data_Get_Struct(self, Event, p);
  return ULONG2NUM(p->m_event_header->next_position);
}

VALUE Event::get_flags(VALUE self) {
  Event *p;
  Data_Get_Struct(self, Event, p);
  return UINT2NUM(p->m_event_header->flags);
}

VALUE Event::get_event_type(VALUE self) {
  Event *p;
  Data_Get_Struct(self, Event, p);
  return rb_str_new2(
    mysql::system::get_event_type_str(static_cast<enum Log_event_type>(p->m_event_header->type_code)));
}

void Event::init(VALUE clazz) {
  rb_define_method(clazz, "marker",        __F(&get_marker),        0);
  rb_define_method(clazz, "timestamp",     __F(&get_timestamp),     0);
  rb_define_method(clazz, "type_code",     __F(&get_type_code),     0);
  rb_define_method(clazz, "server_id",     __F(&get_server_id),     0);
  rb_define_method(clazz, "event_length",  __F(&get_event_length),  0);
  rb_define_method(clazz, "next_position", __F(&get_next_position), 0);
  rb_define_method(clazz, "flags",         __F(&get_flags),         0);
  rb_define_method(clazz, "event_type",    __F(&get_event_type),    0);
}

} // namespace binlog
} // namespace ruby
