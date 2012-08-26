#include "ruby_binlog.h"

VALUE rb_cBinlogUserVarEvent;

namespace ruby {
namespace binlog {

void UserVarEvent::free(UserVarEvent *p) {
  if (p->m_event) {
    delete p->m_event;
    p->m_event = 0;
  }

  p->m_event_header = 0;

  delete p;
}

VALUE UserVarEvent::alloc(VALUE klass) {
  UserVarEvent *p;

  p = new UserVarEvent();
  p->m_event = 0;
  p->m_event_header = 0;

  return Data_Wrap_Struct(klass, 0, &free, p);
}

void UserVarEvent::set_event(VALUE self, mysql::Binary_log_event *event) {
  UserVarEvent *p;

  Data_Get_Struct(self, UserVarEvent, p);
  p->m_event = static_cast<User_var_event*>(event);
  p->m_event_header = event->header();
}

void UserVarEvent::init() {
  rb_cBinlogUserVarEvent = rb_define_class_under(rb_mBinlog, "UserVarEvent", rb_cBinlogEvent);
  rb_define_alloc_func(rb_cBinlogUserVarEvent, &alloc);

  Event::init(rb_cBinlogUserVarEvent);

  rb_define_method(rb_cBinlogUserVarEvent, "name",     __F(&get_name),    0);
  rb_define_method(rb_cBinlogUserVarEvent, "is_null",  __F(&get_is_null), 0);
  rb_define_method(rb_cBinlogUserVarEvent, "var_type", __F(&get_type),    0);
  rb_define_method(rb_cBinlogUserVarEvent, "charset",  __F(&get_charset), 0);
  rb_define_method(rb_cBinlogUserVarEvent, "value",    __F(&get_value),   0);
}

VALUE UserVarEvent::get_name(VALUE self) {
  UserVarEvent *p;
  Data_Get_Struct(self, UserVarEvent, p);
  return rb_str_new2(p->m_event->name.c_str());
}

VALUE UserVarEvent::get_is_null(VALUE self) {
  UserVarEvent *p;
  Data_Get_Struct(self, UserVarEvent, p);
  return UINT2NUM(p->m_event->is_null);
}

VALUE UserVarEvent::get_type(VALUE self) {
  UserVarEvent *p;
  Data_Get_Struct(self, UserVarEvent, p);
  return UINT2NUM(p->m_event->type);
}

VALUE UserVarEvent::get_charset(VALUE self) {
  UserVarEvent *p;
  Data_Get_Struct(self, UserVarEvent, p);
  return ULONG2NUM(p->m_event->charset);
}

VALUE UserVarEvent::get_value(VALUE self) {
  UserVarEvent *p;
  Data_Get_Struct(self, UserVarEvent, p);
  return rb_str_new2(p->m_event->value.c_str());
}

} // namespace binlog
} // namespace ruby
