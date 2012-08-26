#include "ruby_binlog.h"

VALUE rb_cBinlogFormatEvent;

namespace ruby {
namespace binlog {

void FormatEvent::free(FormatEvent *p) {
  if (p->m_event) {
    delete p->m_event;
    p->m_event = 0;
  }

  p->m_event_header = 0;

  delete p;
}

VALUE FormatEvent::alloc(VALUE klass) {
  FormatEvent *p;

  p = new FormatEvent();
  p->m_event = 0;
  p->m_event_header = 0;

  return Data_Wrap_Struct(klass, 0, &free, p);
}

void FormatEvent::set_event(VALUE self, mysql::Binary_log_event *event) {
  FormatEvent *p;

  Data_Get_Struct(self, FormatEvent, p);
  p->m_event = static_cast<Format_event*>(event);
  p->m_event_header = event->header();
}

void FormatEvent::init() {
  rb_cBinlogFormatEvent = rb_define_class_under(rb_mBinlog, "FormatEvent", rb_cBinlogEvent);
  rb_define_alloc_func(rb_cBinlogFormatEvent, &alloc);

  Event::init(rb_cBinlogFormatEvent);

  rb_define_method(rb_cBinlogFormatEvent, "binlog_version", __F(&get_binlog_version), 0);
  // XXX: [BUG] Segmentation fault
  //rb_define_method(rb_cBinlogFormatEvent, "master_version", __F(&get_master_version), 0);
  rb_define_method(rb_cBinlogFormatEvent, "created_ts",     __F(&get_created_ts),     0);
  rb_define_method(rb_cBinlogFormatEvent, "log_header_len", __F(&get_log_header_len), 0);
}

VALUE FormatEvent::get_binlog_version(VALUE self) {
  FormatEvent *p;
  Data_Get_Struct(self, FormatEvent, p);
  return UINT2NUM(p->m_event->binlog_version);
}

VALUE FormatEvent::get_master_version(VALUE self) {
  FormatEvent *p;
  Data_Get_Struct(self, FormatEvent, p);
  return rb_str_new2(p->m_event->master_version.c_str());
}

VALUE FormatEvent::get_created_ts(VALUE self) {
  FormatEvent *p;
  Data_Get_Struct(self, FormatEvent, p);
  return ULONG2NUM(p->m_event->created_ts);
}

VALUE FormatEvent::get_log_header_len(VALUE self) {
  FormatEvent *p;
  Data_Get_Struct(self, FormatEvent, p);
  return UINT2NUM(p->m_event->log_header_len);
}

} // namespace binlog
} // namespace ruby
