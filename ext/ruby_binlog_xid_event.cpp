#include "ruby_binlog.h"

VALUE rb_cBinlogXid;

namespace ruby {
namespace binlog {

void XidEvent::free(XidEvent *p) {
  if (p->m_event) {
    delete p->m_event;
    p->m_event = 0;
  }

  p->m_event_header = 0;

  delete p;
}

VALUE XidEvent::alloc(VALUE klass) {
  XidEvent *p;

  p = new XidEvent();
  p->m_event = 0;
  p->m_event_header = 0;

  return Data_Wrap_Struct(klass, 0, &free, p);
}

void XidEvent::set_event(VALUE self, mysql::Binary_log_event *event) {
  XidEvent *p;

  Data_Get_Struct(self, XidEvent, p);
  p->m_event = static_cast<Xid*>(event);
  p->m_event_header = event->header();
}

void XidEvent::init() {
  rb_cBinlogXid = rb_define_class_under(rb_mBinlog, "Xid", rb_cBinlogEvent);
  rb_define_alloc_func(rb_cBinlogXid, &alloc);

  Event::init(rb_cBinlogXid);

  rb_define_method(rb_cBinlogXid, "xid_id", __F(&get_xid_id), 0);
}

VALUE XidEvent::get_xid_id(VALUE self) {
  XidEvent *p;
  Data_Get_Struct(self, XidEvent, p);
  return ULL2NUM(p->m_event->xid_id);
}

} // namespace binlog
} // namespace ruby
