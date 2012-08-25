#include "ruby_binlog.h"

namespace ruby {
namespace binlog {

struct Client {
  Binary_log *m_binlog;

  static void free(Client *p) {
    if (p->m_binlog) {
      delete p->m_binlog;
    }

    delete p;
  }

  static VALUE alloc(VALUE klass) {
    Client *p = new Client();
    return Data_Wrap_Struct(klass, 0, &free, p);
  }

  static VALUE initialize(VALUE self, VALUE uri) {
    Client *p;

    Check_Type(uri, T_STRING);
    Data_Get_Struct(self, Client, p);
    p->m_binlog = new mysql::Binary_log(
      mysql::system::create_transport(StringValuePtr(uri)));

    return Qnil;
  }

  static VALUE connect(VALUE self) {
    Client *p;
    int result;

    Data_Get_Struct(self, Client, p);
    result = p->m_binlog->connect();

    return (result == 0) ? Qtrue : Qfalse;
  }

  static VALUE wait_for_next_event(VALUE self) {
    Client *p;
    Binary_log_event *event;
    int result;
    VALUE retval = Qnil;

    Data_Get_Struct(self, Client, p);

    TRAP_BEG;
    result = p->m_binlog->wait_for_next_event(&event);
    TRAP_END;

    if (result == ERR_EOF) {
      return Qfalse;
    }

    switch (event->get_event_type()) {
    case QUERY_EVENT:
      retval = rb_funcall(rb_cBinlogQueryEvent, rb_intern("new"), 0);
      QueryEvent::set_event(retval, event);
      break;
    default: // XXX:
      break;
    }

    return retval;
  }

  static VALUE set_position(int argc, VALUE *argv, VALUE self) {
    Client *p;
    VALUE filename, position, retval = Qnil;
    int result;

    Data_Get_Struct(self, Client, p);
    rb_scan_args(argc, argv, "11", &filename, &position);

    if (NIL_P(position)) {
      unsigned long i_position;
      i_position = NUM2LONG(filename);
      result = p->m_binlog->set_position(i_position);
    } else {
      unsigned long i_position;
      Check_Type(filename, T_STRING);
      i_position = NUM2LONG(position);
      std::string s_filename(StringValuePtr(filename));
      result = p->m_binlog->set_position(s_filename, i_position);
    }

    switch (result) {
    case ERR_OK:
      retval = Qtrue;
      break;
    case ERR_EOF:
      retval = Qfalse;
    default:
      rb_raise(rb_eRuntimeError, "An unspecified error occurred (%d)", result);
      break;
    }

    return retval;
  }

  static VALUE get_position(int argc, VALUE *argv, VALUE self) {
    Client *p;
    VALUE filename, retval = Qnil;
    unsigned long position;

    Data_Get_Struct(self, Client, p);
    rb_scan_args(argc, argv, "01", &filename);

    if (NIL_P(filename)) {
      position = p->m_binlog->get_position();
    } else {
      Check_Type(filename, T_STRING);
      std::string s_filename(StringValuePtr(filename));
      position = p->m_binlog->get_position(s_filename);
    }

    return LONG2NUM(position);
  }

  static void init() {
    VALUE rb_cBinlogClient = rb_define_class_under(rb_mBinlog, "Client", rb_cObject);
    rb_define_alloc_func(rb_cBinlogClient, &alloc);
    rb_define_private_method(rb_cBinlogClient, "initialize", __F(&initialize), 1);
    rb_define_method(rb_cBinlogClient, "connect", __F(&connect), 0);
    rb_define_method(rb_cBinlogClient, "wait_for_next_event", __F(&wait_for_next_event), 0);
    rb_define_method(rb_cBinlogClient, "set_position", __F(&set_position), -1);
    rb_define_method(rb_cBinlogClient, "get_position", __F(&get_position), -1);
  }
};

} // namespace binlog
} // namespace ruby

VALUE rb_mBinlog;
VALUE rb_cBinlogEvent;

void Init_binlog() {
  rb_mBinlog = rb_define_module("Binlog");
  rb_cBinlogEvent = rb_define_class_under(rb_mBinlog, "Event", rb_cObject);

  ruby::binlog::Client::init();
  ruby::binlog::QueryEvent::init();
}
