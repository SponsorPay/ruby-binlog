#include "ruby_binlog.h"

extern VALUE rb_cBinlogQueryEvent;
extern VALUE rb_cBinlogRotateEvent;
extern VALUE rb_cBinlogFormatEvent;
extern VALUE rb_cBinlogUserVarEvent;
extern VALUE rb_cBinlogTableMapEvent;
extern VALUE rb_cBinlogRowEvent;
extern VALUE rb_cBinlogIntVarEvent;
extern VALUE rb_cBinlogIncidentEvent;
extern VALUE rb_cBinlogXid;
extern VALUE rb_cBinlogUnimplementedEvent;

namespace ruby {
namespace binlog {

struct Client {
  Binary_log *m_binlog;
  VALUE m_table_map;

  static void free(Client *p) {
    if (p->m_binlog) {
      delete p->m_binlog;
      p->m_binlog = 0;
    }

    p->m_table_map = 0;

    delete p;
  }

  static void mark(Client *p) {
    if (p->m_table_map) {
      rb_gc_mark(p->m_table_map);
    }
  }

  static VALUE alloc(VALUE klass) {
    Client *p = new Client();
    p->m_binlog = 0;
    p->m_table_map = 0;
    return Data_Wrap_Struct(klass, &mark, &free, p);
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
    TRAP_BEG;
    result = p->m_binlog->connect();
    TRAP_END;

    return (result == 0) ? Qtrue : Qfalse;
  }

  // XXX: Don't use
  /*
  static VALUE disconnect(VALUE self) {
    Client *p;
    mysql::system::Binlog_tcp_driver *driver;

    Data_Get_Struct(self, Client, p);
    driver = cast_to_tcp_driver(p->m_binlog->m_driver);

    if (driver) {
      driver->disconnect();
    }

    return Qnil;
  }
   */

  // XXX: Don't use
  /*
  static VALUE reconnect(VALUE self) {
    Client *p;
    mysql::system::Binlog_tcp_driver *driver;

    Data_Get_Struct(self, Client, p);
    driver = cast_to_tcp_driver(p->m_binlog->m_driver);

    if (driver) {
      driver->reconnect();
    }

    return Qnil;
  }
   */

  static VALUE is_closed(VALUE self) {
    Client *p;
    mysql::system::Binlog_tcp_driver *driver;

    Data_Get_Struct(self, Client, p);
    driver = cast_to_tcp_driver(p->m_binlog->m_driver);

    if (!driver) {
      return Qfalse;
    }

    if (driver->m_socket) {
      bool socket_is_open;

      TRAP_BEG;
      socket_is_open = driver->m_socket->is_open();
      TRAP_END;

      return socket_is_open ? Qfalse : Qtrue;
    } else {
      return Qtrue;
    }
  }

  static VALUE wait_for_next_event(VALUE self) {
    Client *p;
    Binary_log_event *event;
    int result = ERR_EOF;
    VALUE retval = Qnil;
    mysql::system::Binlog_tcp_driver *driver;

    Data_Get_Struct(self, Client, p);
    driver = cast_to_tcp_driver(p->m_binlog->m_driver);

    if (driver) {
      int closed = 0;
      timeval interval = { 0, WAIT_INTERVAL };

      while (1) {
        if (driver->m_event_queue->is_not_empty()) {
          TRAP_BEG;
          result = p->m_binlog->wait_for_next_event(&event);
          TRAP_END;
          break;
        } else {
          if (driver->m_socket && driver->m_socket->is_open()) {
            rb_thread_wait_for(interval);
          } else {
            closed = 1;
            driver->shutdown();
            rb_thread_wait_for(interval);
            break;
          }
        }
      }

      if (closed) {
        driver->disconnect();
        rb_raise(rb_eBinlogError, "MySQL server has gone away");
      }
    } else {
      TRAP_BEG;
      result = p->m_binlog->wait_for_next_event(&event);
      TRAP_END;
    }


    if (result == ERR_EOF) {
      return Qfalse;
    }

    switch (event->get_event_type()) {
    case QUERY_EVENT:
      retval = rb_funcall(rb_cBinlogQueryEvent, rb_intern("new"), 0);
      QueryEvent::set_event(retval, event);
      break;

    case ROTATE_EVENT:
      retval = rb_funcall(rb_cBinlogRotateEvent, rb_intern("new"), 0);
      RotateEvent::set_event(retval, event);
      break;

    case FORMAT_DESCRIPTION_EVENT: // XXX: Is it right?
      retval = rb_funcall(rb_cBinlogFormatEvent, rb_intern("new"), 0);
      FormatEvent::set_event(retval, event);
      break;

    case USER_VAR_EVENT:
      retval = rb_funcall(rb_cBinlogUserVarEvent, rb_intern("new"), 0);
      UserVarEvent::set_event(retval, event);
      break;

    case TABLE_MAP_EVENT:
      retval = rb_funcall(rb_cBinlogTableMapEvent, rb_intern("new"), 0);
      TableMapEvent::set_event(retval, event);
      p->m_table_map = retval;
      break;

    // XXX: Is it right?
    //case PRE_GA_WRITE_ROWS_EVENT:
    //case PRE_GA_UPDATE_ROWS_EVENT:
    //case PRE_GA_DELETE_ROWS_EVENT:
    case WRITE_ROWS_EVENT: 
    case UPDATE_ROWS_EVENT:
    case DELETE_ROWS_EVENT:
      retval = rb_funcall(rb_cBinlogRowEvent, rb_intern("new"), 0);
      RowEvent::set_event(retval, event, p->m_table_map);
      break;

    case INTVAR_EVENT:
      retval = rb_funcall(rb_cBinlogIntVarEvent, rb_intern("new"), 0);
      IntVarEvent::set_event(retval, event);
      break;

    case INCIDENT_EVENT:
      retval = rb_funcall(rb_cBinlogIncidentEvent, rb_intern("new"), 0);
      IncidentEvent::set_event(retval, event);
      break;

    case XID_EVENT:
      retval = rb_funcall(rb_cBinlogXid, rb_intern("new"), 0);
      XidEvent::set_event(retval, event);
      break;

    default:
      retval = rb_funcall(rb_cBinlogUnimplementedEvent, rb_intern("new"), 0);
      UnimplementedEvent::set_event(retval, event);
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
      i_position = NUM2ULONG(filename);
      TRAP_BEG;
      result = p->m_binlog->set_position(i_position);
      TRAP_END;
    } else {
      unsigned long i_position;
      Check_Type(filename, T_STRING);
      i_position = NUM2ULONG(position);
      std::string s_filename(StringValuePtr(filename));
      TRAP_BEG;
      result = p->m_binlog->set_position(s_filename, i_position);
      TRAP_END;
    }

    switch (result) {
    case ERR_OK:
      retval = Qtrue;
      break;
    case ERR_EOF:
      retval = Qfalse;
    default:
      rb_raise(rb_eBinlogError, "An unspecified error occurred (%d)", result);
      break;
    }

    return retval;
  }

  static VALUE set_position2(VALUE self, VALUE position) {
    Client *p;
    VALUE retval = Qnil;
    int result;

    Data_Get_Struct(self, Client, p);
    TRAP_BEG;
    result = p->m_binlog->set_position(NUM2ULONG(position));
    TRAP_END;

    switch (result) {
    case ERR_OK:
      retval = Qtrue;
      break;
    case ERR_EOF:
      retval = Qfalse;
    default:
      rb_raise(rb_eBinlogError, "An unspecified error occurred (%d)", result);
      break;
    }

    return retval;
  }

  static VALUE get_position(int argc, VALUE *argv, VALUE self) {
    Client *p;
    VALUE filename;
    unsigned long position;

    Data_Get_Struct(self, Client, p);
    rb_scan_args(argc, argv, "01", &filename);

    if (NIL_P(filename)) {
      position = p->m_binlog->get_position();
    } else {
      Check_Type(filename, T_STRING);
      std::string s_filename(StringValuePtr(filename));
      TRAP_BEG;
      position = p->m_binlog->get_position(s_filename);
      TRAP_END;
    }

    return ULONG2NUM(position);
  }

  static VALUE get_position2(VALUE self) {
    Client *p;
    Data_Get_Struct(self, Client, p);
    unsigned long position;

    TRAP_BEG;
    position = p->m_binlog->get_position();
    TRAP_END;

    return ULONG2NUM(position);
  }

  static void init() {
    VALUE rb_cBinlogClient = rb_define_class_under(rb_mBinlog, "Client", rb_cObject);
    rb_define_alloc_func(rb_cBinlogClient, &alloc);
    rb_define_private_method(rb_cBinlogClient, "initialize", __F(&initialize), 1);
    rb_define_method(rb_cBinlogClient, "connect", __F(&connect), 0);
    // XXX: Don't use
    //rb_define_method(rb_cBinlogClient, "disconnect", __F(&disconnect), 0);
    //rb_define_method(rb_cBinlogClient, "reconnect", __F(&reconnect), 0);
    rb_define_method(rb_cBinlogClient, "closed?", __F(&is_closed), 0);
    rb_define_method(rb_cBinlogClient, "wait_for_next_event", __F(&wait_for_next_event), 0);
    rb_define_method(rb_cBinlogClient, "set_position", __F(&set_position), -1);
    rb_define_method(rb_cBinlogClient, "position=", __F(&set_position2), 1);
    rb_define_method(rb_cBinlogClient, "get_position", __F(&get_position), -1);
    rb_define_method(rb_cBinlogClient, "position", __F(&get_position2), 0);
  }
};

} // namespace binlog
} // namespace ruby

VALUE rb_mBinlog;
VALUE rb_cBinlogEvent;
VALUE rb_eBinlogError;

void Init_binlog() {
  rb_mBinlog = rb_define_module("Binlog");
  rb_cBinlogEvent = rb_define_class_under(rb_mBinlog, "Event", rb_cObject);
  rb_eBinlogError = rb_define_class_under(rb_mBinlog, "Error", rb_eRuntimeError);

  rb_define_const(rb_cBinlogEvent, "UNKNOWN_EVENT",            INT2NUM(0));
  rb_define_const(rb_cBinlogEvent, "START_EVENT_V3",           INT2NUM(1));
  rb_define_const(rb_cBinlogEvent, "QUERY_EVENT",              INT2NUM(2));
  rb_define_const(rb_cBinlogEvent, "STOP_EVENT",               INT2NUM(3));
  rb_define_const(rb_cBinlogEvent, "ROTATE_EVENT",             INT2NUM(4));
  rb_define_const(rb_cBinlogEvent, "INTVAR_EVENT",             INT2NUM(5));
  rb_define_const(rb_cBinlogEvent, "LOAD_EVENT",               INT2NUM(6));
  rb_define_const(rb_cBinlogEvent, "SLAVE_EVENT",              INT2NUM(7));
  rb_define_const(rb_cBinlogEvent, "CREATE_FILE_EVENT",        INT2NUM(8));
  rb_define_const(rb_cBinlogEvent, "APPEND_BLOCK_EVENT",       INT2NUM(9));
  rb_define_const(rb_cBinlogEvent, "EXEC_LOAD_EVENT",          INT2NUM(10));
  rb_define_const(rb_cBinlogEvent, "DELETE_FILE_EVENT",        INT2NUM(11));
  rb_define_const(rb_cBinlogEvent, "NEW_LOAD_EVENT",           INT2NUM(12));
  rb_define_const(rb_cBinlogEvent, "RAND_EVENT",               INT2NUM(13));
  rb_define_const(rb_cBinlogEvent, "USER_VAR_EVENT",           INT2NUM(14));
  rb_define_const(rb_cBinlogEvent, "FORMAT_DESCRIPTION_EVENT", INT2NUM(15));
  rb_define_const(rb_cBinlogEvent, "XID_EVENT",                INT2NUM(16));
  rb_define_const(rb_cBinlogEvent, "BEGIN_LOAD_QUERY_EVENT",   INT2NUM(17));
  rb_define_const(rb_cBinlogEvent, "EXECUTE_LOAD_QUERY_EVENT", INT2NUM(18));
  rb_define_const(rb_cBinlogEvent, "TABLE_MAP_EVENT",          INT2NUM(19));
  rb_define_const(rb_cBinlogEvent, "PRE_GA_WRITE_ROWS_EVENT",  INT2NUM(20));
  rb_define_const(rb_cBinlogEvent, "PRE_GA_UPDATE_ROWS_EVENT", INT2NUM(21));
  rb_define_const(rb_cBinlogEvent, "PRE_GA_DELETE_ROWS_EVENT", INT2NUM(22));
  rb_define_const(rb_cBinlogEvent, "WRITE_ROWS_EVENT",         INT2NUM(23));
  rb_define_const(rb_cBinlogEvent, "UPDATE_ROWS_EVENT",        INT2NUM(24));
  rb_define_const(rb_cBinlogEvent, "DELETE_ROWS_EVENT",        INT2NUM(25));
  rb_define_const(rb_cBinlogEvent, "INCIDENT_EVENT",           INT2NUM(26));
  rb_define_const(rb_cBinlogEvent, "USER_DEFINED",             INT2NUM(27));

  ruby::binlog::Client::init();
  ruby::binlog::QueryEvent::init();
  ruby::binlog::RotateEvent::init();
  ruby::binlog::FormatEvent::init();
  ruby::binlog::UserVarEvent::init();
  ruby::binlog::TableMapEvent::init();
  ruby::binlog::RowEvent::init();
  ruby::binlog::IntVarEvent::init();
  ruby::binlog::IncidentEvent::init();
  ruby::binlog::XidEvent::init();
  ruby::binlog::UnimplementedEvent::init();
}
