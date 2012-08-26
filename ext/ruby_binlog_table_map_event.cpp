#include "ruby_binlog.h"

VALUE rb_cBinlogTableMapEvent;

namespace ruby {
namespace binlog {

void TableMapEvent::free(TableMapEvent *p) {
  if (p->m_event) {
    delete p->m_event;
    p->m_event = 0;
  }

  p->m_event_header = 0;

  delete p;
}

VALUE TableMapEvent::alloc(VALUE klass) {
  TableMapEvent *p;

  p = new TableMapEvent();
  p->m_event = 0;
  p->m_event_header = 0;

  return Data_Wrap_Struct(klass, 0, &free, p);
}

void TableMapEvent::set_event(VALUE self, mysql::Binary_log_event *event) {
  TableMapEvent *p;

  Data_Get_Struct(self, TableMapEvent, p);
  p->m_event = static_cast<Table_map_event*>(event);
  p->m_event_header = event->header();
}

void TableMapEvent::init() {
  rb_cBinlogTableMapEvent = rb_define_class_under(rb_mBinlog, "TableMapEvent", rb_cBinlogEvent);
  rb_define_alloc_func(rb_cBinlogTableMapEvent, &alloc);

  Event::init(rb_cBinlogTableMapEvent);

  rb_define_method(rb_cBinlogTableMapEvent, "table_id",    __F(&get_table_id),     0);
  rb_define_method(rb_cBinlogTableMapEvent, "flags",       __F(&get_flags),        0);
  rb_define_method(rb_cBinlogTableMapEvent, "db_name",     __F(&get_db_name),      0);
  rb_define_method(rb_cBinlogTableMapEvent, "table_name",  __F(&get_table_name),   0);
  rb_define_method(rb_cBinlogTableMapEvent, "raw_columns", __F(&get_columns),      0);
  rb_define_method(rb_cBinlogTableMapEvent, "columns",     __F(&get_column_types), 0);
  rb_define_method(rb_cBinlogTableMapEvent, "metadata",    __F(&get_metadata),     0);
  rb_define_method(rb_cBinlogTableMapEvent, "null_bits",   __F(&get_null_bits),    0);
}

VALUE TableMapEvent::get_table_id(VALUE self) {
  TableMapEvent *p;
  Data_Get_Struct(self, TableMapEvent, p);
  return ULL2NUM(p->m_event->table_id);
}

VALUE TableMapEvent::get_flags(VALUE self) {
  TableMapEvent *p;
  Data_Get_Struct(self, TableMapEvent, p);
  return UINT2NUM(p->m_event->flags);
}

VALUE TableMapEvent::get_db_name(VALUE self) {
  TableMapEvent *p;
  Data_Get_Struct(self, TableMapEvent, p);
  return rb_str_new2(p->m_event->db_name.c_str());
}

VALUE TableMapEvent::get_table_name(VALUE self) {
  TableMapEvent *p;
  Data_Get_Struct(self, TableMapEvent, p);
  return rb_str_new2(p->m_event->table_name.c_str());
}

VALUE TableMapEvent::get_columns(VALUE self) {
  TableMapEvent *p;
  VALUE retval = rb_ary_new();

  Data_Get_Struct(self, TableMapEvent, p);

  for (std::vector<uint8_t>::iterator itor = p->m_event->columns.begin();
       itor != p->m_event->columns.end(); itor++) {
    rb_ary_push(retval, UINT2NUM(*itor));
  }

  return retval;
}

VALUE TableMapEvent::get_column_types(VALUE self) {
  TableMapEvent *p;
  VALUE retval = rb_ary_new();

  Data_Get_Struct(self, TableMapEvent, p);

  for (std::vector<uint8_t>::iterator itor = p->m_event->columns.begin();
       itor != p->m_event->columns.end(); itor++) {
    const char *colname = get_field_type_str(static_cast<mysql::system::enum_field_types>(*itor));
    rb_ary_push(retval, (colname ? rb_str_new2(colname) : Qnil));
  }

  return retval;
}

VALUE TableMapEvent::get_metadata(VALUE self) {
  TableMapEvent *p;
  VALUE retval = rb_ary_new();

  Data_Get_Struct(self, TableMapEvent, p);

  for (std::vector<uint8_t>::iterator itor = p->m_event->metadata.begin();
       itor != p->m_event->metadata.end(); itor++) {
    rb_ary_push(retval, UINT2NUM(*itor));
  }

  return retval;
}

VALUE TableMapEvent::get_null_bits(VALUE self) {
  TableMapEvent *p;
  VALUE retval = rb_ary_new();

  Data_Get_Struct(self, TableMapEvent, p);

  for (std::vector<uint8_t>::iterator itor = p->m_event->null_bits.begin();
       itor != p->m_event->null_bits.end(); itor++) {
    rb_ary_push(retval, UINT2NUM(*itor));
  }

  return retval;
}

} // namespace binlog
} // namespace ruby
