#include "ruby_binlog.h"

VALUE rb_cBinlogRowEvent;

namespace ruby {
namespace binlog {

void RowEvent::free(RowEvent *p) {
  if (p->m_event) {
    delete p->m_event;
    p->m_event = 0;
  }

  p->m_event_header = 0;
  p->m_table_map = 0;

  delete p;
}

void RowEvent::mark(RowEvent *p) {
  if (p->m_table_map) {
    rb_gc_mark(p->m_table_map);
  }
}

VALUE RowEvent::alloc(VALUE klass) {
  RowEvent *p;

  p = new RowEvent();
  p->m_event = 0;
  p->m_table_map = 0;
  p->m_event_header = 0;

  return Data_Wrap_Struct(klass, &mark, &free, p);
}

void RowEvent::set_event(VALUE self, mysql::Binary_log_event *event, VALUE table_map) {
  RowEvent *p;

  Data_Get_Struct(self, RowEvent, p);
  p->m_event = static_cast<Row_event*>(event);
  p->m_table_map = table_map;
  p->m_event_header = event->header();
}

void RowEvent::init() {
  rb_cBinlogRowEvent = rb_define_class_under(rb_mBinlog, "RowEvent", rb_cBinlogEvent);
  rb_define_alloc_func(rb_cBinlogRowEvent, &alloc);

  Event::init(rb_cBinlogRowEvent);

  rb_define_method(rb_cBinlogRowEvent, "table_id",                 __F(&get_table_id),             0);
  rb_define_method(rb_cBinlogRowEvent, "db_name",                  __F(&get_db_name),              0);
  rb_define_method(rb_cBinlogRowEvent, "table_name",               __F(&get_table_name),           0);
  rb_define_method(rb_cBinlogRowEvent, "columns",                  __F(&get_column_types),         0);
  rb_define_method(rb_cBinlogRowEvent, "flags",                    __F(&get_flags),                0);
  rb_define_method(rb_cBinlogRowEvent, "columns_len",              __F(&get_columns_len),          0);
  rb_define_method(rb_cBinlogRowEvent, "null_bits_len",            __F(&get_null_bits_len),        0);
  rb_define_method(rb_cBinlogRowEvent, "raw_columns_before_image", __F(&get_columns_before_image), 0);
  rb_define_method(rb_cBinlogRowEvent, "raw_used_columns",         __F(&get_used_columns),         0);
  rb_define_method(rb_cBinlogRowEvent, "raw_row",                  __F(&get_row),                  0);
  rb_define_method(rb_cBinlogRowEvent, "rows",                     __F(&get_rows),                 0);
}

VALUE RowEvent::get_table_id(VALUE self) {
  RowEvent *p;
  Data_Get_Struct(self, RowEvent, p);
  return ULL2NUM(p->m_event->table_id);
}

VALUE RowEvent::get_db_name(VALUE self) {
  RowEvent *p;
  Data_Get_Struct(self, RowEvent, p);

  if (p->m_table_map) {
    TableMapEvent *tme;
    Data_Get_Struct(p->m_table_map, TableMapEvent, tme);
    return rb_str_new2(tme->m_event->db_name.c_str());
  } else {
    return Qnil;
  }
}

VALUE RowEvent::get_table_name(VALUE self) {
  RowEvent *p;
  Data_Get_Struct(self, RowEvent, p);

  if (p->m_table_map) {
    TableMapEvent *tme;
    Data_Get_Struct(p->m_table_map, TableMapEvent, tme);
    return rb_str_new2(tme->m_event->table_name.c_str());
  } else {
    return Qnil;
  }
}

VALUE RowEvent::get_column_types(VALUE self) {
  RowEvent *p;
  Data_Get_Struct(self, RowEvent, p);
  VALUE retval = Qnil;

  if (p->m_table_map) {
    TableMapEvent *tme;
    retval = rb_ary_new();

    Data_Get_Struct(p->m_table_map, TableMapEvent, tme);

    for (std::vector<uint8_t>::iterator itor = tme->m_event->columns.begin();
         itor != tme->m_event->columns.end(); itor++) {
      const char *colname = get_field_type_str(static_cast<mysql::system::enum_field_types>(*itor));
      rb_ary_push(retval, (colname ? rb_str_new2(colname) : Qnil));
    }
  }

  return retval;
}

VALUE RowEvent::get_flags(VALUE self) {
  RowEvent *p;
  Data_Get_Struct(self, RowEvent, p);
  return UINT2NUM(p->m_event->flags);
}

VALUE RowEvent::get_columns_len(VALUE self) {
  RowEvent *p;
  Data_Get_Struct(self, RowEvent, p);
  return ULONG2NUM(p->m_event->columns_len);
}

VALUE RowEvent::get_null_bits_len(VALUE self) {
  RowEvent *p;
  Data_Get_Struct(self, RowEvent, p);
  return UINT2NUM(p->m_event->null_bits_len);
}

VALUE RowEvent::get_columns_before_image(VALUE self) {
  RowEvent *p;
  VALUE retval = rb_ary_new();

  Data_Get_Struct(self, RowEvent, p);

  for (std::vector<uint8_t>::iterator itor = p->m_event->columns_before_image.begin();
       itor != p->m_event->columns_before_image.end(); itor++) {
    rb_ary_push(retval, UINT2NUM(*itor));
  }

  return retval;
}

VALUE RowEvent::get_used_columns(VALUE self) {
  RowEvent *p;
  VALUE retval = rb_ary_new();

  Data_Get_Struct(self, RowEvent, p);

  for (std::vector<uint8_t>::iterator itor = p->m_event->used_columns.begin();
       itor != p->m_event->used_columns.end(); itor++) {
    rb_ary_push(retval, UINT2NUM(*itor));
  }

  return retval;
}

VALUE RowEvent::get_row(VALUE self) {
  RowEvent *p;
  VALUE retval = rb_ary_new();

  Data_Get_Struct(self, RowEvent, p);

  for (std::vector<uint8_t>::iterator itor = p->m_event->row.begin();
       itor != p->m_event->row.end(); itor++) {
    rb_ary_push(retval, UINT2NUM(*itor));
  }

  return retval;
}

VALUE RowEvent::get_rows(VALUE self) {
  RowEvent *p;
  TableMapEvent *tme;
  VALUE retval = rb_ary_new();

  Data_Get_Struct(self, RowEvent, p);

  if (!p->m_table_map) {
    return retval;
  }

  Data_Get_Struct(p->m_table_map , TableMapEvent, tme);

  mysql::Row_event_set rows(p->m_event, tme->m_event);
  mysql::Row_event_set::iterator itor = rows.begin();

  do {
    VALUE rb_row = Qnil;
    mysql::Row_of_fields fields = *itor;
    Log_event_type event_type = p->m_event->get_event_type();

    if (event_type == mysql::WRITE_ROWS_EVENT) {
      rb_row = proc_insert(fields);
    } else if (event_type == mysql::UPDATE_ROWS_EVENT) {
      itor++;
      mysql::Row_of_fields fields2 = *itor;
      rb_row = proc_update(fields, fields2);
    } else if (event_type == mysql::DELETE_ROWS_EVENT) {
      rb_row = proc_delete(fields);
    }

    rb_ary_push(retval, rb_row);
  } while (++itor != rows.end());

  return retval;
}

void RowEvent::proc0(mysql::Row_of_fields &fields, VALUE rb_fields) {
  mysql::Converter converter;
  mysql::Row_of_fields::iterator itor = fields.begin();

  do {
    VALUE rval = Qnil;
    mysql::system::enum_field_types type = itor->type();

    if (itor->is_null()) {
      rval = Qnil;
    } else if (type == mysql::system::MYSQL_TYPE_FLOAT) {
      rval = rb_float_new(itor->as_float());
    } else if (type == mysql::system::MYSQL_TYPE_DOUBLE) {
      rval = rb_float_new(itor->as_double());
    } else {
      std::string out;
      converter.to(out, *itor);
      rval = rb_str_new2(out.c_str());
    }

    rb_ary_push(rb_fields, rval);
  } while(++itor != fields.end());
}

VALUE RowEvent::proc_insert(mysql::Row_of_fields &fields) {
  VALUE rb_new_fields = rb_ary_new();
  proc0(fields, rb_new_fields);
  return rb_new_fields;
}

VALUE RowEvent::proc_update(mysql::Row_of_fields &old_fields, mysql::Row_of_fields &new_fields) {
  VALUE rb_row, rb_old_fields, rb_new_fields;

  rb_row = rb_ary_new();
  rb_old_fields = rb_ary_new();
  rb_new_fields = rb_ary_new();

  proc0(old_fields, rb_old_fields);
  proc0(new_fields, rb_new_fields);
  rb_ary_push(rb_row, rb_old_fields);
  rb_ary_push(rb_row, rb_new_fields);

  return rb_row;
}

VALUE RowEvent::proc_delete(mysql::Row_of_fields &fields) {
  VALUE rb_old_fields = rb_ary_new();
  proc0(fields, rb_old_fields);
  return rb_old_fields;
}

} // namespace binlog
} // namespace ruby
