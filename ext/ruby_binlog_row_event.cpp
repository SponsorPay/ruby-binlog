#include "ruby_binlog.h"
#include <boost/lexical_cast.hpp>
#include <iomanip>
#include <sstream>

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
  p->m_table_map = rb_hash_aref(table_map, ULL2NUM(p->m_event->table_id));
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
      rb_ary_push(retval, (colname ? ID2SYM(rb_intern(colname)) : Qnil));
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
  mysql::Row_of_fields::iterator itor = fields.begin();

  do {
    VALUE rval = Qnil;

    if (itor->is_null()) {
      rb_ary_push(rb_fields, rval);
      continue;
    }

    switch(itor->type()) {
      case mysql::system::MYSQL_TYPE_FLOAT:
        rval = rb_float_new(itor->as_float());
        break;

      case mysql::system::MYSQL_TYPE_DOUBLE:
        rval = rb_float_new(itor->as_double());
        break;

      case mysql::system::MYSQL_TYPE_TINY:
        rval = UINT2NUM(itor->as_int8());
        break;

      case mysql::system::MYSQL_TYPE_SHORT:
        rval = UINT2NUM((boost::uint16_t)(itor->as_int16()));
        break;

      case mysql::system::MYSQL_TYPE_INT24: {
        const unsigned char* buf = (const unsigned char*) itor->storage();
//      if (buf[2] & 0x80) {
//        rval = INT2NUM((0xff << 24) | (buf[2] << 16) | (buf[1] << 8) | (buf[0] << 0));
//      } else {
          rval = UINT2NUM((buf[2] << 16) | (buf[1] << 8) | (buf[0] << 0));
//      }
      } break;

      case mysql::system::MYSQL_TYPE_LONG:
        rval = INT2NUM(itor->as_int32());
        break;

      case mysql::system::MYSQL_TYPE_LONGLONG:
        rval = LL2NUM(itor->as_int64());
        break;

      case mysql::system::MYSQL_TYPE_BIT: {
        const unsigned char* buf = (const unsigned char*) itor->storage();
	int len = (itor->metadata() >> 8U) & 0xff;
        if (itor->metadata() & 0xff) len++;

        boost::uint64_t result = 0;
        for(int i = 0; i < len; i++) {
          result |= buf[len - i - 1] << (i * 8);
        }

        rval = INT2NUM(result);
      } break;

      case mysql::system::MYSQL_TYPE_VAR_STRING:
        rval = rb_str_new(itor->storage(), itor->length());
        break;

      case mysql::system::MYSQL_TYPE_TIMESTAMP:
        rval = INT2NUM((boost::uint32_t)itor->as_int32());
        break;

      case mysql::system::MYSQL_TYPE_NEWDECIMAL: {
        std::string s = decimal2str(*itor);
        VALUE BigDecimal = rb_const_get(rb_cObject, rb_intern("BigDecimal"));
        rval = rb_funcall(BigDecimal, rb_intern("new"), 1, rb_str_new(s.c_str(), s.length()));
      } break;

      case mysql::system::MYSQL_TYPE_DATE: {
        const char* storage = itor->storage();
        unsigned int date = (storage[0] & 0xff) + ((storage[1] & 0xff) << 8) + ((storage[2] & 0xff) << 16);
        unsigned int year = date >> 9;
        date-= (year << 9);
        unsigned int month = date >> 5;
        unsigned int day = date - (month << 5);

        VALUE Date = rb_const_get(rb_cObject, rb_intern("Date"));
        rval = rb_funcall(Date, rb_intern("new"), 3, UINT2NUM(year), UINT2NUM(month), UINT2NUM(day));
      } break;

      case mysql::system::MYSQL_TYPE_TIME: {
        const char* storage = itor->storage();
        unsigned int time = (storage[0] & 0xff) + ((storage[1] & 0xff) << 8) + ((storage[2] & 0xff) << 16);
        unsigned int sec = time % 100;
        time -= sec;
        unsigned int min = (time % 10000) / 100;
        unsigned int hour = (time - min) / 10000;

        VALUE Time = rb_const_get(rb_cObject, rb_intern("Time"));
        rval = rb_funcall(Time, rb_intern("utc"), 6, 2000, 1, 1, UINT2NUM(hour), UINT2NUM(min), UINT2NUM(sec));
      } break;

      case mysql::system::MYSQL_TYPE_YEAR: {
        const char* storage = itor->storage();
        unsigned int year = (storage[0] & 0xff);
        rval = INT2NUM(year > 0 ? (year + 1900) : 0);
      } break;

      case mysql::system::MYSQL_TYPE_DATETIME: {
        boost::uint64_t timestamp = itor->as_int64();

        std::stringstream datestream;
        datestream << std::setfill('0') << std::setw(14) << boost::lexical_cast<std::string>(timestamp);
        std::string date;
        datestream >> date;
        std::cerr << "before: " + date;
        std::cerr << "\n";

        // Format date
        date.insert(4, "-");
        date.insert(7, "-");
        date.insert(10, " ");
        date.insert(13, ":");
        date.insert(16, ":");

        std::cerr << "after: " + date;
        std::cerr << "\n";

        rval = rb_str_new(date.c_str(), date.length());
      } break;
      case mysql::system::MYSQL_TYPE_DATETIME2: {
        boost::uint64_t timestamp;
        boost::uint64_t intpart= mi_uint5korr(itor->storage()) - DATETIMEF_INT_OFS;

        int frac;

        switch (itor->metadata())
        {
        case 0:
        default:
	  timestamp = MY_PACKED_TIME_MAKE_INT(intpart);
          break;
        case 1:
        case 2:
          frac= ((int) (signed char) itor->storage()[5]) * 10000;
	  timestamp = MY_PACKED_TIME_MAKE(intpart, frac);
          break;
        case 3:
        case 4:
          frac= mi_sint2korr(itor->storage() + 5) * 100;
	  timestamp = MY_PACKED_TIME_MAKE(intpart, frac);
          break;
        case 5:
        case 6:
          frac= mi_sint3korr(itor->storage() + 5);
	  timestamp = MY_PACKED_TIME_MAKE(intpart, frac);
          break;
        }

        unsigned long ymd, hms;
        unsigned long ymdhms, ym;
        if (timestamp < 0)
          timestamp= -timestamp;

        unsigned long second_part = ((timestamp) % (1LL << 24));
        ymdhms= ((timestamp) >> 24);

        ymd= ymdhms >> 17;
        ym= ymd >> 5;
        hms= ymdhms % (1 << 17);

        int day = ymd % (1 << 5);
        int month = ym % 13;
        int year = ym / 13;

        int hours = hms >> 12;
        int minutes = (hms >> 6) % (1 << 6);
        int seconds = hms % (1 << 6);

        VALUE DateTime = rb_const_get(rb_cObject, rb_intern("DateTime"));
        rval = rb_funcall(DateTime, rb_intern("new"), 6,
            INT2FIX(year), INT2FIX(month), INT2FIX(day),
            INT2FIX(hours), INT2FIX(minutes), INT2FIX(seconds));
      } break;

      case mysql::system::MYSQL_TYPE_STRING:
      case mysql::system::MYSQL_TYPE_VARCHAR: {
        unsigned long size;
        char *ptr = itor->as_c_str(size);
        rval = rb_str_new(ptr, size);
      } break;

      case mysql::system::MYSQL_TYPE_TINY_BLOB:
      case mysql::system::MYSQL_TYPE_MEDIUM_BLOB:
      case mysql::system::MYSQL_TYPE_LONG_BLOB:
      case mysql::system::MYSQL_TYPE_BLOB: {
        unsigned long size;
        unsigned char *ptr = itor->as_blob(size);
        rval = rb_str_new((const char *)ptr, size);
      } break;

      default: break;
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
