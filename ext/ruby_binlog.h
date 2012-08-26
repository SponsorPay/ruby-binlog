#ifndef __RUBY_BINLOG_H__
#define __RUBY_BINLOG_H__

#include <string>
#include <binlog_api.h>
#include <ruby.h>
#include <rubysig.h>

#include "ruby_binlog_event.h"

#ifndef RSTRING_PTR
#define RSTRING_PTR(s) (RSTRING(s)->ptr)
#endif
#ifndef RSTRING_LEN
#define RSTRING_LEN(s) (RSTRING(s)->len)
#endif

#ifdef _WIN32
#define __F(f) (reinterpret_cast<VALUE (__cdecl *)(...)>(f))
#else
#define __F(f) (reinterpret_cast<VALUE (*)(...)>(f))
#endif

extern VALUE rb_mBinlog;

namespace ruby {
namespace binlog {
const char* get_field_type_str(mysql::system::enum_field_types type);
} // namespace binlog
} // namespace ruby

extern "C" {
#ifdef _WIN32
__declspec(dllexport)
#endif
void Init_binlog();
}

#endif // __RUBY_BINLOG_H__
