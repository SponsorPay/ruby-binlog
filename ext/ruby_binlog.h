#ifndef __RUBY_BINLOG_H__
#define __RUBY_BINLOG_H__

// XXX:
#define private public

#include <string>
#include <binlog_api.h>
#include <ruby.h>

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

#define WAIT_INTERVAL 300

#define mi_sint1korr(A) ((boost::int8_t)(*A))

#define mi_sint2korr(A) ((boost::int16_t)    (((boost::int16_t) (((boost::uint8_t*) (A))[1])) + ((boost::int16_t) ((boost::int16_t) ((char*) (A))[0]) << 8)))

#define mi_sint3korr(A) ((boost::int32_t)    (((((boost::uint8_t*) (A))[0]) & 128) ?          \
                                             (((boost::uint32_t) 255L << 24) |                \
                                             (((boost::uint32_t) ((boost::uint8_t*) (A))[0]) << 16) |  \
                                             (((boost::uint32_t) ((boost::uint8_t*) (A))[1]) << 8) |   \
                                             ((boost::uint32_t) ((boost::uint8_t*) (A))[2])) :         \
                                             (((boost::uint32_t) ((boost::uint8_t*) (A))[0]) << 16) |  \
                                             (((boost::uint32_t) ((boost::uint8_t*) (A))[1]) << 8) |   \
                                             ((boost::uint32_t) ((boost::uint8_t*) (A))[2])))

#define mi_sint4korr(A) ((boost::int32_t)    (((boost::int32_t) (((boost::uint8_t*) (A))[3])) +        \
                                             ((boost::int32_t) (((boost::uint8_t*) (A))[2]) << 8) +    \
                                             ((boost::int32_t) (((boost::uint8_t*) (A))[1]) << 16) +   \
                                             ((boost::int32_t) ((boost::int16_t) ((char*) (A))[0]) << 24)))

#define mi_uint5korr(A) ((boost::uint64_t)   (((boost::uint32_t) (((const unsigned char*) (A))[4])) +\
                                             (((boost::uint32_t) (((const unsigned char*) (A))[3])) << 8) +\
                                             (((boost::uint32_t) (((const unsigned char*) (A))[2])) << 16) +\
                                             (((boost::uint32_t) (((const unsigned char*) (A))[1])) << 24)) +\
                                             (((boost::uint64_t) (((const unsigned char*) (A))[0])) << 32))

#define DATETIMEF_INT_OFS 0x8000000000LL

#define MY_PACKED_TIME_GET_INT_PART(x)     ((x) >> 24)
#define MY_PACKED_TIME_GET_FRAC_PART(x)    ((x) % (1LL << 24))
#define MY_PACKED_TIME_MAKE(i, f)          ((((boost::uint64_t) (i)) << 24) + (f))
#define MY_PACKED_TIME_MAKE_INT(i)         ((((boost::uint64_t) (i)) << 24))

extern VALUE rb_mBinlog;
extern VALUE rb_eBinlogError;

namespace ruby {
namespace binlog {
const char* get_field_type_str(mysql::system::enum_field_types type);
mysql::system::Binlog_tcp_driver *cast_to_tcp_driver(mysql::system::Binary_log_driver *driver);
std::string decimal2str(const mysql::Value& val);
} // namespace binlog
} // namespace ruby

extern "C" {
#ifdef _WIN32
__declspec(dllexport)
#endif
void Init_binlog();
}

#endif // __RUBY_BINLOG_H__
