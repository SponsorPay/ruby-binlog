#include "ruby_binlog.h"

namespace ruby {
namespace binlog {

typedef boost::int8_t int8;
typedef boost::int16_t int16;
typedef boost::int32_t int32;
typedef boost::uint8_t uint8;
typedef boost::uint16_t uint16;
typedef boost::uint32_t uint32;

#define DIG_PER_DEC1    9

#define mi_sint1korr(A) ((int8)(*A))

#define mi_sint2korr(A) ((int16)    (((int16) (((uint8*) (A))[1])) + ((int16) ((int16) ((char*) (A))[0]) << 8)))

#define mi_sint3korr(A) ((int32)    (((((uint8*) (A))[0]) & 128) ?          \
                                    (((uint32) 255L << 24) |                \
                                    (((uint32) ((uint8*) (A))[0]) << 16) |  \
                                    (((uint32) ((uint8*) (A))[1]) << 8) |   \
                                    ((uint32) ((uint8*) (A))[2])) :         \
                                    (((uint32) ((uint8*) (A))[0]) << 16) |  \
                                    (((uint32) ((uint8*) (A))[1]) << 8) |   \
                                    ((uint32) ((uint8*) (A))[2])))

#define mi_sint4korr(A) ((int32)    (((int32) (((uint8*) (A))[3])) +        \
                                    ((int32) (((uint8*) (A))[2]) << 8) +    \
                                    ((int32) (((uint8*) (A))[1]) << 16) +   \
                                    ((int32) ((int16) ((char*) (A))[0]) << 24)))

typedef int32 decimal_digit_t;
typedef decimal_digit_t dec1;

static const int dig2bytes[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

std::string decimal2str(const mysql::Value& val) {
  const unsigned char* from = (const unsigned char*)val.storage();
  int precision = val.metadata() & 0xff;
  int scale = val.metadata() >> 8;

  int intg   = precision-scale,
      intg0  = intg / DIG_PER_DEC1, 
      frac0  = scale / DIG_PER_DEC1,
      intg0x = intg - intg0 * DIG_PER_DEC1, 
      frac0x = scale - frac0 * DIG_PER_DEC1;

  dec1 mask = (*from & 0x80) ? 0 : -1;

  const unsigned char *stop;
  unsigned char *d_copy;
  int bin_size = intg0 * sizeof(dec1) + dig2bytes[intg0x] + frac0 * sizeof(dec1) + dig2bytes[frac0x];

  d_copy = (unsigned char*) malloc(bin_size);
  memset(d_copy, 0, bin_size);
  memcpy(d_copy, from, bin_size);
  d_copy[0] ^= 0x80;
  from = d_copy;

  std::stringstream out;
  if (mask != 0) out << "-";
  out << std::setfill('0');

  if (intg0x) {
    int i = dig2bytes[intg0x];
    dec1 x = x;
    switch (i) {
      case 1: x = mi_sint1korr(from); break;
      case 2: x = mi_sint2korr(from); break;
      case 3: x = mi_sint3korr(from); break;
      case 4: x = mi_sint4korr(from); break;
    }
    from += i;
    out << std::setw(intg0x) << (x ^ mask);
  }

  for (stop = from + intg0 * sizeof(dec1); from < stop; from += sizeof(dec1)) {
    out << std::setw(DIG_PER_DEC1) << (mi_sint4korr(from) ^ mask);
  }

  out << ".";

  for (stop = from + frac0 * sizeof(dec1); from < stop; from += sizeof(dec1)) {
    out << std::setw(DIG_PER_DEC1) << (mi_sint4korr(from) ^ mask);
  }

  if (frac0x) {
    int i = dig2bytes[frac0x];
    dec1 x = x;

    switch (i) {
      case 1: x=mi_sint1korr(from); break;
      case 2: x=mi_sint2korr(from); break;
      case 3: x=mi_sint3korr(from); break;
      case 4: x=mi_sint4korr(from); break;
    }
    
    out << std::setw(frac0x) << (x ^ mask);
  }
  free(d_copy);
  return out.str();
}

} // namespace binlog
} // namespace ruby
