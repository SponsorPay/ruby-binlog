#include "ruby_binlog.h"

#define DIG_PER_DEC1        9

typedef boost::int8_t int8;
typedef boost::int16_t int16;
typedef boost::int32_t int32;
typedef boost::uint8_t uint8;
typedef boost::uint16_t uint16;
typedef boost::uint32_t uint32;

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

typedef struct st_decimal_t {
  int  intg, frac, len;
  bool sign;
  decimal_digit_t *buf;
} decimal_t;

static const int dig2bytes[DIG_PER_DEC1 + 1] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
static const dec1 powers10[DIG_PER_DEC1 + 1] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

int bin2decimal(const unsigned char *from, decimal_t *to, int precision, int scale) {
  int intg   = precision-scale,
      intg0  = intg / DIG_PER_DEC1, 
      frac0  = scale/DIG_PER_DEC1,
      intg0x = intg - intg0 * DIG_PER_DEC1, 
      frac0x = scale-frac0 * DIG_PER_DEC1;

  dec1 *buf = to->buf, mask = (*from & 0x80) ? 0 : -1;
  const unsigned char *stop;
  unsigned char *d_copy;
  int bin_size = intg0 * sizeof(dec1) + dig2bytes[intg0x] + frac0 * sizeof(dec1) + dig2bytes[frac0x];

  d_copy = (unsigned char*) malloc(bin_size);
  memset(d_copy, 0, bin_size);
  memcpy(d_copy, from, bin_size);
  d_copy[0] ^= 0x80;
  from = d_copy;

  to->sign = (mask != 0);
  to->intg = intg0 * DIG_PER_DEC1 + intg0x;
  to->frac = frac0 * DIG_PER_DEC1 + frac0x;

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
    *buf = x ^ mask;
    if (buf > to->buf || *buf != 0) buf++; else to->intg -= intg0x;
  }

  for (stop = from + intg0 * sizeof(dec1); from < stop; from += sizeof(dec1)) {
    *buf = mi_sint4korr(from) ^ mask;
    if (buf > to->buf || *buf != 0) buf++; else to->intg -= DIG_PER_DEC1;
  }
  
  for (stop = from + frac0 * sizeof(dec1); from < stop; from += sizeof(dec1)) {
    *buf = mi_sint4korr(from) ^ mask;
    buf++;
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
    
    *buf = (x ^ mask) * powers10[DIG_PER_DEC1 - frac0x];
    buf++;
  }
  free(d_copy);
  return 0;
}
