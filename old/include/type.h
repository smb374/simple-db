#ifndef TYPE_H
#define TYPE_H

#include "utils.h"

enum {
    TYPE_INTEGER = 0,
    TYPE_REAL = 1,
    TYPE_TEXT = 2,
    TYPE_BLOB = 3,
};

void encode_integer_key(u8 *key, i64 val);
void encode_real_key(u8 *key, double val);
void encode_blob_key(u8 *key, const u8 *data, u32 len);

#endif /* ifndef TYPE_H */
