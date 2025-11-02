#include "type.h"

#include <string.h>

#include "btree.h"
#include "utils.h"

void encode_integer_key(u8 *key, i64 val) {
    memset(key, 0, MAX_KEY);
    // Flip sign bit for correct lexicographic ordering
    u64 encoded = val ^ 0x8000000000000000ULL;
    u8 byt[8];
    store64be(encoded, byt);
    memcpy(key, byt, 8);
    // Pad rest of key with zeros
}

void encode_real_key(u8 *key, double val) {
    memset(key, 0, MAX_KEY);
    u64 bits;
    memcpy(&bits, &val, 8);

    // Encode for lexicographic comparison
    if (bits & 0x8000000000000000ULL) { // Negative
        bits = ~bits;
    } else { // Positive
        bits ^= 0x8000000000000000ULL;
    }

    u8 byt[8];
    store64be(bits, byt);
    memcpy(key, byt, 8);
}

void encode_blob_key(u8 *key, const u8 *data, u32 len) {
    memset(key, 0, MAX_KEY);
    u32 size = MIN(len, MAX_KEY);
    memcpy(key, data, size);
}
