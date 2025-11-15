#ifndef RECORD_H
#define RECORD_H

#include "catalog.h"
#include "utils.h"

#define KEY_INLINE_SIZE 24
#define KEY_PREFIX_SIZE (KEY_INLINE_SIZE - sizeof(struct VPtr))

struct KeyRef {
    u16 size; // key size
    u16 _pad;
    u32 key_hash; // FNV-1a key hash for quick inequality check
    union {
        u8 full[KEY_INLINE_SIZE];
        struct {
            u8 prefix[KEY_PREFIX_SIZE];
            struct VPtr data_ptr;
        } partial;
    };
};
_Static_assert(sizeof(struct KeyRef) == 32, "KeyRef should be 32-byte long");

void key_encode_int(u8 *buf, i64 val);
void key_encode_double(u8 *buf, double val);

i32 create_key(struct Catalog *c, struct KeyRef *kr, const u8 *data, u16 size);
i32 read_key(struct Catalog *c, const struct KeyRef *kr, u8 *buf);
i32 free_key(struct Catalog *c, const struct KeyRef *kr);
// KeyRef comparison
// returns (k1 > k2) - (k1 < k2)
i32 fast_compare_key(const struct KeyRef *k1, const struct KeyRef *k2, bool *need_full_cmp);
i32 full_compare_key(struct Catalog *c, const struct KeyRef *k1, const struct KeyRef *k2, i32 *cmp_res);
// KeyRef & external data comparison
// returns (k > data) - (k < data)
i32 fast_compare_key_ext(const struct KeyRef *k, const u8 *data, u16 size, bool *need_full_cmp);
i32 full_compare_key_ext(struct Catalog *c, const struct KeyRef *k, const u8 *data, u16 size, i32 *cmp_res);

#endif /* ifndef RECORD_H */
