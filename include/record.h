#ifndef RECORD_H
#define RECORD_H

#include "catalog.h"
#include "schema.h"
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

#define COL_OVERFLOW_THRES 1024
#define COL_PREFIX_SIZE (COL_OVERFLOW_THRES - sizeof(struct VPtr))

enum RecordFlags {
    REC_NONE = 0,
    REC_DEL = 1,
};

#define NULL_BITMAPS ((MAX_COLUMNS + 7) / 8)
struct RecordHeader {
    u32 schema_id;
    u32 size;
    u16 version;
    u8 ncols;
    u8 flags;
    u8 null_bitmap[NULL_BITMAPS];
};

struct RecordEntry {
    struct RecordHeader header;
    struct VPtr data;
};

struct MemRecord {
    struct RecordHeader header;
    const struct MemSchema *schema;
    void *cols[MAX_COLUMNS];
    u16 col_size[MAX_COLUMNS];
};

i32 record_overflow_cols(struct Catalog *c, struct MemRecord *rec);
i32 record_recover_cols(struct Catalog *c, struct MemRecord *rec);
i32 record_encode(const struct MemRecord *rec, u8 *buf, u32 cap, u32 *out_size);
struct MemRecord *record_decode(const struct MemSchema *schema, const struct RecordHeader *header, const u8 *buf,
                                u32 size);
void free_record(struct MemRecord *rec);

#endif /* ifndef RECORD_H */
