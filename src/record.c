#include "record.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "catalog.h"
#include "pagestore.h"
#include "schema.h"
#include "utils.h"

// memcmp friendly encoding
void key_encode_int(u8 *buf, i64 val) {
    u64 encoded = ((u64) val) ^ 0x8000000000000000ULL;
    store64be(encoded, buf);
}

// memcmp friendly encoding
void key_encode_double(u8 *buf, double val) {
    u64 bits;

    memcpy(&bits, &val, 8);

    if (bits & 0x8000000000000000ULL) {
        bits = ~bits;
    } else {
        bits ^= 0x8000000000000000ULL;
    }

    store64be(bits, buf);
}

i32 create_key(struct Catalog *c, struct KeyRef *kr, const u8 *data, u16 size) {
    memset(kr, 0, sizeof(struct KeyRef));
    kr->size = size;
    kr->key_hash = fnv1a_32(data, size);
    if (size <= KEY_INLINE_SIZE) {
        memcpy(kr->full, data, size);
    } else {
        memcpy(kr->partial.prefix, data, KEY_PREFIX_SIZE);
        struct VPtr ptr = catalog_write_key(c, data, size); // stores full key in catalog
        if (ptr.page_num == INVALID_PAGE)
            return -1;
        kr->partial.data_ptr = ptr;
    }

    return 0;
}

i32 read_key(struct Catalog *c, const struct KeyRef *kr, u8 *buf) {
    if (kr->size <= KEY_INLINE_SIZE) {
        memcpy(buf, kr->full, kr->size);
        return 0;
    }
    return catalog_read(c, &kr->partial.data_ptr, buf, kr->size > NORMAL_DATA_LIMIT);
}

i32 free_key(struct Catalog *c, const struct KeyRef *kr) {
    if (kr->size <= KEY_INLINE_SIZE)
        return 0;

    return catalog_free(c, &kr->partial.data_ptr, kr->size > NORMAL_DATA_LIMIT);
}

i32 fast_compare_key(const struct KeyRef *k1, const struct KeyRef *k2, bool *need_full_cmp) {
    *need_full_cmp = false;
    if (k1->size <= KEY_INLINE_SIZE && k2->size <= KEY_INLINE_SIZE) {
        i32 res = memcmp(k1->full, k2->full, KEY_INLINE_SIZE); // full is zero-padded to KEY_INLINE_SIZE
        return res ? res : (k1->size > k2->size) - (k1->size < k2->size);
    } else {
        *need_full_cmp = true;
        // Since we use union, full & partial.prefix are overlapped so
        // it's safe to compare on partial.prefix only.
        return memcmp(k1->partial.prefix, k2->partial.prefix, KEY_PREFIX_SIZE);
    }
}

i32 fast_compare_key_ext(const struct KeyRef *k, const u8 *data, u16 size, bool *need_full_cmp) {
    *need_full_cmp = false;
    if (k->size <= KEY_INLINE_SIZE && size <= KEY_INLINE_SIZE) {
        i32 res = memcmp(k->full, data, MIN(k->size, size));
        return res ? res : (k->size > size) - (k->size < size);
    } else {
        *need_full_cmp = true;
        return memcmp(k->partial.prefix, data, MIN(size, KEY_PREFIX_SIZE));
    }
}

i32 full_compare_key(struct Catalog *c, const struct KeyRef *k1, const struct KeyRef *k2, i32 *cmp_res) {
    u8 *k1buf = calloc(k1->size, 1);
    u8 *k2buf = calloc(k2->size, 1);

    if (!k1buf || !k2buf)
        return -1;
    if (read_key(c, k1, k1buf) < 0)
        goto error;
    if (read_key(c, k2, k2buf) < 0)
        goto error;

    i32 res = memcmp(k1buf, k2buf, MIN(k1->size, k2->size));
    *cmp_res = res ? res : (k1->size > k2->size) - (k1->size < k2->size);

    free(k1buf);
    free(k2buf);
    return 0;

error:
    free(k1buf);
    free(k2buf);

    return -1;
}

i32 full_compare_key_ext(struct Catalog *c, const struct KeyRef *k, const u8 *data, u16 size, i32 *cmp_res) {
    u8 *kbuf = calloc(k->size, 1);

    if (!kbuf)
        return -1;
    if (read_key(c, k, kbuf) < 0)
        goto error;

    i32 res = memcmp(kbuf, data, MIN(k->size, size));
    *cmp_res = res ? res : (k->size > size) - (k->size < size);

    free(kbuf);
    return 0;

error:
    free(kbuf);

    return -1;
}

static inline bool record_col_null(const struct RecordHeader *header, u16 cidx) {
    return (header->null_bitmap[cidx / 8] >> (cidx % 8)) & 1;
}

static inline bool is_var_type(u8 tag) {
    switch (tag & 0xF) {
        case TYPE_DECIMAL:
        case TYPE_TEXT:
        case TYPE_BLOB:
            return true;
        default:
            return false;
    }
}

static inline u16 fixed_type_size(u8 tag) {
    switch (tag & 0xF) {
        case TYPE_BOOL:
            return 1;
        case TYPE_INTEGER:
        case TYPE_REAL:
        case TYPE_TIMESTAMP:
            return 8;
        case TYPE_UUID:
            return 16;
        default:
            return 0;
    }
}

i32 record_encode(const struct MemRecord *rec, u8 *buf, const u32 cap, u32 *out_size) {
    u8 ncols = rec->header.ncols;
    const struct MemSchema *sch = rec->schema;
    u32 cursor = ncols * sizeof(u16);
    u16 *cot = (u16 *) buf;
    u64 numeric_buf;

    u8 vcols[MAX_COLUMNS];
    u16 vtop = 0;

    for (u8 i = 0; i < ncols; i++) {
        if (record_col_null(&rec->header, i)) {
            cot[i] = 0;
            continue;
        } else if (is_var_type(sch->defs[i].tag)) {
            vcols[vtop++] = i;
            continue;
        }

        u16 size = fixed_type_size(sch->defs[i].tag);
        if (cap < size || cursor > cap - size)
            return -1;

        switch (sch->defs[i].tag & 0xF) {
            case TYPE_BOOL:
                cot[i] = cursor;
                buf[cot[i]] = *(bool *) rec->cols[i];
                break;
            case TYPE_INTEGER:
            case TYPE_REAL:
            case TYPE_TIMESTAMP:
                cot[i] = cursor;
                memcpy(&numeric_buf, rec->cols[i], 8);
                store64le(numeric_buf, &buf[cot[i]]);
                break;
            case TYPE_UUID:
                cot[i] = cursor;
                memcpy(&buf[cot[i]], rec->cols[i], 16);
                break;
            default:
                break;
        }
        cursor += size;
    }

    for (u16 i = 0; i < vtop; i++) {
        u8 idx = vcols[i];
        u16 ssize = MIN(rec->col_size[idx], COL_OVERFLOW_THRES);
        if (cap < (ssize + 2) || cursor > cap - (ssize + 2))
            return -1;

        cot[idx] = cursor;
        store16le(rec->col_size[idx], &buf[cot[idx]]);
        memcpy(&buf[cot[idx] + 2], rec->cols[idx], ssize);

        cursor += ssize + 2;
    }

    *out_size = cursor;

    return 0;
}

struct MemRecord *record_decode(const struct MemSchema *schema, const struct RecordHeader *header, const u8 *buf,
                                u32 bsize) {
    struct MemRecord *rec = calloc(1, sizeof(struct MemRecord));

    memcpy(&rec->header, header, sizeof(struct RecordHeader));
    rec->schema = schema;
    u8 ncols = rec->header.ncols;
    u16 *cot = (u16 *) buf;

    for (u8 i = 0; i < ncols; i++) {
        if (record_col_null(header, i)) {
            rec->cols[i] = NULL;
            rec->col_size[i] = 0;
            continue;
        }
        u16 off = cot[i];
        u16 size;

        if (is_var_type(schema->defs[i].tag)) {
            size = load16le(&buf[off]);
            off += 2;
        } else {
            size = fixed_type_size(schema->defs[i].tag);
        }
        u16 ssize = MIN(size, COL_OVERFLOW_THRES); // size directly stored in record data
        if (off > bsize - ssize) {
            goto error;
        }
        rec->col_size[i] = size;
        rec->cols[i] = calloc(1, size);
        memcpy(rec->cols[i], &buf[off], ssize);
    }

    return rec;

error:
    free_record(rec);
    return NULL;
}

void free_record(struct MemRecord *rec) {
    for (u8 i = 0; i < rec->header.ncols; i++) {
        if (rec->cols[i] != NULL) {
            free(rec->cols[i]);
        }
    }
    free(rec);
}


i32 record_overflow_cols(struct Catalog *c, struct MemRecord *rec) {
    for (u8 i = 0; i < rec->header.ncols; i++) {
        const u16 size = rec->col_size[i];
        if (size > COL_OVERFLOW_THRES) {
            const u16 suff_start = COL_PREFIX_SIZE;
            u8 *col_bytes = rec->cols[i];

            struct VPtr ptr = catalog_write_data(c, &col_bytes[suff_start], size - suff_start);
            if (ptr.page_num == INVALID_PAGE)
                return -1;

            memcpy(&col_bytes[suff_start], &ptr, sizeof(struct VPtr));
        }
    }
    return 0;
}

i32 record_recover_cols(struct Catalog *c, struct MemRecord *rec) {
    for (u8 i = 0; i < rec->header.ncols; i++) {
        const u16 size = rec->col_size[i];
        if (size > COL_OVERFLOW_THRES) {
            const u16 suff_start = COL_PREFIX_SIZE;
            u8 *col_bytes = rec->cols[i];
            struct VPtr ptr;

            memcpy(&ptr, &col_bytes[suff_start], sizeof(struct VPtr));
            if (ptr.page_num == INVALID_PAGE)
                return -1;
            if (catalog_read(c, &ptr, &col_bytes[suff_start], (size - suff_start) > NORMAL_DATA_LIMIT) < 0)
                return -1;
        }
    }
    return 0;
}
