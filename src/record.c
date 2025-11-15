#include "record.h"

#include <stdlib.h>
#include <string.h>

#include "catalog.h"
#include "pagestore.h"
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
