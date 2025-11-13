#include "shtable.h"

#include <assert.h>
#include <stdlib.h>

#include "utils.h"

struct SHTable *sht_init(u32 cap) {
    struct SHTable *t = calloc(1, sizeof(struct SHTable));
    t->entries = calloc(cap, sizeof(struct Entry));
    t->cap = cap;

    for (u32 i = 0; i < cap; i++) {
        t->entries[i].key = EMPTY;
        t->entries[i].val = EMPTY;
    }

    return t;
}

void sht_destroy(struct SHTable *t) {
    free(t->entries);
    free(t);
}

i32 sht_get(struct SHTable *t, u32 key, u32 *val_out) {
    assert(val_out);
    assert(key != EMPTY && key != DELETED); // Keys cannot be sentinel values

    u8 kbytes[4];
    store32le(key, kbytes);
    u32 hash = fnv1a_32(kbytes, 4);

    for (u32 i = 0; i < t->cap; i++) {
        u32 idx = (hash + i) % t->cap;
        u32 pkey = LOAD(&t->entries[idx].key, ACQUIRE);

        if (pkey == key) {
            u32 val = LOAD(&t->entries[idx].val, ACQUIRE);
            *val_out = val;
            return 0;
        }
        if (pkey == EMPTY) {
            return -1;
        }
    }
    return -1;
}

i32 sht_set(struct SHTable *t, u32 key, u32 val) {
    assert(key != EMPTY && key != DELETED);

    u8 kbytes[4];
    store32le(key, kbytes);
    u32 hash = fnv1a_32(kbytes, 4);

    for (u32 i = 0; i < t->cap; i++) {
        u32 idx = (hash + i) % t->cap;
        u32 pkey = LOAD(&t->entries[idx].key, ACQUIRE);

        if (pkey == key) {
            STORE(&t->entries[idx].val, val, RELEASE);
            return 0;
        }

        if (pkey == EMPTY || pkey == DELETED) {
            u32 expected = pkey;
            if (CMPXCHG(&t->entries[idx].key, &expected, key, ACQ_REL, RELAXED)) {
                STORE(&t->entries[idx].val, val, RELEASE);
                FADD(&t->size, 1, RELEASE);
                return 0;
            }

            if (expected == key) {
                STORE(&t->entries[idx].val, val, RELEASE);
                return 0;
            }
        }
    }
    return -1;
}

i32 sht_unset(struct SHTable *t, u32 key) {
    assert(key != EMPTY && key != DELETED);

    u8 kbytes[4];
    store32le(key, kbytes);
    u32 hash = fnv1a_32(kbytes, 4);

    for (u32 i = 0; i < t->cap; i++) {
        u32 idx = (hash + i) % t->cap;
        u32 pkey = LOAD(&t->entries[idx].key, ACQUIRE);

        if (pkey == key) {
            u32 expected = key;
            if (CMPXCHG(&t->entries[idx].key, &expected, DELETED, ACQ_REL, RELAXED)) {
                FSUB(&t->size, 1, RELEASE);
                return 0;
            }
            return 0;
        }

        if (pkey == EMPTY) {
            return -1;
        }
    }
    return -1;
}
