#ifndef SHTABLE_H
#define SHTABLE_H

#include "utils.h"

#define EMPTY 0xFFFFFFFF
#define DELETED 0xFFFFFFFE

struct Entry {
    atomic_u32 key, val;
};

struct SHTable {
    struct Entry *entries;
    atomic_u32 size;
    u32 mask;
};

struct SHTable *sht_init(u32 cap);
void sht_destroy(struct SHTable *t);
i32 sht_get(struct SHTable *t, u32 key, u32 *val_out);
i32 sht_set(struct SHTable *t, u32 key, u32 val);
i32 sht_unset(struct SHTable *t, u32 key);
u32 sht_size(struct SHTable *t);

#endif /* ifndef SHTABLE_H */
