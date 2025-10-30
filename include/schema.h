#ifndef SCHEMA_H
#define SCHEMA_H

#include "dblock.h"
#include "gdt_page.h"
#include "utils.h"

#define MAX_COLUMN 50

enum {
    S_TABLE = 0,
    S_INDEX = 1,
};

enum {
    TYPE_BLOB = 0,
};

enum {
    KEY_NONE = 0,
    KEY_PRIM = 1,
    KEY_UNIQ = 2,
};

struct Column {
    char name[MAX_NAME];
    u8 type;
    u8 uniq;
    u8 size;
};

#define SCHEMA_PADDING (DATA_HUGE_SPACE - (4 * 2 + MAX_NAME * 2 + sizeof(struct Column) * MAX_COLUMN))
struct StaticSchema {
    u8 type;
    u8 num_cols;
    u16 _pad1;
    u32 root_page;
    char name[MAX_NAME];
    char table_name[MAX_NAME];
    struct Column cols[MAX_COLUMN];
    u8 _pad2[SCHEMA_PADDING];
};
_Static_assert(sizeof(struct StaticSchema) == DATA_HUGE_SPACE, "Superblock should be DATA_HUGE_SPACE long");

static inline void *get_schema_root(struct GdtPageBank *b) { return (u8 *) b->pages + SCHEMA_PAGE * PAGE_SIZE; }

#endif /* ifndef SCHEMA_H */
