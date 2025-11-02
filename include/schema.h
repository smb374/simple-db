#ifndef SCHEMA_H
#define SCHEMA_H

#include "btree.h"
#include "dblock.h"
#include "gdt_page.h"
#include "utils.h"

#define MAX_COLUMN 50

enum {
    S_TABLE = 0,
    S_INDEX = 1,
};

enum {
    KEY_NONE = 0,
    KEY_PRIM = 1,
    KEY_UNIQ = 2,
};

struct Column {
    char name[MAX_NAME + 1];
    u8 type;
    u8 uniq;
    u8 size;
};

#define SCHEMA_PADDING (DATA_HUGE_SPACE - (8 + (MAX_NAME + 1) * 2 + sizeof(struct Column) * MAX_COLUMN))
struct StaticSchema {
    u8 type;
    u8 num_cols;
    u8 prim_col;
    u8 _pad1;
    u32 root_page;
    char name[MAX_NAME + 1];
    char table_name[MAX_NAME + 1];
    struct Column cols[MAX_COLUMN];
    u8 _pad2[SCHEMA_PADDING];
};
_Static_assert(sizeof(struct StaticSchema) == DATA_HUGE_SPACE, "Superblock should be DATA_HUGE_SPACE long");

static inline void *get_schema_root(struct GdtPageBank *b) { return (u8 *) b->pages + SCHEMA_PAGE * PAGE_SIZE; }
i32 create_tree(struct BTreeHandle *handle, struct GdtPageBank *bank, struct StaticSchema *s);
i32 init_schema_tree(struct GdtPageBank *bank);

enum SSBuilderResult {
    SSB_OK = 0, // OK
    SSB_UNKNOWN_SCHEMA_TYPE = 1,
    SSB_TOO_MUCH_COLS = 2,
    SSB_NAME_TOO_LONG = 3,
    SSB_NO_NAME = 4,
    SSB_TABLE_NAME_TOO_LONG = 5,
    SSB_NO_TABLE_NAME = 6,
    SSB_COL_NAME_TOO_LONG = 7,
    SSB_COL_UNKNOWN_TYPE = 8,
    SSB_COL_UNKNOWN_UNIQ = 9,
    SSB_DUPLICATE_PRIM = 10,
    SSB_NO_PRIM = 11,
    SSB_KEY_TOO_LONG = 12,
    SSB_KEY_SIZE_REQUIRED = 13,
};

struct StaticSchemaBuilder {
    u8 type;
    u8 num_cols;
    u8 prim_col;
    char *name;
    char *table_name;
    struct Column cols[MAX_COLUMN];
};

i32 ssb_init(struct StaticSchemaBuilder *builder, u8 type);
// Fixed size for numeric types
// Variable length types
// size = 0 means unlimited (for non-key columns)
// size > 0 means max length (required for key columns)
i32 ssb_add_column(struct StaticSchemaBuilder *builder, const char *name, u8 type, u8 uniq, u8 size);
i32 ssb_finalize(struct StaticSchema *s, const struct StaticSchemaBuilder *builder);

#endif /* ifndef SCHEMA_H */
