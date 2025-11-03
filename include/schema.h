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

struct ColumnDef {
    char name[MAX_NAME];
    u8 type;
    u8 uniq;
    u8 size_hint;
};

#define SCHEMA_PADDING (DATA_HUGE_SPACE - (8 + (MAX_NAME + 1) * 2 + sizeof(struct ColumnDef) * MAX_COLUMN))
struct StaticSchema {
    u8 type;
    u8 num_cols;
    u8 prim_col;
    u8 _pad1;
    u32 root_page;
    char name[MAX_NAME + 1];
    char table_name[MAX_NAME + 1];
    struct ColumnDef cols[MAX_COLUMN];
    u8 _pad2[SCHEMA_PADDING];
};
_Static_assert(sizeof(struct StaticSchema) == DATA_HUGE_SPACE, "Superblock should be DATA_HUGE_SPACE long");

struct IndexHandle {
    u8 type; // S_TABLE or S_INDEX
    u8 key_idx;
    u16 sversion;
    u32 spage;
    bool cache_valid;
    struct BTreeHandle th;
    struct StaticSchema *schema; // Heap ptr
};

static inline void *get_schema_root(struct GdtPageBank *b) { return (u8 *) b->pages + SCHEMA_PAGE * PAGE_SIZE; }
i32 init_schema_tree(struct GdtPageBank *bank);
i32 create_tree(struct IndexHandle *handle, struct GdtPageBank *bank, const struct StaticSchema *s);
i32 open_tree(struct IndexHandle *handle, struct GdtPageBank *bank, const char *name);
void close_tree(struct IndexHandle *handle);
i32 validate_tree_cache(struct IndexHandle *handle);
i32 refresh_tree_cache(struct IndexHandle *handle);

enum SSBuilderResult {
    SSB_OK = 0, // OK
    SSB_UNKNOWN_SCHEMA_TYPE = -1,
    SSB_TOO_MUCH_COLS = -2,
    SSB_NAME_TOO_LONG = -3,
    SSB_NO_NAME = -4,
    SSB_TABLE_NAME_TOO_LONG = -5,
    SSB_NO_TABLE_NAME = -6,
    SSB_COL_NAME_TOO_LONG = -7,
    SSB_COL_UNKNOWN_TYPE = -8,
    SSB_COL_UNKNOWN_UNIQ = -9,
    SSB_DUPLICATE_PRIM = -10,
    SSB_NO_PRIM = -11,
    SSB_KEY_TOO_LONG = -12,
    SSB_KEY_SIZE_REQUIRED = -13,
};

struct StaticSchemaBuilder {
    u8 type;
    u8 num_cols;
    u8 prim_col;
    char *name;
    char *table_name;
    struct ColumnDef cols[MAX_COLUMN];
};

i32 ssb_init(struct StaticSchemaBuilder *builder, u8 type);
// Fixed size for numeric types
// Variable length types
// size = 0 means unlimited (for non-key columns)
// size > 0 means max length (required for key columns)
i32 ssb_add_column(struct StaticSchemaBuilder *builder, const char *name, u8 type, u8 uniq, u8 size);
i32 ssb_finalize(struct StaticSchema *s, const struct StaticSchemaBuilder *builder);

#define NULL_BITMAPS ((MAX_COLUMN + 7) / 8)
struct RecordMetadata {
    u32 size;
    u8 num_cols;
    u8 null_bitmap[NULL_BITMAPS];
    struct VPtr ptr;
};
_Static_assert(sizeof(struct RecordMetadata) <= MAX_INLINE, "Metadata must fit inline");

// Saved record encode:
// [COT][fixed-sized column pack][len1 | blob data1][len2 | blob data2]...
// COT uses the same column order in the schema
// If the column is null, COT[idx] = 0 and the bit in NULL bitmap of the metadata should be set.

struct RecordColumn {
    u8 col_idx;
    u8 type;
    union {
        i64 ival;
        double dval;
        struct {
            const u8 *dat;
            u32 size;
        } blob;
    };
};

enum {
    ENC_REC_OK = 0,
    ENC_REC_INVALID_ARGS = -1,
    ENC_REC_COL_TYPE_UNMATCH = -2,
    ENC_REC_COL_INVALID_IDX = -3,
    ENC_REC_COL_DUPLICATED = -4,
    ENC_REC_BUF_TOO_SMALL = -5,
    ENC_REC_NO_PRIM = -6,
    ENC_REC_ALLOC_FAILED = -7,
};

enum {
    DEC_REC_OK = 0,
    DEC_REC_INVALID_ARGS = -1,
    DEC_REC_INVALID_METADATA = -2,
    DEC_REC_INVALID_RECORD = -3,
    DEC_REC_ALLOC_FAILED = -4,
};

struct RecordEnc {
    u8 *buf;
    u32 buf_capacity; // Allocated size
    u32 buf_len; // Used size (output)
    bool managed;
    u8 prim_col;
    struct RecordMetadata meta;
    const struct IndexHandle *handle;
};

struct RecordDec {
    const struct RecordMetadata *meta;
    const struct IndexHandle *handle;
    struct RecordColumn *cols;
    u8 n_cols;
};

i32 encode_record(struct RecordEnc *enc, const struct RecordColumn *cols, u8 n_cols);
i32 decode_record(struct RecordDec *dec, const u8 *buf, u32 buf_len);

#endif /* ifndef SCHEMA_H */
