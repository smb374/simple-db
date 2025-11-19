#ifndef SCHEMA_H
#define SCHEMA_H

#include "catalog.h"
#include "utils.h"

#define MAX_NAME 31
#define MAX_COLUMNS 0xFF

// Tag encoding: [4-bit tag | 4-bit type]
enum DataType {
    // Fixed Sized Types
    TYPE_BOOL = 0,
    TYPE_INTEGER = 1,
    TYPE_REAL = 2,
    TYPE_UUID = 3,
    TYPE_TIMESTAMP = 4,
    // Variable Sized Types
    TYPE_TEXT = 5,
    TYPE_DECIMAL = 6,
    TYPE_BLOB = 7,
};

enum DataFlag {
    FLAG_NONE = 0, // Regular
    FLAG_UNIQ = 1, // Uniq value
    FLAG_PRIM = 3, // Primary key (implies uniq)
    FLAG_NULL = 4, // Nullable
};

enum SchemaType {
    S_TABLE = 0,
    S_INDEX = 1,
};

struct ColumnDef {
    char name[MAX_NAME + 1];
    u8 tag;
    u16 size;
};

struct SchemaHeader {
    u32 id; // Schema ID, auto increment
    u32 src_id; // For S_INDEX, undefined for S_TABLE
    u32 root_page; // Index root page
    u8 type; // Schema Type
    u8 ncols; // # of columns
    u8 prim_col; // Primary column
    u16 version; // Schema Version
    char name[MAX_NAME + 1]; // Table name for S_TABLE, index name for S_INDEX
};

// Schema Entry stored in Schema Table BTree
struct SchemaEntry {
    struct SchemaHeader header;
    struct VPtr columns; // VPtr to column definition storage
};

// For in-memory schema reference, read-only mostly
struct MemSchema {
    struct SchemaHeader header;
    struct ColumnDef *defs;
};

#endif /* ifndef SCHEMA_H */
