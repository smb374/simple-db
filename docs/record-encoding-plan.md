# Record Encoding Plan

## Overview

This document describes the record encoding format and API design for simple-db. The design prioritizes:

- **Stateless encode/decode** - No I/O during encode/decode operations
- **Caller-controlled overflow** - Explicit overflow/recover calls
- **Consistent patterns** - Similar to KeyRef overflow design
- **Index-only scans** - Header metadata in BTree leaf

---

## Data Structures

### RecordHeader (on-disk, in BTree leaf)

```c
#define MAX_COLUMNS 255
#define NULL_BITMAPS ((MAX_COLUMNS + 7) / 8)  // 32 bytes

struct RecordHeader {
    u32 schema_id;              // Schema reference
    u32 size;                   // Encoded buffer size
    u16 version;                // Schema version
    u8 ncols;                   // Number of columns
    u8 flags;                   // Record flags
    u8 null_bitmap[NULL_BITMAPS];  // NULL flags (256 bits)
};
// Total: 44 bytes
```

**Flags:**
```c
#define REC_FLAG_DELETED  0x01
#define REC_FLAG_OVERFLOW 0x02  // Has overflowed columns
```

### RecordEntry (BTree leaf entry)

```c
struct RecordEntry {
    struct RecordHeader header;  // 44 bytes - metadata inline
    struct VPtr data;            // 8 bytes - pointer to encoded data
};
// Total: 52 bytes per record in BTree leaf
```

**Benefits:**
- Header in leaf enables index-only scans for NULL checks, flags
- Actual column data referenced via VPtr (in catalog)

### MemRecord (in-memory working structure)

```c
struct MemRecord {
    struct RecordHeader header;
    struct MemSchema *schema;
    u8 *cols[MAX_COLUMNS];       // Pointers to column data
    u16 col_size[MAX_COLUMNS];   // Column sizes (original length, not stored size)
};
```

**Benefits:**
- Easy to build (fill arrays)
- Easy to read (direct access)
- Fixed arrays = no dynamic allocation during operations

---

## Encoded Buffer Layout

The encoded buffer (stored via VPtr) contains:

```
┌─────────────────────────────────────────┐
│ Column Offset Table: u16[ncols]         │
│   - Offset from buffer start to column  │
│   - Order matches schema definition     │
├─────────────────────────────────────────┤
│ Column Data (packed)                    │
│   Fixed-size: raw bytes                 │
│   Variable: [u16 len][data or suffix]   │
└─────────────────────────────────────────┘
```

**Note:** RecordHeader is NOT in the encoded buffer. It's stored separately in RecordEntry (BTree leaf).

---

## Column Storage Format

### Fixed-Size Types

Stored directly without length prefix:

| Type | Size | Format |
|------|------|--------|
| TYPE_BOOL | 1 byte | 0 or 1 |
| TYPE_INTEGER | 8 bytes | i64, little-endian |
| TYPE_REAL | 8 bytes | double, IEEE 754 |
| TYPE_UUID | 16 bytes | raw bytes |
| TYPE_TIMESTAMP | 8 bytes | i64 microseconds |

### Variable-Size Types

Length-prefixed with overflow support:

```c
#define COLUMN_OVERFLOW_THRESHOLD 1024  // 1KB

// Small column (len <= threshold)
[u16 len][u8 data[len]]

// Large column (len > threshold) - suffix only
[u16 len][u8 suffix[threshold - 8]][VPtr to suffix data]
```

**Key difference from KeyRef:** Record columns store only the **suffix** (not prefix), since records don't need prefix for comparison. This saves space for the majority of cases where columns fit inline.

**Example with 1KB threshold:**
```c
#define COLUMN_SUFFIX_SIZE (COLUMN_OVERFLOW_THRESHOLD - sizeof(struct VPtr))  // 1016 bytes

// 500-byte column (inline)
[u16 len=500][u8 data[500]]

// 2000-byte column (overflow)
// Stores last 1016 bytes inline, VPtr points to full data
[u16 len=2000][u8 suffix[1016]][VPtr]
```

**Benefits:**
- Self-describing (len tells you if overflow needed)
- No separate overflow bitmap needed
- Suffix available inline (often useful for trailing data)

---

## API Functions

### Core Operations

```c
// Overflow columns > threshold to catalog
// For overflowed columns: stores suffix inline, VPtr to full data
// col_size[i] retains original length
i32 record_overflow_cols(struct Catalog *c, struct MemRecord *rec);

// Recover full column data for overflowed columns
// Reads full data from catalog via VPtr
i32 record_recover_cols(struct Catalog *c, struct MemRecord *rec);

// Pure encode: MemRecord → buffer (no catalog access)
// Writes offset table + column data
i32 record_encode(const struct MemRecord *rec, u8 *buf, u32 cap, u32 *out_size);

// Pure decode: buffer → MemRecord (no catalog access)
// Allocates MemRecord and column buffers
struct MemRecord *record_decode(const struct MemSchema *schema,
                                const struct RecordHeader *header,
                                const u8 *buf, u32 size);

// Free decoded record and all allocated buffers
void record_free(struct MemRecord *rec);
```

### Helper Functions

```c
// Check if column is NULL
static inline bool record_col_is_null(const struct RecordHeader *hdr, u8 col_idx) {
    return (hdr->null_bitmap[col_idx / 8] >> (col_idx % 8)) & 1;
}

// Set column NULL bit
static inline void record_set_null(struct RecordHeader *hdr, u8 col_idx) {
    hdr->null_bitmap[col_idx / 8] |= (1 << (col_idx % 8));
}

// Check if type is variable-size
static inline bool is_variable_type(u8 type) {
    return type == TYPE_TEXT || type == TYPE_BLOB || type == TYPE_DECIMAL;
}

// Get fixed type size
static inline u16 fixed_type_size(u8 type) {
    switch (type) {
        case TYPE_BOOL: return 1;
        case TYPE_INTEGER: return 8;
        case TYPE_REAL: return 8;
        case TYPE_UUID: return 16;
        case TYPE_TIMESTAMP: return 8;
        default: return 0;
    }
}
```

---

## Usage Patterns

### INSERT Record

```c
// 1. Build in-memory record
struct MemRecord rec = {0};
rec.schema = schema;
rec.header.schema_id = schema->ent.id;
rec.header.version = schema->ent.version;
rec.header.ncols = schema->ent.ncols;

// Set columns (NULL columns left as NULL pointer)
rec.cols[0] = (u8 *)&id_value;
rec.col_size[0] = 8;
rec.cols[1] = (u8 *)name_string;
rec.col_size[1] = strlen(name_string);
// Set NULL column
record_set_null(&rec.header, 2);

// 2. Overflow large columns
record_overflow_cols(catalog, &rec);

// 3. Encode to buffer
u8 buf[8192];
u32 size;
record_encode(&rec, buf, sizeof(buf), &size);
rec.header.size = size;

// 4. Store in catalog
struct VPtr data_ptr = catalog_write_data(catalog, buf, size);

// 5. Create BTree entry
struct RecordEntry entry = {
    .header = rec.header,
    .data = data_ptr
};
// Insert entry into BTree
btree_insert(tree, key, &entry);
```

### SELECT Record

```c
// 1. Get RecordEntry from BTree
struct RecordEntry *entry = btree_search(tree, key);

// 2. Read encoded data from catalog
u8 buf[8192];
catalog_read(catalog, &entry->data, buf, entry->header.size >= NORMAL_DATA_LIMIT);

// 3. Decode (no catalog access)
struct MemRecord *rec = record_decode(schema, &entry->header, buf, entry->header.size);

// 4. Recover overflow columns if needed
if (entry->header.flags & REC_FLAG_OVERFLOW) {
    record_recover_cols(catalog, rec);
}

// 5. Use record
if (!record_col_is_null(&rec->header, 1)) {
    printf("Name: %.*s\n", rec->col_size[1], rec->cols[1]);
}

// 6. Free
record_free(rec);
```

### Index-Only Scan (no data fetch)

```c
// Just read header from BTree entry
struct RecordEntry *entry = btree_search(tree, key);

// Check NULL directly from header (no catalog access!)
if (record_col_is_null(&entry->header, col_idx)) {
    // Handle NULL
}

// Check flags
if (entry->header.flags & REC_FLAG_DELETED) {
    // Skip deleted record
}
```

---

## Implementation Details

### record_overflow_cols

```c
i32 record_overflow_cols(struct Catalog *c, struct MemRecord *rec) {
    struct MemSchema *schema = rec->schema;

    for (u8 i = 0; i < rec->header.ncols; i++) {
        if (record_col_is_null(&rec->header, i)) continue;

        u8 type = schema->defs[i].tag & 0x0F;
        if (!is_variable_type(type)) continue;

        u16 len = rec->col_size[i];
        if (len <= COLUMN_OVERFLOW_THRESHOLD) continue;

        // Write full data to catalog
        struct VPtr ptr = catalog_write_data(c, rec->cols[i], len);
        if (ptr.page_num == INVALID_PAGE) return -1;

        // Allocate buffer for suffix + VPtr
        u8 *new_buf = malloc(COLUMN_OVERFLOW_THRESHOLD);
        if (!new_buf) return -1;

        // Copy suffix (last COLUMN_SUFFIX_SIZE bytes)
        u16 suffix_offset = len - COLUMN_SUFFIX_SIZE;
        memcpy(new_buf, rec->cols[i] + suffix_offset, COLUMN_SUFFIX_SIZE);

        // Append VPtr
        memcpy(new_buf + COLUMN_SUFFIX_SIZE, &ptr, sizeof(struct VPtr));

        // Replace column pointer
        // Note: caller is responsible for original data
        rec->cols[i] = new_buf;
        // col_size retains original length (not stored size)

        rec->header.flags |= REC_FLAG_OVERFLOW;
    }

    return 0;
}
```

### record_recover_cols

```c
i32 record_recover_cols(struct Catalog *c, struct MemRecord *rec) {
    struct MemSchema *schema = rec->schema;

    for (u8 i = 0; i < rec->header.ncols; i++) {
        if (record_col_is_null(&rec->header, i)) continue;

        u8 type = schema->defs[i].tag & 0x0F;
        if (!is_variable_type(type)) continue;

        u16 len = rec->col_size[i];
        if (len <= COLUMN_OVERFLOW_THRESHOLD) continue;

        // Extract VPtr from end of stored data
        struct VPtr *ptr = (struct VPtr *)(rec->cols[i] + COLUMN_SUFFIX_SIZE);

        // Allocate buffer for full data
        u8 *full_data = malloc(len);
        if (!full_data) return -1;

        // Read full data from catalog
        if (catalog_read(c, ptr, full_data, len >= NORMAL_DATA_LIMIT) < 0) {
            free(full_data);
            return -1;
        }

        // Replace column pointer
        free(rec->cols[i]);  // Free suffix+VPtr buffer
        rec->cols[i] = full_data;
    }

    return 0;
}
```

### record_encode

```c
i32 record_encode(const struct MemRecord *rec, u8 *buf, u32 cap, u32 *out_size) {
    u8 ncols = rec->header.ncols;
    struct MemSchema *schema = rec->schema;

    // Calculate header size (offset table)
    u32 header_size = ncols * sizeof(u16);
    if (header_size > cap) return -1;

    u16 *offsets = (u16 *)buf;
    u32 data_offset = header_size;

    for (u8 i = 0; i < ncols; i++) {
        if (record_col_is_null(&rec->header, i)) {
            offsets[i] = 0;  // NULL marker
            continue;
        }

        offsets[i] = data_offset;
        u8 type = schema->defs[i].tag & 0x0F;

        if (is_variable_type(type)) {
            u16 len = rec->col_size[i];

            // Write length
            if (data_offset + 2 > cap) return -1;
            *(u16 *)(buf + data_offset) = len;
            data_offset += 2;

            // Determine stored size
            u16 stored_size = (len <= COLUMN_OVERFLOW_THRESHOLD)
                            ? len
                            : COLUMN_OVERFLOW_THRESHOLD;

            // Write data (inline or suffix+VPtr)
            if (data_offset + stored_size > cap) return -1;
            memcpy(buf + data_offset, rec->cols[i], stored_size);
            data_offset += stored_size;
        } else {
            // Fixed-size column
            u16 size = fixed_type_size(type);
            if (data_offset + size > cap) return -1;
            memcpy(buf + data_offset, rec->cols[i], size);
            data_offset += size;
        }
    }

    *out_size = data_offset;
    return 0;
}
```

### record_decode

```c
struct MemRecord *record_decode(const struct MemSchema *schema,
                                const struct RecordHeader *header,
                                const u8 *buf, u32 size) {
    struct MemRecord *rec = calloc(1, sizeof(struct MemRecord));
    if (!rec) return NULL;

    rec->header = *header;
    rec->schema = (struct MemSchema *)schema;  // Assume schema outlives record

    u8 ncols = header->ncols;
    u16 *offsets = (u16 *)buf;

    for (u8 i = 0; i < ncols; i++) {
        if (record_col_is_null(header, i) || offsets[i] == 0) {
            rec->cols[i] = NULL;
            rec->col_size[i] = 0;
            continue;
        }

        u8 type = schema->defs[i].tag & 0x0F;
        const u8 *col_ptr = buf + offsets[i];

        if (is_variable_type(type)) {
            u16 len = *(u16 *)col_ptr;
            rec->col_size[i] = len;

            // Determine stored size
            u16 stored_size = (len <= COLUMN_OVERFLOW_THRESHOLD)
                            ? len
                            : COLUMN_OVERFLOW_THRESHOLD;

            // Allocate and copy
            rec->cols[i] = malloc(stored_size);
            if (!rec->cols[i]) goto error;
            memcpy(rec->cols[i], col_ptr + 2, stored_size);
        } else {
            // Fixed-size column
            u16 col_size = fixed_type_size(type);
            rec->col_size[i] = col_size;
            rec->cols[i] = malloc(col_size);
            if (!rec->cols[i]) goto error;
            memcpy(rec->cols[i], col_ptr, col_size);
        }
    }

    return rec;

error:
    record_free(rec);
    return NULL;
}
```

### record_free

```c
void record_free(struct MemRecord *rec) {
    if (!rec) return;

    for (u8 i = 0; i < rec->header.ncols; i++) {
        free(rec->cols[i]);
    }
    free(rec);
}
```

---

## Schema Integration

### CatalogPage Update

Add auto-increment counter for schema IDs:

```c
struct CatalogPage {
    struct PageHeader header;
    u32 schema_root;
    u32 fsm_head;
    u32 kfsm_head;
    u32 next_schema_id;  // Auto-increment counter

    u8 _pad[PAGE_SIZE - sizeof(u32) * 4 - sizeof(struct PageHeader)];
};
```

### Schema Loading

```c
// Load schema from catalog
struct MemSchema *schema_load(struct Catalog *c, u32 schema_id) {
    // 1. Search schema BTree for schema_id
    // 2. Load SchemaEntry
    // 3. Load ColumnDefs via VPtr
    // 4. Return MemSchema
}
```

---

## Constants

```c
#define MAX_COLUMNS 255
#define NULL_BITMAPS ((MAX_COLUMNS + 7) / 8)  // 32 bytes

#define COLUMN_OVERFLOW_THRESHOLD 1024  // 1KB
#define COLUMN_SUFFIX_SIZE (COLUMN_OVERFLOW_THRESHOLD - sizeof(struct VPtr))  // 1016 bytes

#define REC_FLAG_DELETED  0x01
#define REC_FLAG_OVERFLOW 0x02
```

---

## Summary

**Key design decisions:**

- ✅ **Stateless encode/decode** - No I/O, easy to test
- ✅ **Suffix-only overflow** - Unlike KeyRef, stores suffix (not prefix) to save space
- ✅ **Self-describing format** - Length field indicates if overflow
- ✅ **Header in BTree leaf** - Enables index-only scans for NULLs, flags
- ✅ **Fixed arrays in MemRecord** - No dynamic allocation during operations
- ✅ **Caller-controlled lifecycle** - Explicit overflow/recover calls

**Benefits:**

- Simple and predictable behavior
- Easy to reason about memory ownership
- Testable without full database setup
- Efficient for common cases (most columns inline)
