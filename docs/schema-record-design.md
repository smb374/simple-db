# Schema & Record Design Plan

## Overview

This document defines the schema, record, and index storage format for simple-db. The design prioritizes:

- **Index-only scans:** Store metadata in BTree leaves to avoid record fetches
- **Space efficiency:** Inline small data, overflow large data with clear thresholds
- **Scan performance:** Secondary indexes store primary keys for efficient scans
- **Flexibility:** Support multi-page records, many columns, large BLOBs

---

## Design Decisions Summary

| Aspect | Decision |
|--------|----------|
| Storage model | Heap + unclustered indexes |
| Primary index | (Key → Metadata + RID) |
| Secondary index | (Key → Primary Key) |
| Key inline size | 16 bytes minimum (UUID) |
| Key hash | FNV-1a (32-bit) for fast inequality checks |
| Metadata in leaves | NULL bitmap + small covering columns |
| BLOB threshold | 1KB (separate pages above threshold) |
| Main record target | 2KB (triggers overflow) |
| Max columns | 256 (u8 for num_cols) |
| Record relocation | Allowed for better locality |
| Covering columns | Future: auto-detect small KEY_UNIQ + ≤8B cols |

---

## 1. Storage Architecture

### Overall Layout

```
┌─────────────────────────────────────────────────────────────┐
│                      Page Allocator                         │
├─────────────────────────────────────────────────────────────┤
│  Heap Pages  │  BTree Pages  │  BLOB Pages  │  Overflow     │
│  (Records)   │  (Indexes)    │  (Large data)│  (Rec+Key)    │
└─────────────────────────────────────────────────────────────┘
```

**Heap Pages:**

- Store main record data (header + fixed cols + small vars + BLOB pointers)
- Slotted page format
- Unordered (not clustered by key)

**BTree Pages:**

- Primary index: (PK → Metadata + RID)
- Secondary index: (SK → PK)
- Store covering data in leaves for index-only scans

**BLOB Pages:**

- Store large TEXT/BLOB columns (> 1KB threshold)
- Chained pages for values larger than one page
- Separate from main record for compactness

**Overflow Pages:**

- Multi-page records (record > 2KB)
- Large BTree keys (key > 16 byte inline prefix)

---

## 2. BTree Node Structure

### Node Header (Slotted Page)

```c
struct TreeNode {
    u8 type;              // BNODE_INT or BNODE_LEAF
    u8 nkeys;             // Number of keys (max 255)
    u16 frag_bytes;       // Fragmentation bytes
    u16 ent_off;          // Entry offset (grows down)
    u16 version;          // Leaf only: version for cache validation
    u32 parent_page;      // Parent page number
    u32 prev_page;        // Previous sibling (leaf only)
    u32 next_page;        // Next sibling (leaf only)
    u32 head_page;        // Smallest child (internal only)
    u16 slots[];          // Slot array (grows up)
};
// Header: 28 bytes
```

### Key Structure

```c
struct BTreeKey {
    u16 key_len;              // Full key length
    u8 key_inline[16];        // First 16 bytes (UUID, prefix)
    u32 key_overflow_page;    // Overflow page num (32-bit for 4TB DB)
    u16 key_overflow_slot;    // Slot within overflow page
    u32 key_hash;             // FNV-1a hash of full key
};
// Size: 28 bytes (was 26, +2 bytes for proper addressing)
```

**Key hash (FNV-1a):**

- Fast inequality detection (especially for composite keys)
- 32-bit hash reduces overhead
- Equality check: hash mismatch → keys definitely different

### Internal Node Entry

```c
struct InternalEntry {
    struct BTreeKey key;      // 28 bytes
    u32 child_page;           // 4 bytes
};
// Total: 32 bytes
```

### Primary Index Leaf Entry

```c
struct PrimaryLeafEntry {
    struct BTreeKey key;      // 28 bytes

    // Covering metadata (for index-only scans)
    u16 metadata_len;         // Length of metadata section
    u8 metadata[];            // Variable-length metadata

    // Record locator
    u32 page_num;             // Heap page number
    u16 slot_num;             // Slot within page
    u16 flags;                // Future: moved/deleted flags
};
// Base size (no metadata): 38 bytes
// With metadata (avg 16B): 54 bytes
```

**Metadata format:**

```c
struct LeafMetadata {
    u16 schema_version;       // 2 bytes
    u8 null_bitmap[];         // (num_cols + 7) / 8 bytes
    u8 covering_data[];       // Optional: small frequently-accessed columns
};
```

**Metadata benefits:**

- NULL checks without loading record: `WHERE col IS NOT NULL`
- Covering queries: `SELECT id, status FROM table` (if status is covering)
- Aggregations: `COUNT(*)` scans index only

### Secondary Index Leaf Entry

```c
struct SecondaryLeafEntry {
    struct BTreeKey sec_key;  // 28 bytes (secondary key)
    struct BTreeKey prim_key; // 28 bytes (primary key)
};
// Total: 56 bytes
```

**Why store Primary Key instead of RID?**

- Stable across updates (PK rarely changes)
- Index-only scans can return PK: `SELECT id, sec_col ORDER BY sec_col`
- Simpler update logic (update record without touching secondary indexes)

---

## 3. BTree Fan-out Analysis

### Internal Node Fan-out

- Available space: 4096 - 28 (header) = 4068 bytes
- Per entry: 2 (slot) + 32 (entry) = 34 bytes
- **Max entries: 119**
- **Conservative (with fragmentation): ~105 entries**

### Primary Index Leaf Fan-out

- Available space: 4068 bytes
- Per entry: 2 (slot) + 54 (entry with 16B metadata) = 56 bytes
- **Max entries: 72**
- **Conservative: ~62-65 entries**

### Secondary Index Leaf Fan-out

- Per entry: 2 (slot) + 58 (entry) = 60 bytes
- **Max entries: 67**
- **Conservative: ~58-62 entries**

### Tree Height for Different Dataset Sizes

**With fan-out:** Internal = 105, Leaf = 62

| Records | Tree Height | Index Pages | Disk Size |
|---------|-------------|-------------|-----------|
| 62 | 1 (root leaf) | 1 | 4 KB |
| 6,510 | 2 | ~106 | 424 KB |
| 683,550 | 3 | ~11,106 | 44 MB |
| 71,772,750 | 4 | ~1,166,106 | 4.6 GB |

**For a 1M record table:**

- Height: 3 levels
- Leaf pages: ~16,130 pages
- Internal pages: ~154 pages
- Total index pages: ~16,284 pages
- Index size: ~64 MB
- Lookups: 3 page fetches per search

**Still excellent fan-out!** The 2-byte increase in key size has minimal impact on tree structure.

---

## 4. Key Comparison Algorithm

### FNV-1a Hash Function

```c
#define FNV_OFFSET_BASIS 0x811c9dc5
#define FNV_PRIME 0x01000193

u32 fnv1a_hash(const u8 *data, u32 len) {
    u32 hash = FNV_OFFSET_BASIS;
    for (u32 i = 0; i < len; i++) {
        hash ^= data[i];
        hash *= FNV_PRIME;
    }
    return hash;
}
```

### Key Comparison with Hash Fast Path

```c
i32 compare_keys(BTreeKey *k1, BTreeKey *k2) {
    // Fast path 1: Check hash for inequality
    if (k1->key_hash != k2->key_hash) {
        // Hash mismatch → keys definitely different
        // Can't determine order from hash alone, use prefix
        goto compare_prefix;
    }

    // Fast path 2: Hash match, check length
    if (k1->key_len != k2->key_len) {
        return (k1->key_len < k2->key_len) ? -1 : 1;
    }

compare_prefix:
    // Compare inline prefix (16 bytes)
    i32 prefix_cmp = memcmp(k1->key_inline, k2->key_inline, 16);

    if (prefix_cmp != 0) {
        return prefix_cmp;  // Prefix differs
    }

    // Prefix matches
    if (k1->key_len <= 16) {
        return 0;  // Keys are identical
    }

    // Need full keys - fetch overflow
    u8 *full_k1 = fetch_full_key(k1);
    u8 *full_k2 = fetch_full_key(k2);

    i32 result = memcmp(full_k1, full_k2, k1->key_len);

    free(full_k1);
    free(full_k2);

    return result;
}
```

**Performance:**

- Small keys (≤16B): 1 memcmp (16 bytes)
- Large keys, different: 1 hash check + 1 memcmp (16 bytes)
- Large keys, same prefix: hash check + memcmp + overflow fetch

---

## 5. Record Format (Heap Storage)

### Record Header

```c
struct RecordHeader {
    u16 schema_version;       // For future schema evolution
    u16 record_len;           // Total length of main record part
    u16 num_cols;             // Number of columns (max 256)
    u8 flags;                 // FLAG_DELETED, FLAG_MOVED
    u8 _pad;
    u32 overflow_page;        // Multi-page continuation OR forward pointer
};
// Size: 12 bytes

#define FLAG_DELETED  0x01
#define FLAG_MOVED    0x02    // Record relocated, overflow_page is new location
```

### Full Record Layout

```c
struct Record {
    RecordHeader header;      // 12 bytes
    u8 null_bitmap[];         // (num_cols + 7) / 8 bytes
    u16 col_offsets[];        // num_cols × 2 bytes (u16 offsets)
    u8 data[];                // Column data
};
```

**Example for 50 columns:**

- Header: 12 bytes
- NULL bitmap: 7 bytes (⌈50/8⌉)
- Column offsets: 100 bytes (50 × 2)
- Data: ~1900 bytes available (before overflow at 2KB)

### Column Offset Table (COT)

**Offset encoding:**

- `0` → Column is NULL (check null_bitmap for clarity)
- `> 0` → Offset to column data (relative to `data[]` start)

**Use u16 offsets:**

- Max 65,535 bytes per record part
- Sufficient for most records
- If record > 64KB, use overflow pages

### Data Section Layout

```
data[]:
┌────────────────────────────────────────────────────┐
│ [Fixed Col 1: 8B] (INTEGER/REAL)                   │
│ [Fixed Col 2: 8B] (INTEGER/REAL)                   │
│ [Small Var: len(u16) | data] (TEXT/BLOB ≤ 256B)    │
│ [Small Var: len(u16) | data]                       │
│ [Large BLOB: BlobPointer (12B)] (TEXT/BLOB > 1KB)  │
└────────────────────────────────────────────────────┘
```

---

## 6. Thresholds & Limits

### Column Size Thresholds

```c
#define INLINE_SMALL_VAR  256    // Store in main record if ≤ 256B
#define BLOB_THRESHOLD    1024   // Separate BLOB pages if > 1KB
```

**Decision tree for variable-length column:**

1. If `len ≤ 256B` (INLINE_SMALL_VAR)
   - Store inline in main record: `[len(u16)][data]`

2. If `256B < len ≤ 1KB` (BLOB_THRESHOLD)
   - Store inline in main record (may trigger overflow page)
   - `[len(u16)][data]`

3. If `len > 1KB` (BLOB_THRESHOLD)
   - Store in separate BLOB pages
   - Main record stores pointer: `BlobPointer` (12 bytes)

### Record Size Limits

```c
#define MAIN_RECORD_TARGET  2048   // Try to keep main record < 2KB
#define MAX_RECORD_SIZE     65535  // Hard limit (u16 offsets)
#define MAX_COLUMNS         256    // Max columns per table
```

**Overflow strategy:**

- If main record > 2KB: move large var columns to overflow page
- Overflow page linked from `header.overflow_page`
- Allows relocation for better locality

---

## 7. BLOB Storage

### BLOB Pointer (in main record)

```c
struct BlobPointer {
    u32 total_len;            // Total BLOB size
    u32 first_page;           // First BLOB page
    u32 _reserved;            // Future: compression, encryption
};
// Size: 12 bytes
```

### BLOB Page Format

```c
struct BlobPage {
    u32 next_page;            // Next chunk, or INVALID_PAGE
    u16 chunk_len;            // Data length in this page
    u8 _pad[2];
    u8 data[PAGE_SIZE - 8];   // ~4088 bytes per page
};
```

### BLOB Read Algorithm

```c
i32 read_blob(BlobPointer *ptr, u8 *output) {
    u32 curr_page = ptr->first_page;
    u32 remaining = ptr->total_len;
    u32 offset = 0;

    while (remaining > 0 && curr_page != INVALID_PAGE) {
        PageFrame *frame = fetch_page(curr_page, LATCH_SHARED);
        BlobPage *page = (BlobPage*)frame->data;

        u32 chunk = MIN(page->chunk_len, remaining);
        memcpy(output + offset, page->data, chunk);

        offset += chunk;
        remaining -= chunk;
        curr_page = page->next_page;

        unpin_page(curr_page, false);
    }

    return offset;
}
```

---

## 8. Record Relocation for Locality

### Rationale

Allow records to be relocated on update for:

- Better heap organization (active records together)
- Reduced fragmentation
- Improved cache locality

### Forward Pointer Mechanism

**When record is moved:**

- Old slot stores tombstone: `[header with FLAG_MOVED][new_page][new_slot]`
- `header.overflow_page` = new page number
- Following bytes store new slot number

**Lookup with relocation:**

```c
Record* fetch_record(RID rid) {
    PageFrame *frame = fetch_page(rid.page_num, LATCH_SHARED);
    RecordHeader *header = get_slot(frame, rid.slot_num);

    if (header->flags & FLAG_MOVED) {
        // Follow forward pointer
        u32 new_page = header->overflow_page;
        u16 new_slot = *(u16*)(header + 1);

        unpin_page(rid.page_num, false);
        return fetch_record((RID){new_page, new_slot, 0});
    }

    return (Record*)header;
}
```

### Update with Relocation

```c
i32 update_record(RID old_rid, Record *new_record) {
    // Try in-place first
    if (can_fit_in_place(old_rid, new_record)) {
        overwrite_record(old_rid, new_record);
        return 0;
    }

    // Relocate to a better location
    RID new_rid = find_best_page(new_record->header.record_len);
    write_record(new_rid, new_record);

    // Mark old slot as moved
    RecordHeader *old = get_slot_header(old_rid);
    old->flags |= FLAG_MOVED;
    old->overflow_page = new_rid.page_num;
    *(u16*)(old + 1) = new_rid.slot_num;

    // Update primary index (RID changed)
    update_primary_index_rid(old_rid, new_rid);

    return 0;
}
```

### VACUUM Compaction

- Scan heap pages, follow forward pointers
- Update indexes to point directly to new location (remove indirection)
- Reclaim old tombstone slots

---

## 9. Key Overflow Pages (Slotted Format)

### Design Rationale

**Problem:** Dedicating a full 4KB page for a 17-byte overflow key wastes 99.6% of space.

**Solution:** Use slotted page format to pack multiple overflow keys per page (shared across all indexes).

**Space savings:**
- 17-byte key: 215 keys/page instead of 1 key/page (99.5% savings)
- 64-byte key: ~62 keys/page instead of 1 key/page (98.4% savings)

### Key Overflow Page Structure

```c
struct KeyOverflowPage {
    u16 num_slots;        // Number of keys stored in this page
    u16 free_offset;      // Offset to start of free space
    u16 frag_bytes;       // Fragmented space
    u16 fsm_class;        // Current free space class (0-2)
    u32 fsm_next;         // Next page in same FSM class
    u16 slots[];          // Offsets to key data (grows up from offset 12)
};
// Header: 12 bytes
// Each slot: 2 bytes (offset)
// Key data stored as: [u16 len][u8 data[]] (grows down from PAGE_SIZE)
```

**Page layout:**
```
┌────────────────────────────────────────┐
│ Header (12 bytes)                      │
│ Slots [0..n] (2 bytes each, grows →)  │
│                                        │
│            Free Space                  │
│                                        │
│ (← grows) [len|key][len|key]...       │
└────────────────────────────────────────┘
```

**Key data format:**
```c
struct OverflowKeyData {
    u16 len;              // Length of key remainder (not including prefix)
    u8 data[];            // Key data (beyond first 16 bytes)
};
```

### Overflow Pointer Direct Addressing

**BTreeKey now directly stores page and slot:**
```c
struct BTreeKey {
    ...
    u32 key_overflow_page;    // 32-bit page number (supports 4TB DB)
    u16 key_overflow_slot;    // 16-bit slot index within page
    ...
};

#define INVALID_OVERFLOW_PAGE 0xFFFFFFFF
#define INVALID_OVERFLOW_SLOT 0xFFFF
```

**Limits:**
- Max overflow pages: 2^32 = entire database (4TB)
- Max slots per page: 65,535 (more than enough for 4KB pages)
- Total overflow capacity: essentially unlimited

### Fetch Full Key

```c
u8* fetch_full_key(BTreeKey *key) {
    if (key->key_len <= 16) {
        // Key fits inline
        u8 *buf = malloc(key->key_len);
        memcpy(buf, key->key_inline, key->key_len);
        return buf;
    }

    // Allocate buffer for full key
    u8 *buf = malloc(key->key_len);
    memcpy(buf, key->key_inline, 16);  // Copy prefix

    // Fetch overflow key data using direct page and slot
    PageFrame *frame = fetch_page(key->key_overflow_page, LATCH_SHARED);
    KeyOverflowPage *page = (KeyOverflowPage*)frame->data;

    // Get key data from slot
    u16 key_offset = page->slots[key->key_overflow_slot];
    OverflowKeyData *key_data = (OverflowKeyData*)((u8*)page + key_offset);

    // Copy overflow portion
    memcpy(buf + 16, key_data->data, key_data->len);

    unpin_page(key->key_overflow_page, false);

    return buf;
}
```

### Allocate Overflow Key

```c
void alloc_overflow_key(BufferPool *pool, const u8 *key_remainder, u16 len,
                        u32 *out_page, u16 *out_slot) {
    // Find overflow page with enough space, or allocate new one
    u32 overflow_page = find_overflow_page_with_space(pool, len + 2);

    if (overflow_page == INVALID_PAGE) {
        // Allocate new overflow page
        overflow_page = allocator_alloc_page(pool->allocator, INVALID_PAGE);
        init_overflow_page(overflow_page);
    }

    PageFrame *frame = fetch_page(overflow_page, LATCH_EXCLUSIVE);
    KeyOverflowPage *page = (KeyOverflowPage*)frame->data;

    // Check if defrag needed
    u32 space_needed = 2 + len + 2;  // slot + length + data
    if (PAGE_SIZE - page->free_offset - page->num_slots * 2 < space_needed) {
        if (page->frag_bytes >= space_needed) {
            defrag_overflow_page(page);
        }
    }

    // Allocate slot
    u16 slot_idx = page->num_slots++;

    // Allocate space for key data
    u16 data_size = sizeof(u16) + len;
    page->free_offset -= data_size;
    page->slots[slot_idx] = page->free_offset;

    // Write key data
    OverflowKeyData *key_data = (OverflowKeyData*)((u8*)page + page->free_offset);
    key_data->len = len;
    memcpy(key_data->data, key_remainder, len);

    unpin_page(overflow_page, true);  // Mark dirty

    // Return page and slot directly
    *out_page = overflow_page;
    *out_slot = slot_idx;
}
```

**Usage:**
```c
BTreeKey key;
key.key_len = full_key_len;
memcpy(key.key_inline, full_key, MIN(16, full_key_len));
key.key_hash = fnv1a_hash(full_key, full_key_len);

if (full_key_len > 16) {
    alloc_overflow_key(pool, full_key + 16, full_key_len - 16,
                       &key.key_overflow_page, &key.key_overflow_slot);
} else {
    key.key_overflow_page = INVALID_OVERFLOW_PAGE;
    key.key_overflow_slot = INVALID_OVERFLOW_SLOT;
}
```

### Free Overflow Key

```c
void free_overflow_key(BufferPool *pool, BTreeKey *key) {
    if (key->key_overflow_page == INVALID_OVERFLOW_PAGE) {
        return;  // No overflow to free
    }

    PageFrame *frame = fetch_page(key->key_overflow_page, LATCH_EXCLUSIVE);
    KeyOverflowPage *page = (KeyOverflowPage*)frame->data;

    // Get key size
    u16 key_offset = page->slots[key->key_overflow_slot];
    OverflowKeyData *key_data = (OverflowKeyData*)((u8*)page + key_offset);
    u16 data_size = sizeof(u16) + key_data->len;

    // Mark slot as free (offset = 0)
    page->slots[key->key_overflow_slot] = 0;
    page->frag_bytes += data_size;

    // Update overflow FSM
    update_overflow_fsm(key->key_overflow_page, calculate_free_space(page));

    unpin_page(key->key_overflow_page, true);
}
```

### Defragmentation

```c
void defrag_overflow_page(KeyOverflowPage *page) {
    u8 tmp[PAGE_SIZE];
    memcpy(tmp, page, PAGE_SIZE);
    KeyOverflowPage *src = (KeyOverflowPage*)tmp;

    page->free_offset = PAGE_SIZE;
    page->frag_bytes = 0;

    // Repack all non-freed slots
    u16 new_slot = 0;
    for (u16 i = 0; i < src->num_slots; i++) {
        if (src->slots[i] == 0) {
            continue;  // Skip freed slot
        }

        OverflowKeyData *key_data = (OverflowKeyData*)((u8*)src + src->slots[i]);
        u16 data_size = sizeof(u16) + key_data->len;

        page->free_offset -= data_size;
        memcpy((u8*)page + page->free_offset, key_data, data_size);
        page->slots[new_slot++] = page->free_offset;
    }

    page->num_slots = new_slot;
}
```

### Overflow Free Space Map (FSM)

**Managed via linked lists in System Catalog:**

The system catalog (page 131) stores three FSM class heads in `overflow_fsm[3]`:
- **Class 0:** Pages with 0-500 bytes free
- **Class 1:** Pages with 500-1500 bytes free
- **Class 2:** Pages with 1500+ bytes free (prefer for allocation)

Each `KeyOverflowPage` includes:
- `fsm_class`: Current free space class (0-2)
- `fsm_next`: Next page in same class (linked list)

When free space changes (after alloc/free), update the class membership by:
1. Remove page from old class list
2. Insert page into new class list

**Find page with space:**
```c
u32 find_overflow_page_with_space(BufferPool *pool, u16 needed) {
    SystemCatalog *catalog = get_system_catalog();

    // Try class 2 (most space) first
    for (i32 cls = 2; cls >= 0; cls--) {
        u32 page = catalog->overflow_fsm[cls];
        while (page != INVALID_PAGE) {
            PageFrame *frame = fetch_page(page, LATCH_SHARED);
            KeyOverflowPage *kpage = (KeyOverflowPage*)frame->data;

            u16 free = calculate_free_space(kpage);
            if (free >= needed) {
                unpin_page(page, false);
                return page;
            }

            page = kpage->fsm_next;
            unpin_page(page, false);
        }
    }

    return INVALID_PAGE;  // No space, allocate new page
}
```

---

## 10. System Catalog

### System Catalog Page (Page 131)

```c
struct SystemCatalog {
    u32 schema_btree_root;    // Root of schema metadata BTree
    u32 next_table_id;        // Auto-increment for table IDs
    u32 heap_fsm_root;        // Free space map for heap pages

    // Overflow key management
    u32 overflow_fsm[3];      // Heads of FSM classes for key overflow pages
                              // [0]: 0-500B free
                              // [1]: 500-1500B free
                              // [2]: 1500+B free

    u32 _reserved[9];
};
```

### Schema BTree

**Key:** Table name (TEXT)
**Value:** StaticSchema

### Static Schema Structure

```c
struct StaticSchema {
    u8 type;                  // S_TABLE or S_INDEX
    u32 table_id;             // Unique table ID
    u16 num_cols;             // Number of columns
    u16 schema_version;       // For schema evolution

    u32 primary_idx_root;     // Root page of primary index BTree
    u32 heap_head_page;       // First heap page for this table

    char name[MAX_NAME];      // Table name
    ColumnDef cols[MAX_COLUMN]; // Column definitions

    // Future: covering index configuration
    u8 num_covering;          // Number of covering columns
    u8 covering_col_idx[];    // Indexes of columns to include in leaf metadata
};
```

### Column Definition

```c
struct ColumnDef {
    char name[MAX_NAME];      // Column name (64 bytes)
    u8 type;                  // TYPE_INTEGER, TYPE_REAL, TYPE_TEXT, TYPE_BLOB
    u8 key_type;              // KEY_PRIM, KEY_UNIQ, KEY_NONE
    u8 size_hint;             // For fixed types: actual size; for var: max size
    u8 _pad;
};
```

---

## 11. Record Lifecycle

### Insert

```
1. Encode record:
   - Build header, null_bitmap, COT
   - For each column:
     - Fixed/small var (≤256B): inline in data[]
     - Medium var (256B-1KB): inline in data[]
     - Large BLOB (>1KB): allocate BLOB pages, store BlobPointer
   - If record > 2KB: allocate overflow page

2. Allocate heap page slot:
   - Find page with free space (use heap FSM)
   - Write record to slot
   - Get RID (page + slot)

3. Insert into primary index:
   - Build BTreeKey (key_inline, key_overflow, key_hash)
   - Build LeafMetadata (schema_version, null_bitmap, covering_data)
   - Insert (key, metadata, RID) into primary BTree

4. Insert into secondary indexes:
   - For each secondary index:
     - Build secondary key
     - Insert (sec_key, prim_key) into secondary BTree
```

### Search

```
1. Lookup in primary index:
   - BTree search: key → (metadata, RID)
   - Check metadata: if covering query, return without fetching record

2. If record fetch needed:
   - Fetch heap page: RID.page_num
   - Get slot: RID.slot_num
   - If FLAG_MOVED: follow forward pointer
   - Decode record
```

### Update

```
1. Locate record:
   - Lookup primary index: key → metadata + RID
   - Fetch record from heap

2. Check what changed:
   - Primary key changed? → Delete + Insert (expensive)
   - Secondary key changed? → Update secondary index
   - Non-key column? → Update in-place if possible

3. Update record:
   - If new record fits in same slot: overwrite
   - Else: relocate to better page (new RID)
     - Mark old slot as FLAG_MOVED
     - Write forward pointer

4. Update indexes:
   - Update primary index metadata if covering columns changed
   - If RID changed: update primary index RID
   - Update secondary indexes if secondary keys changed
```

### Delete

```
1. Lookup primary index: key → RID
2. Mark record header: FLAG_DELETED
3. Delete from all indexes (primary + secondary)
4. Heap slot reclaimed during VACUUM
```

---

## 12. Future: Covering Column Auto-Detection

### Strategy (Not Immediate)

**Auto-include in leaf metadata:**

1. Small KEY_UNIQ columns (size ≤ 8 bytes)
2. Fixed-size columns ≤ 8 bytes that are:
   - Frequently filtered: `WHERE status = 1`
   - Frequently projected: `SELECT id, status`
   - Frequently aggregated: `COUNT(*) WHERE active = true`

**Implementation:**

- Track query patterns (statistics)
- Promote columns to covering based on access frequency
- Rebuild index to include new covering columns

---

## 13. Schema Evolution

### Approach 1: Rebuild (Initial Implementation)

- On `ALTER TABLE ADD COLUMN`: create new table, copy records, drop old
- Simple but slow for large tables

### Approach 2: Versioning (Future)

**Record header includes schema_version:**

```c
struct RecordHeader {
    u16 schema_version;       // Schema version when record was written
    ...
};
```

**Read path checks version:**

- If `record.schema_version < current_schema_version`: adapt on read
- New columns return NULL for old records
- Lazy migration on update

**Schema metadata stores version history:**

```c
struct StaticSchema {
    u16 schema_version;       // Current version
    u16 num_versions;         // Number of historical versions
    SchemaVersion versions[]; // History of schema changes
};

struct SchemaVersion {
    u16 version;
    u16 num_cols;
    ColumnDef cols[];
};
```

---

## Summary

### Key Design Choices

✅ **Heap + unclustered indexes** for flexibility
✅ **Primary index stores metadata** for index-only scans
✅ **Secondary indexes store PK** for stability
✅ **16-byte inline keys** (UUID) + overflow for larger keys
✅ **FNV-1a hash** for fast inequality checks (especially composite keys)
✅ **1KB BLOB threshold** for separate storage
✅ **2KB main record target** before overflow
✅ **256 column limit** (u8 for num_cols)
✅ **Record relocation** allowed for better locality

### Fan-out Summary

- **Internal nodes:** ~105 entries per page
- **Primary leaf:** ~62-65 entries per page (with 16B metadata)
- **Secondary leaf:** ~58-62 entries per page
- **Tree height:** 3-4 levels for 1M-70M records

### Next Steps

With schema/record design locked in, the next discussion:

**BTree Implementation Details**

1. Latch coupling for concurrent access
2. Node split/merge algorithms
3. Bulk loading for initial table creation
4. Prefix compression (future optimization)
