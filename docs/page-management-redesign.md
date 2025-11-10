# Page Management System Design

## Overview

This document describes the completed layered page management system for simple-db, providing proper buffer pool management, lock-free page allocation, and concurrency support.

## Core Specifications

- **Buffer Pool:** 32,768 frames (128MB cache)
- **Initial Size:** 1 group = 65,536 pages = 256MB
- **Growth:** Add 1 group (256MB) at a time via DCLI pattern
- **Metadata Region:** 66 pages (1 superblock + 64 GDT pages + 1 catalog)
- **Page Size:** 4KB
- **Max Database Size:** 4TB (16,384 groups)
- **Group Structure:** Bitmap-based allocation with atomic operations

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  Upper Layers (BTree, Schema, etc)                      │
├─────────────────────────────────────────────────────────┤
│  Layer 3: Page Allocator                                │  ← Lock-free allocation, metadata
├─────────────────────────────────────────────────────────┤
│  Layer 2: Buffer Pool Manager                           │  ← QDLP eviction, caching, latching
├─────────────────────────────────────────────────────────┤
│  Layer 1: Page Store (File I/O)                         │  ← Raw page read/write, file growth
└─────────────────────────────────────────────────────────┘
```

**Implementation Status:**
- ✅ Layer 1: Page Store - Complete
- ✅ Layer 2: Buffer Pool - Complete
- ✅ Layer 3: Page Allocator - Complete

---

## On-Disk Layout

```
Page 0:         Superblock (metadata + CRC-32C checksums)
Pages 1-64:     Group Descriptor Table (GDT) - 64 pages (static)
                - 64 pages × 256 descriptors/page = 16,384 groups max
                - 16,384 groups × 256MB = 4TB max database size
Page 65:        CATALOG_PAGE (reserved for schema system)
Page 66:        Group 0 starts (HEAD_OFFSET)
  Pages 66-67:     Bitmap pages (2 pages cover 64K pages)
  Pages 68-65,601: Data pages in Group 0
Page 65,602:    Group 1 starts (if grown)
  Pages 65,602-65,603: Bitmap pages
  Pages 65,604+:       Data pages
```

### Key Constants

```c
#define HEAD_OFFSET 66              // Metadata region size
#define INITIAL_PAGES 65602         // 66 + 65,536 (~256MB)
#define GROUP_SIZE 65536            // 64K pages per group
#define GROUP_BITMAPS 2             // 2 bitmap pages per group
#define GDT_PAGES 64                // Static: 64 GDT pages
#define GDT_DESCRIPTORS 256         // 256 descriptors per page
#define MAX_GROUPS 16384            // 4TB max capacity
```

### Layout Calculations

- Group size: 65,536 pages = 256MB
- Bitmap for 64K pages: 65,536 bits = 8KB = 2 pages
- Group 0: pages 66-65,601 (bitmap at 66-67, data at 68+)
- Group N: starts at `HEAD_OFFSET + N × GROUP_SIZE`
- First data page: 68 (after superblock, GDT, catalog, and bitmap)

---

## Layer 1: Page Store

### Responsibilities

- Abstract file I/O operations (file-backed and in-memory modes)
- Handle file growth via extent allocation
- Thread-safe page reads/writes using `pread()`/`pwrite()`
- Direct I/O for metadata (bypasses buffer pool)

### Data Structure

```c
struct PageStore {
    i32 fd;                      // File descriptor (-1 for in-memory)
    u64 store_size;              // Current file size in bytes
    void *mmap_addr;             // For in-memory mode (NULL for file mode)
    pthread_mutex_t grow_lock;   // Serialize file growth
};
```

### Interface

```c
PageStore* pstore_create(const char *path, u32 num_pages);
PageStore* pstore_open(const char *path, u32 *out_num_pages);
i32 pstore_read(PageStore *ps, u32 page_num, void *buf);
i32 pstore_write(PageStore *ps, u32 page_num, const void *buf);
i32 pstore_grow(PageStore *ps, u32 num_pages);
i32 pstore_sync(PageStore *ps);
void pstore_close(PageStore *ps);
```

### Key Design Features

- **Thread-safe I/O:** Uses `pread()`/`pwrite()` (no seek operations)
- **Dual mode support:** File-backed or in-memory (for testing)
- **Growth serialization:** `grow_lock` mutex prevents concurrent file extensions
- **Explicit capacity:** `num_pages` parameter avoids magic numbers
- **Crash-safe growth:** File extended atomically via `ftruncate()`

---

## Layer 2: Buffer Pool Manager

### Responsibilities

- Cache hot pages in memory (32,768 frames = 128MB)
- QDLP eviction algorithm (Quick Demotion, Lazy Promotion)
- Pin/unpin semantics to prevent eviction of in-use pages
- TLB (Translation Lookaside Buffer) for fast page→frame lookup
- Dirty page tracking and write-back on eviction/flush
- Handle-based API with epoch validation

### Data Structures

```c
struct FrameData {
    atomic_bool loading;         // Page being loaded from disk
    RWSXLock latch;             // Page content latch (S/X/SX modes)
    u8 data[PAGE_SIZE];         // Page data (4KB)
};

struct PageFrame {
    atomic_u32 epoch;           // Eviction counter (for handle validation)
    atomic_u32 pin_cnt;         // Pin count (0 = evictable)
    atomic_u8 qtype;            // Queue type: QUEUE_QD or QUEUE_MAIN
    atomic_bool is_dirty;       // Dirty flag
    atomic_bool visited;        // Second-chance flag
    atomic_u32 page_num;        // Page number (INVALID_PAGE = empty)
    struct FrameData fdata;     // Page data + latch
};

struct FrameHandle {
    struct FrameData *fdata;    // Direct pointer to frame data
    u32 epoch;                  // Epoch snapshot for validation
    u32 frame_idx;              // Frame index
    u32 page_num;               // Page number
};

struct BufPool {
    // QDLP eviction queues (lock-free MPSC)
    struct CQ qd;               // Quick Demotion queue (probation)
    struct CQ main;             // Main queue (proven hot pages)
    struct CQ ghost;            // Ghost queue (evicted page tracking)

    RWSXLock latch;             // Pool-level latch (SX for cold load)
    atomic_u32 warmup_cursor;   // Sequential warmup allocation

    PageStore *store;           // Shared page store
    PageFrame *frames;          // Array[POOL_SIZE] of frames
    SHTable *index;             // page_num → frame_idx (hash table)
    SHTable *gindex;            // page_num → ghost presence
};
```

### Interface

```c
// Pool management
BufPool* bpool_init(PageStore *store);
void bpool_destroy(BufPool *bp);

// Page access (returns handle)
FrameHandle* bpool_fetch_page(BufPool *bp, u32 page_num);
i32 bpool_mark_write(BufPool *bp, FrameHandle *h);
i32 bpool_release_handle(BufPool *bp, FrameHandle *h);

// Flush operations
i32 bpool_flush_page(BufPool *bp, u32 page_num);
i32 bpool_flush_all(BufPool *bp);
```

### Key Design Features

#### QDLP Eviction Algorithm

Better than traditional CLOCK algorithm:

1. **Scan resistance:** New pages start in QD queue, evicted quickly if not re-accessed
2. **Ghost tracking:** Detects temporal locality of evicted pages
3. **Adaptive promotion:** Pages prove reuse before moving to MAIN
4. **Lock-free queues:** MPSC queues use atomic operations

**Queue Sizes:**
- `QD_SIZE = POOL_SIZE / 8 = 4,096` (12.5% of pool)
- `MAIN_SIZE = POOL_SIZE = 32,768` (100%, allows overflow from QD)
- `GHOST_SIZE = POOL_SIZE = 32,768` (track recently evicted)

#### Handle-Based API

- `FrameHandle` encapsulates frame access with epoch validation
- Prevents use-after-eviction bugs
- User must manually latch frame via `h->fdata->latch` for concurrent access
- Handles remain valid as long as pinned

#### Concurrency Model

**Two-level latching:**
1. **Buffer pool latch (RWSXLock):** Protects TLB and eviction
   - S-latch: Hot path TLB lookups (concurrent)
   - SX-latch: Cold loads (allows concurrent hot path)
   - X-latch: Never used (SX sufficient)

2. **Per-frame latch (RWSXLock):** Protects page data
   - S-latch: Multiple readers
   - X-latch: Exclusive writer
   - SX-latch: Structural modifications with concurrent readers

**Latch ordering:** Always acquire buffer pool latch before frame latch

#### Hot vs Cold Path

**Hot path (page in pool):**
1. Acquire pool S-latch
2. TLB lookup (hash table)
3. Pin frame (atomic increment)
4. Verify page_num and epoch
5. Release pool S-latch
6. Spin-wait if loading

**Cold path (page not in pool):**
1. Acquire pool SX-latch (allows concurrent hot path!)
2. Double-check TLB (DCLI pattern)
3. Find victim using QDLP
4. Flush victim if dirty
5. Insert page into TLB
6. Release SX-latch
7. Perform I/O (outside latch!)
8. Mark loading complete

#### Race Condition Prevention

1. **Duplicate cold load:** Double-check TLB under SX-latch
2. **Torn writes during flush:** Acquire frame S-latch before reading
3. **Eviction during access:** Pin count protects frames from eviction
4. **Premature access:** `loading` flag makes threads spin-wait

---

## Layer 3: Page Allocator

### Responsibilities

- Track free/allocated pages via bitmaps
- Lock-free allocation using atomic CAS operations
- Round-robin allocation with locality hints
- Database growth using DCLI pattern with SX-latch
- Metadata persistence with CRC-32C checksums

### Data Structures

```c
struct SuperBlock {
    u32 magic;                    // 0x53494D44 ("SIMD")
    u32 version;                  // 1
    u32 page_size;                // 4096
    atomic_u32 total_pages;       // Current file size in pages
    atomic_u32 total_groups;      // Number of groups allocated
    u32 gdt_start;                // Page 1
    u32 gdt_pages;                // 64
    u32 catalog_page;             // Page 65
    u32 gdt_checksum[GDT_PAGES];  // CRC-32C for each GDT page
    u32 sb_checksum;              // CRC-32C of superblock itself
    u32 catalog_checksum;         // CRC-32C of catalog page
    u8 _pad[...];                 // Pad to PAGE_SIZE
};

struct GroupDesc {
    u32 start;                    // Start page of this group
    atomic_u16 free_pages;        // Free pages in this group (atomic!)
    atomic_u16 last_set;          // Hint: last allocated page index
    u8 _pad[8];                   // Reserved
};

struct GDTPage {
    GroupDesc descriptors[256];   // 256 descriptors per page
};

struct BitmapPage {
    atomic_u64 bitmap[512];       // 512 × 64 bits = 32,768 bits
};

struct PageAllocator {
    SuperBlock *sb_cache;         // Cached superblock
    GDTPage *gdt_cache;           // Cached GDT (64 pages)
    BufPool *pool;                // Buffer pool for bitmap pages
    PageStore *store;             // Page store (for direct metadata I/O)
    atomic_u32 last_group;        // Hint: last group allocated from
    RWSXLock latch;               // For DCLI growth pattern
};
```

### Interface

```c
PageAllocator* allocator_init(BufPool *pool, bool create);
void allocator_destroy(PageAllocator *pa);
u32 alloc_page(PageAllocator *pa, u32 hint);
void free_page(PageAllocator *pa, u32 page_num);
```

### Key Design Features

#### Lock-Free Bitmap Allocation

**Allocation uses `fetch_or` (atomic):**
- Fetch bitmap pages with **S-latch** (not X-latch!)
- Search for zero bit
- Use `atomic_fetch_or` to set bit atomically
- Check return value to see if we won the race
- Multiple allocators can search same bitmap concurrently

**Deallocation uses `fetch_and` (atomic):**
- Fetch bitmap page with S-latch
- Use `atomic_fetch_and` to clear bit
- No CAS loop needed (idempotent operation)
- Single atomic instruction

**Why S-latch instead of X-latch?**
- Atomic operations don't require exclusive access
- Multiple threads can search bitmaps simultaneously
- 10-100× throughput improvement under contention
- Synchronization happens at CPU instruction level

#### DCLI Growth Pattern

**Double-Checked Locking with Initialization:**

1. **First check (no latch):** Read `total_pages` to see if growth needed
2. **Acquire SX-latch:** Allows concurrent allocations in existing groups
3. **Double-check:** Verify growth still needed under latch
4. **Grow file:** Extend PageStore (slow I/O under SX-latch)
5. **Initialize new group:** Create bitmap pages, flush them
6. **Update metadata:** Increment `total_pages` and `total_groups`
7. **Persist metadata:** Write SuperBlock and GDT via direct I/O
8. **Release SX-latch**

**Key insight:** File I/O (~100ms) happens under SX-latch, which allows concurrent allocations from existing groups. Only metadata updates block other operations.

**Race prevention:**
- GDT descriptor initialized **before** incrementing `total_groups`
- Uninitialized descriptors have `start = INVALID_PAGE`
- Prevents allocators from accessing half-initialized groups

#### Metadata Management

**Bypass buffer pool:**
- SuperBlock and GDT use direct `pstore_read()`/`pstore_write()`
- Avoids double-caching (once in cache, once in buffer pool)
- Gives allocator direct control over metadata durability

**CRC-32C checksums:**
- SuperBlock checksum covers all fields except checksum itself
- Each GDT page has its own checksum
- Catalog page checksum (for future schema system)
- Validated on database open
- Detects corruption from crashes or bit rot

**Flush strategy:**
- Bitmap pages: Flushed only during group initialization
- Runtime: Bitmap pages written on eviction or pool destroy
- Metadata: Written on growth and allocator destroy
- Crash-safe ordering: Bitmaps flushed before metadata updates

#### Round-Robin Allocation

- Each `GroupDesc` tracks `last_set` hint
- Allocation starts from `last_set` and wraps around
- Spreads allocations across bitmap words
- Reduces contention on same u64 slots
- Improves cache locality

#### Concurrency Summary

| Operation | Synchronization | Contention | Performance |
|-----------|----------------|------------|-------------|
| Read free_pages | Atomic load | None | Lock-free |
| Alloc bitmap bit | S-latch + atomic fetch_or | **Very Low** | 10-100× faster than X-latch |
| Free bitmap bit | S-latch + atomic fetch_and | **None** | Single instruction |
| Update free_pages | Atomic inc/dec | None | Lock-free |
| GDT update | Direct I/O (bypasses pool) | Low | 64 GDT pages striped |
| Growth | SX-latch | Low | Concurrent with existing allocations |

---

## Concurrency Primitives

### RWSXLock (SX-Latch)

From InnoDB MySQL 8.0, supports three modes:

**Compatibility Matrix:**
```
        S    X    SX
S       ✓    ✗    ✓     ← Key: S and SX are compatible!
X       ✗    ✗    ✗
SX      ✓    ✗    ✗
```

**Usage:**
- **S (Shared):** Multiple readers, compatible with SX
- **X (Exclusive):** Single writer, no other locks
- **SX (Shared-Exclusive):** Single structural modifier + multiple readers

**DCLI Pattern for Upgrade Starvation Prevention:**

The `upgrading` atomic flag prevents new S-latch acquisitions during SX→X upgrade:
- S-latch acquisition checks `upgrading` flag and waits if true
- Upgrade sets flag first, then waits for readers to drain
- Prevents indefinite starvation of upgrader

**API:**
```c
void rwsx_lock(RWSXLock *lock, LatchMode mode);
void rwsx_unlock(RWSXLock *lock, LatchMode mode);
i32 rwsx_upgrade_sx(RWSXLock *lock);    // Returns -1 if wrong thread
i32 rwsx_downgrade_sx(RWSXLock *lock);  // Returns -1 if not holding X
```

### Lock-Free Data Structures

#### Circular Queue (MPSC)

**Multi-Producer Single-Consumer queue:**
- Used for QDLP eviction queues
- Atomic operations for concurrent `cq_put()`
- Single consumer (eviction) calls `cq_pop()`
- Bounded size with overflow handling

#### Hash Table (SHTable)

**Single-level hash table:**
- Used for TLB (page_num → frame_idx)
- Linear probing for collision resolution
- Not resizable (fixed size on creation)
- Fast O(1) lookups with low overhead

---

## Memory Ordering

**Atomic operations use explicit memory ordering:**

```c
RELAXED   // No ordering guarantees
ACQUIRE   // Synchronizes with RELEASE, makes prior writes visible
RELEASE   // Synchronizes with ACQUIRE, makes current writes visible
ACQ_REL   // Both ACQUIRE and RELEASE
SEQ_CST   // Sequentially consistent (strongest)
```

**Key patterns:**
- **Bitmap allocation:** `ACQUIRE` on load, `RELEASE` on modify
- **Metadata updates:** `ACQ_REL` for atomic fetch-and-add
- **Visibility:** Ensures changes visible across threads on weak memory models

---

## Crash Consistency

### Database Creation

1. Initialize SuperBlock with magic, version, page size
2. Initialize all 64 GDT pages (uninitialized groups have `start = INVALID_PAGE`)
3. Initialize Group 0 descriptor in GDT
4. Initialize Group 0 bitmap pages (mark bitmap pages 0-1 as allocated)
5. **Flush bitmap pages** (durability)
6. Compute and store CRC-32C checksums
7. **Flush SuperBlock and GDT** (durability)

**Ordering ensures:** Bitmaps hit disk before metadata, preventing inconsistency.

### Database Open

1. Read SuperBlock and all GDT pages (direct I/O)
2. Validate magic and version
3. Verify file size matches `total_pages`
4. **Validate all CRC-32C checksums**
5. Return error if any validation fails

### Database Growth

1. Acquire SX-latch
2. Double-check growth needed
3. Extend file via `pstore_grow()`
4. Initialize new group's bitmap pages
5. **Flush bitmap pages** (durability)
6. Update GDT descriptor
7. Increment `total_pages` and `total_groups` atomically
8. Recompute checksums
9. **Flush SuperBlock and GDT** (durability)
10. Release SX-latch

**Ordering ensures:** New group fully initialized before visible to allocators.

### Crash Recovery

**If crash occurs:**
- **Before bitmap flush:** New allocations lost (volatile memory)
- **After bitmap flush, before metadata:** New group not visible (safe)
- **After metadata flush:** New group fully visible and consistent

**Checksums detect:**
- Torn writes to metadata pages
- Silent corruption (bit rot)
- Incomplete transactions (future WAL integration)

---

## Performance Characteristics

### Buffer Pool

**Hot path (page in pool):**
- TLB lookup: ~100-200ns (hash table + atomic pin)
- S-latch on pool: High concurrency (unlimited concurrent lookups)
- Frame latch: User-controlled (acquire only when needed)

**Cold path (page not in pool):**
- SX-latch on pool: Blocks other cold loads, allows hot path
- Victim selection: ~1-10μs (QDLP queue scans)
- I/O: ~10-50μs (page read from disk)
- Total: ~15-70μs

**Scalability:**
- Hot path: Linear scalability with thread count
- Cold path: Limited by SX-latch (1 cold load at a time)
- Mixed workload: Hot path dominates, excellent concurrency

### Page Allocator

**Allocation:**
- Fast path (group has free pages): ~1-5μs
  - TLB lookup for bitmap page
  - Bitmap scan with atomic CAS retry
  - Metadata update
- Slow path (growth): ~100ms
  - File extension (I/O bound)
  - Bitmap initialization
  - Metadata persistence

**Concurrency:**
- Multiple allocators: Lock-free bitmap operations
- Growth: SX-latch allows concurrent allocations in existing groups
- Natural segmentation: 128 bitmap pages (2 per group × 64 groups)

---

## Testing Strategy

### Unit Tests

**Layer 1 (PageStore):**
- File creation, read/write, growth
- In-memory mode
- Concurrent read/write (pread/pwrite thread-safety)

**Layer 2 (Buffer Pool):**
- Pin/unpin semantics
- QDLP eviction (fill pool, verify behavior)
- Concurrent fetch (multi-threaded)
- Dirty page write-back
- Handle epoch validation

**Layer 3 (Allocator):**
- Single-threaded alloc/free
- Group exhaustion and growth
- Concurrent allocation (no duplicates)
- Persist and reopen
- Checksum validation (including corruption detection)
- Stress tests (5000+ allocations)

### Integration Tests

**Mixed workload:**
- Allocate pages, write data via buffer pool, read back
- Concurrent allocations with concurrent buffer pool access
- Growth while buffer pool is active
- Persist, reopen, verify data integrity

**Crash simulation:**
- Kill process during growth
- Verify database opens correctly
- Verify checksums detect corruption

---

## Future Enhancements

### Write-Ahead Log (WAL)

- SuperBlock tracks `checkpoint_lsn`
- Page LSN in each page header (for ARIES recovery)
- WAL writes bypass buffer pool (direct page_store)
- Crash recovery via log replay

### Transaction Support

- Allocator tracks tentative allocations
- Commit: make allocations permanent
- Abort: rollback bitmap changes
- MVCC for lock-free reads

### Sharded Buffer Pool

- Hash-based sharding (page_num → shard_id)
- Multiple independent buffer pools
- Reduced latch contention
- Linear scalability with thread count

### Adaptive Allocation

- Track allocation patterns per table/index
- Group-level affinity for sequential scans
- Defragmentation and page compaction

---

## Implementation Files

```
src/
  ├── pagestore.c/h      (Layer 1: File I/O)
  ├── bufpool.c/h        (Layer 2: Buffer pool + QDLP)
  ├── alloc.c/h          (Layer 3: Page allocator)
  ├── rwsxlock.c/h       (SX-latch implementation)
  ├── cqueue.c/h         (Lock-free MPSC queue)
  ├── shtable.c/h        (Single-level hash table)
  └── utils.c/h          (Atomics, CRC-32C, helpers)

tests/
  ├── test_pagestore.c   (Layer 1 tests)
  ├── test_bufpool.c     (Layer 2 tests)
  ├── test_alloc.c       (Layer 3 tests)
  └── test_rwsxlock.c    (Concurrency primitive tests)
```

---

## Summary

The page management system provides a robust foundation for simple-db with:

✅ **Three-layer architecture:** Clean separation of concerns
✅ **QDLP eviction:** Scan-resistant, adaptive caching
✅ **Lock-free allocation:** High-throughput bitmap operations
✅ **DCLI growth:** Non-blocking database expansion
✅ **SX-latches:** Concurrent readers during structural modifications
✅ **Crash consistency:** Ordered flushes and CRC-32C checksums
✅ **Handle-based API:** Safe concurrent access with epoch validation
✅ **Comprehensive testing:** Unit tests for all layers and concurrency scenarios

The system is production-ready for building upper layers (BTree, Schema, Transactions).
