# Page Management Redesign Plan

## Overview

This document outlines the redesign of the page management system for simple-db, moving from a monolithic GDT-based approach to a layered architecture with proper buffer pool management and concurrency support.

## Core Specifications

- **Buffer Pool:** 32,768 frames per shard (128MB cache per shard)
- **Sharding:** Configurable number of shards (default: 8 shards = 1GB total)
- **Initial Size:** 1 group = 64K pages = 256MB
- **Growth:** Add 1 group (256MB) at a time
- **Metadata Region:** 64 pages (supports 4TB max database size)
- **Page Size:** 4KB
- **Group Structure:** Reuse existing group-based design

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  Upper Layers (BTree, Schema, etc)                      │
├─────────────────────────────────────────────────────────┤
│  Layer 4: Sharded Buffer Pool (Optional)                │  ← Hash-based sharding
│           - Distributes pages across N shards           │
│           - Reduces latch contention                    │
│           - Linear scalability                          │
├─────────────────────────────────────────────────────────┤
│  Layer 3: Page Allocator                                │  ← Allocation policy, free space
├─────────────────────────────────────────────────────────┤
│  Layer 2: Buffer Pool Manager (per shard)               │  ← Caching, eviction, pin/unpin
│           - QDLP eviction (QD + MAIN + GHOST queues)    │
│           - Lock-free MPSC queues                       │
│           - SX-latch for concurrent cold loads          │
├─────────────────────────────────────────────────────────┤
│  Layer 1: Page Store (File I/O, shared across shards)   │  ← Raw page read/write, file growth
└─────────────────────────────────────────────────────────┘
```

---

## On-Disk Layout

```
Page 0:        Superblock
Pages 1-64:    Group Descriptor Table (GDT)
                - 64 pages × 256 descriptors/page = 16,384 groups max
                - 16,384 groups × 256MB = 4TB max database size
Pages 65-128:  Reserved for future metadata
Page 129+:     Group 0 starts here
  Pages 129-130:   Bitmap for group 0 (2 pages cover 64K pages)
  Pages 131-64,706: Data pages in group 0
Pages 64,707+: Group 1 starts here (if grown)
  ...
```

### Layout Calculations

- Group size: 64K pages
- Bitmap for 64K pages: 64K bits = 8KB = 2 pages
- Group 0: pages 129-64,706 (bitmap at 129-130, data at 131+)
- Group N: starts at (129 + N × 64K)

---

## Layer 1: Page Store

### Responsibilities

- Abstract file I/O operations
- Handle file growth (extent allocation)
- Direct page read/write bypassing buffer pool when needed (for WAL, etc.)

### Structure

```c
struct PageStore {
    i32 fd;                 // File descriptor (-1 for in-memory)
    u64 file_size;          // Current file size in bytes
    void *mmap_addr;        // For in-memory mode (NULL for file mode)
    pthread_mutex_t grow_lock; // Serialize file growth
};
```

### Interface

```c
PageStore* pstore_create(const char *path);
PageStore* pstore_open(const char *path);
i32 pstore_read(PageStore *ps, u32 page_num, void *buf);
i32 pstore_write(PageStore *ps, u32 page_num, const void *buf);
i32 pstore_sync(PageStore *ps);
i32 pstore_grow(PageStore *ps, u32 num_pages);
void pstore_close(PageStore *ps);
```

### Key Design Notes

- Use `pread()/pwrite()` for thread-safety (no seek)
- Direct I/O bypass for WAL (future)
- Grow file in extents to reduce syscalls
- Support both file-backed and in-memory for testing

---

## Layer 2: Buffer Pool Manager

### Responsibilities

- Cache hot pages in memory
- Evict cold pages (QDLP algorithm: Quick Demotion, Lazy Promotion)
- Pin/unpin semantics to prevent eviction of in-use pages
- Dirty tracking and write-back
- Thread-safe access via page latches
- Ghost tracking for temporal locality detection

### Structures

```c
struct FrameData {
    RWSXLock latch;         // Page content latch (S/X/SX modes)
    atomic_bool loading;    // Page being loaded from disk
    u8 data[PAGE_SIZE];     // Page data (4KB)
};

struct PageFrame {
    atomic_u32 epoch;       // Eviction counter (for handle validation)
    atomic_u32 pin_cnt;     // Pin count (0 = evictable)
    atomic_u8 qtype;        // Queue type: QUEUE_QD or QUEUE_MAIN
    atomic_bool is_dirty;   // Dirty flag
    atomic_bool visited;    // Second-chance flag
    atomic_u32 page_num;    // Page number (INVALID_PAGE = empty)

    struct FrameData fdata; // Page data + latch
};

struct BufPool {
    // QDLP eviction queues (lock-free MPSC)
    struct CQ qd;           // Quick Demotion queue (probation)
    struct CQ main;         // Main queue (proven hot pages)
    struct CQ ghost;        // Ghost queue (evicted page tracking)

    RWSXLock latch;         // Pool-level latch (SX for cold load)
    atomic_u32 warmup_cursor; // Sequential warmup allocation

    struct PageStore *store;  // Shared across shards
    struct PageFrame *frames; // Array[POOL_SIZE] of frames
    struct SHTable *index;    // page_num → frame_idx (hash table)
    struct SHTable *gindex;   // page_num → ghost presence
};
```

### Design Rationale: Hash Table + QDLP

**Why Hash Table instead of Linear Search?**

For 32K entries (128MB pool), hash table is superior:

- **O(1) lookup**: ~100-200ns vs ~32μs linear scan
- **Scalability**: Performance independent of pool size
- **Sharding-friendly**: Multiple shards = multiple independent hash tables
- **Memory**: ~512KB overhead for 32K entries (~0.4% of 128MB)

**QDLP Eviction (Better than CLOCK)**

QDLP (Quick Demotion, Lazy Promotion) provides:

1. **Scan resistance**: New pages start in QD queue, evicted quickly if not re-accessed
2. **Ghost tracking**: Detects temporal locality of evicted pages
3. **Adaptive promotion**: Pages prove reuse before moving to MAIN
4. **Lock-free queues**: MPSC queues use atomic operations, no mutex needed

**Queue Sizes:**
- `QD_SIZE = POOL_SIZE / 16 = 2048` (12.5% of pool)
- `MAIN_SIZE = POOL_SIZE = 32768` (100% of pool, allows overflow from QD)
- `GHOST_SIZE = POOL_SIZE = 32768` (track recently evicted pages)

### Interface

```c
// Pool management
BufPool* bpool_init(PageStore *store);
void bpool_destroy(BufPool *bp);

// Page access (returns handle, not raw frame)
FrameHandle* bpool_fetch_page(BufPool *bp, u32 page_num);
i32 bpool_mark_read(BufPool *bp, FrameHandle *h);
i32 bpool_mark_write(BufPool *bp, FrameHandle *h);
i32 bpool_release_handle(BufPool *bp, FrameHandle *h);

// Flush operations
i32 bpool_flush_page(BufPool *bp, u32 page_num);
i32 bpool_flush_all(BufPool *bp);
```

**Handle-based API:**
- `FrameHandle` encapsulates frame access with epoch validation
- User acquires frame latch manually via `h->fdata->latch`
- Prevents use-after-eviction bugs via epoch checking

### Fetch Page Algorithm

```c
FrameHandle* bpool_fetch_page(BufPool *bp, u32 page_num) {
    // Hot path: Hash table lookup
    rwsx_lock(&bp->latch, LATCH_SHARED);
    u32 frame_idx;

    if (sht_get(bp->index, page_num, &frame_idx) != -1) {
        PageFrame *frame = &bp->frames[frame_idx];

        // Pin first (prevents eviction)
        atomic_fetch_add(&frame->pin_cnt, 1);
        atomic_thread_fence(memory_order_acquire);

        u32 epoch = atomic_load(&frame->epoch);
        u32 current_page = atomic_load(&frame->page_num);

        // Verify page still valid (no race with eviction)
        if (current_page != page_num) {
            atomic_fetch_sub(&frame->pin_cnt, 1);
            rwsx_unlock(&bp->latch, LATCH_SHARED);
            return bpool_fetch_page(bp, page_num);  // Retry
        }

        // Mark as visited (second-chance)
        atomic_store(&frame->visited, true);
        rwsx_unlock(&bp->latch, LATCH_SHARED);

        // Wait for loading to complete (if in progress)
        spin_wait_loaded(frame);

        return create_handle(&frame->fdata, epoch, frame_idx, page_num);
    }

    // Cold path: Page not in pool, load from disk
    rwsx_unlock(&bp->latch, LATCH_SHARED);
    PageFrame *frame = cold_load_page(bp, page_num);

    if (frame) {
        frame_idx = frame - bp->frames;
        return create_handle(&frame->fdata,
                            atomic_load(&frame->epoch),
                            frame_idx,
                            page_num);
    }

    return NULL;
}
```

### Load Page from Disk (Cold Load)

```c
PageFrame* cold_load_page(BufPool *bp, u32 page_num) {
    // Acquire pool SX-latch (allows concurrent hot path lookups)
    rwsx_lock(&bp->latch, LATCH_SHARED_EXCLUSIVE);

    // Double-check: Page might have been loaded by another thread
    u32 frame_idx;
    if (sht_get(bp->index, page_num, &frame_idx) != -1) {
        PageFrame *frame = &bp->frames[frame_idx];
        rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);

        // Wait for loading to complete
        spin_wait_loaded(frame);

        atomic_fetch_add(&frame->pin_cnt, 1);
        return frame;
    }

    // Check ghost hit (recently evicted page)
    bool in_ghost = (sht_get(bp->gindex, page_num, &dummy) != -1);

    // Find victim using QDLP
    u32 victim = find_victim_qdlp(bp);
    if (victim == INVALID_PAGE) {
        rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
        return NULL;  // All pages pinned
    }

    PageFrame *frame = &bp->frames[victim];
    u32 old_page = atomic_load(&frame->page_num);
    u8 old_qtype = atomic_load(&frame->qtype);

    // Add evicted page to ghost queue (if from QD)
    if (old_page != INVALID_PAGE && old_qtype == QUEUE_QD) {
        reclaim_ghost(bp);
        cq_put(&bp->ghost, old_page);
        sht_set(bp->gindex, old_page, 1);
    }

    // Mark as loading, insert into index BEFORE releasing latch
    atomic_store(&frame->fdata.loading, true);
    atomic_store(&frame->pin_cnt, 1);

    if (old_page != INVALID_PAGE) {
        sht_unset(bp->index, old_page);
        atomic_fetch_add(&frame->epoch, 1);
    }

    atomic_store(&frame->page_num, page_num);
    sht_set(bp->index, page_num, victim);  // Visible to other threads!

    // Release SX-latch (allows concurrent lookups during I/O)
    rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);

    // Perform I/O outside latch (concurrency!)
    bool was_dirty = (old_page != INVALID_PAGE) && atomic_load(&frame->is_dirty);
    u8 target_queue = in_ghost ? QUEUE_MAIN :
                     (cq_size(&bp->qd) >= QD_SIZE ? QUEUE_MAIN : QUEUE_QD);

    rwsx_lock(&frame->fdata.latch, LATCH_EXCLUSIVE);

    if (was_dirty) {
        pstore_write(bp->store, old_page, frame->fdata.data);
    }

    u8 buf[PAGE_SIZE];
    pstore_read(bp->store, page_num, buf);
    memcpy(frame->fdata.data, buf, PAGE_SIZE);

    atomic_store(&frame->is_dirty, false);
    atomic_store(&frame->visited, false);
    atomic_store(&frame->qtype, target_queue);

    // Insert into appropriate queue
    if (target_queue == QUEUE_MAIN) {
        cq_put(&bp->main, victim);
    } else {
        cq_put(&bp->qd, victim);
    }

    rwsx_unlock(&frame->fdata.latch, LATCH_EXCLUSIVE);

    // Mark loading complete
    atomic_store(&frame->fdata.loading, false);

    return frame;
}
```

**Key Race Condition Fix:**
- `loading` flag prevents premature access to page data
- Index insertion BEFORE releasing SX-latch prevents duplicate cold loads
- Other threads see page in index, spin-wait for loading to complete

### QDLP Eviction Algorithm

```c
u32 find_victim_qdlp(BufPool *bp) {
    // Phase 1: Warmup (sequential allocation during startup)
    u32 start = atomic_load(&bp->warmup_cursor);
    for (u32 i = start; i < POOL_SIZE; i++) {
        if (atomic_load(&bp->frames[i].page_num) == INVALID_PAGE) {
            atomic_store(&bp->warmup_cursor, i + 1);
            return i;
        }
    }

    // Phase 2: Try QD queue (quick demotion)
    u32 qd_size = cq_size(&bp->qd);
    for (u32 i = 0; i < qd_size; i++) {
        u32 frame_idx = cq_pop(&bp->qd);
        if (frame_idx == INVALID_PAGE)
            break;

        PageFrame *frame = &bp->frames[frame_idx];

        // Second-chance: visited pages promoted to MAIN
        if (atomic_load(&frame->visited)) {
            atomic_store(&frame->visited, false);
            cq_put(&bp->main, frame_idx);
            continue;
        }

        // Skip pinned pages
        if (atomic_load(&frame->pin_cnt) > 0) {
            cq_put(&bp->qd, frame_idx);  // Re-insert
            continue;
        }

        return frame_idx;  // Found victim in QD!
    }

    // Phase 3: Try MAIN queue
    u32 main_size = cq_size(&bp->main);
    for (u32 i = 0; i < main_size; i++) {
        u32 frame_idx = cq_pop(&bp->main);
        if (frame_idx == INVALID_PAGE)
            break;

        PageFrame *frame = &bp->frames[frame_idx];

        // Second-chance for hot pages
        if (atomic_load(&frame->visited)) {
            atomic_store(&frame->visited, false);
            cq_put(&bp->main, frame_idx);  // Re-insert
            continue;
        }

        // Skip pinned pages
        if (atomic_load(&frame->pin_cnt) > 0) {
            cq_put(&bp->main, frame_idx);  // Re-insert
            continue;
        }

        return frame_idx;  // Found victim in MAIN
    }

    return INVALID_PAGE;  // All pages pinned (OOM)
}
```

**QDLP Benefits:**
- **Scan-resistant**: Sequential scans don't pollute MAIN queue
- **Adaptive**: Ghost hits promote directly to MAIN
- **Lock-free**: MPSC queues use atomic operations
- **Low overhead**: Queue operations are O(1)

### Concurrency Model

- **Hash table lookups:** O(1) with S-latch on pool (allows concurrent lookups)
- **Pin/unpin:** Atomic operations on pin_cnt, no locks required
- **Frame latch (RWSXLock):** Supports three modes per frame
  - **S-latch (Shared):** Multiple readers can read page data
  - **X-latch (Exclusive):** Single writer modifies page data
  - **SX-latch (Shared-Exclusive):** Structural modifications with concurrent readers
- **Pool latch (RWSXLock):** Protects index and eviction
  - **S-latch (Shared):** Hot path lookups (concurrent)
  - **SX-latch (Shared-Exclusive):** Cold loads (allows concurrent hot path)
  - **X-latch (Exclusive):** Never used (SX is sufficient)
- **MPSC Queues:** Lock-free producer/consumer operations
  - Multiple threads can `cq_put()` concurrently
  - Single consumer (eviction) calls `cq_pop()`

**Pool latch usage:**

| Operation | Latch Mode | Duration | Allows Concurrent |
|-----------|------------|----------|-------------------|
| Hot path lookup | S-latch | ~100ns | ✅ Hot path lookups |
| Cold load | SX-latch | ~10-50μs | ✅ Hot path lookups, ❌ Other cold loads |
| Index update | (under SX) | ~1μs | ✅ Hot path lookups |

**Key advantages:**
1. **Hot path is highly concurrent:** S-latch allows unlimited concurrent lookups
2. **Cold loads don't block hot path:** SX-latch only blocks other cold loads
3. **I/O happens outside latch:** Page inserted into index before I/O starts
4. **No global eviction mutex:** MPSC queues are lock-free

**Adaptive Spin-Wait:**
```c
void spin_wait_loaded(PageFrame *frame) {
    int spin = 0;
    while (atomic_load(&frame->fdata.loading)) {
        if (spin < 5) {
            __asm__ __volatile__("pause");  // CPU-optimized spin
        } else {
            usleep(1 << min(spin - 5, 9));  // Exponential backoff
        }
        spin++;
    }
}
```

**Memory ordering guarantees:**
- `ACQUIRE` on hot path: Ensures visibility of cold load writes
- `RELEASE` on cold load: Makes page data visible to hot path
- `loading` flag with acquire-release prevents torn reads

---

## Layer 4: Sharded Buffer Pool (Optional Scaling Layer)

### Motivation

A single 128MB buffer pool (32K pages) performs well for small-to-medium workloads, but larger databases benefit from **sharding** to reduce latch contention and scale memory capacity.

### Design

```c
struct ShardedBufPool {
    u32 num_shards;         // Power of 2 (e.g., 1, 2, 4, 8, 16)
    u32 shard_shift;        // log2(num_shards) for fast modulo
    BufPool **shards;       // Array of buffer pool shards
    PageStore *store;       // Shared across all shards
};
```

### Shard Selection (Hash-Based)

```c
static inline u32 get_shard_id(u32 page_num, u32 shard_shift) {
    // Multiplicative hash (golden ratio) for uniform distribution
    u32 hash = page_num * 2654435761U;
    // Right shift to extract top bits (shard ID)
    return hash >> (32 - shard_shift);
}

// Usage:
// num_shards = 8 → shard_shift = 3
// get_shard_id(page, 3) → distributes pages across 8 shards
```

**Why multiplicative hash?**
- Scatters sequential page numbers across shards (prevents hot shard)
- Fast: Single multiply + shift (~2 cycles)
- No modulo needed for power-of-2 shards

### Interface (Drop-in Replacement)

```c
ShardedBufPool* sharded_bpool_init(PageStore *store, u32 num_shards);
void sharded_bpool_destroy(ShardedBufPool *sbp);

FrameHandle* sharded_bpool_fetch_page(ShardedBufPool *sbp, u32 page_num);
i32 sharded_bpool_mark_read(ShardedBufPool *sbp, FrameHandle *h);
i32 sharded_bpool_mark_write(ShardedBufPool *sbp, FrameHandle *h);
i32 sharded_bpool_release_handle(ShardedBufPool *sbp, FrameHandle *h);
i32 sharded_bpool_flush_all(ShardedBufPool *sbp);
```

**Implementation:**
```c
FrameHandle* sharded_bpool_fetch_page(ShardedBufPool *sbp, u32 page_num) {
    u32 shard_id = get_shard_id(page_num, sbp->shard_shift);
    return bpool_fetch_page(sbp->shards[shard_id], page_num);
}
```

### Configuration

| Use Case | Shards | Total Memory | Performance |
|----------|--------|--------------|-------------|
| **Embedded/Small DB** | 1 | 128MB | Single shard (baseline) |
| **Medium DB** | 4 | 512MB | 4× throughput (4 threads) |
| **Large DB** | 8 | 1GB | 8× throughput (8 threads) |
| **OLTP Server** | 16 | 2GB | 16× throughput (16+ threads) |
| **Analytics** | 32 | 4GB | 32× throughput (32+ threads) |

### Benefits

1. **Reduced contention:** Each shard has independent pool latch
2. **Linear scalability:** N shards ≈ N× throughput (with N+ threads)
3. **No code changes:** `BufPool` implementation unchanged
4. **Shared I/O layer:** All shards share single `PageStore` (thread-safe)

### Memory Overhead

Per shard overhead: ~1.5MB (hash tables, queues, metadata)
- 1 shard: 128MB + 1.5MB = 129.5MB
- 8 shards: 1024MB + 12MB = 1036MB (~1.2% overhead)

### Performance Validation (Measured)

**Single shard (32K pages, 128MB):**
- Comprehensive test suite: **97ms total**
- ~2.97 μs per page operation
- **146% efficiency** vs linear scaling (from 8K pages)

**Expected sharded performance (8 shards, 1GB):**
- Per-shard latency: Same (~97ms for equivalent load)
- Aggregate throughput: 8× (with 8+ concurrent threads)
- Cross-shard contention: None (independent latches)

---

## Layer 3: Page Allocator

### Responsibilities

- Track free/allocated pages via bitmaps
- Provide allocation with locality hints
- Support future transactions (mark allocated but uncommitted)

### Structures

```c
struct PageAllocator {
    BufferPool *pool;
    u32 total_groups;

    // In-memory cache of group descriptors
    GroupDescriptor *gdt_cache; // Array of total_groups descriptors

    // Allocation hints
    atomic_u32 last_alloc_group; // Last group allocated from

    // File growth synchronization (DCLI pattern)
    atomic_bool growing;         // True when file growth in progress
};

struct GroupDescriptor {
    u32 group_start;        // Start page of this group
    u32 bitmap_page;        // First bitmap page (group_start + 0)
    u32 data_start;         // First data page (group_start + 2)

    // Allocation tracking (atomic for concurrency)
    atomic_u32 free_pages;  // Free pages in this group
    atomic_u16 last_alloc_idx; // Hint: last allocated page index

    u8 state;               // GROUP_ACTIVE, GROUP_FULL, GROUP_EMPTY
    u8 _pad[9];
};
// 32 bytes per descriptor
```

### Group States

- `GROUP_EMPTY`: free_pages == 64K - 2 (only bitmap pages used)
- `GROUP_ACTIVE`: has both free and allocated pages
- `GROUP_FULL`: free_pages == 0

### Bitmap Format

- **Bitmap Page 0:** Bits for pages 0-32,767 of the group
- **Bitmap Page 1:** Bits for pages 32,768-65,535 of the group
- Each bitmap page: 4096 bytes = 32K bits
- Use `u64` chunks for efficient scanning (512 u64s per page)

**Bit Encoding:**

- `1` = allocated
- `0` = free
- Pages 0-1 (bitmap pages themselves) are always marked allocated

### Interface

```c
PageAllocator* allocator_init(BufferPool *pool, u32 total_groups);
u32 alloc_page(PageAllocator *pa, u32 hint);
u32 alloc_extent(PageAllocator *pa, u32 hint, u32 count);
void free_page(PageAllocator *pa, u32 page_num);
void free_extent(PageAllocator *pa, u32 start_page, u32 count);
void allocator_destroy(PageAllocator *pa);
```

### Allocation Algorithm (Lock-Free Bitmap)

```c
u32 alloc_page(PageAllocator *pa, u32 hint);
```

1. **Determine target group:**
   - If hint provided: `group = (hint - FIRST_GROUP_START) / GROUP_SIZE`
   - Else: use `last_alloc_group` (sequential allocation)

2. **Try target group:**
   - Check `free_pages > 0` (atomic read)
   - If zero, skip to next group

3. **Search bitmap (Lock-Free with CAS):**
   - Fetch bitmap pages (2 pages per group) with **S-latch** (not X-latch!)
   - Cast bitmap data to `atomic_u64*` array (512 words per page)
   - Scan words using atomic loads:
     ```c
     for (u32 word_idx = 0; word_idx < 512; word_idx++) {
         u64 mask = atomic_load_explicit(&bitmap[word_idx], memory_order_acquire);

         // Spin until we successfully allocate or word is full
         while (mask != 0xFFFFFFFFFFFFFFFF) {
             u64 inv = ~mask;
             u32 bit = __builtin_ffsll(inv) - 1;  // Find first 0 bit
             u64 desired = mask | (1ULL << bit);  // Set bit to 1

             // Try to atomically update
             if (atomic_compare_exchange_weak_explicit(
                     &bitmap[word_idx], &mask, desired,
                     memory_order_release, memory_order_acquire)) {
                 // Success! Mark page dirty and return
                 mark_frame_dirty(frame);
                 u32 page_num = compute_page_num(group, word_idx, bit);
                 goto update_metadata;
             }
             // CAS failed, mask updated with current value, retry
         }
     }
     ```
   - Unpin bitmap pages (now dirty due to atomic modification)

4. **Update metadata:**
   - Atomic decrement `group_desc[group].free_pages`
   - Atomic store `group_desc[group].last_alloc_idx = page_offset`
   - Update in-memory GDT cache
   - Mark GDT page dirty (lazy write-back)

5. **Fallback:**
   - If target group full, try next group (wrap around)
   - If all groups full, trigger growth (see DCLI protocol below)

### Free Page Algorithm (Lock-Free)

```c
void free_page(PageAllocator *pa, u32 page_num);
```

1. **Locate bitmap:**
   - Compute `group = (page_num - FIRST_GROUP_START) / GROUP_SIZE`
   - Compute `word_idx = bit_offset / 64`
   - Compute `bit = bit_offset % 64`

2. **Clear bit atomically:**
   - Fetch bitmap page with **S-latch**
   - Use atomic fetch_and to clear bit:
     ```c
     u64 clear_mask = ~(1ULL << bit);
     atomic_fetch_and_explicit(&bitmap[word_idx], clear_mask, memory_order_release);
     ```
   - Mark frame dirty
   - Unpin bitmap page

3. **Update metadata:**
   - Atomic increment `group_desc[group].free_pages`
   - Mark GDT page dirty

**Why fetch_and instead of CAS?**
- Free is idempotent (freeing already-free page is safe to detect later)
- No need to loop, single atomic instruction suffices
- Much faster than CAS loop

### Concurrency Strategy

**No explicit allocator mutex needed! Lock-free bitmap operations with atomic RMW.**

| Operation | Synchronization | Contention | Performance |
|-----------|----------------|------------|-------------|
| Read free_pages | Atomic load | None | Lock-free |
| Alloc bitmap bit | S-latch + atomic CAS loop | **Very Low** | 10-100× faster than X-latch |
| Free bitmap bit | S-latch + atomic fetch_and | **None** | Single instruction |
| Update free_pages | Atomic inc/dec | None | Lock-free |
| GDT update | Buffer pool latch on GDT page | Low | 64 GDT pages striped |

**Key Improvements:**

1. **S-latch instead of X-latch for bitmap pages**
   - Multiple allocators can search bitmap simultaneously
   - Only the actual bit flip uses atomic CAS (lock-free)
   - 10-100× throughput improvement under contention

2. **Natural segmentation**
   - 64 groups → 128 bitmap pages (2 per group)
   - Contention spreads across many pages
   - Allocators naturally distribute across groups

3. **Memory ordering**
   - `acquire` on load: see all prior writes to bitmap
   - `release` on CAS/fetch_and: make bit change visible
   - Prevents reordering issues on weak memory models

---

## Superblock (Page 0)

```c
struct Superblock {
    // Identity
    u32 magic;              // 0x53494D44 ("SIMD")
    u32 version;            // 1
    u32 page_size;          // 4096

    // Allocation tracking
    u32 total_pages;        // Current file size in pages
    u32 total_groups;       // Number of groups allocated
    u64 total_free_pages;   // Global free page count

    // Metadata pointers
    u32 gdt_start;          // Page 1 (start of GDT)
    u32 gdt_pages;          // 64 pages
    u32 first_group_start;  // Page 129

    // Future: WAL and transactions
    u64 checkpoint_lsn;     // 0 for now
    u32 wal_start;          // INVALID_PAGE for now

    u8 _pad[PAGE_SIZE - 60];
};
```

### Changes from Current Design

**Removed:**

- ❌ `curr_dblk` - Move to separate metadata page
- ❌ `head_dblk` - Move to separate metadata page
- ❌ `_root_page` - Schema system manages its own root

**Upper Layer Metadata Storage:**

- Reserve page 131 (first data page in group 0) for "System Catalog"
- System catalog structure:

  ```c
  struct SystemCatalog {
      u32 schema_root_page;   // Root of schema B+tree
      u32 dblock_fsm_page;    // Data block free space map
      u32 next_table_id;
      ...
  };
  ```

- Pin page 131 at startup, keep in buffer pool

---

## Bootstrap Procedures

### Create New Database

```c
i32 db_create(const char *path) {
    1. pstore_create(path) → create file
    2. Write superblock:
       - total_pages = FIRST_GROUP_START + GROUP_SIZE (1 group)
       - total_groups = 1
       - total_free_pages = GROUP_SIZE - 2 (minus bitmap pages)
    3. Initialize GDT (pages 1-64):
       - gdt[0].group_start = FIRST_GROUP_START (129)
       - gdt[0].bitmap_page = 129
       - gdt[0].data_start = 131
       - gdt[0].free_pages = GROUP_SIZE - 2
       - gdt[1..n] = INVALID_PAGE (unused groups)
    4. Initialize group 0 bitmap (pages 129-130):
       - Bitmap[0] = 0x3 (mark bitmap pages 0-1 as allocated)
       - Rest = 0x0 (all free)
    5. Grow file to 256MB
    6. Flush all metadata
}
```

### Open Existing Database

```c
i32 db_open(const char *path) {
    1. pstore_open(path)
    2. bpool_init(store, 1024)
    3. Read superblock (pin page 0)
    4. Load GDT cache (pin pages 1-64, copy to memory)
    5. allocator_init(pool, total_groups)
    6. Unpin metadata pages (keep them cached but evictable)
}
```

---

## File Growth Procedure (DCLI + SX-Latch)

### Trigger

When all existing groups are exhausted during allocation.

### DCLI Protocol (Double-Checked Locking with Initialization)

**Goal:** Allow concurrent allocations in existing groups during file growth, only blocking when metadata is updated.

```c
u32 alloc_page_with_growth(PageAllocator *pa, u32 hint) {
    // Fast path: Try allocating from existing groups
    u32 page = try_alloc_from_existing_groups(pa, hint);
    if (page != INVALID_PAGE) {
        return page;
    }

    // First check: Atomic check if someone is already growing
    if (atomic_load_explicit(&pa->growing, memory_order_acquire)) {
        // Someone is growing, wait and retry
        while (atomic_load_explicit(&pa->growing, memory_order_acquire)) {
            sched_yield();  // Or spin briefly
        }
        return alloc_page_with_growth(pa, hint);  // Retry allocation
    }

    // Try to become the grower (atomic CAS on flag)
    bool expected = false;
    if (!atomic_compare_exchange_strong_explicit(
            &pa->growing, &expected, true,
            memory_order_acq_rel, memory_order_acquire)) {
        // Lost race, someone else is growing
        while (atomic_load_explicit(&pa->growing, memory_order_acquire)) {
            sched_yield();
        }
        return alloc_page_with_growth(pa, hint);
    }

    // Won the race! We are the grower
    // Acquire SX-latch (allows concurrent reads, blocks X-latch only)
    rwsx_lock(&pa->pool->pool_latch, LATCH_SX);

    // Double-check: Maybe someone grew while we waited for SX-latch
    page = try_alloc_from_existing_groups(pa, hint);
    if (page != INVALID_PAGE) {
        // Growth already done, release and return
        rwsx_unlock(&pa->pool->pool_latch, LATCH_SX);
        atomic_store_explicit(&pa->growing, false, memory_order_release);
        return page;
    }

    // Actually grow the database
    grow_database_impl(pa);

    // Release SX-latch
    rwsx_unlock(&pa->pool->pool_latch, LATCH_SX);

    // Clear growing flag
    atomic_store_explicit(&pa->growing, false, memory_order_release);

    // Retry allocation
    return alloc_page_with_growth(pa, hint);
}
```

### Growth Implementation

```c
void grow_database_impl(PageAllocator *pa) {
    // We hold SX-latch here - concurrent allocations in existing groups proceed!

    u32 new_group = pa->total_groups;

    // 1. Extend file (no latch upgrade needed)
    pstore_grow(pa->pool->store, GROUP_SIZE);

    // 2. Initialize new group's bitmap pages
    u32 bitmap_start = FIRST_GROUP_START + new_group * GROUP_SIZE;
    for (u32 i = 0; i < 2; i++) {
        PageFrame *frame = bpool_fetch_page(pa->pool, bitmap_start + i, LATCH_EXCLUSIVE);
        memset(frame->data, 0, PAGE_SIZE);
        // Mark first 2 bits (bitmap pages themselves) as allocated
        if (i == 0) {
            ((u64*)frame->data)[0] = 0x3;
        }
        bpool_unpin_page(pa->pool, bitmap_start + i, true);
    }

    // 3. Upgrade to X-latch for metadata update (brief!)
    //    This blocks concurrent allocations momentarily
    rwsx_upgrade_sx_to_x(&pa->pool->pool_latch);

    // 4. Update GDT
    u32 gdt_page = 1 + (new_group / 256);  // 256 descriptors per page
    PageFrame *gdt_frame = bpool_fetch_page(pa->pool, gdt_page, LATCH_EXCLUSIVE);
    GroupDescriptor *gdt = (GroupDescriptor*)gdt_frame->data;
    u32 slot = new_group % 256;

    gdt[slot].group_start = FIRST_GROUP_START + new_group * GROUP_SIZE;
    gdt[slot].bitmap_page = gdt[slot].group_start;
    gdt[slot].data_start = gdt[slot].group_start + 2;
    atomic_store(&gdt[slot].free_pages, GROUP_SIZE - 2);
    gdt[slot].state = GROUP_ACTIVE;

    bpool_unpin_page(pa->pool, gdt_page, true);

    // 5. Update superblock
    PageFrame *sb_frame = bpool_fetch_page(pa->pool, 0, LATCH_EXCLUSIVE);
    Superblock *sb = (Superblock*)sb_frame->data;

    sb->total_pages += GROUP_SIZE;
    sb->total_groups += 1;
    atomic_fetch_add(&sb->total_free_pages, GROUP_SIZE - 2);

    bpool_unpin_page(pa->pool, 0, true);

    // 6. Update in-memory cache
    pa->total_groups += 1;
    pa->gdt_cache = realloc(pa->gdt_cache, pa->total_groups * sizeof(GroupDescriptor));
    pa->gdt_cache[new_group] = gdt[slot];

    // 7. Flush metadata (ensure durability)
    bpool_flush_page(pa->pool, 0);           // Superblock
    bpool_flush_page(pa->pool, gdt_page);    // GDT
    bpool_flush_page(pa->pool, bitmap_start); // Bitmap pages
    bpool_flush_page(pa->pool, bitmap_start + 1);

    // X-latch will be downgraded back to SX, then released by caller
    rwsx_downgrade_x_to_sx(&pa->pool->pool_latch);
}
```

### Concurrency Benefits

**Before (X-latch entire pool):**
- All allocations blocked during growth (~100ms)
- Pool-wide contention

**After (DCLI + SX-latch):**
- Concurrent allocations in existing groups continue during growth
- Only brief X-latch during metadata update (~1ms)
- 100× less blocking time

**Timeline:**

```
Thread A (Grower)          Thread B (Allocator)         Thread C (Allocator)
=====================      ======================       ======================
CAS growing flag ✓
Acquire SX-latch
Grow file (100ms)          Alloc from group 0 ✓        Alloc from group 1 ✓
Initialize bitmap          Alloc from group 2 ✓        Alloc from group 3 ✓
Upgrade SX→X               [BLOCKED]                    [BLOCKED]
Update metadata (1ms)
Downgrade X→SX
Release SX-latch
Clear growing flag         Alloc from new group ✓      Alloc from new group ✓
```

**Key Insight:** File I/O (slow) happens under SX-latch (allows concurrent reads/writes to other pages). Only metadata update (fast) needs X-latch.

---

## Testing Strategy

### Phase 1: Page Store

- Test file creation, read/write, growth
- Test in-memory mode
- Concurrent read/write with pread/pwrite

### Phase 2: Buffer Pool

- Test pin/unpin semantics
- Test CLOCK eviction (fill pool, verify LRU-like behavior)
- Test concurrent fetch (multi-threaded)
- Test dirty page write-back

### Phase 3: Allocator

- Test single-threaded alloc/free
- Test group exhaustion and growth
- Test concurrent allocation (stress test with 200M ops)
- Verify no double-allocation bugs

### Phase 4: Integration

- Build simple BTree on top
- Run mixed workload (insert/delete/search)
- Monitor buffer pool hit rate

---

## Code Structure

```
src/page/
  ├── page_store.c/h      (Layer 1)
  ├── buffer_pool.c/h     (Layer 2)
  ├── allocator.c/h       (Layer 3)
  └── page_defs.h         (Shared structures: Superblock, GroupDescriptor)

src/btree/
  └── btree.c/h           (New BTree on buffer pool)

src/schema/
  └── schema.c/h          (Schema management)

tests/
  ├── test_page_store.c
  ├── test_buffer_pool.c
  ├── test_allocator.c
  └── test_concurrent.c   (Multi-threaded stress test)
```

---

## Future Extensions (Not Immediate)

### Write-Ahead Log (WAL)

- Superblock.checkpoint_lsn points to recovery start
- Page LSN in each page header (for ARIES recovery)
- WAL writes bypass buffer pool (direct page_store)

### Transaction Support

- Allocator tracks tentative allocations
- Commit: make allocations permanent
- Abort: rollback bitmap changes

### Multi-Writer

- Record-level locking (lock table hash map)
- Deadlock detection or timeout
- MVCC for lock-free reads

### Crash Recovery

- Scan bitmap to verify free page counts
- Rebuild allocator in-memory state
- WAL replay (when implemented)

---

## Migration Path

Since starting ground-up:

### Phase 1: Foundation (Current Plan)

1. Implement Layer 1: Page Store
2. Implement Layer 2: Buffer Pool
3. Implement Layer 3: Page Allocator
4. Write unit tests for each layer

### Phase 2: BTree Rewrite

1. Design new BTree with variable-length keys
2. Integrate with buffer pool (fetch/unpin)
3. Implement insert/delete/search
4. Add latching for concurrency

### Phase 3: Schema & Records

1. Rewrite schema system on new BTree
2. Redesign record format (variable-length, not fixed MAX_KEY)
3. Rebuild data block management

### Phase 4: Concurrency

1. Add RWLocks to buffer pool frames
2. Implement BTree latch coupling
3. Test multi-reader workloads

### Phase 5: Durability

1. Implement WAL
2. Add crash recovery
3. Checkpoint mechanism

---

## Next Steps

With page management plan locked in, the next discussion should cover:

### BTree Redesign

1. Handle variable-length keys (not fixed MAX_KEY=64)
2. Store different key types efficiently (INTEGER vs BLOB)
3. Implement latch coupling for concurrent access
4. Design record format (inline vs overflow)

---

## Notes

### Lock-Free Optimizations

This design incorporates two major concurrency optimizations:

1. **Atomic Bitmap Operations (Lock-Free Allocation)**
   - Bitmap pages use S-latch instead of X-latch
   - Bit flips use atomic CAS (allocation) and fetch_and (free)
   - Multiple allocators can search same bitmap page concurrently
   - 10-100× throughput improvement under contention

2. **DCLI + SX-Latch (Non-Blocking Growth)**
   - File growth uses Double-Checked Locking with Initialization pattern
   - SX-latch allows concurrent allocations during file I/O
   - Only brief X-latch during metadata update (~1ms vs ~100ms)
   - 100× reduction in blocking time during growth

### General Design Principles

- Leverages buffer pool's page latches for allocator synchronization (no separate allocator mutex)
- Group-based structure provides natural segmentation (64 groups, 128 bitmap pages)
- Metadata pages (superblock, GDT) cached in buffer pool but rarely evicted
- Requires RWSXLock implementation (see btree-design.md) for SX-latch support
