# Page Management Redesign Plan

## Overview

This document outlines the redesign of the page management system for simple-db, moving from a monolithic GDT-based approach to a layered architecture with proper buffer pool management and concurrency support.

## Core Specifications

- **Buffer Pool:** 1024 frames (4MB cache)
- **Initial Size:** 1 group = 64K pages = 256MB
- **Growth:** Add 1 group (256MB) at a time
- **Metadata Region:** 64 pages (supports 4TB max database size)
- **Page Size:** 4KB
- **Group Structure:** Reuse existing group-based design

## Architecture Overview

```
┌─────────────────────────────────────────┐
│  Upper Layers (BTree, Schema, etc)      │
├─────────────────────────────────────────┤
│  Layer 3: Page Allocator                │  ← Allocation policy, free space tracking
├─────────────────────────────────────────┤
│  Layer 2: Buffer Pool Manager           │  ← Caching, eviction, pin/unpin
├─────────────────────────────────────────┤
│  Layer 1: Page Store (File I/O)         │  ← Raw page read/write, file growth
└─────────────────────────────────────────┘
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
- Evict cold pages (CLOCK algorithm)
- Pin/unpin semantics to prevent eviction of in-use pages
- Dirty tracking and write-back
- Thread-safe access via page latches

### Structures

```c
struct PageFrame {
    atomic_u32 pin_count;   // Pin count (0 = evictable)
    atomic_bool is_dirty;   // Dirty flag
    atomic_u8 clock_bit;    // For CLOCK eviction
    pthread_rwlock_t latch; // Page content latch
    u8 data[PAGE_SIZE];     // Page data
};

struct BufferPool {
    PageStore *store;
    u32 pool_size;          // 1024
    PageFrame *frames;      // Array of frames

    // TLB-style page table: frame[i] contains which page_num
    atomic_u32 *page_nums;  // Array[1024] of page numbers
                            // INVALID_PAGE means frame is empty

    // CLOCK eviction
    atomic_u32 clock_hand;  // Current position for eviction

    // Pool-level latch (only for eviction coordination and file growth)
    RWSXLock pool_latch;    // Custom RWLock with SX-mode support
                            // (see btree-design.md for RWSXLock implementation)
};
```

### Design Rationale: Linear Search vs Hash Table

For a fixed 1024-entry pool, **linear search is superior** to hash tables:

- **Cache efficiency:** 1024 × 4 bytes = 4KB, fits in L1 cache
- **Sequential access:** Modern CPUs prefetch automatically, ~3-4 cycles per comparison
- **No hash overhead:** Eliminates hash function computation and collision handling
- **Lock-free reads:** Simple atomic loads, no bucket-level locking
- **Compiler optimization:** Fixed iteration count allows vectorization

Worst case: ~1.3μs @ 3GHz. Average case is much better due to spatial locality (hot pages clustered near clock hand).

### Interface

```c
BufferPool* bpool_init(PageStore *store, u32 pool_size);
PageFrame* bpool_fetch_page(BufferPool *bp, u32 page_num, LatchMode mode);
void bpool_unpin_page(BufferPool *bp, u32 page_num, bool is_dirty);
void bpool_flush_page(BufferPool *bp, u32 page_num);
void bpool_flush_all(BufferPool *bp);
void bpool_destroy(BufferPool *bp);
```

### Fetch Page Algorithm

```c
PageFrame* bpool_fetch_page(BufferPool *bp, u32 page_num, LatchMode mode) {
    // Fast path: Linear scan of page_nums array
    for (u32 i = 0; i < bp->pool_size; i++) {
        u32 cached_page = atomic_load(&bp->page_nums[i]);
        if (cached_page == page_num) {
            PageFrame *frame = &bp->frames[i];

            // Pin the frame (prevents eviction)
            atomic_fetch_add(&frame->pin_count, 1);

            // Acquire latch
            if (mode == LATCH_SHARED)
                pthread_rwlock_rdlock(&frame->latch);
            else
                pthread_rwlock_wrlock(&frame->latch);

            // Set clock bit (recently used)
            atomic_store(&frame->clock_bit, 1);

            return frame;
        }
    }

    // Slow path: Page not in pool, need to load from disk
    return load_page_from_disk(bp, page_num, mode);
}
```

### Load Page from Disk

```c
PageFrame* load_page_from_disk(BufferPool *bp, u32 page_num, LatchMode mode) {
    // Acquire pool X-latch (brief hold)
    rwsx_lock(&bp->pool_latch, LATCH_EXCLUSIVE);

    // Find victim using CLOCK
    u32 victim_idx = find_victim_clock(bp);
    PageFrame *frame = &bp->frames[victim_idx];

    // Evict old page if dirty
    u32 old_page = atomic_load(&bp->page_nums[victim_idx]);
    if (old_page != INVALID_PAGE && atomic_load(&frame->is_dirty)) {
        pstore_write(bp->store, old_page, frame->data);
    }

    // Load new page
    pstore_read(bp->store, page_num, frame->data);

    // Update TLB
    atomic_store(&bp->page_nums[victim_idx], page_num);

    // Initialize frame state
    atomic_store(&frame->pin_count, 1);
    atomic_store(&frame->clock_bit, 1);
    atomic_store(&frame->is_dirty, false);

    // Release pool latch
    rwsx_unlock(&bp->pool_latch, LATCH_EXCLUSIVE);

    // Acquire frame latch
    if (mode == LATCH_SHARED)
        pthread_rwlock_rdlock(&frame->latch);
    else
        pthread_rwlock_wrlock(&frame->latch);

    return frame;
}
```

### CLOCK Eviction Algorithm

```c
u32 find_victim_clock(BufferPool *bp) {
    u32 start = atomic_load(&bp->clock_hand);

    for (u32 iter = 0; iter < bp->pool_size * 2; iter++) {
        u32 idx = (start + iter) % bp->pool_size;
        PageFrame *frame = &bp->frames[idx];

        // Skip pinned pages
        if (atomic_load(&frame->pin_count) > 0)
            continue;

        // Check clock bit
        if (atomic_load(&frame->clock_bit) == 1) {
            atomic_store(&frame->clock_bit, 0);
            continue;
        }

        // Found victim
        atomic_store(&bp->clock_hand, (idx + 1) % bp->pool_size);
        return idx;
    }

    // All pages pinned - should never happen in practice
    return INVALID_PAGE;
}
```

### Unpin Page

```c
void bpool_unpin_page(BufferPool *bp, u32 page_num, bool is_dirty) {
    // Find frame (linear search)
    for (u32 i = 0; i < bp->pool_size; i++) {
        if (atomic_load(&bp->page_nums[i]) == page_num) {
            PageFrame *frame = &bp->frames[i];

            // Update dirty flag if needed
            if (is_dirty) {
                atomic_store(&frame->is_dirty, true);
            }

            // Decrement pin count
            atomic_fetch_sub(&frame->pin_count, 1);

            return;
        }
    }
}
```

### Concurrency Model

- **Page table (TLB) lookups:** Lock-free atomic loads, no contention
- **Pin/unpin:** Atomic operations on pin_count, no locks required
- **Page latch:** RWLock per frame
  - Multiple readers can hold S-latch simultaneously
  - Single writer holds X-latch exclusively
- **Pool latch (RWSXLock):** Supports three modes:
  - **S-latch (Shared):** Multiple threads read concurrently
  - **X-latch (Exclusive):** Single thread modifies, blocks all others
  - **SX-latch (Shared-Exclusive):** Single thread modifies with concurrent readers
    - Used during file growth: allows concurrent allocations while growing
    - Upgradable to X-latch when metadata update needed

**Pool latch usage:**

| Operation | Latch Mode | Duration | Blocks |
|-----------|------------|----------|--------|
| Page lookup | None | - | - |
| Page eviction/load | X-latch | ~1ms | All operations |
| File growth | SX-latch → X-latch | ~100ms SX, ~1ms X | X blocks all, SX blocks only X |

**Key advantage:** Most operations (lookup, pin, unpin) require no pool-level synchronization. Page loading needs X-latch (brief). File growth uses SX-latch to allow concurrent allocations during slow I/O.

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
