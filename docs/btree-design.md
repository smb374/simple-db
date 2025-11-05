# BTree Design Plan

## Overview

This document defines the BTree index structure for simple-db, featuring industry-leading concurrency control using SX-latches (InnoDB MySQL 8.0 technique) and crash-resistant design with leaf-level durability.

## Design Principles

1. **SX-latch concurrency:** Allow readers during structural modifications
2. **Leaf durability:** Flush leaves before parent updates for crash recovery
3. **Buffer pool integration:** All node access via fetch/unpin protocol
4. **Optimistic execution:** Fast path for non-conflicting operations
5. **Prefetching iterators:** Pin multiple pages ahead for sequential scans
6. **Bulk loading:** Efficient bottom-up construction for initial creation
7. **Crash recovery:** Rebuild internals from durable leaf chain

---

## 1. SX-Latch Protocol

### Latch Modes

```c
typedef enum {
    LATCH_NONE = 0,
    LATCH_SHARED = 1,      // S: Multiple readers, no writers
    LATCH_SX = 2,          // SX: Single modifier + multiple readers
    LATCH_EXCLUSIVE = 3    // X: Single writer, no readers/writers
} LatchMode;
```

### Compatibility Matrix

```
        S    X    SX
S       ✓    ✗    ✓     ← Key: S and SX are compatible!
X       ✗    ✗    ✗
SX      ✓    ✗    ✗
```

**Critical insight:** SX-latch allows **concurrent reads during structural modifications** (splits/merges). Only the final "link-in" operation needs brief X-latch.

### RWSXLock Implementation

```c
struct RWSXLock {
    pthread_mutex_t mutex;
    pthread_cond_t cond;

    i32 readers;          // Number of S-latch holders
    bool writer;          // X-latch held
    bool sx_holder;       // SX-latch held

    pthread_t sx_owner;   // Thread holding SX (for upgrade to X)

    // DCLI: Prevent upgrade starvation
    atomic_bool upgrading; // True when SX → X upgrade pending
};
```

### Latch Acquisition

```c
void rwsx_lock(RWSXLock *lock, LatchMode mode) {
    pthread_mutex_lock(&lock->mutex);

    switch (mode) {
        case LATCH_SHARED:
            // Wait for X-latch to be released (SX is OK!)
            // Also wait if upgrade is pending (prevent starvation)
            while (lock->writer || atomic_load_explicit(&lock->upgrading, memory_order_acquire)) {
                pthread_cond_wait(&lock->cond, &lock->mutex);
            }
            lock->readers++;
            break;

        case LATCH_SX:
            // Wait for X-latch and other SX-latch to be released
            while (lock->writer || lock->sx_holder) {
                pthread_cond_wait(&lock->cond, &lock->mutex);
            }
            lock->sx_holder = true;
            lock->sx_owner = pthread_self();
            break;

        case LATCH_EXCLUSIVE:
            // Wait for all S/SX/X latches to be released
            while (lock->readers > 0 || lock->writer || lock->sx_holder) {
                pthread_cond_wait(&lock->cond, &lock->mutex);
            }
            lock->writer = true;
            break;
    }

    pthread_cond_broadcast(&lock->cond);
    pthread_mutex_unlock(&lock->mutex);
}

void rwsx_unlock(RWSXLock *lock, LatchMode mode) {
    pthread_mutex_lock(&lock->mutex);

    switch (mode) {
        case LATCH_SHARED:
            lock->readers--;
            break;
        case LATCH_SX:
            lock->sx_holder = false;
            break;
        case LATCH_EXCLUSIVE:
            lock->writer = false;
            break;
    }

    pthread_cond_broadcast(&lock->cond);
    pthread_mutex_unlock(&lock->mutex);
}

// SX holder can upgrade to X (atomic, no intervening operations)
void rwsx_upgrade_sx_to_x(RWSXLock *lock) {
    pthread_mutex_lock(&lock->mutex);

    assert(lock->sx_holder && lock->sx_owner == pthread_self());

    // Set upgrading flag FIRST (prevents new S-latch acquisitions)
    atomic_store_explicit(&lock->upgrading, true, memory_order_release);

    // Broadcast to wake threads checking upgrading flag
    pthread_cond_broadcast(&lock->cond);

    // Unlock mutex briefly to allow new S-latch requests to see upgrading flag
    pthread_mutex_unlock(&lock->mutex);

    // Reacquire mutex
    pthread_mutex_lock(&lock->mutex);

    // Wait for all existing readers to drain
    // (New readers are blocked by upgrading flag)
    while (lock->readers > 0) {
        pthread_cond_wait(&lock->cond, &lock->mutex);
    }

    // Perform upgrade
    lock->sx_holder = false;
    lock->writer = true;

    // Clear upgrading flag
    atomic_store_explicit(&lock->upgrading, false, memory_order_release);

    pthread_cond_broadcast(&lock->cond);
    pthread_mutex_unlock(&lock->mutex);
}

// X holder can downgrade to SX (allows readers back in)
void rwsx_downgrade_x_to_sx(RWSXLock *lock) {
    pthread_mutex_lock(&lock->mutex);

    assert(lock->writer);
    assert(!lock->sx_holder);

    lock->writer = false;
    lock->sx_holder = true;
    lock->sx_owner = pthread_self();

    // Ensure upgrading is cleared (defensive)
    atomic_store_explicit(&lock->upgrading, false, memory_order_release);

    pthread_cond_broadcast(&lock->cond);
    pthread_mutex_unlock(&lock->mutex);
}
```

### Upgrade Starvation Prevention (DCLI Pattern)

**Problem:** Without protection, new S-latch acquisitions can starve SX→X upgrade indefinitely.

**Scenario without protection:**
```
Thread A (SX holder)       Thread B, C, D... (readers)
====================       ===========================
Hold SX-latch
Want to upgrade to X
Wait for readers == 0
                           Acquire S-latch ✓
                           Release S-latch
                           Acquire S-latch ✓ (again)
                           Release S-latch
                           (pattern repeats indefinitely)
[STARVED FOREVER]
```

**Solution: DCLI (Double-Checked Locking with Initialization)**

The atomic `upgrading` flag prevents new S-latch acquisitions when an upgrade is pending:

```
Thread A (Upgrader)        Thread B (New Reader)        Existing Readers
===================        =====================        ================
Hold SX-latch
upgrading = true
                           Try S-latch
                           See upgrading == true
                           Sleep (cond_wait)            Continue work
                                                        Release S-latch
readers == 0 ✓
Upgrade to X
upgrading = false
Broadcast
                           Wake up, retry S-latch ✓
```

**Key Implementation Details:**

1. **Set flag before waiting:**
   - `upgrading = true` happens atomically
   - Memory order: `release` (makes flag visible to all threads)

2. **Readers check flag:**
   - S-latch acquisition checks `upgrading` flag
   - Memory order: `acquire` (sees all prior writes)
   - Blocks on condition variable if flag is set

3. **Unlock/relock pattern:**
   - Brief unlock after setting flag allows waiting threads to check it
   - Prevents deadlock where threads are waiting on cond_wait

4. **Guarantee fairness:**
   - Existing readers can finish (not preempted)
   - New readers wait (prevents starvation)
   - Upgrade completes in bounded time

**Performance:**
- Typical case: No contention, upgrade takes ~1μs
- High contention: Upgrade guaranteed to complete once existing readers drain
- No livelock or starvation

---

## 2. BTree Node Structure

### Node Header

```c
struct BTreeNode {
    u8 type;              // BNODE_INT or BNODE_LEAF
    u8 nkeys;             // Number of keys (max 255)
    u16 frag_bytes;       // Fragmentation bytes
    u16 ent_off;          // Entry offset (grows down)
    u16 flags;            // Node flags (future use)

    u32 parent_page;      // Parent page number
    u32 prev_page;        // Previous sibling (leaf only)
    u32 next_page;        // Next sibling (leaf only)
    u32 head_page;        // Smallest child (internal only)

    u64 lsn;              // Log sequence number (0 for now, future WAL)

    u16 slots[];          // Slot array (grows up)
};
// Header: 40 bytes
```

**Node types:**
```c
#define BNODE_INT  0    // Internal node
#define BNODE_LEAF 1    // Leaf node
```

**Future flags:**
```c
#define NODE_DIRTY   0x01   // Modified but not flushed
#define NODE_DELETED 0x02   // Marked for deletion
#define NODE_ROOT    0x04   // Is root node
```

### Internal Node Entry

```c
struct InternalEntry {
    struct BTreeKey key;      // 28 bytes (from schema design)
    u32 child_page;           // 4 bytes
};
// Total: 32 bytes per entry
```

**Key invariant:** `key` is the **minimum key** in subtree rooted at `child_page`.

**Fan-out:**
- Available space: 4096 - 40 (header) = 4056 bytes
- Per entry: 2 (slot) + 32 (entry) = 34 bytes
- **Max entries: ~119**
- **Conservative (with fragmentation): ~105 entries**

### Leaf Node Entry (Primary Index)

```c
struct PrimaryLeafEntry {
    struct BTreeKey key;      // 28 bytes
    u16 metadata_len;         // 2 bytes
    u8 metadata[];            // Variable (NULL bitmap + covering data)
    RID rid;                  // 6 bytes (page + slot + flags)
};
// Base: 36 bytes
// With metadata (~16B avg): ~52 bytes
```

**Fan-out:** ~62-65 entries per page

### Leaf Node Entry (Secondary Index)

```c
struct SecondaryLeafEntry {
    struct BTreeKey sec_key;  // 28 bytes
    struct BTreeKey prim_key; // 28 bytes
};
// Total: 56 bytes
```

**Fan-out:** ~58-62 entries per page

---

## 3. Search (Read-Only)

### Algorithm: S-Latch Coupling

```c
i32 btree_search(BTreeHandle *h, BTreeKey *key, void *value_out, u32 *len_out) {
    u32 current = h->root_page;
    PageFrame *parent_frame = NULL;

    while (true) {
        // Fetch with S-latch
        PageFrame *frame = fetch_page(h->pool, current, LATCH_SHARED);
        BTreeNode *node = (BTreeNode*)frame->data;

        // Release parent S-latch (latch coupling)
        if (parent_frame) {
            unpin_page(h->pool, parent_frame->page_num, false);
        }

        if (node->type == BNODE_LEAF) {
            // Found leaf, search for key
            i32 result = search_leaf(node, key, value_out, len_out);
            unpin_page(h->pool, current, false);
            return result;
        }

        // Internal node, find child
        u32 child = find_child_page(node, key);
        parent_frame = frame;
        current = child;
    }
}
```

**Concurrency:** Many threads can search concurrently with S-latches. No blocking on reads!

---

## 4. Insert Operations

### Two-Phase Insert

**Phase 1: Optimistic (Fast Path)**
- Try insert with S-latches
- If leaf has room: upgrade to X-latch, insert, done
- If split needed: fall back to pessimistic

**Phase 2: Pessimistic (with SX-Latches)**
- Traverse with SX-latches on nodes that may need modification
- Perform split while holding SX (readers can still search!)
- Upgrade SX → X only for final parent pointer update

### Optimistic Insert

```c
i32 btree_insert(BTreeHandle *h, BTreeKey *key, void *payload, u32 len) {
    // Phase 1: Optimistic - try with S-latches
    u32 leaf_page;
    if (find_leaf_optimistic(h, key, &leaf_page) == 0) {
        // Found leaf, check if room
        PageFrame *frame = fetch_page(h->pool, leaf_page, LATCH_SHARED);
        BTreeNode *leaf = (BTreeNode*)frame->data;

        if (leaf->nkeys < MAX_ENTS) {
            // Leaf has room! Need X-latch for insert
            unpin_page(h->pool, leaf_page, false);

            // Acquire X-latch
            frame = fetch_page(h->pool, leaf_page, LATCH_EXCLUSIVE);
            leaf = (BTreeNode*)frame->data;

            // Verify still has room (may have changed)
            if (leaf->nkeys < MAX_ENTS) {
                insert_entry(leaf, key, payload, len);
                unpin_page(h->pool, leaf_page, true);
                return 0;  // Success!
            }

            // Lost race, retry with pessimistic
            unpin_page(h->pool, leaf_page, false);
        } else {
            unpin_page(h->pool, leaf_page, false);
        }
    }

    // Phase 2: Pessimistic - need SMO (structural modification)
    return btree_insert_pessimistic(h, key, payload, len);
}
```

### Pessimistic Insert

```c
i32 btree_insert_pessimistic(BTreeHandle *h, BTreeKey *key, void *payload, u32 len) {
    // 1. Find path to leaf
    u32 path[MAX_HEIGHT];
    u16 path_len = 0;
    find_path_to_leaf(h, key, path, &path_len);

    // 2. Determine which nodes are "unsafe" (may need split)
    u16 sx_start_level = find_first_unsafe_level(h, path, path_len);

    // 3. Re-traverse, acquiring SX-latches on unsafe nodes
    PageFrame *frames[MAX_HEIGHT];
    LatchMode modes[MAX_HEIGHT];

    for (u16 i = 0; i < path_len; i++) {
        LatchMode mode = (i >= sx_start_level) ? LATCH_SX : LATCH_SHARED;
        frames[i] = fetch_page(h->pool, path[i], mode);
        modes[i] = mode;

        BTreeNode *node = (BTreeNode*)frames[i]->data;

        // If node is safe and we hold SX, can release earlier latches
        if (mode == LATCH_SX && is_safe_for_insert(node)) {
            // Release all previous latches
            for (u16 j = 0; j < i; j++) {
                unpin_page(h->pool, path[j], false);
            }
            sx_start_level = i;
        }
    }

    // 4. Perform split with SX-latches held
    u32 leaf_idx = path_len - 1;
    return split_and_insert(h, frames, modes, leaf_idx, key, payload, len);
}
```

**Safe node definition:**
- For insert: `nkeys < MAX_ENTS - 1` (room for one more + potential separator)
- For delete: `nkeys > MIN_ENTS` (won't underflow)

---

## 5. Split Algorithm

### Leaf Split (60/40 Biased)

```c
i32 split_leaf(BTreeHandle *h, PageFrame *left_frame, BTreeKey *new_key,
               void *payload, u32 len, BTreeKey *promote_key_out, u32 *new_page_out) {

    BTreeNode *left = (BTreeNode*)left_frame->data;

    // 1. Allocate new leaf page (locality hint: near left page)
    u32 new_page = allocator_alloc_page(h->pool->allocator, left_frame->page_num);
    PageFrame *right_frame = fetch_page(h->pool, new_page, LATCH_EXCLUSIVE);
    BTreeNode *right = (BTreeNode*)right_frame->data;

    init_node(right, BNODE_LEAF);

    // 2. Collect all entries (including new one) into temp buffer
    Entry temp[MAX_ENTS + 1];
    u8 temp_count = 0;

    bool inserted = false;
    for (u8 i = 0; i < left->nkeys; i++) {
        Entry *e = get_entry(left, i);

        // Insert new entry in sorted order
        if (!inserted && compare_keys(new_key, &e->key) < 0) {
            build_entry(&temp[temp_count++], new_key, payload, len);
            inserted = true;
        }

        copy_entry(&temp[temp_count++], e);
    }

    if (!inserted) {
        build_entry(&temp[temp_count++], new_key, payload, len);
    }

    // 3. Split entries (60% left, 40% right)
    u8 split_at = (temp_count * 6) / 10;

    // 4. Rebuild left node
    left->nkeys = 0;
    left->ent_off = PAGE_SIZE;
    left->frag_bytes = 0;

    for (u8 i = 0; i < split_at; i++) {
        insert_entry_at_end(left, &temp[i]);
    }

    // 5. Build right node
    for (u8 i = split_at; i < temp_count; i++) {
        insert_entry_at_end(right, &temp[i]);
    }

    // 6. Update leaf links (readers with S-latch can still traverse!)
    right->next_page = left->next_page;
    right->prev_page = left_frame->page_num;
    left->next_page = new_page;

    // Update next sibling's prev pointer
    if (right->next_page != INVALID_PAGE) {
        PageFrame *next_frame = fetch_page(h->pool, right->next_page, LATCH_EXCLUSIVE);
        BTreeNode *next = (BTreeNode*)next_frame->data;
        next->prev_page = new_page;
        unpin_page(h->pool, right->next_page, true);
    }

    // 7. **CRITICAL:** Flush leaves to disk before updating parent
    flush_page(h->pool, left_frame->page_num);
    flush_page(h->pool, new_page);

    // 8. Extract promote key (first key of right node)
    Entry *first_right = get_entry(right, 0);
    memcpy(promote_key_out, &first_right->key, sizeof(BTreeKey));

    unpin_page(h->pool, new_page, false);  // Already flushed

    *new_page_out = new_page;
    return 0;
}
```

### Internal Split

Similar to leaf split, but:
- No prev/next pointers
- Promote key is **removed** from right node (becomes separator in parent)
- Right node's head_page = promoted key's child page

### Update Parent (SX → X Upgrade)

```c
i32 update_parent_after_split(BTreeHandle *h, PageFrame *parent_frame,
                               BTreeKey *promote_key, u32 new_child) {

    BTreeNode *parent = (BTreeNode*)parent_frame->data;

    // Currently hold SX-latch on parent
    // Upgrade to X-latch for atomic link-in
    rwsx_upgrade_sx_to_x(&parent_frame->latch);

    if (parent->nkeys < MAX_ENTS) {
        // Parent has room, insert separator
        insert_internal_entry(parent, promote_key, new_child);

        // Mark dirty, unpin
        unpin_page(h->pool, parent_frame->page_num, true);
        return 0;
    } else {
        // Parent also needs split, propagate up
        return split_internal_and_propagate(h, parent_frame, promote_key, new_child);
    }
}
```

**Key point:** Only the parent pointer update needs X-latch. During the entire split operation, readers with S-latch can still search the tree!

---

## 6. Delete Operations

### Algorithm

Similar two-phase approach:
1. **Optimistic:** S-latch to leaf, check if simple delete (no merge needed)
   - If safe: X-latch, delete, done
2. **Pessimistic:** SX-latch path, perform merge/redistribute

### Merge/Redistribute

**Redistribute (prefer):**
- Borrow entries from sibling
- No parent update needed
- Faster

**Merge (if redistribute impossible):**
- Combine with sibling
- Remove separator from parent
- May cascade up

---

## 7. Leaf Durability & Crash Recovery

### Flush Protocol

**Rule:** Leaves must be flushed **before** parent pointers are updated.

```c
void flush_page(BufferPool *pool, u32 page_num) {
    // Find frame in buffer pool TLB
    for (u32 i = 0; i < pool->pool_size; i++) {
        if (atomic_load(&pool->page_nums[i]) == page_num) {
            PageFrame *frame = &pool->frames[i];

            if (atomic_load(&frame->is_dirty)) {
                // Write to disk
                pstore_write(pool->store, page_num, frame->data);

                // Force sync (fsync)
                pstore_sync(pool->store);

                atomic_store(&frame->is_dirty, false);
            }
            return;
        }
    }
}
```

### Crash Recovery: Rebuild Index

**Strategy:** Always rebuild internal nodes from leaves on recovery.

```c
i32 btree_recover(BTreeHandle *h, u32 schema_root_page) {
    // 1. Find first leaf by scanning leaf chain
    //    (leaf chain is maintained via prev/next pointers)
    u32 first_leaf = find_first_leaf_from_schema(h, schema_root_page);

    if (first_leaf == INVALID_PAGE) {
        return -1;  // Empty index
    }

    // 2. Scan all leaves, collect page numbers
    u32 *leaf_pages = NULL;
    u32 num_leaves = 0;
    u32 current = first_leaf;

    while (current != INVALID_PAGE) {
        PageFrame *frame = fetch_page(h->pool, current, LATCH_SHARED);
        BTreeNode *leaf = (BTreeNode*)frame->data;

        // Verify leaf is valid (check magic, type, etc.)
        if (!validate_leaf(leaf)) {
            unpin_page(h->pool, current, false);
            return -1;
        }

        leaf_pages = realloc(leaf_pages, (num_leaves + 1) * sizeof(u32));
        leaf_pages[num_leaves++] = current;

        current = leaf->next_page;
        unpin_page(h->pool, current, false);
    }

    // 3. Build internal levels bottom-up
    i32 result = build_internals_from_leaves(h, leaf_pages, num_leaves);

    free(leaf_pages);
    return result;
}
```

### Build Internals from Leaves

```c
i32 build_internals_from_leaves(BTreeHandle *h, u32 *leaf_pages, u32 num_leaves) {
    if (num_leaves == 0) {
        return -1;
    }

    if (num_leaves == 1) {
        // Single leaf is root
        h->root_page = leaf_pages[0];
        return 0;
    }

    // Build internal levels bottom-up
    u32 *child_pages = leaf_pages;
    u32 num_children = num_leaves;
    u32 int_fill = (MAX_ENTS * 3) / 4;  // 75% fill factor

    while (num_children > 1) {
        u32 num_parents = (num_children + int_fill - 1) / int_fill;
        u32 *parent_pages = malloc(num_parents * sizeof(u32));

        u32 child_idx = 0;
        for (u32 i = 0; i < num_parents; i++) {
            // Allocate parent page
            parent_pages[i] = allocator_alloc_page(h->pool->allocator, INVALID_PAGE);

            PageFrame *frame = fetch_page(h->pool, parent_pages[i], LATCH_EXCLUSIVE);
            BTreeNode *parent = (BTreeNode*)frame->data;

            init_node(parent, BNODE_INT);

            // Set head_page (first child)
            parent->head_page = child_pages[child_idx++];

            // Add separator keys (extract from children)
            while (child_idx < num_children && parent->nkeys < int_fill) {
                BTreeKey sep_key = extract_first_key(h->pool, child_pages[child_idx]);
                insert_internal_entry_at_end(parent, &sep_key, child_pages[child_idx]);
                child_idx++;
            }

            unpin_page(h->pool, parent_pages[i], true);
        }

        // Move up one level
        if (child_pages != leaf_pages) {
            free(child_pages);
        }
        child_pages = parent_pages;
        num_children = num_parents;
    }

    // Root is the single remaining node
    h->root_page = child_pages[0];
    free(child_pages);

    return 0;
}
```

---

## 8. Bulk Loading

### Bottom-Up Construction

```c
i32 btree_bulk_load(BTreeHandle *h, Entry *sorted_entries, u32 num_entries) {
    // 1. Calculate structure
    u32 leaf_fill = (MAX_ENTS * 9) / 10;  // 90% fill factor
    u32 int_fill = (MAX_ENTS * 3) / 4;     // 75% fill factor

    u32 num_leaves = (num_entries + leaf_fill - 1) / leaf_fill;

    // 2. Build leaf level
    u32 *leaf_pages = malloc(num_leaves * sizeof(u32));
    u32 entry_idx = 0;

    for (u32 i = 0; i < num_leaves; i++) {
        // Allocate sequential pages (locality hint)
        u32 hint = (i == 0) ? INVALID_PAGE : leaf_pages[i - 1];
        leaf_pages[i] = allocator_alloc_page(h->pool->allocator, hint);

        PageFrame *frame = fetch_page(h->pool, leaf_pages[i], LATCH_EXCLUSIVE);
        BTreeNode *leaf = (BTreeNode*)frame->data;

        init_node(leaf, BNODE_LEAF);

        // Fill leaf to 90%
        u32 entries_in_leaf = MIN(leaf_fill, num_entries - entry_idx);
        for (u32 j = 0; j < entries_in_leaf; j++) {
            insert_entry_at_end(leaf, &sorted_entries[entry_idx++]);
        }

        // Link leaves
        leaf->next_page = (i < num_leaves - 1) ? leaf_pages[i + 1] : INVALID_PAGE;
        leaf->prev_page = (i > 0) ? leaf_pages[i - 1] : INVALID_PAGE;

        // Flush leaf immediately (durability)
        flush_page(h->pool, leaf_pages[i]);
        unpin_page(h->pool, leaf_pages[i], false);
    }

    // 3. Build internal levels bottom-up
    return build_internals_from_leaves(h, leaf_pages, num_leaves);
}
```

**Performance:**
- O(n) time for sorted input (vs O(n log n) for repeated inserts)
- Sequential I/O (pages allocated with locality hints)
- Optimal space utilization (90%/75% fill factors)

---

## 9. Range Scan Iterator

### Iterator Structure

```c
struct BTreeIterator {
    BTreeHandle *handle;

    // Current position
    u32 current_leaf;         // Current leaf page
    u16 current_slot;         // Current slot in leaf

    // Prefetch buffer (pin multiple pages ahead)
    PageFrame *pinned[PREFETCH_COUNT];
    u32 pinned_pages[PREFETCH_COUNT];
    u16 pinned_count;
    u16 pinned_index;         // Which pinned page we're on

    // Range bounds
    BTreeKey end_key;
    bool has_end;

    bool valid;
};

#define PREFETCH_COUNT 4      // Pin 4 pages ahead
```

### Iterator Initialization

```c
i32 btree_iter_init(BTreeIterator *iter, BTreeHandle *h,
                    BTreeKey *start_key, BTreeKey *end_key) {
    iter->handle = h;
    iter->valid = true;

    // Find start leaf
    iter->current_leaf = find_leaf(h, start_key);
    if (iter->current_leaf == INVALID_PAGE) {
        iter->valid = false;
        return -1;
    }

    // Find start slot within leaf
    PageFrame *frame = fetch_page(h->pool, iter->current_leaf, LATCH_SHARED);
    BTreeNode *leaf = (BTreeNode*)frame->data;
    iter->current_slot = find_slot_gte(leaf, start_key);
    unpin_page(h->pool, iter->current_leaf, false);

    // Set end key
    if (end_key) {
        memcpy(&iter->end_key, end_key, sizeof(BTreeKey));
        iter->has_end = true;
    } else {
        iter->has_end = false;
    }

    // Prefetch initial pages
    iter->pinned_count = 0;
    iter->pinned_index = 0;
    prefetch_leaves(iter);

    return 0;
}
```

### Prefetch Pages

```c
void prefetch_leaves(BTreeIterator *iter) {
    u32 page = iter->current_leaf;

    for (u16 i = 0; i < PREFETCH_COUNT && page != INVALID_PAGE; i++) {
        iter->pinned[i] = fetch_page(iter->handle->pool, page, LATCH_SHARED);
        iter->pinned_pages[i] = page;
        iter->pinned_count++;

        BTreeNode *leaf = (BTreeNode*)iter->pinned[i]->data;
        page = leaf->next_page;
    }
}
```

### Iterator Next

```c
i32 btree_iter_next(BTreeIterator *iter, BTreeKey *key_out, void *value_out) {
    if (!iter->valid) {
        return -1;
    }

    PageFrame *frame = iter->pinned[iter->pinned_index];
    BTreeNode *leaf = (BTreeNode*)frame->data;

    // Check bounds
    if (iter->current_slot >= leaf->nkeys) {
        // Move to next leaf
        return advance_to_next_leaf(iter);
    }

    // Check end key
    Entry *entry = get_entry(leaf, iter->current_slot);
    if (iter->has_end && compare_keys(&entry->key, &iter->end_key) > 0) {
        iter->valid = false;
        return -1;  // Past end
    }

    // Read entry
    memcpy(key_out, &entry->key, sizeof(BTreeKey));
    read_entry_payload(entry, value_out);

    // Advance
    iter->current_slot++;

    return 0;
}
```

### Advance to Next Leaf

```c
i32 advance_to_next_leaf(BTreeIterator *iter) {
    iter->current_slot = 0;
    iter->pinned_index++;

    if (iter->pinned_index >= iter->pinned_count) {
        // Exhausted prefetch buffer, get next batch

        // Unpin old pages
        for (u16 i = 0; i < iter->pinned_count; i++) {
            unpin_page(iter->handle->pool, iter->pinned_pages[i], false);
        }

        // Get next leaf from last pinned page
        BTreeNode *last_leaf = (BTreeNode*)iter->pinned[iter->pinned_count - 1]->data;
        iter->current_leaf = last_leaf->next_page;

        if (iter->current_leaf == INVALID_PAGE) {
            iter->valid = false;
            return -1;  // End of scan
        }

        // Prefetch next batch
        iter->pinned_count = 0;
        iter->pinned_index = 0;
        prefetch_leaves(iter);
    }

    return 0;
}
```

**Benefits:**
- Amortize latch acquisition (once per 4 pages)
- Sequential I/O for cache misses
- Reduced contention (release old pages early)

---

## 10. Integration with Buffer Pool

### Fetch Node

```c
PageFrame* btree_fetch_node(BufferPool *pool, u32 page_num, LatchMode mode) {
    PageFrame *frame = fetch_page(pool, page_num, mode);

    // Validate node (basic sanity checks)
    BTreeNode *node = (BTreeNode*)frame->data;
    if (node->type != BNODE_INT && node->type != BNODE_LEAF) {
        // Corrupted node
        unpin_page(pool, page_num, false);
        return NULL;
    }

    return frame;
}
```

### Node Modification

```c
void btree_mark_dirty(BufferPool *pool, PageFrame *frame) {
    atomic_store(&frame->is_dirty, true);
}
```

Buffer pool handles:
- Dirty tracking
- Write-back on eviction
- fsync on explicit flush

---

## 11. Key Operations Summary

### Search

**Latching:** S-latch coupling
**Time:** O(log n)
**Concurrency:** Many concurrent readers

### Insert

**Latching:**
- Optimistic: S → X on leaf (if room)
- Pessimistic: SX on path, upgrade to X for link-in

**Time:** O(log n)
**Concurrency:** Readers can search during split (except brief X-latch)

### Delete

**Latching:** Similar to insert
**Time:** O(log n)
**Concurrency:** Similar to insert

### Range Scan

**Latching:** S-latch on prefetched pages
**Time:** O(log n + k) where k = result size
**Concurrency:** Readers can scan concurrently

### Bulk Load

**Latching:** X-latch during construction
**Time:** O(n) for sorted input
**Concurrency:** None (exclusive construction)

---

## 12. Crash Recovery Workflow

### On Startup

```
1. Open page store and buffer pool
2. Read schema catalog
3. For each index:
   a. Attempt to validate internal structure (future: check LSNs)
   b. If invalid or first boot: rebuild from leaves
4. Index ready for queries
```

### Rebuild Steps

```
1. Find first leaf (via schema metadata or scan)
2. Traverse leaf chain (prev/next pointers)
3. Collect all leaf page numbers
4. Build internal nodes bottom-up
5. Update root pointer in schema
```

**Why it works:**
- Leaves are flushed before parent updates
- Leaf chain is always consistent
- Internals are disposable (can be rebuilt)

---

## 13. Performance Characteristics

### Fan-out

| Level | Node Type | Entries/Page | Cumulative |
|-------|-----------|--------------|------------|
| 0 (leaf) | Primary | ~62 | 62 |
| 1 | Internal | ~105 | 6,510 |
| 2 | Internal | ~105 | 683,550 |
| 3 | Internal | ~105 | 71,772,750 |

**For 1M records:** 3 levels, 3 page fetches per search

### Concurrency

**Traditional B+tree (X-latch):**
- Split blocks all readers on path
- Low read throughput during writes

**With SX-latch:**
- Split allows concurrent readers (only brief X-latch for link-in)
- High read throughput even during writes

**Benchmark expectation:**
- Read throughput: 200K+ ops/sec (multi-threaded)
- Write throughput: 50K+ ops/sec (with splits)
- Mixed workload: Minimal read degradation during writes

---

## 14. Future Enhancements

### Write-Ahead Log (WAL)

- Add LSN to node header
- Log structural changes before applying
- ARIES-style recovery with redo/undo

### Prefix Compression

- Store common prefix once per node
- Reduces key size, increases fan-out

### Adaptive Split Points

- Track insert patterns (sequential vs random)
- Adjust split ratio dynamically

### Online Reorganization

- Defragment nodes in background
- Merge underfull nodes without blocking

---

## Summary

### Key Design Choices

✅ **SX-latch protocol** for industry-leading concurrency
✅ **Optimistic + pessimistic insert** for fast path
✅ **Leaf durability** via flush-before-update protocol
✅ **60/40 split** for sequential insert optimization
✅ **Prefetching iterator** with 4-page lookahead
✅ **Bottom-up bulk load** for O(n) construction
✅ **Crash recovery** via leaf chain rebuild
✅ **Unpinned root** for TLB efficiency

### Concurrency Summary

- **Search:** S-latch only, no blocking
- **Insert:** SX-latch on path, brief X-latch for link-in
- **Delete:** Similar to insert
- **Readers during split:** Fully supported (key innovation!)

### Durability Summary

- **Leaves:** Flushed before parent updates
- **Internals:** Lazy flush, rebuilt on crash
- **Recovery:** Always rebuild from leaves (simple, correct)

---

## Next Steps

With all three layers designed:
1. ✅ **Page Management** (buffer pool, allocator)
2. ✅ **Schema & Records** (heap, keys, overflow)
3. ✅ **BTree Index** (SX-latch, durability, bulk load)

Ready for implementation!
