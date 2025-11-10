#include "alloc.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "bufpool.h"
#include "pagestore.h"
#include "rwsxlock.h"
#include "utils.h"

static inline struct GroupDesc *get_gdesc(struct PageAllocator *pa, const u32 page_num) {
    const u32 gidx = (page_num - HEAD_OFFSET) / GROUP_SIZE;
    return &pa->gdt_cache[gidx / GDT_DESCRIPTORS].descriptors[gidx % GDT_DESCRIPTORS];
}

static i32 verify_checksum(struct PageAllocator *pa) {
    u32 sb_crc = crc32c((u8 *) pa->sb_cache, SB_CHKSUM_OFF);
    if (sb_crc != pa->sb_cache->sb_checksum) {
        return -1;
    }

    for (u32 i = 0; i < GDT_PAGES; i++) {
        u32 gdt_crc = crc32c((u8 *) &pa->gdt_cache[i], PAGE_SIZE);
        if (gdt_crc != pa->sb_cache->gdt_checksum[i])
            return -1;
    }

    return 0;
}

static void calculate_checksum(struct PageAllocator *pa) {
    for (u32 i = 0; i < GDT_PAGES; i++) {
        pa->sb_cache->gdt_checksum[i] = crc32c((u8 *) &pa->gdt_cache[i], PAGE_SIZE);
    }

    pa->sb_cache->sb_checksum = crc32c((u8 *) pa->sb_cache, SB_CHKSUM_OFF);
}

// Find and claim an empty spot in group, retuns relative page to group start page.
static u32 find_and_claim_page_group(struct PageAllocator *pa, const u32 gidx) {
    u32 res = INVALID_PAGE;
    struct GroupDesc *desc = &pa->gdt_cache[gidx / GDT_DESCRIPTORS].descriptors[gidx % GDT_DESCRIPTORS];
    if (LOAD(&desc->free_pages, ACQUIRE) == 0)
        return INVALID_PAGE;

    const u32 gstart = desc->start;
    struct FrameHandle *h1 = bpool_fetch_page(pa->pool, gstart);
    struct FrameHandle *h2 = bpool_fetch_page(pa->pool, gstart + 1);
    assert(h1 != NULL && h2 != NULL);

    struct BitmapPage *bp1 = handle_data(h1);
    struct BitmapPage *bp2 = handle_data(h2);

    u32 start_mask = LOAD(&desc->last_set, ACQUIRE);
    u32 mask_idx = 0;
    for (u32 i = 0; i < 2 * BITMAPS_PER_PAGE; i++) {
        mask_idx = (start_mask + i) % (2 * BITMAPS_PER_PAGE);
        atomic_u64 *slot =
                (mask_idx >= BITMAPS_PER_PAGE) ? &bp2->bitmap[mask_idx - BITMAPS_PER_PAGE] : &bp1->bitmap[mask_idx];

        u64 mask = LOAD(slot, ACQUIRE);
        while (mask != 0xFFFFFFFFFFFFFFFF) {
            i32 bit = ffz(mask);
            assert(bit >= 0);

            u64 prev = FOR(slot, 1ULL << bit, ACQ_REL);
            if (!(prev & (1ULL << bit))) {
                res = mask_idx * 64 + bit;
                goto END;
            }
            mask = LOAD(slot, ACQUIRE);
        }
    }


END:
    if (res != INVALID_PAGE) {
        bpool_mark_write(pa->pool, mask_idx >= BITMAPS_PER_PAGE ? h2 : h1);
        FSUB(&desc->free_pages, 1, RELEASE);
        STORE(&desc->last_set, mask_idx, RELEASE);
    }

    bpool_release_handle(pa->pool, h1);
    bpool_release_handle(pa->pool, h2);

    return res;
}

static i32 sync_blocks(struct PageAllocator *pa) {
    calculate_checksum(pa);

    if (pstore_write(pa->store, 0, pa->sb_cache) < 0)
        return -1;

    for (u32 i = 0; i < GDT_PAGES; i++) {
        if (pstore_write(pa->store, GDT_START + i, &pa->gdt_cache[i]) < 0)
            return -1;
    }
    return 0;
}

static i32 grow(struct PageAllocator *pa) {
    u32 total_pages = LOAD(&pa->sb_cache->total_pages, ACQUIRE);
    rwsx_lock(&pa->latch, LATCH_SHARED_EXCLUSIVE);
    if (LOAD(&pa->sb_cache->total_pages, ACQUIRE) > total_pages) {
        rwsx_unlock(&pa->latch, LATCH_SHARED_EXCLUSIVE);
        return 0;
    }

    if (pstore_grow(pa->store, GROUP_SIZE) < 0) {
        rwsx_unlock(&pa->latch, LATCH_SHARED_EXCLUSIVE);
        return -1;
    }

    u32 start = LOAD(&pa->sb_cache->total_pages, ACQUIRE);
    u32 group = LOAD(&pa->sb_cache->total_groups, ACQUIRE);

    u32 gdt_idx = group / GDT_DESCRIPTORS;
    u32 didx = group % GDT_DESCRIPTORS;

    pa->gdt_cache[gdt_idx].descriptors[didx].start = start;
    pa->gdt_cache[gdt_idx].descriptors[didx].free_pages = GROUP_SIZE - GROUP_BITMAPS;

    struct FrameHandle *h1 = bpool_fetch_page(pa->pool, start);
    struct FrameHandle *h2 = bpool_fetch_page(pa->pool, start + 1);
    struct BitmapPage *bp1 = handle_data(h1);
    struct BitmapPage *bp2 = handle_data(h2);

    memset(bp1->bitmap, 0, PAGE_SIZE);
    memset(bp2->bitmap, 0, PAGE_SIZE);

    bp1->bitmap[0] = 0x3;

    bpool_mark_write(pa->pool, h1);
    bpool_mark_write(pa->pool, h2);
    bpool_flush_page(pa->pool, h1->page_num);
    bpool_flush_page(pa->pool, h2->page_num);
    bpool_release_handle(pa->pool, h1);
    bpool_release_handle(pa->pool, h2);

    FADD(&pa->sb_cache->total_pages, GROUP_SIZE, ACQ_REL);
    FADD(&pa->sb_cache->total_groups, 1, ACQ_REL);
    i32 ret = sync_blocks(pa);
    rwsx_unlock(&pa->latch, LATCH_SHARED_EXCLUSIVE);
    return ret;
}

static i32 init_pa(struct PageAllocator *pa) {
    struct SuperBlock *sb = pa->sb_cache;

    sb->magic = MAGIC;
    sb->version = VERSION;
    sb->page_size = PAGE_SIZE;

    sb->total_pages = INITIAL_PAGES;
    sb->total_groups = 1;

    sb->gdt_start = GDT_START;
    sb->gdt_pages = GDT_PAGES;
    sb->catalog_page = CATALOG_PAGE;

    for (u32 i = 0; i < GDT_PAGES; i++) {
        for (u32 j = 0; j < GDT_DESCRIPTORS; j++) {
            pa->gdt_cache[i].descriptors[j].start = INVALID_PAGE;
        }
    }

    pa->gdt_cache[0].descriptors[0].start = HEAD_OFFSET;
    pa->gdt_cache[0].descriptors[0].free_pages = GROUP_SIZE - GROUP_BITMAPS;


    struct FrameHandle *h1 = bpool_fetch_page(pa->pool, HEAD_OFFSET);
    struct FrameHandle *h2 = bpool_fetch_page(pa->pool, HEAD_OFFSET + 1);
    struct BitmapPage *bp1 = handle_data(h1);
    struct BitmapPage *bp2 = handle_data(h2);

    memset(bp1->bitmap, 0, PAGE_SIZE);
    memset(bp2->bitmap, 0, PAGE_SIZE);

    bp1->bitmap[0] = 0x3;

    bpool_mark_write(pa->pool, h1);
    bpool_mark_write(pa->pool, h2);
    bpool_flush_page(pa->pool, h1->page_num);
    bpool_flush_page(pa->pool, h2->page_num);
    bpool_release_handle(pa->pool, h1);
    bpool_release_handle(pa->pool, h2);

    return sync_blocks(pa);
}

static i32 open_pa(struct PageAllocator *pa) {
    if (pstore_read(pa->store, 0, pa->sb_cache) < 0)
        return -1;

    for (u32 i = 0; i < GDT_PAGES; i++) {
        if (pstore_read(pa->store, GDT_START + i, &pa->gdt_cache[i]) < 0)
            return -1;
    }

    // Fast check MAGIC and VERSION
    if (pa->sb_cache->magic != MAGIC || pa->sb_cache->version != VERSION)
        return -1;
    // Check size
    if (pa->store->store_size < pa->sb_cache->total_pages * PAGE_SIZE)
        return -1;
    if (pa->sb_cache->total_groups * GROUP_SIZE + HEAD_OFFSET != pa->sb_cache->total_pages)
        return -1;
    // Verify Checksum
    if (verify_checksum(pa) < 0)
        return -1;

    return 0;
}

struct PageAllocator *allocator_init(struct BufPool *pool, bool create) {
    struct PageAllocator *pa = calloc(1, sizeof(struct PageAllocator));
    pa->pool = pool;
    pa->store = pool->store;
    pa->last_group = 0;
    rwsx_init(&pa->latch);
    pa->sb_cache = calloc(1, sizeof(struct SuperBlock));
    pa->gdt_cache = calloc(GDT_PAGES, sizeof(struct GDTPage));

    if (create) {
        if (init_pa(pa) < 0)
            goto error;
    } else {
        if (open_pa(pa) < 0)
            goto error;
    }

    return pa;

error:
    rwsx_destroy(&pa->latch);
    free(pa->sb_cache);
    free(pa->gdt_cache);
    free(pa);
    return NULL;
}

void allocator_destroy(struct PageAllocator *pa) {
    sync_blocks(pa);

    rwsx_destroy(&pa->latch);
    free(pa->sb_cache);
    free(pa->gdt_cache);
    free(pa);
}

u32 alloc_page(struct PageAllocator *pa, u32 hint) {
    u32 start;
    if (hint == 0 || hint == INVALID_PAGE || hint < HEAD_OFFSET) {
        start = LOAD(&pa->last_group, ACQUIRE);
    } else {
        start = (hint - HEAD_OFFSET) / GROUP_SIZE;
    }

    const u32 total_groups = LOAD(&pa->sb_cache->total_groups, ACQUIRE);
    for (u32 i = 0; i < total_groups; i++) {
        u32 gidx = (start + i) % total_groups;

        u32 pidx = find_and_claim_page_group(pa, gidx);
        if (pidx != INVALID_PAGE) {
            u32 page_num = HEAD_OFFSET + gidx * GROUP_SIZE + pidx;
            STORE(&pa->last_group, gidx, RELEASE);
            return page_num;
        }
    }

    if (grow(pa) < 0)
        return INVALID_PAGE;

    return alloc_page(pa, HEAD_OFFSET + total_groups * GROUP_SIZE);
}

void free_page(struct PageAllocator *pa, u32 page_num) {
    if (page_num >= LOAD(&pa->sb_cache->total_pages, ACQUIRE)) {
        return;
    }
    u32 rpage = page_num - HEAD_OFFSET;
    u32 pidx = rpage % GROUP_SIZE;
    u32 mask_idx = pidx / 64;
    u32 bit = pidx % 64;

    struct GroupDesc *desc = get_gdesc(pa, page_num);
    const u32 start = desc->start;

    if (mask_idx >= BITMAPS_PER_PAGE) {
        struct FrameHandle *h = bpool_fetch_page(pa->pool, start + 1);
        struct BitmapPage *bp = handle_data(h);
        FAND(&bp->bitmap[mask_idx - BITMAPS_PER_PAGE], ~(1ULL << bit), ACQ_REL);
        bpool_mark_write(pa->pool, h);
        bpool_release_handle(pa->pool, h);
    } else {
        struct FrameHandle *h = bpool_fetch_page(pa->pool, start);
        struct BitmapPage *bp = handle_data(h);
        FAND(&bp->bitmap[mask_idx], ~(1ULL << bit), ACQ_REL);
        bpool_mark_write(pa->pool, h);
        bpool_release_handle(pa->pool, h);
    }

    STORE(&desc->last_set, mask_idx, RELEASE);
    FADD(&desc->free_pages, 1, RELEASE);
}
