#include "bufpool.h"
#include <stdlib.h>
#include <string.h>

#include "pagestore.h"
#include "rwsxlock.h"
#include "utils.h"

static struct PageFrame *cold_load_page(struct BufPool *bp, u32 page_num, u32 *idx_hint, LatchMode mode);
static u32 find_victim_clock(struct BufPool *bp);

struct BufPool *bpool_init(struct PageStore *store) {
    struct BufPool *bp = calloc(1, sizeof(struct BufPool));

    bp->store = store;
    bp->clock_hand = 0;

    memset(bp->tlb, 0, sizeof(u32) * POOL_SIZE);
    memset(bp->frames, 0, sizeof(struct PageFrame) * POOL_SIZE);

    rwsx_init(&bp->latch);

    for (u32 i = 0; i < POOL_SIZE; i++) {
        rwsx_init(&bp->frames[i].latch);
    }

    return bp;
}

struct PageFrame *bpool_fetch_page(struct BufPool *bp, u32 page_num, u32 *idx_hint, LatchMode mode) {
    rwsx_lock(&bp->latch, LATCH_SHARED);
    u32 start = (idx_hint && *idx_hint < POOL_SIZE) ? *idx_hint : 0;
    for (u32 i = 0; i < POOL_SIZE; i++) {
        u32 idx = (start + i) % POOL_SIZE;
        u32 cache = LOAD(&bp->tlb[idx], ACQUIRE);

        if (cache == page_num) {
            if (idx_hint) {
                *idx_hint = idx;
            }
            struct PageFrame *frame = &bp->frames[idx];
            FADD(&frame->pin_cnt, 1, RELEASE);
            STORE(&frame->clock_bit, 1, RELEASE);

            rwsx_unlock(&bp->latch, LATCH_SHARED);

            rwsx_lock(&frame->latch, mode);
            return frame;
        }
    }

    rwsx_unlock(&bp->latch, LATCH_SHARED);
    return cold_load_page(bp, page_num, idx_hint, mode);
}

void bpool_unpin_page(struct BufPool *bp, u32 page_num, u32 *idx_hint, bool is_dirty) {
    rwsx_lock(&bp->latch, LATCH_SHARED);
    u32 start = (idx_hint && *idx_hint < POOL_SIZE) ? *idx_hint : 0;
    for (u32 i = 0; i < POOL_SIZE; i++) {
        u32 idx = (start + i) % POOL_SIZE;
        if (LOAD(&bp->tlb[idx], ACQUIRE) == page_num) {
            if (idx_hint) {
                *idx_hint = idx;
            }
            struct PageFrame *frame = &bp->frames[idx];

            if (is_dirty) {
                STORE(&frame->is_dirty, true, RELEASE);
            }

            FSUB(&frame->pin_cnt, 1, RELEASE);
            rwsx_unlock(&bp->latch, LATCH_SHARED);
            return;
        }
    }
    rwsx_unlock(&bp->latch, LATCH_SHARED);
}

void bpool_flush_page(struct BufPool *bp, u32 page_num, u32 *idx_hint) {
    rwsx_lock(&bp->latch, LATCH_SHARED);
    u32 start = (idx_hint && *idx_hint < POOL_SIZE) ? *idx_hint : 0;
    for (u32 i = 0; i < POOL_SIZE; i++) {
        u32 idx = (start + i) % POOL_SIZE;
        if (LOAD(&bp->tlb[idx], ACQUIRE) == page_num) {
            if (idx_hint) {
                *idx_hint = idx;
            }
            struct PageFrame *frame = &bp->frames[idx];
            if (LOAD(&frame->is_dirty, ACQUIRE)) {
                pstore_write(bp->store, page_num, frame->data);
                STORE(&frame->is_dirty, false, RELEASE);
            }

            rwsx_unlock(&bp->latch, LATCH_SHARED);
            return;
        }
    }
    rwsx_unlock(&bp->latch, LATCH_SHARED);
}

void bpool_flush_all(struct BufPool *bp) {
    rwsx_lock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
    for (u32 i = 0; i < POOL_SIZE; i++) {
        u32 page_num = LOAD(&bp->tlb[i], ACQUIRE);
        if (page_num != INVALID_PAGE) {
            struct PageFrame *frame = &bp->frames[i];
            if (LOAD(&frame->is_dirty, ACQUIRE)) {
                pstore_write(bp->store, page_num, frame->data);
                STORE(&frame->is_dirty, false, RELEASE);
            }
        }
    }
    rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
}

void bpool_destroy(struct BufPool *bp) {
    for (u32 i = 0; i < POOL_SIZE; i++) {
        u32 page_num = LOAD(&bp->tlb[i], ACQUIRE);
        if (page_num != INVALID_PAGE) {
            struct PageFrame *frame = &bp->frames[i];
            if (LOAD(&frame->is_dirty, ACQUIRE)) {
                pstore_write(bp->store, page_num, frame->data);
            }
            rwsx_destroy(&frame->latch);
        }
    }

    rwsx_destroy(&bp->latch);

    free(bp);
}

static struct PageFrame *cold_load_page(struct BufPool *bp, u32 page_num, u32 *idx_hint, LatchMode mode) {
    rwsx_lock(&bp->latch, LATCH_EXCLUSIVE);

    u32 victim = find_victim_clock(bp);
    if (victim == INVALID_PAGE) {
        rwsx_unlock(&bp->latch, LATCH_EXCLUSIVE);
        return NULL;
    }
    if (idx_hint) {
        *idx_hint = victim;
    }
    struct PageFrame *frame = &bp->frames[victim];


    u32 old_page = LOAD(&bp->tlb[victim], ACQUIRE);
    if (old_page != INVALID_PAGE && LOAD(&frame->is_dirty, ACQUIRE)) {
        pstore_write(bp->store, old_page, frame->data);
    }

    pstore_read(bp->store, page_num, frame->data);

    STORE(&bp->tlb[victim], page_num, RELEASE);

    STORE(&frame->pin_cnt, 1, RELEASE);
    STORE(&frame->clock_bit, 1, RELEASE);
    STORE(&frame->is_dirty, false, RELEASE);

    rwsx_unlock(&bp->latch, LATCH_EXCLUSIVE);

    rwsx_lock(&frame->latch, mode);

    return frame;
}

static u32 find_victim_clock(struct BufPool *bp) {
    u32 start = LOAD(&bp->clock_hand, ACQUIRE);

    for (u32 i = 0; i < POOL_SIZE * 2; i++) {
        u32 idx = (start + i) % POOL_SIZE;
        struct PageFrame *frame = &bp->frames[idx];

        if (LOAD(&frame->pin_cnt, ACQUIRE) > 0) {
            continue;
        }

        if (LOAD(&frame->clock_bit, ACQUIRE) == 1) {
            STORE(&frame->clock_bit, 0, RELEASE);
            continue;
        }

        STORE(&bp->clock_hand, (idx + 1) % POOL_SIZE, RELEASE);

        return idx;
    }

    return INVALID_PAGE;
}
