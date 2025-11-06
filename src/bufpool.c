#include "bufpool.h"

#include <stdlib.h>
#include <string.h>

#include "pagestore.h"
#include "rwsxlock.h"
#include "shtable.h"
#include "utils.h"

static struct PageFrame *cold_load_page(struct BufPool *bp, u32 page_num, LatchMode mode);
static u32 find_victim_clock(struct BufPool *bp);

struct BufPool *bpool_init(struct PageStore *store) {
    struct BufPool *bp = calloc(1, sizeof(struct BufPool));

    bp->store = store;
    bp->clock_hand = 0;
    bp->index = sht_init(2 * POOL_SIZE);

    memset(bp->tlb, 0xFF, sizeof(u32) * POOL_SIZE);
    memset(bp->frames, 0, sizeof(struct PageFrame) * POOL_SIZE);

    rwsx_init(&bp->latch);

    for (u32 i = 0; i < POOL_SIZE; i++) {
        rwsx_init(&bp->frames[i].latch);
    }

    return bp;
}

struct PageFrame *bpool_fetch_page(struct BufPool *bp, u32 page_num, LatchMode mode) {
    rwsx_lock(&bp->latch, LATCH_SHARED);
    u32 frame_idx;
    if (sht_get(bp->index, page_num, &frame_idx) != -1) {
        struct PageFrame *frame = &bp->frames[frame_idx];

        FADD(&frame->pin_cnt, 1, RELEASE);
        STORE(&frame->clock_bit, 1, RELEASE);

        rwsx_unlock(&bp->latch, LATCH_SHARED);
        rwsx_lock(&frame->latch, mode);
        return frame;
    }

    rwsx_unlock(&bp->latch, LATCH_SHARED);
    return cold_load_page(bp, page_num, mode);
}

void bpool_unpin_page(struct BufPool *bp, u32 page_num, bool is_dirty) {
    u32 frame_idx;
    if (sht_get(bp->index, page_num, &frame_idx) != -1) {
        struct PageFrame *frame = &bp->frames[frame_idx];

        if (is_dirty) {
            STORE(&frame->is_dirty, true, RELEASE);
        }
        FSUB(&frame->pin_cnt, 1, RELEASE);
    }
}

void bpool_flush_page(struct BufPool *bp, u32 page_num) {
    rwsx_lock(&bp->latch, LATCH_SHARED);
    u32 frame_idx;
    if (sht_get(bp->index, page_num, &frame_idx) != -1) {
        struct PageFrame *frame = &bp->frames[frame_idx];
        FADD(&frame->pin_cnt, 1, RELEASE);
        rwsx_unlock(&bp->latch, LATCH_SHARED);
        rwsx_lock(&frame->latch, LATCH_SHARED);

        if (LOAD(&frame->is_dirty, ACQUIRE)) {
            pstore_write(bp->store, page_num, frame->data);
            STORE(&frame->is_dirty, false, RELEASE);
        }

        rwsx_unlock(&frame->latch, LATCH_SHARED);
        FSUB(&frame->pin_cnt, 1, RELEASE);
        return;
    }
    rwsx_unlock(&bp->latch, LATCH_SHARED);
}

void bpool_flush_all(struct BufPool *bp) {
    rwsx_lock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
    for (u32 i = 0; i < POOL_SIZE; i++) {
        u32 page_num = LOAD(&bp->tlb[i], ACQUIRE);
        if (page_num != INVALID_PAGE) {
            struct PageFrame *frame = &bp->frames[i];

            rwsx_lock(&frame->latch, LATCH_SHARED);

            if (LOAD(&frame->is_dirty, ACQUIRE)) {
                pstore_write(bp->store, page_num, frame->data);
                STORE(&frame->is_dirty, false, RELEASE);
            }

            rwsx_unlock(&frame->latch, LATCH_SHARED);
        }
    }
    rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
}

void bpool_destroy(struct BufPool *bp) {
    bpool_flush_all(bp);

    for (u32 i = 0; i < POOL_SIZE; i++) {
        rwsx_destroy(&bp->frames[i].latch);
    }

    rwsx_destroy(&bp->latch);
    sht_destroy(bp->index);
    free(bp);
}

static struct PageFrame *cold_load_page(struct BufPool *bp, u32 page_num, LatchMode mode) {
    rwsx_lock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
    // Check again to ensure that the page_num isn't pulled by others
    u32 frame_idx;
    if (sht_get(bp->index, page_num, &frame_idx) != -1) {
        struct PageFrame *frame = &bp->frames[frame_idx];

        FADD(&frame->pin_cnt, 1, RELEASE);
        STORE(&frame->clock_bit, 1, RELEASE);

        rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
        rwsx_lock(&frame->latch, mode);
        return frame;
    }

    u32 victim = find_victim_clock(bp);
    if (victim == INVALID_PAGE) {
        rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
        return NULL;
    }

    struct PageFrame *frame = &bp->frames[victim];

    u32 old_page = LOAD(&bp->tlb[victim], ACQUIRE);

    if (old_page != INVALID_PAGE && LOAD(&frame->is_dirty, ACQUIRE)) {
        pstore_write(bp->store, old_page, frame->data);
    }

    u8 buf[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    if (pstore_read(bp->store, page_num, buf) < 0) {
        rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
        return NULL;
    }
    memcpy(frame->data, buf, PAGE_SIZE);

    if (old_page != INVALID_PAGE) {
        sht_unset(bp->index, old_page);
    }
    STORE(&frame->pin_cnt, 1, RELEASE);
    STORE(&frame->clock_bit, 1, RELEASE);
    STORE(&frame->is_dirty, false, RELEASE);

    sht_set(bp->index, page_num, victim);
    STORE(&bp->tlb[victim], page_num, RELEASE);

    rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
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
