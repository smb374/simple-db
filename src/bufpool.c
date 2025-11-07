#include "bufpool.h"

#include <stdlib.h>
#include <string.h>

#include "cqueue.h"
#include "pagestore.h"
#include "rwsxlock.h"
#include "shtable.h"
#include "utils.h"

static struct PageFrame *cold_load_page(struct BufPool *bp, u32 page_num, LatchMode mode);

struct BufPool *bpool_init(struct PageStore *store) {
    struct BufPool *bp = calloc(1, sizeof(struct BufPool) + sizeof(u32) * POOL_SIZE);

    bp->tlb = (void *) bp->data;
    memset(bp->tlb, 0xFF, sizeof(u32) * POOL_SIZE);

    bp->store = store;
    bp->index = sht_init(POOL_SIZE + PAGE_SIZE / 2); // MAX load factor = 75%
    bp->gindex = sht_init(POOL_SIZE + PAGE_SIZE / 2);
    bp->frames = calloc(POOL_SIZE, sizeof(struct PageFrame));
    bp->fnodes = calloc(POOL_SIZE, sizeof(struct FNode));
    bp->gnodes = calloc(POOL_SIZE, sizeof(struct GNode));

    bp->tlb_cursor = 0;
    cq_init(&bp->qd, QD_SIZE);
    cq_init(&bp->main, MAIN_SIZE);
    cq_init(&bp->ghost, POOL_SIZE);

    rwsx_init(&bp->latch);

    for (u32 i = 0; i < POOL_SIZE; i++) {
        rwsx_init(&bp->frames[i].latch);
        bp->gnodes[i].page_num = INVALID_PAGE;
        bp->gnodes[i].gidx = i;
        bp->fnodes[i].fidx = i;
    }

    return bp;
}

struct PageFrame *bpool_fetch_page(struct BufPool *bp, u32 page_num, LatchMode mode) {
    rwsx_lock(&bp->latch, LATCH_SHARED);
    u32 frame_idx;
    if (sht_get(bp->index, page_num, &frame_idx) != -1) {
        struct PageFrame *frame = &bp->frames[frame_idx];

        FADD(&frame->pin_cnt, 1, RELEASE);
        STORE(&frame->visited, true, RELEASE);

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
        FADD(&frame->pin_cnt, 1, RELEASE); // tmp pin.
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
    cq_destroy(&bp->qd);
    cq_destroy(&bp->main);
    cq_destroy(&bp->ghost);
    sht_destroy(bp->index);
    sht_destroy(bp->gindex);
    free(bp->frames);
    free(bp->fnodes);
    free(bp->gnodes);
    free(bp);
}

static u32 find_victim_qdlp(struct BufPool *bp) {
    u32 start = LOAD(&bp->tlb_cursor, ACQUIRE);
    for (u32 i = start; i < POOL_SIZE; i++) {
        if (LOAD(&bp->tlb[i], ACQUIRE) == INVALID_PAGE) {
            STORE(&bp->tlb_cursor, i + 1, RELEASE);
            return i;
        }
    }

    u32 qd_size = cq_size(&bp->qd);
    for (u32 i = 0; i < qd_size; i++) {
        struct CNode *n = cq_pop(&bp->qd);
        if (!n)
            break;

        struct FNode *fn = container_of(n, struct FNode, node);
        u32 frame_idx = fn->fidx;
        struct PageFrame *frame = &bp->frames[frame_idx];

        if (LOAD(&frame->visited, ACQUIRE)) {
            STORE(&frame->visited, false, RELEASE);
            cq_put(&bp->main, &fn->node);
            continue;
        } else {
            if (LOAD(&frame->pin_cnt, ACQUIRE) > 0) {
                cq_put(&bp->qd, &fn->node);
                continue;
            }
            return frame_idx;
        }
    }

    u32 main_size = cq_size(&bp->main);
    for (u32 i = 0; i < main_size; i++) {
        struct CNode *n = cq_pop(&bp->main);
        if (!n)
            break;

        struct FNode *fn = container_of(n, struct FNode, node);
        u32 frame_idx = fn->fidx;
        struct PageFrame *frame = &bp->frames[frame_idx];

        if (LOAD(&frame->visited, ACQUIRE)) {
            STORE(&frame->visited, false, RELEASE);
            cq_put(&bp->main, &fn->node);
            continue;
        } else {
            if (LOAD(&frame->pin_cnt, ACQUIRE) > 0) {
                cq_put(&bp->main, &fn->node);
                continue;
            }
            return frame_idx;
        }
    }

    return INVALID_PAGE;
}

static struct GNode *find_free_ghost(struct BufPool *bp) {
    for (u32 i = 0; i < POOL_SIZE; i++) {
        if (bp->gnodes[i].page_num == INVALID_PAGE) {
            return &bp->gnodes[i];
        }
    }
    return NULL;
}

static void reclaim_ghost(struct BufPool *bp) {
    if (cq_size(&bp->ghost) >= POOL_SIZE) {
        struct CNode *n = cq_pop(&bp->ghost);
        if (n) {
            struct GNode *gn = container_of(n, struct GNode, node);
            sht_unset(bp->gindex, gn->page_num);
            gn->page_num = INVALID_PAGE; // Mark as free
        }
    }
}

static struct PageFrame *cold_load_page(struct BufPool *bp, u32 page_num, LatchMode mode) {
    if (page_num == INVALID_PAGE || page_num * PAGE_SIZE >= bp->store->store_size) {
        return NULL;
    }
    u8 target_queue;
    rwsx_lock(&bp->latch, LATCH_SHARED_EXCLUSIVE);

    // Double-check: page might have been loaded by another thread
    u32 frame_idx;
    if (sht_get(bp->index, page_num, &frame_idx) != -1) {
        struct PageFrame *frame = &bp->frames[frame_idx];
        FADD(&frame->pin_cnt, 1, RELEASE);
        rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
        rwsx_lock(&frame->latch, mode);
        return frame;
    }

    u32 gidx;
    bool in_ghost = sht_get(bp->gindex, page_num, &gidx) != -1;

    u32 victim = find_victim_qdlp(bp);
    if (victim == INVALID_PAGE) {
        rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
        return NULL;
    }

    struct PageFrame *frame = &bp->frames[victim];
    u32 old_page = LOAD(&bp->tlb[victim], ACQUIRE);
    u8 old_qtype = LOAD(&frame->qtype, ACQUIRE);

    if (old_page != INVALID_PAGE && old_qtype == QUEUE_QD) {
        reclaim_ghost(bp);
        struct GNode *gn = find_free_ghost(bp);
        if (gn) {
            gn->page_num = old_page;
            sht_set(bp->gindex, old_page, gn->gidx);
            cq_put(&bp->ghost, &gn->node);
        }
    }

    STORE(&frame->pin_cnt, 1, RELEASE);
    if (old_page != INVALID_PAGE) {
        sht_unset(bp->index, old_page);
    }
    STORE(&bp->tlb[victim], INVALID_PAGE, RELEASE);
    rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);

    bool was_dirty = (old_page != INVALID_PAGE) && LOAD(&frame->is_dirty, ACQUIRE);

    if (in_ghost) {
        target_queue = QUEUE_MAIN;
        sht_unset(bp->gindex, page_num);
    } else {
        target_queue = QUEUE_QD;
    }

    rwsx_lock(&frame->latch, LATCH_EXCLUSIVE);

    if (was_dirty) {
        pstore_write(bp->store, old_page, frame->data);
    }

    u8 buf[PAGE_SIZE];
    if (pstore_read(bp->store, page_num, buf) < 0) {
        STORE(&frame->pin_cnt, 0, RELEASE);
        rwsx_unlock(&frame->latch, LATCH_EXCLUSIVE);
        cq_put(&bp->qd, &bp->fnodes[victim].node);

        return NULL;
    }
    memcpy(frame->data, buf, PAGE_SIZE);

    STORE(&frame->is_dirty, false, RELEASE);
    STORE(&frame->visited, false, RELEASE);
    STORE(&frame->qtype, target_queue, RELEASE);

    if (target_queue == QUEUE_MAIN) {
        cq_put(&bp->main, &bp->fnodes[victim].node);
    } else {
        cq_put(&bp->qd, &bp->fnodes[victim].node);
    }

    rwsx_lock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
    sht_set(bp->index, page_num, victim); // Visible to index
    STORE(&bp->tlb[victim], page_num, RELEASE); // Visible to TLB
    rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);

    switch (mode) {
        case LATCH_SHARED:
            rwsx_unlock(&frame->latch, LATCH_EXCLUSIVE);
            rwsx_lock(&frame->latch, LATCH_SHARED);
            break;
        case LATCH_SHARED_EXCLUSIVE:
            rwsx_unlock(&frame->latch, LATCH_EXCLUSIVE);
            rwsx_lock(&frame->latch, LATCH_SHARED_EXCLUSIVE);
            break;
        case LATCH_EXCLUSIVE:
            break;
        default:
            rwsx_unlock(&frame->latch, LATCH_EXCLUSIVE);
            break;
    }

    return frame;
}
