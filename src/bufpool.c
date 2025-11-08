#include "bufpool.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "cqueue.h"
#include "pagestore.h"
#include "rwsxlock.h"
#include "shtable.h"
#include "utils.h"

static struct PageFrame *cold_load_page(struct BufPool *bp, u32 page_num);

static struct FrameHandle *create_handle(struct FrameData *fdata, const u32 epoch, const u32 frame_idx,
                                         const u32 page_num) {
    struct FrameHandle *h = calloc(1, sizeof(struct FrameHandle));

    h->fdata = fdata;
    h->epoch = epoch;
    h->frame_idx = frame_idx;
    h->page_num = page_num;
    return h;
}

struct BufPool *bpool_init(struct PageStore *store) {
    struct BufPool *bp = calloc(1, sizeof(struct BufPool) + sizeof(u32) * POOL_SIZE);

    bp->store = store;
    bp->index = sht_init(POOL_SIZE + PAGE_SIZE / 2); // MAX load factor = 75%
    bp->gindex = sht_init(POOL_SIZE + PAGE_SIZE / 2);
    bp->frames = calloc(POOL_SIZE, sizeof(struct PageFrame));
    bp->fnodes = calloc(POOL_SIZE, sizeof(struct FNode));
    bp->gnodes = calloc(POOL_SIZE, sizeof(struct GNode));

    bp->warmup_cursor = 0;
    cq_init(&bp->qd, QD_SIZE);
    cq_init(&bp->main, MAIN_SIZE);
    cq_init(&bp->ghost, POOL_SIZE);

    rwsx_init(&bp->latch);

    for (u32 i = 0; i < POOL_SIZE; i++) {
        rwsx_init(&bp->frames[i].fdata.latch);
        bp->frames[i].page_num = INVALID_PAGE;
        bp->gnodes[i].page_num = INVALID_PAGE;
        bp->gnodes[i].gidx = i;
        bp->fnodes[i].fidx = i;
    }

    return bp;
}

void bpool_destroy(struct BufPool *bp) {
    bpool_flush_all(bp);

    for (u32 i = 0; i < POOL_SIZE; i++) {
        rwsx_destroy(&bp->frames[i].fdata.latch);
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

struct FrameHandle *bpool_fetch_page(struct BufPool *bp, u32 page_num) {
    if (page_num == INVALID_PAGE) {
        return NULL;
    }
    rwsx_lock(&bp->latch, LATCH_SHARED);
    u32 frame_idx;
    if (sht_get(bp->index, page_num, &frame_idx) != -1) {
        struct PageFrame *frame = &bp->frames[frame_idx];

        // Pin first
        FADD(&frame->pin_cnt, 1, RELEASE);
        // Memory barrier to ensure pin is visible before epoch read
        atomic_thread_fence(memory_order_acquire);

        u32 epoch = LOAD(&frame->epoch, ACQUIRE);
        u32 current_page = LOAD(&frame->page_num, ACQUIRE);

        // Verify page still matches
        if (current_page != page_num) {
            // Page was evicted between lookup and pin
            FSUB(&frame->pin_cnt, 1, RELEASE);
            rwsx_unlock(&bp->latch, LATCH_SHARED);
            // Retry as cold load
            return bpool_fetch_page(bp, page_num); // Tail recursion
        }

        STORE(&frame->visited, true, RELEASE);
        rwsx_unlock(&bp->latch, LATCH_SHARED);
        return create_handle(&frame->fdata, epoch, frame_idx, page_num);
    }

    rwsx_unlock(&bp->latch, LATCH_SHARED);
    struct PageFrame *frame = cold_load_page(bp, page_num);
    if (frame) {
        frame_idx = frame - bp->frames;
        return create_handle(&frame->fdata, LOAD(&frame->epoch, ACQUIRE), frame_idx, page_num);
    }
    return NULL;
}

i32 bpool_mark_read(struct BufPool *bp, struct FrameHandle *h) {
    struct PageFrame *frame = &bp->frames[h->frame_idx];

    if (LOAD(&frame->epoch, ACQUIRE) != h->epoch) {
        return -1;
    }

    STORE(&frame->visited, true, RELEASE);

    return 0;
}

i32 bpool_mark_write(struct BufPool *bp, struct FrameHandle *h) {
    struct PageFrame *frame = &bp->frames[h->frame_idx];

    if (LOAD(&frame->epoch, ACQUIRE) != h->epoch) {
        return -1;
    }

    STORE(&frame->visited, true, RELEASE);
    STORE(&frame->is_dirty, true, RELEASE);

    return 0;
}

i32 bpool_release_handle(struct BufPool *bp, struct FrameHandle *h) {
    struct PageFrame *frame = &bp->frames[h->frame_idx];

    if (LOAD(&frame->epoch, ACQUIRE) != h->epoch) {
        return -1;
    }

    FSUB(&frame->pin_cnt, 1, RELEASE);
    free(h);

    return 0;
}

i32 bpool_flush_page(struct BufPool *bp, u32 page_num) {
    if (page_num == INVALID_PAGE) {
        return -1;
    }
    rwsx_lock(&bp->latch, LATCH_SHARED);
    u32 frame_idx;
    if (sht_get(bp->index, page_num, &frame_idx) != -1) {
        i32 ret = 0;
        struct PageFrame *frame = &bp->frames[frame_idx];
        FADD(&frame->pin_cnt, 1, RELEASE); // tmp pin.
        rwsx_unlock(&bp->latch, LATCH_SHARED);
        rwsx_lock(&frame->fdata.latch, LATCH_SHARED);

        if (LOAD(&frame->is_dirty, ACQUIRE)) {
            if (pstore_write(bp->store, page_num, frame->fdata.data) < 0) {
                rwsx_unlock(&frame->fdata.latch, LATCH_SHARED);
                return -1;
            }
            STORE(&frame->is_dirty, false, RELEASE);
        }

        rwsx_unlock(&frame->fdata.latch, LATCH_SHARED);
        FSUB(&frame->pin_cnt, 1, RELEASE);
        return ret;
    }
    rwsx_unlock(&bp->latch, LATCH_SHARED);
    return -1;
}

i32 bpool_flush_all(struct BufPool *bp) {
    rwsx_lock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
    for (u32 i = 0; i < POOL_SIZE; i++) {
        u32 page_num = LOAD(&bp->frames[i].page_num, ACQUIRE);
        if (page_num != INVALID_PAGE) {
            struct PageFrame *frame = &bp->frames[i];

            rwsx_lock(&frame->fdata.latch, LATCH_SHARED);

            if (LOAD(&frame->is_dirty, ACQUIRE)) {
                if (pstore_write(bp->store, page_num, frame->fdata.data) < 0) {
                    rwsx_unlock(&frame->fdata.latch, LATCH_SHARED);
                    rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);

                    return -1;
                }
                STORE(&frame->is_dirty, false, RELEASE);
            }

            rwsx_unlock(&frame->fdata.latch, LATCH_SHARED);
        }
    }
    rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
    return 0;
}

static u32 find_victim_qdlp(struct BufPool *bp) {
    u32 start = LOAD(&bp->warmup_cursor, ACQUIRE);
    for (u32 i = start; i < POOL_SIZE; i++) {
        if (LOAD(&bp->frames[i].page_num, ACQUIRE) == INVALID_PAGE) {
            STORE(&bp->warmup_cursor, i + 1, RELEASE);
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
        if (LOAD(&bp->gnodes[i].page_num, ACQUIRE) == INVALID_PAGE) {
            return &bp->gnodes[i];
        }
    }
    return NULL;
}

static void reclaim_ghost(struct BufPool *bp) {
    if (cq_size(&bp->ghost) > POOL_SIZE) {
        struct CNode *n = cq_pop(&bp->ghost);
        if (n) {
            struct GNode *gn = container_of(n, struct GNode, node);
            sht_unset(bp->gindex, gn->page_num);
            STORE(&gn->page_num, INVALID_PAGE, RELEASE);
        }
    }
}

static struct PageFrame *cold_load_page(struct BufPool *bp, u32 page_num) {
    if (page_num * PAGE_SIZE >= bp->store->store_size) {
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
    u32 old_page = LOAD(&frame->page_num, ACQUIRE);
    u8 old_qtype = LOAD(&frame->qtype, ACQUIRE);

    if (old_page != INVALID_PAGE && old_qtype == QUEUE_QD) {
        reclaim_ghost(bp);
        struct GNode *gn = find_free_ghost(bp);
        if (gn) {
            STORE(&gn->page_num, old_page, RELEASE);
            sht_set(bp->gindex, old_page, gn->gidx);
            cq_put(&bp->ghost, &gn->node);
        }
    }

    STORE(&frame->pin_cnt, 1, RELEASE);
    if (old_page != INVALID_PAGE) {
        sht_unset(bp->index, old_page);
        FADD(&frame->epoch, 1, RELEASE);
    }
    STORE(&frame->page_num, INVALID_PAGE, RELEASE);
    rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);

    bool was_dirty = (old_page != INVALID_PAGE) && LOAD(&frame->is_dirty, ACQUIRE);

    if (in_ghost) {
        target_queue = QUEUE_MAIN;
        sht_unset(bp->gindex, page_num);
    } else {
        // Check if QD has reasonable capacity
        if (cq_size(&bp->qd) >= QD_SIZE) {
            // QD saturated - bypass admission queue
            target_queue = QUEUE_MAIN;
        } else {
            target_queue = QUEUE_QD;
        }
    }

    rwsx_lock(&frame->fdata.latch, LATCH_EXCLUSIVE);

    if (was_dirty) {
        pstore_write(bp->store, old_page, frame->fdata.data);
    }

    u8 buf[PAGE_SIZE];
    if (pstore_read(bp->store, page_num, buf) < 0) {
        int err = errno;
        error_logger(stderr, err, "Failed to read in-range page %u at offset %lu\n", page_num,
                     (u64) page_num * PAGE_SIZE);
        abort();
    }
    memcpy(frame->fdata.data, buf, PAGE_SIZE);

    STORE(&frame->is_dirty, false, RELEASE);
    STORE(&frame->visited, false, RELEASE);
    STORE(&frame->qtype, target_queue, RELEASE);

    if (target_queue == QUEUE_MAIN) {
        cq_put(&bp->main, &bp->fnodes[victim].node);
    } else {
        cq_put(&bp->qd, &bp->fnodes[victim].node);
    }

    rwsx_lock(&bp->latch, LATCH_SHARED_EXCLUSIVE);
    STORE(&frame->page_num, page_num, RELEASE);
    sht_set(bp->index, page_num, victim); // Visible to index
    rwsx_unlock(&bp->latch, LATCH_SHARED_EXCLUSIVE);

    rwsx_unlock(&frame->fdata.latch, LATCH_EXCLUSIVE);

    return frame;
}
