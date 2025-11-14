#ifndef BUFPOOL_H
#define BUFPOOL_H

#include <stdatomic.h>

#include "cqueue.h"
#include "pagestore.h"
#include "rwsxlock.h"
#include "shtable.h"
#include "utils.h"

#define POOL_SIZE 32768
#define QD_SIZE (POOL_SIZE / 8) // 8 to be 2^n compared to 10 in source
#define MAIN_SIZE POOL_SIZE

enum QueueType {
    QUEUE_NONE = 0,
    QUEUE_QD = 1,
    QUEUE_MAIN = 2,
};

struct FrameData {
    atomic_bool loading;
    struct RWSXLock latch;
    u8 data[PAGE_SIZE];
};

struct PageFrame {
    atomic_u32 epoch;
    atomic_u32 pin_cnt;
    atomic_u8 qtype;
    atomic_bool is_dirty;
    atomic_bool visited;

    atomic_u32 page_num;

    struct FrameData fdata;
};

struct FrameHandle {
    struct FrameData *fdata;
    u32 epoch, frame_idx, page_num;
};

struct BufPool {
    // In-struct
    struct CQ qd, main, ghost;
    struct RWSXLock latch;
    atomic_u32 warmup_cursor;
    // Ptrs
    struct PageStore *store;
    struct PageFrame *frames;
    struct SHTable *index; // page_num -> frame_idx
    struct SHTable *gindex; // page_num -> presence (for ghost)
};

static inline void *handle_data(struct FrameHandle *h) { return &h->fdata->data[0]; }

// Pool API
struct BufPool *bpool_init(struct PageStore *store);
void bpool_destroy(struct BufPool *bp);
struct FrameHandle *bpool_fetch_page(struct BufPool *bp, u32 page_num);
i32 bpool_flush_all(struct BufPool *bp);
i32 bpool_flush_page(struct BufPool *bp, u32 page_num);

// Handle API
i32 bpool_mark_read(struct BufPool *bp, struct FrameHandle *h);
i32 bpool_mark_write(struct BufPool *bp, struct FrameHandle *h);
i32 bpool_release_handle(struct BufPool *bp, struct FrameHandle *h);

// Helpers
struct FrameHandle *bpool_acquire_page(struct BufPool *bp, u32 page_num, LatchMode mode);
i32 bpool_release_page(struct BufPool *bp, struct FrameHandle *h, bool is_write, LatchMode mode);

#endif /* ifndef BUFPOOL_H */
