#ifndef BUFPOOL_H
#define BUFPOOL_H

#include <stdatomic.h>

#include "pagestore.h"
#include "rwsxlock.h"
#include "utils.h"

#define POOL_SIZE 1024

struct PageFrame {
    atomic_u32 pin_cnt;
    atomic_bool is_dirty;
    atomic_u8 clock_bit;

    struct RWSXLock latch;
    u8 data[PAGE_SIZE];
};

struct BufPool {
    struct PageStore *store;

    struct PageFrame frames[POOL_SIZE];

    atomic_u32 tlb[POOL_SIZE];
    atomic_u32 clock_hand;

    struct RWSXLock latch;
};

struct BufPool *bpool_init(struct PageStore *store);
// Can return NULL if all of the pages are pinned for some reason and page_num mismatched for some reason.
struct PageFrame *bpool_fetch_page(struct BufPool *bp, u32 page_num, u32 *idx_hint, LatchMode mode);
void bpool_unpin_page(struct BufPool *bp, u32 page_num, u32 *idx_hint, bool is_dirty);
void bpool_flush_page(struct BufPool *bp, u32 page_num, u32 *idx_hint);
void bpool_flush_all(struct BufPool *bp);
void bpool_destroy(struct BufPool *bp);

#endif /* ifndef BUFPOOL_H */
