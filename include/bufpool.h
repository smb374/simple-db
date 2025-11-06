#ifndef BUFPOOL_H
#define BUFPOOL_H

#include <stdatomic.h>

#include "pagestore.h"
#include "rwsxlock.h"
#include "shtable.h"
#include "utils.h"

#define POOL_SIZE 4096

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

    struct SHTable *index; // page_num -> frame
    atomic_u32 tlb[POOL_SIZE]; // frame -> page_num
    atomic_u32 clock_hand;

    struct RWSXLock latch;
};

struct BufPool *bpool_init(struct PageStore *store);
struct PageFrame *bpool_fetch_page(struct BufPool *bp, u32 page_num, LatchMode mode);
void bpool_unpin_page(struct BufPool *bp, u32 page_num, bool is_dirty);
void bpool_flush_page(struct BufPool *bp, u32 page_num);
void bpool_flush_all(struct BufPool *bp);
void bpool_destroy(struct BufPool *bp);

#endif /* ifndef BUFPOOL_H */
