#ifndef BUFPOOL_H
#define BUFPOOL_H

#include <stdatomic.h>

#include "cqueue.h"
#include "pagestore.h"
#include "rwsxlock.h"
#include "shtable.h"
#include "utils.h"

#define POOL_SIZE 8192
#define QD_SIZE (POOL_SIZE / 8)
#define MAIN_SIZE (POOL_SIZE - QD_SIZE)

enum QueueType {
    QUEUE_NONE = 0,
    QUEUE_QD = 1,
    QUEUE_MAIN = 2,
};

struct PageFrame {
    atomic_u32 pin_cnt;
    atomic_u8 qtype;
    atomic_bool is_dirty;
    atomic_bool visited;

    struct RWSXLock latch;
    u8 data[PAGE_SIZE];
};

struct FNode {
    struct CNode node;
    u32 fidx;
};

struct GNode {
    struct CNode node;
    u32 page_num;
    u32 gidx;
};

struct BufPool {
    // In-struct
    atomic_u32 tlb_cursor;
    struct CQ qd, main, ghost;
    struct RWSXLock latch;
    // Ptrs
    struct PageStore *store;
    struct PageFrame *frames;
    struct SHTable *index; // page_num -> frame
    struct SHTable *gindex; // page_num -> GNode
    struct FNode *fnodes;
    struct GNode *gnodes;
    atomic_u32 *tlb;

    u8 data[];
};

struct BufPool *bpool_init(struct PageStore *store);
struct PageFrame *bpool_fetch_page(struct BufPool *bp, u32 page_num, LatchMode mode);
void bpool_unpin_page(struct BufPool *bp, u32 page_num, bool is_dirty);
void bpool_flush_page(struct BufPool *bp, u32 page_num);
void bpool_flush_all(struct BufPool *bp);
void bpool_destroy(struct BufPool *bp);

#endif /* ifndef BUFPOOL_H */
