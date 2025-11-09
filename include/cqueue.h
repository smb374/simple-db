#ifndef CQUEUE_H
#define CQUEUE_H

#include <stdbool.h>

#include "utils.h"

#define Q_SENTINEL 0xFFFFFFFF

struct CQ {
    atomic_u64 head, count;
    u64 tail, cap;
    atomic_u32 *buf;
    bool is_alloc;
};

struct CQ *cq_init(struct CQ *q, size_t cap);
void cq_destroy(struct CQ *q);
bool cq_put(struct CQ *q, u32 n);
u32 cq_pop(struct CQ *q);
size_t cq_size(struct CQ *q);
size_t cq_cap(struct CQ *q);


#endif /* ifndef CQUEUE_H */
