#ifndef CQUEUE_H
#define CQUEUE_H

#include <stdbool.h>

#include "utils.h"

struct CNode {
    u8 _pad[4];
};

struct CQ {
    atomic_u64 head, count;
    u64 tail, cap;
    struct CNode *_Atomic *buf;
    bool is_alloc;
};

struct CQ *cq_init(struct CQ *q, size_t cap);
void cq_destroy(struct CQ *q);
bool cq_put(struct CQ *q, struct CNode *n);
struct CNode *cq_pop(struct CQ *q);
size_t cq_size(struct CQ *q);
size_t cq_cap(struct CQ *q);


#endif /* ifndef CQUEUE_H */
