#include "cqueue.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"

struct CQ *cq_init(struct CQ *q, size_t cap) {
    if (!q) {
        q = calloc(1, sizeof(struct CQ));
        q->is_alloc = true;
    } else {
        q->is_alloc = false;
    }

    atomic_init(&q->head, 0);
    atomic_init(&q->count, 0);
    q->tail = 0;
    q->cap = cap;
    q->buf = calloc(cap, sizeof(u32));
    memset(q->buf, 0xFF, cap * sizeof(u32));
    atomic_thread_fence(RELEASE);
    return q;
}

void cq_destroy(struct CQ *q) {
    free(q->buf);
    if (q->is_alloc)
        free(q);
}

bool cq_put(struct CQ *q, const u32 n) {
    size_t count = FADD(&q->count, 1, ACQUIRE);
    if (count >= q->cap) {
        // queue is full
        FSUB(&q->count, 1, RELEASE);
        return false;
    }

    size_t head = LOAD(&q->head, ACQUIRE), nhead;
    for (;;) {
        nhead = (head + 1) % q->cap;
        if (CMPXCHG(&q->head, &head, nhead, ACQ_REL, ACQUIRE)) {
            // CMPEXG success, q->head is nhead now.
            break;
        }
        // Acquires new head on fail
    }
    // Since slot is acquired after CMPEXG success for head, we can just store the slot.
    u32 old = XCHG(&q->buf[head], n, RELEASE);
    assert(old == Q_SENTINEL); // Sanity check
    return true;
}

u32 cq_pop(struct CQ *q) {
    u32 ret = XCHG(&q->buf[q->tail], Q_SENTINEL, ACQUIRE);
    if (ret == Q_SENTINEL)
        /* a thread is adding to the queue, but hasn't done the write yet
         * to actually put the item in. Act as if nothing is in the queue.
         * Worst case, other producers write content to tail + 1..n and finish, but
         * the producer that writes to tail doesn't do it in time, and we get here.
         * But that's okay, because once it DOES finish, we can get at all the data
         * that has been filled in. */
        return Q_SENTINEL;

    size_t r = FSUB(&q->count, 1, RELEASE);
    if (r == 0) {
        // recover.
        STORE(&q->count, 0, RELEASE);
        return Q_SENTINEL;
    }
    q->tail = (q->tail + 1) % q->cap;
    return ret;
}

size_t cq_size(struct CQ *q) { return LOAD(&q->count, RELAXED); }
size_t cq_cap(struct CQ *q) { return q->cap; }
