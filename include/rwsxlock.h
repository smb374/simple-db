#ifndef RWSXLOCK_H
#define RWSXLOCK_H

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>

#include "utils.h"

enum LatchMode {
    LATCH_NONE = 0,
    LATCH_SHARED = 1,
    LATCH_SHARED_EXCLUSIVE = 2,
    LATCH_EXCLUSIVE = 3,
};
typedef enum LatchMode LatchMode;

struct RWSXLock {
    pthread_mutex_t mutex;
    pthread_cond_t cond;

    i32 readers;
    bool writer;
    bool sx_holder;

    pthread_t sx_owner;

    atomic_bool upgrading;
};

void rwsx_init(struct RWSXLock *lock);
void rwsx_destroy(struct RWSXLock *lock);
// Lock in specified LatchMode
void rwsx_lock(struct RWSXLock *lock, LatchMode mode);
// Unlock in specified LatchMode
void rwsx_unlock(struct RWSXLock *lock, LatchMode mode);
// Upgrade SX -> X latch
i32 rwsx_upgrade_sx(struct RWSXLock *lock);
// Downgrade X -> SX latch
i32 rwsx_downgrade_sx(struct RWSXLock *lock);

#endif /* ifndef RWSXLOCK_H */
