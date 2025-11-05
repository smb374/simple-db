#include "rwsxlock.h"

#include <assert.h>
#include <pthread.h>

#include "utils.h"

void rwsx_init(struct RWSXLock *lock) {
    assert(lock);

    pthread_mutex_init(&lock->mutex, NULL);
    pthread_cond_init(&lock->cond, NULL);
    lock->sx_holder = false;
    lock->writer = false;
    lock->readers = 0;
    lock->upgrading = false;
}
void rwsx_destroy(struct RWSXLock *lock) {
    assert(lock);

    pthread_mutex_destroy(&lock->mutex);
    pthread_cond_destroy(&lock->cond);
}

// Lock in specified LatchMode
void rwsx_lock(struct RWSXLock *lock, LatchMode mode) {
    pthread_mutex_lock(&lock->mutex);

    switch (mode) {
        case LATCH_SHARED:
            while (lock->writer || LOAD(&lock->upgrading, ACQUIRE)) {
                pthread_cond_wait(&lock->cond, &lock->mutex);
            }
            lock->readers++;
            break;
        case LATCH_SHARED_EXCLUSIVE:
            while (lock->writer || lock->sx_holder) {
                pthread_cond_wait(&lock->cond, &lock->mutex);
            }
            lock->sx_holder = true;
            lock->sx_owner = pthread_self();
            break;
        case LATCH_EXCLUSIVE:
            while (lock->readers || lock->writer || lock->sx_holder) {
                pthread_cond_wait(&lock->cond, &lock->mutex);
            }
            lock->writer = true;
        default:
            break;
    };

    pthread_mutex_unlock(&lock->mutex);
}

// Unlock in specified LatchMode
void rwsx_unlock(struct RWSXLock *lock, LatchMode mode) {
    pthread_mutex_lock(&lock->mutex);

    switch (mode) {
        case LATCH_SHARED:
            lock->readers--;
            if (!lock->readers) {
                pthread_cond_broadcast(&lock->cond);
            }
            break;
        case LATCH_SHARED_EXCLUSIVE:
            lock->sx_holder = false;
            pthread_cond_broadcast(&lock->cond);
            break;
        case LATCH_EXCLUSIVE:
            lock->writer = false;
            pthread_cond_broadcast(&lock->cond);
        default:
            break;
    };

    pthread_mutex_unlock(&lock->mutex);
}

// Upgrade SX -> X latch
i32 rwsx_upgrade_sx(struct RWSXLock *lock) {
    pthread_mutex_lock(&lock->mutex);

    if (!lock->sx_holder || lock->sx_owner != pthread_self()) {
        pthread_mutex_unlock(&lock->mutex);
        return -1;
    }

    STORE(&lock->upgrading, true, RELEASE);

    pthread_mutex_unlock(&lock->mutex);

    pthread_mutex_lock(&lock->mutex);

    while (lock->readers > 0) {
        pthread_cond_wait(&lock->cond, &lock->mutex);
    }

    lock->sx_holder = false;
    lock->writer = true;

    STORE(&lock->upgrading, false, RELEASE);

    pthread_cond_broadcast(&lock->cond);
    pthread_mutex_unlock(&lock->mutex);
    return 0;
}

// Downgrade X -> SX latch
i32 rwsx_downgrade_sx(struct RWSXLock *lock) {
    pthread_mutex_lock(&lock->mutex);

    if (!lock->writer || lock->sx_holder) {
        pthread_mutex_unlock(&lock->mutex);
        return -1;
    }

    lock->writer = false;
    lock->sx_holder = true;
    lock->sx_owner = pthread_self();

    STORE(&lock->upgrading, false, RELEASE);

    pthread_cond_broadcast(&lock->cond);
    pthread_mutex_unlock(&lock->mutex);
    return 0;
}
