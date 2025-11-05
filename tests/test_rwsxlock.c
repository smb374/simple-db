#include "rwsxlock.h"

#include <pthread.h>
#include <stdbool.h>
#include <unistd.h>

#include "unity.h"
#include "utils.h"

// =============================================================================
// BASIC SINGLE-THREADED TESTS
// =============================================================================

void test_init_and_destroy(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    // Check initial state
    TEST_ASSERT_EQUAL(0, lock.readers);
    TEST_ASSERT_FALSE(lock.writer);
    TEST_ASSERT_FALSE(lock.sx_holder);
    TEST_ASSERT_FALSE(LOAD(&lock.upgrading, RELAXED));

    rwsx_destroy(&lock);
}

void test_shared_lock_unlock(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    rwsx_lock(&lock, LATCH_SHARED);
    TEST_ASSERT_EQUAL(1, lock.readers);

    rwsx_unlock(&lock, LATCH_SHARED);
    TEST_ASSERT_EQUAL(0, lock.readers);

    rwsx_destroy(&lock);
}

void test_multiple_shared_locks(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    // Acquire multiple S-latches
    rwsx_lock(&lock, LATCH_SHARED);
    TEST_ASSERT_EQUAL(1, lock.readers);

    rwsx_lock(&lock, LATCH_SHARED);
    TEST_ASSERT_EQUAL(2, lock.readers);

    rwsx_lock(&lock, LATCH_SHARED);
    TEST_ASSERT_EQUAL(3, lock.readers);

    // Release them
    rwsx_unlock(&lock, LATCH_SHARED);
    TEST_ASSERT_EQUAL(2, lock.readers);

    rwsx_unlock(&lock, LATCH_SHARED);
    TEST_ASSERT_EQUAL(1, lock.readers);

    rwsx_unlock(&lock, LATCH_SHARED);
    TEST_ASSERT_EQUAL(0, lock.readers);

    rwsx_destroy(&lock);
}

void test_sx_lock_unlock(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    rwsx_lock(&lock, LATCH_SHARED_EXCLUSIVE);
    TEST_ASSERT_TRUE(lock.sx_holder);
    TEST_ASSERT_EQUAL(pthread_self(), lock.sx_owner);

    rwsx_unlock(&lock, LATCH_SHARED_EXCLUSIVE);
    TEST_ASSERT_FALSE(lock.sx_holder);

    rwsx_destroy(&lock);
}

void test_exclusive_lock_unlock(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    rwsx_lock(&lock, LATCH_EXCLUSIVE);
    TEST_ASSERT_TRUE(lock.writer);

    rwsx_unlock(&lock, LATCH_EXCLUSIVE);
    TEST_ASSERT_FALSE(lock.writer);

    rwsx_destroy(&lock);
}

void test_upgrade_success(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    // Acquire SX-latch
    rwsx_lock(&lock, LATCH_SHARED_EXCLUSIVE);
    TEST_ASSERT_TRUE(lock.sx_holder);
    TEST_ASSERT_FALSE(lock.writer);

    // Upgrade to X-latch
    i32 ret = rwsx_upgrade_sx(&lock);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_FALSE(lock.sx_holder);
    TEST_ASSERT_TRUE(lock.writer);

    rwsx_unlock(&lock, LATCH_EXCLUSIVE);
    rwsx_destroy(&lock);
}

void test_upgrade_failure_not_holding_sx(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    // Try to upgrade without holding SX-latch
    i32 ret = rwsx_upgrade_sx(&lock);
    TEST_ASSERT_EQUAL(-1, ret);
    TEST_ASSERT_FALSE(lock.writer);

    rwsx_destroy(&lock);
}

void test_downgrade_success(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    // Acquire X-latch
    rwsx_lock(&lock, LATCH_EXCLUSIVE);
    TEST_ASSERT_TRUE(lock.writer);
    TEST_ASSERT_FALSE(lock.sx_holder);

    // Downgrade to SX-latch
    i32 ret = rwsx_downgrade_sx(&lock);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_FALSE(lock.writer);
    TEST_ASSERT_TRUE(lock.sx_holder);
    TEST_ASSERT_EQUAL(pthread_self(), lock.sx_owner);

    rwsx_unlock(&lock, LATCH_SHARED_EXCLUSIVE);
    rwsx_destroy(&lock);
}

void test_downgrade_failure_not_holding_x(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    // Try to downgrade without holding X-latch
    i32 ret = rwsx_downgrade_sx(&lock);
    TEST_ASSERT_EQUAL(-1, ret);
    TEST_ASSERT_FALSE(lock.sx_holder);

    rwsx_destroy(&lock);
}

void test_downgrade_failure_holding_sx(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    // Acquire SX-latch
    rwsx_lock(&lock, LATCH_SHARED_EXCLUSIVE);
    TEST_ASSERT_TRUE(lock.sx_holder);

    // Try to downgrade while holding SX (should fail)
    i32 ret = rwsx_downgrade_sx(&lock);
    TEST_ASSERT_EQUAL(-1, ret);

    rwsx_unlock(&lock, LATCH_SHARED_EXCLUSIVE);
    rwsx_destroy(&lock);
}

// =============================================================================
// MULTI-THREADED TESTS
// =============================================================================

struct thread_test_args {
    struct RWSXLock *lock;
    i32 thread_id;
    volatile i32 *counter;
    volatile bool *start_flag;
    volatile bool *done_flag;
    i32 result;
};

static void *shared_lock_worker(void *arg) {
    struct thread_test_args *args = (struct thread_test_args *) arg;

    // Wait for start signal
    while (!*args->start_flag) {
        usleep(100);
    }

    // Acquire S-latch
    rwsx_lock(args->lock, LATCH_SHARED);

    // Critical section: increment counter
    usleep(1000); // Hold lock for a bit
    *args->counter += 1;

    // Release S-latch
    rwsx_unlock(args->lock, LATCH_SHARED);

    return NULL;
}

void test_concurrent_shared_locks(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    const i32 NUM_THREADS = 5;
    pthread_t threads[NUM_THREADS];
    struct thread_test_args args[NUM_THREADS];
    volatile i32 counter = 0;
    volatile bool start_flag = false;

    // Create threads
    for (i32 i = 0; i < NUM_THREADS; i++) {
        args[i].lock = &lock;
        args[i].thread_id = i;
        args[i].counter = &counter;
        args[i].start_flag = &start_flag;
        pthread_create(&threads[i], NULL, shared_lock_worker, &args[i]);
    }

    // Start all threads
    start_flag = true;

    // Wait for all threads
    for (i32 i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // All threads should have incremented the counter
    TEST_ASSERT_EQUAL(NUM_THREADS, counter);

    rwsx_destroy(&lock);
}

static void *sx_holder_worker(void *arg) {
    struct thread_test_args *args = (struct thread_test_args *) arg;

    rwsx_lock(args->lock, LATCH_SHARED_EXCLUSIVE);
    usleep(5000); // Hold for 5ms
    rwsx_unlock(args->lock, LATCH_SHARED_EXCLUSIVE);

    return NULL;
}

static void *shared_with_sx_worker(void *arg) {
    struct thread_test_args *args = (struct thread_test_args *) arg;

    // Wait a bit to let SX holder acquire
    usleep(1000);

    // Try to acquire S-latch (should succeed even with SX holder)
    rwsx_lock(args->lock, LATCH_SHARED);
    args->result = 1; // Mark success
    rwsx_unlock(args->lock, LATCH_SHARED);

    return NULL;
}

void test_sx_allows_shared(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    pthread_t sx_thread, s_thread;
    struct thread_test_args sx_args = {.lock = &lock, .result = 0};
    struct thread_test_args s_args = {.lock = &lock, .result = 0};

    // Start SX holder
    pthread_create(&sx_thread, NULL, sx_holder_worker, &sx_args);

    // Start S-latch acquirer
    pthread_create(&s_thread, NULL, shared_with_sx_worker, &s_args);

    pthread_join(sx_thread, NULL);
    pthread_join(s_thread, NULL);

    // S-latch should have been acquired successfully
    TEST_ASSERT_EQUAL(1, s_args.result);

    rwsx_destroy(&lock);
}

static void *exclusive_holder_worker(void *arg) {
    struct thread_test_args *args = (struct thread_test_args *) arg;

    rwsx_lock(args->lock, LATCH_EXCLUSIVE);
    *args->counter = 1;
    usleep(5000); // Hold for 5ms
    *args->counter = 2;
    rwsx_unlock(args->lock, LATCH_EXCLUSIVE);

    return NULL;
}

static void *blocked_shared_worker(void *arg) {
    struct thread_test_args *args = (struct thread_test_args *) arg;

    // Wait a bit to let X holder acquire
    usleep(1000);

    // Try to acquire S-latch (should block until X releases)
    rwsx_lock(args->lock, LATCH_SHARED);

    // By the time we get here, counter should be 2
    args->result = *args->counter;

    rwsx_unlock(args->lock, LATCH_SHARED);

    return NULL;
}

void test_exclusive_blocks_shared(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    pthread_t x_thread, s_thread;
    volatile i32 counter = 0;
    struct thread_test_args x_args = {.lock = &lock, .counter = &counter};
    struct thread_test_args s_args = {.lock = &lock, .counter = &counter, .result = 0};

    // Start X holder
    pthread_create(&x_thread, NULL, exclusive_holder_worker, &x_args);

    // Start S-latch acquirer (will block)
    pthread_create(&s_thread, NULL, blocked_shared_worker, &s_args);

    pthread_join(x_thread, NULL);
    pthread_join(s_thread, NULL);

    // S-latch should have waited until X released (counter == 2)
    TEST_ASSERT_EQUAL(2, s_args.result);

    rwsx_destroy(&lock);
}

static void *upgrade_worker(void *arg) {
    struct thread_test_args *args = (struct thread_test_args *) arg;

    // Acquire SX-latch
    rwsx_lock(args->lock, LATCH_SHARED_EXCLUSIVE);

    // Wait for start signal
    while (!*args->start_flag) {
        usleep(100);
    }

    // Upgrade to X-latch
    i32 ret = rwsx_upgrade_sx(args->lock);
    args->result = ret;

    if (ret == 0) {
        rwsx_unlock(args->lock, LATCH_EXCLUSIVE);
    }

    return NULL;
}

static void *shared_during_upgrade_worker(void *arg) {
    struct thread_test_args *args = (struct thread_test_args *) arg;

    // Acquire S-latch before upgrade starts
    rwsx_lock(args->lock, LATCH_SHARED);
    *args->done_flag = true; // Signal we got the lock

    // Hold it for a while
    usleep(3000);

    rwsx_unlock(args->lock, LATCH_SHARED);

    return NULL;
}

void *shared_after_upgrade_starts_worker(void *arg) {
    struct thread_test_args *args = (struct thread_test_args *) arg;

    // Wait for upgrade to start (upgrading flag set)
    while (!LOAD(&args->lock->upgrading, ACQUIRE)) {
        usleep(100);
    }

    // Try to acquire S-latch (should block due to upgrading flag)
    // u64 start_time = 0; // Mock timestamp
    rwsx_lock(args->lock, LATCH_SHARED);

    // If we get here after upgrade completed, mark success
    // The upgrade worker should have released by now
    args->result = 1;

    rwsx_unlock(args->lock, LATCH_SHARED);

    return NULL;
}

void test_upgrade_dcli_pattern(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    pthread_t upgrade_thread, existing_s_thread, new_s_thread;
    volatile bool start_upgrade = false;
    volatile bool existing_s_acquired = false;

    struct thread_test_args upgrade_args = {.lock = &lock, .start_flag = &start_upgrade, .result = -1};
    struct thread_test_args existing_s_args = {.lock = &lock, .done_flag = &existing_s_acquired, .result = 0};
    struct thread_test_args new_s_args = {.lock = &lock, .result = 0};

    // Start upgrade worker (acquires SX, waits for signal)
    pthread_create(&upgrade_thread, NULL, upgrade_worker, &upgrade_args);
    usleep(1000); // Let it acquire SX

    // Start existing S-latch holder
    pthread_create(&existing_s_thread, NULL, shared_during_upgrade_worker, &existing_s_args);

    // Wait for existing S-latch to be acquired
    while (!existing_s_acquired) {
        usleep(100);
    }

    // Start new S-latch acquirer (will try after upgrade starts)
    pthread_create(&new_s_thread, NULL, shared_after_upgrade_starts_worker, &new_s_args);
    usleep(500); // Let it spin up

    // Signal upgrade to start
    start_upgrade = true;

    // Wait for all threads
    pthread_join(upgrade_thread, NULL);
    pthread_join(existing_s_thread, NULL);
    pthread_join(new_s_thread, NULL);

    // Upgrade should have succeeded
    TEST_ASSERT_EQUAL(0, upgrade_args.result);

    // New S-latch should have been blocked, then acquired after upgrade
    TEST_ASSERT_EQUAL(1, new_s_args.result);

    rwsx_destroy(&lock);
}

void *wrong_thread_upgrade_worker(void *arg) {
    struct thread_test_args *args = (struct thread_test_args *) arg;

    // Wait for start signal
    while (!*args->start_flag) {
        usleep(100);
    }

    // Try to upgrade (should fail - wrong thread)
    i32 ret = rwsx_upgrade_sx(args->lock);
    args->result = ret;

    return NULL;
}

void test_upgrade_failure_wrong_thread(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    // Main thread acquires SX
    rwsx_lock(&lock, LATCH_SHARED_EXCLUSIVE);
    TEST_ASSERT_TRUE(lock.sx_holder);
    TEST_ASSERT_EQUAL(pthread_self(), lock.sx_owner);

    // Spawn another thread to try upgrade
    pthread_t other_thread;
    volatile bool start_flag = false;
    struct thread_test_args args = {.lock = &lock, .start_flag = &start_flag, .result = 0};

    pthread_create(&other_thread, NULL, wrong_thread_upgrade_worker, &args);

    // Signal the thread to try upgrade
    start_flag = true;

    pthread_join(other_thread, NULL);

    // Upgrade should have failed
    TEST_ASSERT_EQUAL(-1, args.result);

    // Main thread still holds SX
    TEST_ASSERT_TRUE(lock.sx_holder);

    rwsx_unlock(&lock, LATCH_SHARED_EXCLUSIVE);
    rwsx_destroy(&lock);
}

void test_full_cycle_sx_upgrade_downgrade(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    // Acquire SX-latch
    rwsx_lock(&lock, LATCH_SHARED_EXCLUSIVE);
    TEST_ASSERT_TRUE(lock.sx_holder);
    TEST_ASSERT_FALSE(lock.writer);

    // Upgrade to X-latch
    i32 ret = rwsx_upgrade_sx(&lock);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_FALSE(lock.sx_holder);
    TEST_ASSERT_TRUE(lock.writer);

    // Downgrade back to SX-latch
    ret = rwsx_downgrade_sx(&lock);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_TRUE(lock.sx_holder);
    TEST_ASSERT_FALSE(lock.writer);

    // Release SX-latch
    rwsx_unlock(&lock, LATCH_SHARED_EXCLUSIVE);
    TEST_ASSERT_FALSE(lock.sx_holder);

    rwsx_destroy(&lock);
}

// Worker that holds SX for a while
static void *sx_long_holder(void *arg) {
    struct thread_test_args *a = (struct thread_test_args *) arg;
    rwsx_lock(a->lock, LATCH_SHARED_EXCLUSIVE);
    usleep(5000);
    *a->done_flag = true;
    rwsx_unlock(a->lock, LATCH_SHARED_EXCLUSIVE);
    return NULL;
}

// Worker that tries to acquire SX and checks if holder was done
static void *sx_waiter(void *arg) {
    struct thread_test_args *a = (struct thread_test_args *) arg;
    usleep(1000); // Let holder acquire first
    rwsx_lock(a->lock, LATCH_SHARED_EXCLUSIVE);
    // If we get here, holder should be done
    a->result = *a->done_flag ? 1 : 0;
    rwsx_unlock(a->lock, LATCH_SHARED_EXCLUSIVE);
    return NULL;
}

void test_sx_blocks_other_sx(void) {
    struct RWSXLock lock;
    rwsx_init(&lock);

    pthread_t sx_holder_thread, sx_waiter_thread;
    volatile bool holder_done = false;

    struct thread_test_args holder_args = {.lock = &lock, .done_flag = &holder_done};
    struct thread_test_args waiter_args = {.lock = &lock, .done_flag = &holder_done, .result = 0};


    pthread_create(&sx_holder_thread, NULL, sx_long_holder, &holder_args);
    pthread_create(&sx_waiter_thread, NULL, sx_waiter, &waiter_args);

    pthread_join(sx_holder_thread, NULL);
    pthread_join(sx_waiter_thread, NULL);

    // Waiter should have waited for holder to finish
    TEST_ASSERT_EQUAL(1, waiter_args.result);

    rwsx_destroy(&lock);
}

// =============================================================================
// MAIN
// =============================================================================

int suite_setUp(void) { return 0; }

int suite_tearDown(void) { return 0; }

void setUp(void) {}

void tearDown(void) {}

int main(void) {
    if (suite_setUp() != 0) {
        return -1;
    }

    UNITY_BEGIN();

    // Basic single-threaded tests
    RUN_TEST(test_init_and_destroy);
    RUN_TEST(test_shared_lock_unlock);
    RUN_TEST(test_multiple_shared_locks);
    RUN_TEST(test_sx_lock_unlock);
    RUN_TEST(test_exclusive_lock_unlock);
    RUN_TEST(test_upgrade_success);
    RUN_TEST(test_upgrade_failure_not_holding_sx);
    RUN_TEST(test_downgrade_success);
    RUN_TEST(test_downgrade_failure_not_holding_x);
    RUN_TEST(test_downgrade_failure_holding_sx);

    // Multi-threaded tests
    RUN_TEST(test_concurrent_shared_locks);
    RUN_TEST(test_sx_allows_shared);
    RUN_TEST(test_exclusive_blocks_shared);
    RUN_TEST(test_upgrade_dcli_pattern);
    RUN_TEST(test_upgrade_failure_wrong_thread);
    RUN_TEST(test_full_cycle_sx_upgrade_downgrade);
    RUN_TEST(test_sx_blocks_other_sx);

    return suite_tearDown() + UNITY_END();
}
