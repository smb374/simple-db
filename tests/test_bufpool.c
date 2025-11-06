#include "bufpool.h"

#include <pthread.h>
#include <string.h>
#include <unistd.h>

#include "pagestore.h"
#include "unity.h"
#include "utils.h"

#define TEST_DB_FILE "test_bufpool.db"
#define SMALL_POOL_SIZE 8 // For easier testing of eviction

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

static void fill_page_with_pattern(u8 *page, u32 page_num, u8 pattern) {
    for (u32 i = 0; i < PAGE_SIZE; i++) {
        page[i] = (u8) (pattern + page_num + (i % 256));
    }
}

static void verify_page_pattern(const u8 *page, u32 page_num, u8 pattern) {
    for (u32 i = 0; i < PAGE_SIZE; i++) {
        u8 expected = (u8) (pattern + page_num + (i % 256));
        if (page[i] != expected) {
            TEST_FAIL_MESSAGE("Page pattern mismatch");
        }
    }
}

// =============================================================================
// BASIC FUNCTIONALITY TESTS
// =============================================================================

void test_bpool_init_and_destroy(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    TEST_ASSERT_NOT_NULL(ps);

    struct BufPool *bp = bpool_init(ps);
    TEST_ASSERT_NOT_NULL(bp);
    TEST_ASSERT_EQUAL(ps, bp->store);

    // Verify all frames initialized
    for (u32 i = 0; i < POOL_SIZE; i++) {
        TEST_ASSERT_EQUAL(INVALID_PAGE, LOAD(&bp->tlb[i], RELAXED));
        TEST_ASSERT_EQUAL(0, LOAD(&bp->frames[i].pin_cnt, RELAXED));
        TEST_ASSERT_FALSE(LOAD(&bp->frames[i].is_dirty, RELAXED));
    }

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_fetch_page_cold_load(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    TEST_ASSERT_NOT_NULL(ps);

    // Write initial data to page 5
    u8 write_buf[PAGE_SIZE];
    fill_page_with_pattern(write_buf, 5, 0xAA);
    pstore_write(ps, 5, write_buf);

    struct BufPool *bp = bpool_init(ps);
    TEST_ASSERT_NOT_NULL(bp);

    // Fetch page 5 (cold load)
    u32 hint = 0;
    struct PageFrame *frame = bpool_fetch_page(bp, 5, &hint, LATCH_SHARED);
    TEST_ASSERT_NOT_NULL(frame);

    // Verify data loaded correctly
    verify_page_pattern(frame->data, 5, 0xAA);

    // Verify frame state
    TEST_ASSERT_EQUAL(1, LOAD(&frame->pin_cnt, RELAXED));
    TEST_ASSERT_EQUAL(1, LOAD(&frame->clock_bit, RELAXED));
    TEST_ASSERT_FALSE(LOAD(&frame->is_dirty, RELAXED));

    // Verify TLB updated
    TEST_ASSERT_EQUAL(5, LOAD(&bp->tlb[hint], RELAXED));

    rwsx_unlock(&frame->latch, LATCH_SHARED);
    bpool_unpin_page(bp, 5, &hint, false);

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_fetch_page_hot(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    // Write and fetch page 10
    u8 write_buf[PAGE_SIZE];
    fill_page_with_pattern(write_buf, 10, 0xBB);
    pstore_write(ps, 10, write_buf);

    u32 hint = 0;
    struct PageFrame *frame1 = bpool_fetch_page(bp, 10, &hint, LATCH_SHARED);
    TEST_ASSERT_NOT_NULL(frame1);
    u32 first_hint = hint;

    rwsx_unlock(&frame1->latch, LATCH_SHARED);
    bpool_unpin_page(bp, 10, &hint, false);

    // Fetch again (hot - should be in pool)
    hint = first_hint;
    struct PageFrame *frame2 = bpool_fetch_page(bp, 10, &hint, LATCH_SHARED);
    TEST_ASSERT_NOT_NULL(frame2);
    TEST_ASSERT_EQUAL(frame1, frame2);
    TEST_ASSERT_EQUAL(first_hint, hint);

    // Verify data still correct
    verify_page_pattern(frame2->data, 10, 0xBB);

    rwsx_unlock(&frame2->latch, LATCH_SHARED);
    bpool_unpin_page(bp, 10, &hint, false);

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_unpin_marks_dirty(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    u32 hint = 0;
    struct PageFrame *frame = bpool_fetch_page(bp, 0, &hint, LATCH_EXCLUSIVE);
    TEST_ASSERT_NOT_NULL(frame);

    // Modify page
    memset(frame->data, 0xCC, PAGE_SIZE);

    // Unpin as dirty
    rwsx_unlock(&frame->latch, LATCH_EXCLUSIVE);
    bpool_unpin_page(bp, 0, &hint, true);

    // Verify is_dirty flag set
    TEST_ASSERT_TRUE(LOAD(&frame->is_dirty, RELAXED));

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_flush_dirty_page(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    // Fetch and modify page
    u32 hint = 0;
    struct PageFrame *frame = bpool_fetch_page(bp, 7, &hint, LATCH_EXCLUSIVE);
    TEST_ASSERT_NOT_NULL(frame);

    fill_page_with_pattern(frame->data, 7, 0xDD);

    rwsx_unlock(&frame->latch, LATCH_EXCLUSIVE);
    bpool_unpin_page(bp, 7, &hint, true);

    TEST_ASSERT_TRUE(LOAD(&frame->is_dirty, RELAXED));

    // Flush the page
    bpool_flush_page(bp, 7, &hint);

    TEST_ASSERT_FALSE(LOAD(&frame->is_dirty, RELAXED));

    // Verify data written to store
    u8 read_buf[PAGE_SIZE];
    pstore_read(ps, 7, read_buf);
    verify_page_pattern(read_buf, 7, 0xDD);

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_flush_clean_page_no_write(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    // Write initial pattern
    u8 write_buf[PAGE_SIZE];
    fill_page_with_pattern(write_buf, 3, 0x11);
    pstore_write(ps, 3, write_buf);

    // Fetch page (clean)
    u32 hint = 0;
    struct PageFrame *frame = bpool_fetch_page(bp, 3, &hint, LATCH_SHARED);
    TEST_ASSERT_NOT_NULL(frame);

    rwsx_unlock(&frame->latch, LATCH_SHARED);
    bpool_unpin_page(bp, 3, &hint, false);

    TEST_ASSERT_FALSE(LOAD(&frame->is_dirty, RELAXED));

    // Flush clean page (should not write)
    bpool_flush_page(bp, 3, &hint);

    TEST_ASSERT_FALSE(LOAD(&frame->is_dirty, RELAXED));

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_flush_all(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    // Fetch and modify multiple pages
    for (u32 i = 0; i < 5; i++) {
        u32 hint = 0;
        struct PageFrame *frame = bpool_fetch_page(bp, i, &hint, LATCH_EXCLUSIVE);
        TEST_ASSERT_NOT_NULL(frame);

        fill_page_with_pattern(frame->data, i, 0x22);

        rwsx_unlock(&frame->latch, LATCH_EXCLUSIVE);
        bpool_unpin_page(bp, i, &hint, true);
    }

    // Flush all
    bpool_flush_all(bp);

    // Verify all pages written
    for (u32 i = 0; i < 5; i++) {
        u8 read_buf[PAGE_SIZE];
        pstore_read(ps, i, read_buf);
        verify_page_pattern(read_buf, i, 0x22);
    }

    bpool_destroy(bp);
    pstore_close(ps);
}

// =============================================================================
// PIN/UNPIN SEMANTICS TESTS
// =============================================================================

void test_multiple_pins_same_page(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    u32 hint = 0;
    struct PageFrame *frame1 = bpool_fetch_page(bp, 0, &hint, LATCH_SHARED);
    TEST_ASSERT_NOT_NULL(frame1);
    TEST_ASSERT_EQUAL(1, LOAD(&frame1->pin_cnt, RELAXED));

    // Fetch again (simulating second accessor)
    struct PageFrame *frame2 = bpool_fetch_page(bp, 0, &hint, LATCH_SHARED);
    TEST_ASSERT_EQUAL(frame1, frame2);
    TEST_ASSERT_EQUAL(2, LOAD(&frame1->pin_cnt, RELAXED));

    // Unpin once
    rwsx_unlock(&frame1->latch, LATCH_SHARED);
    bpool_unpin_page(bp, 0, &hint, false);
    TEST_ASSERT_EQUAL(1, LOAD(&frame1->pin_cnt, RELAXED));

    // Unpin again
    rwsx_unlock(&frame2->latch, LATCH_SHARED);
    bpool_unpin_page(bp, 0, &hint, false);
    TEST_ASSERT_EQUAL(0, LOAD(&frame1->pin_cnt, RELAXED));

    bpool_destroy(bp);
    pstore_close(ps);
}

// =============================================================================
// CLOCK EVICTION TESTS
// =============================================================================

void test_eviction_clock_algorithm(void) {
    // Use small pool size for easier testing
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    // Fill pool with POOL_SIZE pages
    u32 hints[POOL_SIZE];
    for (u32 i = 0; i < POOL_SIZE; i++) {
        hints[i] = 0;
        struct PageFrame *frame = bpool_fetch_page(bp, i, &hints[i], LATCH_SHARED);
        TEST_ASSERT_NOT_NULL(frame);

        fill_page_with_pattern(frame->data, i, 0x33);

        rwsx_unlock(&frame->latch, LATCH_SHARED);
        bpool_unpin_page(bp, i, &hints[i], false);
    }

    // All frames should have clock_bit = 1 from fetch
    // Fetch one more page - should trigger eviction
    u32 new_hint = 0;
    struct PageFrame *new_frame = bpool_fetch_page(bp, POOL_SIZE, &new_hint, LATCH_SHARED);
    TEST_ASSERT_NOT_NULL(new_frame);

    // Verify new page loaded
    TEST_ASSERT_EQUAL(POOL_SIZE, LOAD(&bp->tlb[new_hint], RELAXED));

    rwsx_unlock(&new_frame->latch, LATCH_SHARED);
    bpool_unpin_page(bp, POOL_SIZE, &new_hint, false);

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_eviction_skips_pinned_pages(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    // Fill pool
    u32 hints[POOL_SIZE];
    struct PageFrame *frames[POOL_SIZE];
    for (u32 i = 0; i < POOL_SIZE; i++) {
        hints[i] = 0;
        frames[i] = bpool_fetch_page(bp, i, &hints[i], LATCH_SHARED);
        TEST_ASSERT_NOT_NULL(frames[i]);
    }

    // Keep all pages pinned, clear clock bits
    for (u32 i = 0; i < POOL_SIZE; i++) {
        STORE(&frames[i]->clock_bit, 0, RELAXED);
    }

    // Try to fetch a new page - should fail (all pinned)
    u32 new_hint = 0;
    struct PageFrame *new_frame = bpool_fetch_page(bp, POOL_SIZE, &new_hint, LATCH_SHARED);
    TEST_ASSERT_NULL(new_frame);

    // Unpin one page
    rwsx_unlock(&frames[0]->latch, LATCH_SHARED);
    bpool_unpin_page(bp, 0, &hints[0], false);

    // Now fetch should succeed
    new_frame = bpool_fetch_page(bp, POOL_SIZE, &new_hint, LATCH_SHARED);
    TEST_ASSERT_NOT_NULL(new_frame);

    // Clean up
    rwsx_unlock(&new_frame->latch, LATCH_SHARED);
    bpool_unpin_page(bp, POOL_SIZE, &new_hint, false);

    for (u32 i = 1; i < POOL_SIZE; i++) {
        rwsx_unlock(&frames[i]->latch, LATCH_SHARED);
        bpool_unpin_page(bp, i, &hints[i], false);
    }

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_eviction_writes_back_dirty_page(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    // Fill pool with dirty pages
    u32 hints[POOL_SIZE];
    for (u32 i = 0; i < POOL_SIZE; i++) {
        hints[i] = 0;
        struct PageFrame *frame = bpool_fetch_page(bp, i, &hints[i], LATCH_EXCLUSIVE);
        TEST_ASSERT_NOT_NULL(frame);

        fill_page_with_pattern(frame->data, i, 0x44);

        rwsx_unlock(&frame->latch, LATCH_EXCLUSIVE);
        bpool_unpin_page(bp, i, &hints[i], true);
    }

    // Clear clock bits to make eviction deterministic
    for (u32 i = 0; i < POOL_SIZE; i++) {
        STORE(&bp->frames[i].clock_bit, 0, RELAXED);
    }

    // Fetch new page - should evict and write back
    u32 new_hint = 0;
    struct PageFrame *new_frame = bpool_fetch_page(bp, POOL_SIZE, &new_hint, LATCH_SHARED);
    TEST_ASSERT_NOT_NULL(new_frame);

    rwsx_unlock(&new_frame->latch, LATCH_SHARED);
    bpool_unpin_page(bp, POOL_SIZE, &new_hint, false);

    // Verify one of the evicted pages was written back
    // (We don't know which one, but at least one should have correct data)
    bool found_written = false;
    for (u32 i = 0; i < POOL_SIZE && !found_written; i++) {
        u8 read_buf[PAGE_SIZE];
        pstore_read(ps, i, read_buf);

        bool matches = true;
        for (u32 j = 0; j < PAGE_SIZE; j++) {
            u8 expected = (u8) (0x44 + i + (j % 256));
            if (read_buf[j] != expected) {
                matches = false;
                break;
            }
        }
        if (matches) {
            found_written = true;
        }
    }
    TEST_ASSERT_TRUE(found_written);

    bpool_destroy(bp);
    pstore_close(ps);
}

// =============================================================================
// HINT OPTIMIZATION TESTS
// =============================================================================

void test_hint_speeds_up_lookup(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    // Fetch page 50
    u32 hint = 0;
    struct PageFrame *frame1 = bpool_fetch_page(bp, 50, &hint, LATCH_SHARED);
    TEST_ASSERT_NOT_NULL(frame1);
    u32 saved_hint = hint;

    rwsx_unlock(&frame1->latch, LATCH_SHARED);
    bpool_unpin_page(bp, 50, &hint, false);

    // Fetch again with saved hint
    hint = saved_hint;
    struct PageFrame *frame2 = bpool_fetch_page(bp, 50, &hint, LATCH_SHARED);
    TEST_ASSERT_NOT_NULL(frame2);
    TEST_ASSERT_EQUAL(frame1, frame2);
    TEST_ASSERT_EQUAL(saved_hint, hint);

    rwsx_unlock(&frame2->latch, LATCH_SHARED);
    bpool_unpin_page(bp, 50, &hint, false);

    bpool_destroy(bp);
    pstore_close(ps);
}

// =============================================================================
// LATCHING TESTS
// =============================================================================

void test_fetch_with_different_latch_modes(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    // Fetch with S-latch
    u32 hint = 0;
    struct PageFrame *frame = bpool_fetch_page(bp, 0, &hint, LATCH_SHARED);
    TEST_ASSERT_NOT_NULL(frame);
    rwsx_unlock(&frame->latch, LATCH_SHARED);
    bpool_unpin_page(bp, 0, &hint, false);

    // Fetch with SX-latch
    frame = bpool_fetch_page(bp, 1, &hint, LATCH_SHARED_EXCLUSIVE);
    TEST_ASSERT_NOT_NULL(frame);
    rwsx_unlock(&frame->latch, LATCH_SHARED_EXCLUSIVE);
    bpool_unpin_page(bp, 1, &hint, false);

    // Fetch with X-latch
    frame = bpool_fetch_page(bp, 2, &hint, LATCH_EXCLUSIVE);
    TEST_ASSERT_NOT_NULL(frame);
    rwsx_unlock(&frame->latch, LATCH_EXCLUSIVE);
    bpool_unpin_page(bp, 2, &hint, false);

    bpool_destroy(bp);
    pstore_close(ps);
}

// =============================================================================
// CONCURRENT ACCESS TESTS
// =============================================================================

struct concurrent_reader_args {
    struct BufPool *bp;
    u32 page_num;
    i32 result;
    u32 read_count;
};

void *concurrent_reader(void *arg) {
    struct concurrent_reader_args *args = (struct concurrent_reader_args *) arg;

    for (u32 i = 0; i < args->read_count; i++) {
        u32 hint = 0;
        struct PageFrame *frame = bpool_fetch_page(args->bp, args->page_num, &hint, LATCH_SHARED);

        if (frame == NULL) {
            args->result = -1;
            return NULL;
        }

        // Simulate read
        volatile u8 dummy = frame->data[0];
        (void) dummy;

        rwsx_unlock(&frame->latch, LATCH_SHARED);
        bpool_unpin_page(args->bp, args->page_num, &hint, false);
    }

    args->result = 0;
    return NULL;
}

void test_concurrent_readers_same_page(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    const i32 NUM_READERS = 4;
    pthread_t threads[NUM_READERS];
    struct concurrent_reader_args args[NUM_READERS];

    // All threads read same page
    for (i32 i = 0; i < NUM_READERS; i++) {
        args[i].bp = bp;
        args[i].page_num = 0;
        args[i].result = -1;
        args[i].read_count = 10;
        pthread_create(&threads[i], NULL, concurrent_reader, &args[i]);
    }

    // Wait for all
    for (i32 i = 0; i < NUM_READERS; i++) {
        pthread_join(threads[i], NULL);
        TEST_ASSERT_EQUAL(0, args[i].result);
    }

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_concurrent_readers_different_pages(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    struct BufPool *bp = bpool_init(ps);

    const i32 NUM_READERS = 4;
    pthread_t threads[NUM_READERS];
    struct concurrent_reader_args args[NUM_READERS];

    // Each thread reads different page
    for (i32 i = 0; i < NUM_READERS; i++) {
        args[i].bp = bp;
        args[i].page_num = i * 10; // Pages 0, 10, 20, 30
        args[i].result = -1;
        args[i].read_count = 10;
        pthread_create(&threads[i], NULL, concurrent_reader, &args[i]);
    }

    // Wait for all
    for (i32 i = 0; i < NUM_READERS; i++) {
        pthread_join(threads[i], NULL);
        TEST_ASSERT_EQUAL(0, args[i].result);
    }

    bpool_destroy(bp);
    pstore_close(ps);
}

// =============================================================================
// PERSISTENCE TESTS
// =============================================================================

void test_destroy_flushes_dirty_pages(void) {
    unlink(TEST_DB_FILE);

    struct PageStore *ps = pstore_create(TEST_DB_FILE, 100);
    struct BufPool *bp = bpool_init(ps);

    // Modify several pages
    for (u32 i = 0; i < 10; i++) {
        u32 hint = 0;
        struct PageFrame *frame = bpool_fetch_page(bp, i, &hint, LATCH_EXCLUSIVE);
        TEST_ASSERT_NOT_NULL(frame);

        fill_page_with_pattern(frame->data, i, 0x55);

        rwsx_unlock(&frame->latch, LATCH_EXCLUSIVE);
        bpool_unpin_page(bp, i, &hint, true);
    }

    // Destroy (should flush all dirty pages)
    bpool_destroy(bp);
    pstore_close(ps);

    // Reopen and verify
    u32 num_pages = 0;
    ps = pstore_open(TEST_DB_FILE, &num_pages);
    TEST_ASSERT_NOT_NULL(ps);

    for (u32 i = 0; i < 10; i++) {
        u8 read_buf[PAGE_SIZE];
        pstore_read(ps, i, read_buf);
        verify_page_pattern(read_buf, i, 0x55);
    }

    pstore_close(ps);
    unlink(TEST_DB_FILE);
}

// =============================================================================
// RACE CONDITION TESTS
// =============================================================================

struct concurrent_cold_load_args {
    struct BufPool *bp;
    u32 page_num;
    pthread_barrier_t *barrier;
    struct PageFrame *result_frame;
    u32 hint;
    i32 result;
};

void *concurrent_cold_loader(void *arg) {
    struct concurrent_cold_load_args *args = (struct concurrent_cold_load_args *) arg;

    // Wait for all threads to be ready
    pthread_barrier_wait(args->barrier);

    // Both threads try to fetch the same cold page simultaneously
    args->result_frame = bpool_fetch_page(args->bp, args->page_num, &args->hint, LATCH_SHARED);

    if (args->result_frame == NULL) {
        args->result = -1;
        return NULL;
    }

    args->result = 0;
    return NULL;
}

void test_concurrent_cold_load_same_page_no_duplicate(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    TEST_ASSERT_NOT_NULL(ps);

    // Write data to page 42 in the store (but not in buffer pool yet)
    u8 write_buf[PAGE_SIZE];
    fill_page_with_pattern(write_buf, 42, 0x88);
    pstore_write(ps, 42, write_buf);

    struct BufPool *bp = bpool_init(ps);
    TEST_ASSERT_NOT_NULL(bp);

    // Verify page 42 is not in buffer pool
    for (u32 i = 0; i < POOL_SIZE; i++) {
        TEST_ASSERT_NOT_EQUAL(42, LOAD(&bp->tlb[i], RELAXED));
    }

    // Set up barrier for synchronization
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, 2);

    // Create two threads that will race to load page 42
    pthread_t thread1, thread2;
    struct concurrent_cold_load_args args1 = {
            .bp = bp, .page_num = 42, .barrier = &barrier, .result_frame = NULL, .hint = 0, .result = -1};
    struct concurrent_cold_load_args args2 = {
            .bp = bp, .page_num = 42, .barrier = &barrier, .result_frame = NULL, .hint = 0, .result = -1};

    pthread_create(&thread1, NULL, concurrent_cold_loader, &args1);
    pthread_create(&thread2, NULL, concurrent_cold_loader, &args2);

    pthread_join(thread1, NULL);
    pthread_join(thread2, NULL);

    // Both threads should succeed
    TEST_ASSERT_EQUAL(0, args1.result);
    TEST_ASSERT_EQUAL(0, args2.result);
    TEST_ASSERT_NOT_NULL(args1.result_frame);
    TEST_ASSERT_NOT_NULL(args2.result_frame);

    // CRITICAL: Both threads should see the SAME frame (no duplicate)
    TEST_ASSERT_EQUAL_PTR(args1.result_frame, args2.result_frame);

    // Verify data is correct
    verify_page_pattern(args1.result_frame->data, 42, 0x88);

    // Verify only ONE TLB entry exists for page 42
    u32 tlb_count = 0;
    u32 found_idx = INVALID_PAGE;
    for (u32 i = 0; i < POOL_SIZE; i++) {
        if (LOAD(&bp->tlb[i], RELAXED) == 42) {
            tlb_count++;
            found_idx = i;
        }
    }
    TEST_ASSERT_EQUAL(1, tlb_count);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, found_idx);

    // Verify the frame is at the TLB index we found
    TEST_ASSERT_EQUAL_PTR(&bp->frames[found_idx], args1.result_frame);

    // Pin count should be 2 (one from each thread)
    TEST_ASSERT_EQUAL(2, LOAD(&args1.result_frame->pin_cnt, RELAXED));

    // Cleanup: unlock and unpin from both threads
    rwsx_unlock(&args1.result_frame->latch, LATCH_SHARED);
    bpool_unpin_page(bp, 42, &args1.hint, false);

    rwsx_unlock(&args2.result_frame->latch, LATCH_SHARED);
    bpool_unpin_page(bp, 42, &args2.hint, false);

    TEST_ASSERT_EQUAL(0, LOAD(&args1.result_frame->pin_cnt, RELAXED));

    pthread_barrier_destroy(&barrier);
    bpool_destroy(bp);
    pstore_close(ps);
}

struct concurrent_write_flush_args {
    struct BufPool *bp;
    u32 page_num;
    volatile bool *writer_started;
    volatile bool *writer_done;
    i32 result;
};

void *slow_writer_thread(void *arg) {
    struct concurrent_write_flush_args *args = (struct concurrent_write_flush_args *) arg;

    // Fetch page with X-latch
    u32 hint = 0;
    struct PageFrame *frame = bpool_fetch_page(args->bp, args->page_num, &hint, LATCH_EXCLUSIVE);
    if (frame == NULL) {
        args->result = -1;
        return NULL;
    }

    // Write pattern SLOWLY (0xFF byte by byte with sleep)
    // This gives the flusher thread time to try flushing mid-write
    *args->writer_started = true;

    STORE(&frame->is_dirty, true, RELEASE);
    for (u32 i = 0; i < PAGE_SIZE; i++) {
        frame->data[i] = 0xFF;

        // Sleep every 512 bytes to give flusher a chance
        if (i % 512 == 0) {
            usleep(1000); // 1ms
        }
    }

    *args->writer_done = true;

    // Unpin as dirty
    rwsx_unlock(&frame->latch, LATCH_EXCLUSIVE);
    bpool_unpin_page(args->bp, args->page_num, &hint, true);

    args->result = 0;
    return NULL;
}

void *concurrent_flusher_thread(void *arg) {
    struct concurrent_write_flush_args *args = (struct concurrent_write_flush_args *) arg;

    // Wait for writer to start
    while (!*args->writer_started) {
        usleep(100);
    }

    // Sleep a bit to ensure we're in the middle of the write
    usleep(500);

    // Try to flush while writer is still writing
    // WITHOUT the fix: reads torn data (mix of 0x00 and 0xFF)
    // WITH the fix: blocks on frame S-latch until writer finishes
    u32 hint = 0;
    bpool_flush_page(args->bp, args->page_num, &hint);

    args->result = 0;
    return NULL;
}

void test_concurrent_write_and_flush_no_torn_write(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    TEST_ASSERT_NOT_NULL(ps);

    // Initialize page 50 to all 0x00
    u8 init_buf[PAGE_SIZE];
    memset(init_buf, 0x00, PAGE_SIZE);
    pstore_write(ps, 50, init_buf);

    struct BufPool *bp = bpool_init(ps);
    TEST_ASSERT_NOT_NULL(bp);

    // Setup synchronization
    volatile bool writer_started = false;
    volatile bool writer_done = false;

    struct concurrent_write_flush_args writer_args = {
            .bp = bp, .page_num = 50, .writer_started = &writer_started, .writer_done = &writer_done, .result = -1};

    struct concurrent_write_flush_args flusher_args = {
            .bp = bp, .page_num = 50, .writer_started = &writer_started, .writer_done = &writer_done, .result = -1};

    pthread_t writer_thread, flusher_thread;
    pthread_create(&writer_thread, NULL, slow_writer_thread, &writer_args);
    pthread_create(&flusher_thread, NULL, concurrent_flusher_thread, &flusher_args);

    pthread_join(writer_thread, NULL);
    pthread_join(flusher_thread, NULL);

    TEST_ASSERT_EQUAL(0, writer_args.result);
    TEST_ASSERT_EQUAL(0, flusher_args.result);

    // Read from pagestore and verify consistency
    u8 read_buf[PAGE_SIZE];
    pstore_read(ps, 50, read_buf);

    // Data should be ALL 0xFF (writer's final state)
    // NOT a mix of 0x00 and 0xFF (torn write)
    for (u32 i = 0; i < PAGE_SIZE; i++) {
        if (read_buf[i] != 0xFF) {
            TEST_FAIL_MESSAGE("Detected torn write: page contains mix of 0x00 and 0xFF");
        }
    }

    bpool_destroy(bp);
    pstore_close(ps);
}

void *flush_all_thread(void *arg) {
    struct concurrent_write_flush_args *args = (struct concurrent_write_flush_args *) arg;

    while (!*args->writer_started) {
        usleep(100);
    }

    usleep(500);

    // Call flush_all (should acquire frame latch)
    bpool_flush_all(args->bp);

    args->result = 0;
    return NULL;
}

void test_concurrent_write_and_flush_all_no_torn_write(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    TEST_ASSERT_NOT_NULL(ps);

    // Initialize page 60 to all 0x00
    u8 init_buf[PAGE_SIZE];
    memset(init_buf, 0x00, PAGE_SIZE);
    pstore_write(ps, 60, init_buf);

    struct BufPool *bp = bpool_init(ps);
    TEST_ASSERT_NOT_NULL(bp);

    volatile bool writer_started = false;
    volatile bool writer_done = false;

    struct concurrent_write_flush_args writer_args = {
            .bp = bp, .page_num = 60, .writer_started = &writer_started, .writer_done = &writer_done, .result = -1};

    // Flusher thread that calls flush_all
    struct concurrent_write_flush_args flusher_args = {
            .bp = bp, .page_num = 60, .writer_started = &writer_started, .writer_done = &writer_done, .result = -1};

    pthread_t writer_thread, flusher_thread;
    pthread_create(&writer_thread, NULL, slow_writer_thread, &writer_args);
    pthread_create(&flusher_thread, NULL, flush_all_thread, &flusher_args);

    pthread_join(writer_thread, NULL);
    pthread_join(flusher_thread, NULL);

    TEST_ASSERT_EQUAL(0, writer_args.result);
    TEST_ASSERT_EQUAL(0, flusher_args.result);

    // Verify consistency
    u8 read_buf[PAGE_SIZE];
    pstore_read(ps, 60, read_buf);

    for (u32 i = 0; i < PAGE_SIZE; i++) {
        if (read_buf[i] != 0xFF) {
            TEST_FAIL_MESSAGE("Detected torn write in flush_all: page contains mix of 0x00 and 0xFF");
        }
    }

    bpool_destroy(bp);
    pstore_close(ps);
}

// =============================================================================
// MAIN
// =============================================================================

int suite_setUp(void) { return 0; }

int suite_tearDown(void) {
    unlink(TEST_DB_FILE);
    return 0;
}

void setUp(void) {}

void tearDown(void) {}

int main(void) {
    if (suite_setUp() != 0) {
        return -1;
    }

    UNITY_BEGIN();

    // Basic functionality
    RUN_TEST(test_bpool_init_and_destroy);
    RUN_TEST(test_fetch_page_cold_load);
    RUN_TEST(test_fetch_page_hot);
    RUN_TEST(test_unpin_marks_dirty);
    RUN_TEST(test_flush_dirty_page);
    RUN_TEST(test_flush_clean_page_no_write);
    RUN_TEST(test_flush_all);

    // Pin/unpin semantics
    RUN_TEST(test_multiple_pins_same_page);

    // Clock eviction
    RUN_TEST(test_eviction_clock_algorithm);
    RUN_TEST(test_eviction_skips_pinned_pages);
    RUN_TEST(test_eviction_writes_back_dirty_page);

    // Hint optimization
    RUN_TEST(test_hint_speeds_up_lookup);

    // Latching
    RUN_TEST(test_fetch_with_different_latch_modes);

    // Concurrent access
    RUN_TEST(test_concurrent_readers_same_page);
    RUN_TEST(test_concurrent_readers_different_pages);

    // Race condition tests
    RUN_TEST(test_concurrent_cold_load_same_page_no_duplicate);
    RUN_TEST(test_concurrent_write_and_flush_no_torn_write);
    RUN_TEST(test_concurrent_write_and_flush_all_no_torn_write);

    // Persistence
    RUN_TEST(test_destroy_flushes_dirty_pages);

    return suite_tearDown() + UNITY_END();
}
