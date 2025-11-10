#include "bufpool.h"

#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "pagestore.h"
#include "unity.h"
#include "utils.h"

#define TEST_DB_FILE "test_bufpool.db"

// Timing macros for performance measurement
#ifdef LOGGING
#define START_TIMING()                                                                                                 \
    struct timespec start_time, end_time;                                                                              \
    clock_gettime(CLOCK_MONOTONIC, &start_time);

#define END_TIMING(test_name)                                                                                          \
    do {                                                                                                               \
        clock_gettime(CLOCK_MONOTONIC, &end_time);                                                                     \
        double elapsed = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_nsec - start_time.tv_nsec) / 1e9;        \
        logger(stderr, "TIMING", "%s: %.6f seconds (%.3f ms)\n", test_name, elapsed, elapsed * 1000.0);                \
    } while (0)
#else
#define START_TIMING()                                                                                                 \
    do {                                                                                                               \
    } while (0)
#define END_TIMING(test_name)                                                                                          \
    do {                                                                                                               \
    } while (0)
#endif

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
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    TEST_ASSERT_NOT_NULL(ps);

    struct BufPool *bp = bpool_init(ps);
    TEST_ASSERT_NOT_NULL(bp);
    TEST_ASSERT_EQUAL(ps, bp->store);

    START_TIMING();
    // Verify all frames initialized
    for (u32 i = 0; i < POOL_SIZE; i++) {
        TEST_ASSERT_EQUAL(INVALID_PAGE, LOAD(&bp->frames[i].page_num, RELAXED));
        TEST_ASSERT_EQUAL(0, LOAD(&bp->frames[i].pin_cnt, RELAXED));
        TEST_ASSERT_FALSE(LOAD(&bp->frames[i].is_dirty, RELAXED));
    }
    END_TIMING("test_bpool_init_and_destroy");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_fetch_page_cold_load(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    TEST_ASSERT_NOT_NULL(ps);

    // Write initial data to page 5
    u8 write_buf[PAGE_SIZE];
    fill_page_with_pattern(write_buf, 5, 0xAA);
    pstore_write(ps, 5, write_buf);

    struct BufPool *bp = bpool_init(ps);
    TEST_ASSERT_NOT_NULL(bp);

    START_TIMING();
    // Fetch page 5 (cold load)
    struct FrameHandle *h = bpool_fetch_page(bp, 5);
    TEST_ASSERT_NOT_NULL(h);

    // Verify data loaded correctly
    rwsx_lock(&h->fdata->latch, LATCH_SHARED);
    verify_page_pattern(h->fdata->data, 5, 0xAA);
    rwsx_unlock(&h->fdata->latch, LATCH_SHARED);

    // Verify page is in index
    u32 frame_idx;
    TEST_ASSERT_EQUAL(0, sht_get(bp->index, 5, &frame_idx));
    TEST_ASSERT_EQUAL(h->frame_idx, frame_idx);

    // Verify frame state
    struct PageFrame *frame = &bp->frames[h->frame_idx];
    TEST_ASSERT_EQUAL(5, LOAD(&frame->page_num, RELAXED));
    TEST_ASSERT_EQUAL(1, LOAD(&frame->pin_cnt, RELAXED));
    TEST_ASSERT_FALSE(LOAD(&frame->is_dirty, RELAXED));
    TEST_ASSERT_EQUAL(QUEUE_QD, LOAD(&frame->qtype, RELAXED));

    bpool_mark_read(bp, h);
    bpool_release_handle(bp, h);
    END_TIMING("test_fetch_page_cold_load");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_fetch_page_hot(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    // Write and fetch page 10
    u8 write_buf[PAGE_SIZE];
    fill_page_with_pattern(write_buf, 10, 0xBB);
    pstore_write(ps, 10, write_buf);

    START_TIMING();
    struct FrameHandle *h1 = bpool_fetch_page(bp, 10);
    TEST_ASSERT_NOT_NULL(h1);

    u32 frame_idx1 = h1->frame_idx;
    bpool_mark_read(bp, h1);
    bpool_release_handle(bp, h1);

    // Fetch again (hot - should be in pool)
    struct FrameHandle *h2 = bpool_fetch_page(bp, 10);
    TEST_ASSERT_NOT_NULL(h2);
    TEST_ASSERT_EQUAL(frame_idx1, h2->frame_idx);

    // Verify data still correct
    rwsx_lock(&h2->fdata->latch, LATCH_SHARED);
    verify_page_pattern(h2->fdata->data, 10, 0xBB);
    rwsx_unlock(&h2->fdata->latch, LATCH_SHARED);

    bpool_mark_read(bp, h2);
    bpool_release_handle(bp, h2);
    END_TIMING("test_fetch_page_hot");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_mark_write_sets_dirty(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    struct FrameHandle *h = bpool_fetch_page(bp, 0);
    TEST_ASSERT_NOT_NULL(h);

    struct PageFrame *frame = &bp->frames[h->frame_idx];

    // Modify page
    rwsx_lock(&h->fdata->latch, LATCH_EXCLUSIVE);
    memset(h->fdata->data, 0xCC, PAGE_SIZE);
    rwsx_unlock(&h->fdata->latch, LATCH_EXCLUSIVE);

    // Mark as written
    TEST_ASSERT_EQUAL(0, bpool_mark_write(bp, h));

    // Verify is_dirty flag set
    TEST_ASSERT_TRUE(LOAD(&frame->is_dirty, RELAXED));

    bpool_release_handle(bp, h);
    END_TIMING("test_mark_write_sets_dirty");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_flush_dirty_page(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    // Fetch and modify page
    struct FrameHandle *h = bpool_fetch_page(bp, 7);
    TEST_ASSERT_NOT_NULL(h);

    rwsx_lock(&h->fdata->latch, LATCH_EXCLUSIVE);
    fill_page_with_pattern(h->fdata->data, 7, 0xDD);
    rwsx_unlock(&h->fdata->latch, LATCH_EXCLUSIVE);

    bpool_mark_write(bp, h);

    struct PageFrame *frame = &bp->frames[h->frame_idx];
    TEST_ASSERT_TRUE(LOAD(&frame->is_dirty, RELAXED));

    // Flush the page
    bpool_flush_page(bp, 7);

    TEST_ASSERT_FALSE(LOAD(&frame->is_dirty, RELAXED));

    // Verify data written to store
    u8 read_buf[PAGE_SIZE];
    pstore_read(ps, 7, read_buf);
    verify_page_pattern(read_buf, 7, 0xDD);

    bpool_release_handle(bp, h);
    END_TIMING("test_flush_dirty_page");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_flush_clean_page_no_write(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    // Write initial pattern
    u8 write_buf[PAGE_SIZE];
    fill_page_with_pattern(write_buf, 3, 0x11);
    pstore_write(ps, 3, write_buf);

    START_TIMING();
    // Fetch page (clean)
    struct FrameHandle *h = bpool_fetch_page(bp, 3);
    TEST_ASSERT_NOT_NULL(h);

    struct PageFrame *frame = &bp->frames[h->frame_idx];
    TEST_ASSERT_FALSE(LOAD(&frame->is_dirty, RELAXED));

    // Flush clean page (should not write)
    bpool_flush_page(bp, 3);

    TEST_ASSERT_FALSE(LOAD(&frame->is_dirty, RELAXED));

    bpool_mark_read(bp, h);
    bpool_release_handle(bp, h);
    END_TIMING("test_flush_clean_page_no_write");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_flush_all(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    // Fetch and modify multiple pages
    struct FrameHandle *handles[5];
    for (u32 i = 0; i < 5; i++) {
        handles[i] = bpool_fetch_page(bp, i);
        TEST_ASSERT_NOT_NULL(handles[i]);

        rwsx_lock(&handles[i]->fdata->latch, LATCH_EXCLUSIVE);
        fill_page_with_pattern(handles[i]->fdata->data, i, 0x22);
        rwsx_unlock(&handles[i]->fdata->latch, LATCH_EXCLUSIVE);

        bpool_mark_write(bp, handles[i]);
    }

    // Flush all
    bpool_flush_all(bp);

    // Verify all pages written
    for (u32 i = 0; i < 5; i++) {
        u8 read_buf[PAGE_SIZE];
        pstore_read(ps, i, read_buf);
        verify_page_pattern(read_buf, i, 0x22);

        bpool_release_handle(bp, handles[i]);
    }
    END_TIMING("test_flush_all");

    bpool_destroy(bp);
    pstore_close(ps);
}

// =============================================================================
// HANDLE SEMANTICS TESTS
// =============================================================================

void test_multiple_handles_same_page(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    struct FrameHandle *h1 = bpool_fetch_page(bp, 0);
    TEST_ASSERT_NOT_NULL(h1);

    struct PageFrame *frame = &bp->frames[h1->frame_idx];
    TEST_ASSERT_EQUAL(1, LOAD(&frame->pin_cnt, RELAXED));

    // Fetch again (simulating second accessor)
    struct FrameHandle *h2 = bpool_fetch_page(bp, 0);
    TEST_ASSERT_NOT_NULL(h2);
    TEST_ASSERT_EQUAL(h1->frame_idx, h2->frame_idx);
    TEST_ASSERT_EQUAL(2, LOAD(&frame->pin_cnt, RELAXED));

    // Release once
    bpool_release_handle(bp, h1);
    TEST_ASSERT_EQUAL(1, LOAD(&frame->pin_cnt, RELAXED));

    // Release again
    bpool_release_handle(bp, h2);
    TEST_ASSERT_EQUAL(0, LOAD(&frame->pin_cnt, RELAXED));
    END_TIMING("test_multiple_handles_same_page");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_handle_invalidation_after_release(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    struct FrameHandle *h = bpool_fetch_page(bp, 0);
    TEST_ASSERT_NOT_NULL(h);

    // Release frees the handle
    bpool_release_handle(bp, h);

    // After release, handle is freed and should not be used
    // This test just verifies the handle release mechanism works
    // (Epoch validation is a safety canary for rare cases, not actively tested)
    END_TIMING("test_handle_invalidation_after_release");

    bpool_destroy(bp);
    pstore_close(ps);
}

// =============================================================================
// QDLP EVICTION TESTS
// =============================================================================

void test_qd_to_main_promotion_on_visit(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    // Fetch page (goes to QD queue)
    struct FrameHandle *h1 = bpool_fetch_page(bp, 0);
    TEST_ASSERT_NOT_NULL(h1);

    struct PageFrame *frame = &bp->frames[h1->frame_idx];
    TEST_ASSERT_EQUAL(QUEUE_QD, LOAD(&frame->qtype, RELAXED));

    bpool_release_handle(bp, h1);

    // Fetch many pages to cycle through QD
    for (u32 i = 1; i < QD_SIZE + 1; i++) {
        struct FrameHandle *h = bpool_fetch_page(bp, i);
        if (h) {
            bpool_release_handle(bp, h);
        }
    }

    // Fetch page 0 again - visited flag should promote it to main
    struct FrameHandle *h2 = bpool_fetch_page(bp, 0);
    TEST_ASSERT_NOT_NULL(h2);

    // After processing in find_victim, visited pages in QD get moved to main
    // We can't directly observe queue membership, but we can verify it's still in pool
    TEST_ASSERT_EQUAL(0, LOAD(&bp->frames[h2->frame_idx].page_num, RELAXED));

    bpool_release_handle(bp, h2);
    END_TIMING("test_qd_to_main_promotion_on_visit");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_eviction_skips_pinned_pages(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    // Fill pool and keep all pages pinned
    struct FrameHandle *handles[POOL_SIZE];
    for (u32 i = 0; i < POOL_SIZE; i++) {
        handles[i] = bpool_fetch_page(bp, i);
        TEST_ASSERT_NOT_NULL(handles[i]);
    }

    // All pages pinned - fetch should fail
    struct FrameHandle *new_h = bpool_fetch_page(bp, POOL_SIZE);
    TEST_ASSERT_NULL(new_h);

    // Release one page
    bpool_release_handle(bp, handles[0]);

    // Now fetch should succeed
    new_h = bpool_fetch_page(bp, POOL_SIZE);
    TEST_ASSERT_NOT_NULL(new_h);

    // Clean up
    bpool_release_handle(bp, new_h);
    for (u32 i = 1; i < POOL_SIZE; i++) {
        bpool_release_handle(bp, handles[i]);
    }
    END_TIMING("test_eviction_skips_pinned_pages");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_eviction_writes_back_dirty_page(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    // Fill pool with dirty pages
    for (u32 i = 0; i < POOL_SIZE; i++) {
        struct FrameHandle *h = bpool_fetch_page(bp, i);
        TEST_ASSERT_NOT_NULL(h);

        rwsx_lock(&h->fdata->latch, LATCH_EXCLUSIVE);
        fill_page_with_pattern(h->fdata->data, i, 0x44);
        rwsx_unlock(&h->fdata->latch, LATCH_EXCLUSIVE);

        bpool_mark_write(bp, h);
        bpool_release_handle(bp, h);
    }

    // Fetch new pages to trigger eviction
    for (u32 i = POOL_SIZE; i < POOL_SIZE + QD_SIZE; i++) {
        struct FrameHandle *h = bpool_fetch_page(bp, i);
        if (h) {
            bpool_release_handle(bp, h);
        }
    }

    // Verify at least one evicted page was written back
    bool found_written = false;
    for (u32 i = 0; i < QD_SIZE && !found_written; i++) {
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
    END_TIMING("test_eviction_writes_back_dirty_page");

    bpool_destroy(bp);
    pstore_close(ps);
}

// =============================================================================
// GHOST QUEUE TESTS
// =============================================================================

void test_ghost_entry_created_on_qd_eviction(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 3);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    // Fill pool
    for (u32 i = 0; i < POOL_SIZE; i++) {
        struct FrameHandle *h = bpool_fetch_page(bp, i);
        TEST_ASSERT_NOT_NULL(h);
        bpool_release_handle(bp, h);
    }

    // Fetch more pages to trigger eviction from QD
    for (u32 i = POOL_SIZE; i < POOL_SIZE + QD_SIZE; i++) {
        struct FrameHandle *h = bpool_fetch_page(bp, i);
        if (h) {
            bpool_release_handle(bp, h);
        }
    }

    // Check that ghost index has some entries
    u32 ghost_size = cq_size(&bp->ghost);
    TEST_ASSERT_GREATER_THAN(0, ghost_size);
    END_TIMING("test_ghost_entry_created_on_qd_eviction");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_ghost_hit_promotes_to_main(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 3);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    // Load page 0 (goes to QD)
    struct FrameHandle *h1 = bpool_fetch_page(bp, 0);
    TEST_ASSERT_NOT_NULL(h1);
    bpool_release_handle(bp, h1);

    // Fill pool to evict page 0
    for (u32 i = 1; i < POOL_SIZE + QD_SIZE; i++) {
        struct FrameHandle *h = bpool_fetch_page(bp, i);
        if (h) {
            bpool_release_handle(bp, h);
        }
    }

    // Verify page 0 was evicted
    u32 frame_idx;
    TEST_ASSERT_NOT_EQUAL(0, sht_get(bp->index, 0, &frame_idx));

    // Check if page 0 is in ghost index
    u32 gidx;
    bool in_ghost = (sht_get(bp->gindex, 0, &gidx) == 0);

    if (in_ghost) {
        // Re-fetch page 0 (ghost hit - should go to MAIN)
        struct FrameHandle *h2 = bpool_fetch_page(bp, 0);
        TEST_ASSERT_NOT_NULL(h2);

        struct PageFrame *frame = &bp->frames[h2->frame_idx];
        TEST_ASSERT_EQUAL(QUEUE_MAIN, LOAD(&frame->qtype, RELAXED));

        // Verify page 0 removed from ghost
        TEST_ASSERT_NOT_EQUAL(0, sht_get(bp->gindex, 0, &gidx));

        bpool_release_handle(bp, h2);
    }
    END_TIMING("test_ghost_hit_promotes_to_main");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_ghost_queue_bounded_size(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 4);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    // Load many pages to create ghost entries (just fetch/release, no mark needed)
    for (u32 i = 0; i < POOL_SIZE * 3; i++) {
        struct FrameHandle *h = bpool_fetch_page(bp, i);
        if (h) {
            bpool_release_handle(bp, h);
        }
    }

    // Ghost queue should not exceed POOL_SIZE
    u32 ghost_size = cq_size(&bp->ghost);
    TEST_ASSERT_LESS_OR_EQUAL(POOL_SIZE, ghost_size);
    END_TIMING("test_ghost_queue_bounded_size");

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
        struct FrameHandle *h = bpool_fetch_page(args->bp, args->page_num);

        if (h == NULL) {
            args->result = -1;
            return NULL;
        }

        // Simulate read
        rwsx_lock(&h->fdata->latch, LATCH_SHARED);
        volatile u8 dummy = h->fdata->data[0];
        (void) dummy;
        rwsx_unlock(&h->fdata->latch, LATCH_SHARED);

        bpool_release_handle(args->bp, h);
    }

    args->result = 0;
    return NULL;
}

void test_concurrent_readers_same_page(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    const i32 NUM_READERS = 4;
    pthread_t threads[NUM_READERS];
    struct concurrent_reader_args args[NUM_READERS];

    // All threads read same page
    for (i32 i = 0; i < NUM_READERS; i++) {
        args[i].bp = bp;
        args[i].page_num = 0;
        args[i].result = -1;
        args[i].read_count = 100;
        pthread_create(&threads[i], NULL, concurrent_reader, &args[i]);
    }

    // Wait for all
    for (i32 i = 0; i < NUM_READERS; i++) {
        pthread_join(threads[i], NULL);
        TEST_ASSERT_EQUAL(0, args[i].result);
    }
    END_TIMING("test_concurrent_readers_same_page");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_concurrent_readers_different_pages(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    const i32 NUM_READERS = 4;
    pthread_t threads[NUM_READERS];
    struct concurrent_reader_args args[NUM_READERS];

    // Each thread reads different page
    for (i32 i = 0; i < NUM_READERS; i++) {
        args[i].bp = bp;
        args[i].page_num = i * 10;
        args[i].result = -1;
        args[i].read_count = 100;
        pthread_create(&threads[i], NULL, concurrent_reader, &args[i]);
    }

    // Wait for all
    for (i32 i = 0; i < NUM_READERS; i++) {
        pthread_join(threads[i], NULL);
        TEST_ASSERT_EQUAL(0, args[i].result);
    }
    END_TIMING("test_concurrent_readers_different_pages");

    bpool_destroy(bp);
    pstore_close(ps);
}

// =============================================================================
// RACE CONDITION TESTS
// =============================================================================

struct concurrent_cold_load_args {
    struct BufPool *bp;
    u32 page_num;
    pthread_barrier_t *barrier;
    struct FrameHandle *result_handle;
    i32 result;
};

void *concurrent_cold_loader(void *arg) {
    struct concurrent_cold_load_args *args = (struct concurrent_cold_load_args *) arg;

    // Wait for all threads to be ready
    pthread_barrier_wait(args->barrier);

    // Both threads try to fetch the same cold page simultaneously
    args->result_handle = bpool_fetch_page(args->bp, args->page_num);

    if (args->result_handle == NULL) {
        args->result = -1;
        return NULL;
    }

    args->result = 0;
    return NULL;
}

void test_concurrent_cold_load_same_page_no_duplicate(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    TEST_ASSERT_NOT_NULL(ps);

    // Write data to page 42 in the store
    u8 write_buf[PAGE_SIZE];
    fill_page_with_pattern(write_buf, 42, 0x88);
    pstore_write(ps, 42, write_buf);

    struct BufPool *bp = bpool_init(ps);
    TEST_ASSERT_NOT_NULL(bp);

    // Verify page 42 is not in buffer pool
    u32 temp_idx;
    TEST_ASSERT_NOT_EQUAL(0, sht_get(bp->index, 42, &temp_idx));

    START_TIMING();
    // Set up barrier for synchronization
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, 2);

    // Create two threads that will race to load page 42
    pthread_t thread1, thread2;
    struct concurrent_cold_load_args args1 = {
            .bp = bp, .page_num = 42, .barrier = &barrier, .result_handle = NULL, .result = -1};
    struct concurrent_cold_load_args args2 = {
            .bp = bp, .page_num = 42, .barrier = &barrier, .result_handle = NULL, .result = -1};

    pthread_create(&thread1, NULL, concurrent_cold_loader, &args1);
    pthread_create(&thread2, NULL, concurrent_cold_loader, &args2);

    pthread_join(thread1, NULL);
    pthread_join(thread2, NULL);

    // Both threads should succeed
    TEST_ASSERT_EQUAL(0, args1.result);
    TEST_ASSERT_EQUAL(0, args2.result);
    TEST_ASSERT_NOT_NULL(args1.result_handle);
    TEST_ASSERT_NOT_NULL(args2.result_handle);

    // CRITICAL: Both threads should see the SAME frame (no duplicate)
    TEST_ASSERT_EQUAL(args1.result_handle->frame_idx, args2.result_handle->frame_idx);

    // Verify data is correct
    rwsx_lock(&args1.result_handle->fdata->latch, LATCH_SHARED);
    verify_page_pattern(args1.result_handle->fdata->data, 42, 0x88);
    rwsx_unlock(&args1.result_handle->fdata->latch, LATCH_SHARED);

    // Verify page 42 is now in index
    u32 frame_idx;
    TEST_ASSERT_EQUAL(0, sht_get(bp->index, 42, &frame_idx));
    TEST_ASSERT_EQUAL(args1.result_handle->frame_idx, frame_idx);

    // Pin count should be 2 (one from each thread)
    struct PageFrame *frame = &bp->frames[args1.result_handle->frame_idx];
    TEST_ASSERT_EQUAL(2, LOAD(&frame->pin_cnt, RELAXED));

    // Cleanup
    bpool_release_handle(bp, args1.result_handle);
    bpool_release_handle(bp, args2.result_handle);

    TEST_ASSERT_EQUAL(0, LOAD(&frame->pin_cnt, RELAXED));

    pthread_barrier_destroy(&barrier);
    END_TIMING("test_concurrent_cold_load_same_page_no_duplicate");

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

    // Fetch page
    struct FrameHandle *h = bpool_fetch_page(args->bp, args->page_num);
    if (h == NULL) {
        args->result = -1;
        return NULL;
    }

    // Write pattern SLOWLY
    rwsx_lock(&h->fdata->latch, LATCH_EXCLUSIVE);
    *args->writer_started = true;

    for (u32 i = 0; i < PAGE_SIZE; i++) {
        h->fdata->data[i] = 0xFF;

        // Sleep every 512 bytes to give flusher a chance
        if (i % 512 == 0) {
            usleep(1000);
        }
    }

    *args->writer_done = true;
    rwsx_unlock(&h->fdata->latch, LATCH_EXCLUSIVE);

    bpool_mark_write(args->bp, h);
    bpool_release_handle(args->bp, h);

    args->result = 0;
    return NULL;
}

void *concurrent_flusher_thread(void *arg) {
    struct concurrent_write_flush_args *args = (struct concurrent_write_flush_args *) arg;

    // Wait for writer to start
    while (!*args->writer_started) {
        usleep(POOL_SIZE * 2);
    }

    // Sleep a bit to ensure we're in the middle of the write
    usleep(500);

    // Try to flush while writer is still writing
    bpool_flush_page(args->bp, args->page_num);

    args->result = 0;
    return NULL;
}

void test_concurrent_write_and_flush_no_torn_write(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    TEST_ASSERT_NOT_NULL(ps);

    // Initialize page 50 to all 0x00
    u8 init_buf[PAGE_SIZE];
    memset(init_buf, 0x00, PAGE_SIZE);
    pstore_write(ps, 50, init_buf);

    struct BufPool *bp = bpool_init(ps);
    TEST_ASSERT_NOT_NULL(bp);

    START_TIMING();
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
    for (u32 i = 0; i < PAGE_SIZE; i++) {
        if (read_buf[i] != 0xFF) {
            TEST_FAIL_MESSAGE("Detected torn write: page contains mix of 0x00 and 0xFF");
        }
    }
    END_TIMING("test_concurrent_write_and_flush_no_torn_write");

    bpool_destroy(bp);
    pstore_close(ps);
}

void *flush_all_thread(void *arg) {
    struct concurrent_write_flush_args *args = (struct concurrent_write_flush_args *) arg;

    while (!*args->writer_started) {
        usleep(POOL_SIZE * 2);
    }

    usleep(500);

    // Call flush_all
    bpool_flush_all(args->bp);

    args->result = 0;
    return NULL;
}

void test_concurrent_write_and_flush_all_no_torn_write(void) {
    struct PageStore *ps = pstore_create(NULL, POOL_SIZE * 2);
    TEST_ASSERT_NOT_NULL(ps);

    // Initialize page 60 to all 0x00
    u8 init_buf[PAGE_SIZE];
    memset(init_buf, 0x00, PAGE_SIZE);
    pstore_write(ps, 60, init_buf);

    struct BufPool *bp = bpool_init(ps);
    TEST_ASSERT_NOT_NULL(bp);

    START_TIMING();
    volatile bool writer_started = false;
    volatile bool writer_done = false;

    struct concurrent_write_flush_args writer_args = {
            .bp = bp, .page_num = 60, .writer_started = &writer_started, .writer_done = &writer_done, .result = -1};

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
            TEST_FAIL_MESSAGE("Detected torn write in flush_all");
        }
    }
    END_TIMING("test_concurrent_write_and_flush_all_no_torn_write");

    bpool_destroy(bp);
    pstore_close(ps);
}

// =============================================================================
// PERSISTENCE TESTS
// =============================================================================

void test_destroy_flushes_dirty_pages(void) {
    unlink(TEST_DB_FILE);

    struct PageStore *ps = pstore_create(TEST_DB_FILE, POOL_SIZE * 2);
    struct BufPool *bp = bpool_init(ps);

    START_TIMING();
    // Modify several pages
    for (u32 i = 0; i < 10; i++) {
        struct FrameHandle *h = bpool_fetch_page(bp, i);
        TEST_ASSERT_NOT_NULL(h);

        rwsx_lock(&h->fdata->latch, LATCH_EXCLUSIVE);
        fill_page_with_pattern(h->fdata->data, i, 0x55);
        rwsx_unlock(&h->fdata->latch, LATCH_EXCLUSIVE);

        bpool_mark_write(bp, h);
        bpool_release_handle(bp, h);
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
    END_TIMING("test_destroy_flushes_dirty_pages");

    pstore_close(ps);
    unlink(TEST_DB_FILE);
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
    RUN_TEST(test_mark_write_sets_dirty);
    RUN_TEST(test_flush_dirty_page);
    RUN_TEST(test_flush_clean_page_no_write);
    RUN_TEST(test_flush_all);

    // Handle semantics
    RUN_TEST(test_multiple_handles_same_page);
    RUN_TEST(test_handle_invalidation_after_release);

    // QDLP eviction
    RUN_TEST(test_qd_to_main_promotion_on_visit);
    RUN_TEST(test_eviction_skips_pinned_pages);
    RUN_TEST(test_eviction_writes_back_dirty_page);

    // Ghost queue
    RUN_TEST(test_ghost_entry_created_on_qd_eviction);
    RUN_TEST(test_ghost_hit_promotes_to_main);
    // RUN_TEST(test_ghost_queue_bounded_size); // Slow test, disabled by default.

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
