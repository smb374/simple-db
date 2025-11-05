#include "pagestore.h"

#include <pthread.h>
#include <string.h>
#include <unistd.h>

#include "unity.h"
#include "utils.h"

// Test file paths
#define TEST_DB_FILE "test_pagestore.db"
#define TEST_DB_REOPEN "test_pagestore_reopen.db"

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

static void fill_page_with_pattern(u8 *page, u32 page_num, u8 pattern_base) {
    for (u32 i = 0; i < PAGE_SIZE; i++) {
        page[i] = (u8) (pattern_base + page_num + (i % 256));
    }
}

static void verify_page_pattern(const u8 *page, u32 page_num, u8 pattern_base) {
    for (u32 i = 0; i < PAGE_SIZE; i++) {
        u8 expected = (u8) (pattern_base + page_num + (i % 256));
        TEST_ASSERT_EQUAL_HEX8(expected, page[i]);
    }
}

// =============================================================================
// IN-MEMORY MODE TESTS
// =============================================================================

void test_inmem_create_and_close(void) {
    struct PageStore *ps = pstore_create(NULL, 10);
    TEST_ASSERT_NOT_NULL(ps);
    TEST_ASSERT_EQUAL(-1, ps->fd);
    TEST_ASSERT_NOT_NULL(ps->mmap_addr);
    TEST_ASSERT_EQUAL(10 * PAGE_SIZE, ps->store_size);

    pstore_close(ps);
}

void test_inmem_write_and_read_single_page(void) {
    struct PageStore *ps = pstore_create(NULL, 10);
    TEST_ASSERT_NOT_NULL(ps);

    u8 write_buf[PAGE_SIZE];
    u8 read_buf[PAGE_SIZE];

    // Fill with pattern
    fill_page_with_pattern(write_buf, 0, 0xAA);

    // Write page 0
    i32 ret = pstore_write(ps, 0, write_buf);
    TEST_ASSERT_EQUAL(0, ret);

    // Read page 0
    ret = pstore_read(ps, 0, read_buf);
    TEST_ASSERT_EQUAL(0, ret);

    // Verify
    TEST_ASSERT_EQUAL_MEMORY(write_buf, read_buf, PAGE_SIZE);

    pstore_close(ps);
}

void test_inmem_write_and_read_multiple_pages(void) {
    struct PageStore *ps = pstore_create(NULL, 20);
    TEST_ASSERT_NOT_NULL(ps);

    u8 write_buf[PAGE_SIZE];
    u8 read_buf[PAGE_SIZE];

    // Write pages with different patterns
    for (u32 i = 0; i < 20; i++) {
        fill_page_with_pattern(write_buf, i, 0x10);
        i32 ret = pstore_write(ps, i, write_buf);
        TEST_ASSERT_EQUAL(0, ret);
    }

    // Read and verify all pages
    for (u32 i = 0; i < 20; i++) {
        i32 ret = pstore_read(ps, i, read_buf);
        TEST_ASSERT_EQUAL(0, ret);
        verify_page_pattern(read_buf, i, 0x10);
    }

    pstore_close(ps);
}

void test_inmem_grow(void) {
    struct PageStore *ps = pstore_create(NULL, 10);
    TEST_ASSERT_NOT_NULL(ps);
    TEST_ASSERT_EQUAL(10 * PAGE_SIZE, ps->store_size);

    // Write to initial pages
    u8 write_buf[PAGE_SIZE];
    fill_page_with_pattern(write_buf, 5, 0x55);
    i32 ret = pstore_write(ps, 5, write_buf);
    TEST_ASSERT_EQUAL(0, ret);

    // Grow by 10 more pages
    ret = pstore_grow(ps, 10);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_EQUAL(20 * PAGE_SIZE, ps->store_size);

    // Verify old data still intact
    u8 read_buf[PAGE_SIZE];
    ret = pstore_read(ps, 5, read_buf);
    TEST_ASSERT_EQUAL(0, ret);
    verify_page_pattern(read_buf, 5, 0x55);

    // Write to new pages
    fill_page_with_pattern(write_buf, 15, 0x77);
    ret = pstore_write(ps, 15, write_buf);
    TEST_ASSERT_EQUAL(0, ret);

    // Read from new pages
    ret = pstore_read(ps, 15, read_buf);
    TEST_ASSERT_EQUAL(0, ret);
    verify_page_pattern(read_buf, 15, 0x77);

    pstore_close(ps);
}

void test_inmem_grow_multiple_times(void) {
    struct PageStore *ps = pstore_create(NULL, 5);
    TEST_ASSERT_NOT_NULL(ps);

    // Grow in steps
    for (u32 i = 0; i < 5; i++) {
        i32 ret = pstore_grow(ps, 5);
        TEST_ASSERT_EQUAL(0, ret);
        TEST_ASSERT_EQUAL((5 + (i + 1) * 5) * PAGE_SIZE, ps->store_size);
    }

    // Final size should be 30 pages
    TEST_ASSERT_EQUAL(30 * PAGE_SIZE, ps->store_size);

    // Write to various pages across the range
    u8 write_buf[PAGE_SIZE];
    u8 read_buf[PAGE_SIZE];

    u32 test_pages[] = {0, 5, 10, 15, 20, 25, 29};
    for (u32 i = 0; i < 7; i++) {
        u32 page_num = test_pages[i];
        fill_page_with_pattern(write_buf, page_num, 0xCC);
        i32 ret = pstore_write(ps, page_num, write_buf);
        TEST_ASSERT_EQUAL(0, ret);
    }

    // Verify all
    for (u32 i = 0; i < 7; i++) {
        u32 page_num = test_pages[i];
        i32 ret = pstore_read(ps, page_num, read_buf);
        TEST_ASSERT_EQUAL(0, ret);
        verify_page_pattern(read_buf, page_num, 0xCC);
    }

    pstore_close(ps);
}

void test_inmem_sync_is_noop(void) {
    struct PageStore *ps = pstore_create(NULL, 10);
    TEST_ASSERT_NOT_NULL(ps);

    // Sync should succeed but do nothing for in-memory
    i32 ret = pstore_sync(ps);
    TEST_ASSERT_EQUAL(0, ret);

    pstore_close(ps);
}

// =============================================================================
// FILE-BACKED MODE TESTS
// =============================================================================

void test_file_create_and_close(void) {
    // Clean up any existing file
    unlink(TEST_DB_FILE);

    struct PageStore *ps = pstore_create(TEST_DB_FILE, 10);
    TEST_ASSERT_NOT_NULL(ps);
    TEST_ASSERT_NOT_EQUAL(-1, ps->fd);
    TEST_ASSERT_NULL(ps->mmap_addr);
    TEST_ASSERT_EQUAL(10 * PAGE_SIZE, ps->store_size);

    pstore_close(ps);

    // Verify file exists
    TEST_ASSERT_EQUAL(0, access(TEST_DB_FILE, F_OK));

    // Clean up
    unlink(TEST_DB_FILE);
}

void test_file_write_and_read_single_page(void) {
    unlink(TEST_DB_FILE);

    struct PageStore *ps = pstore_create(TEST_DB_FILE, 10);
    TEST_ASSERT_NOT_NULL(ps);

    u8 write_buf[PAGE_SIZE];
    u8 read_buf[PAGE_SIZE];

    fill_page_with_pattern(write_buf, 3, 0xBB);

    // Write page 3
    i32 ret = pstore_write(ps, 3, write_buf);
    TEST_ASSERT_EQUAL(0, ret);

    // Read page 3
    ret = pstore_read(ps, 3, read_buf);
    TEST_ASSERT_EQUAL(0, ret);

    TEST_ASSERT_EQUAL_MEMORY(write_buf, read_buf, PAGE_SIZE);

    pstore_close(ps);
    unlink(TEST_DB_FILE);
}

void test_file_write_and_read_multiple_pages(void) {
    unlink(TEST_DB_FILE);

    struct PageStore *ps = pstore_create(TEST_DB_FILE, 50);
    TEST_ASSERT_NOT_NULL(ps);

    u8 write_buf[PAGE_SIZE];
    u8 read_buf[PAGE_SIZE];

    // Write all pages
    for (u32 i = 0; i < 50; i++) {
        fill_page_with_pattern(write_buf, i, 0x33);
        i32 ret = pstore_write(ps, i, write_buf);
        TEST_ASSERT_EQUAL(0, ret);
    }

    // Read and verify all pages
    for (u32 i = 0; i < 50; i++) {
        i32 ret = pstore_read(ps, i, read_buf);
        TEST_ASSERT_EQUAL(0, ret);
        verify_page_pattern(read_buf, i, 0x33);
    }

    pstore_close(ps);
    unlink(TEST_DB_FILE);
}

void test_file_persistence_after_close(void) {
    unlink(TEST_DB_REOPEN);

    // Create and write data
    struct PageStore *ps = pstore_create(TEST_DB_REOPEN, 20);
    TEST_ASSERT_NOT_NULL(ps);

    u8 write_buf[PAGE_SIZE];
    for (u32 i = 0; i < 20; i++) {
        fill_page_with_pattern(write_buf, i, 0xEE);
        i32 ret = pstore_write(ps, i, write_buf);
        TEST_ASSERT_EQUAL(0, ret);
    }

    // Sync to ensure data is written
    i32 ret = pstore_sync(ps);
    TEST_ASSERT_EQUAL(0, ret);

    pstore_close(ps);

    // Reopen the file
    u32 num_pages = 0;
    ps = pstore_open(TEST_DB_REOPEN, &num_pages);
    TEST_ASSERT_NOT_NULL(ps);
    TEST_ASSERT_EQUAL(20, num_pages);

    // Verify all data persisted
    u8 read_buf[PAGE_SIZE];
    for (u32 i = 0; i < 20; i++) {
        ret = pstore_read(ps, i, read_buf);
        TEST_ASSERT_EQUAL(0, ret);
        verify_page_pattern(read_buf, i, 0xEE);
    }

    pstore_close(ps);
    unlink(TEST_DB_REOPEN);
}

void test_file_open_nonexistent(void) {
    unlink("nonexistent.db");

    u32 num_pages = 0;
    struct PageStore *ps = pstore_open("nonexistent.db", &num_pages);
    TEST_ASSERT_NULL(ps);
}

void test_file_grow(void) {
    unlink(TEST_DB_FILE);

    struct PageStore *ps = pstore_create(TEST_DB_FILE, 10);
    TEST_ASSERT_NOT_NULL(ps);
    TEST_ASSERT_EQUAL(10 * PAGE_SIZE, ps->store_size);

    // Write to initial pages
    u8 write_buf[PAGE_SIZE];
    fill_page_with_pattern(write_buf, 7, 0x99);
    i32 ret = pstore_write(ps, 7, write_buf);
    TEST_ASSERT_EQUAL(0, ret);

    // Grow by 20 pages
    ret = pstore_grow(ps, 20);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_EQUAL(30 * PAGE_SIZE, ps->store_size);

    // Verify old data intact
    u8 read_buf[PAGE_SIZE];
    ret = pstore_read(ps, 7, read_buf);
    TEST_ASSERT_EQUAL(0, ret);
    verify_page_pattern(read_buf, 7, 0x99);

    // Write to new pages
    fill_page_with_pattern(write_buf, 25, 0x88);
    ret = pstore_write(ps, 25, write_buf);
    TEST_ASSERT_EQUAL(0, ret);

    // Read from new pages
    ret = pstore_read(ps, 25, read_buf);
    TEST_ASSERT_EQUAL(0, ret);
    verify_page_pattern(read_buf, 25, 0x88);

    pstore_close(ps);
    unlink(TEST_DB_FILE);
}

void test_file_grow_and_persist(void) {
    unlink(TEST_DB_REOPEN);

    // Create with 10 pages
    struct PageStore *ps = pstore_create(TEST_DB_REOPEN, 10);
    TEST_ASSERT_NOT_NULL(ps);

    // Write to page 5
    u8 write_buf[PAGE_SIZE];
    fill_page_with_pattern(write_buf, 5, 0x11);
    i32 ret = pstore_write(ps, 5, write_buf);
    TEST_ASSERT_EQUAL(0, ret);

    // Grow to 30 pages
    ret = pstore_grow(ps, 20);
    TEST_ASSERT_EQUAL(0, ret);

    // Write to page 25
    fill_page_with_pattern(write_buf, 25, 0x22);
    ret = pstore_write(ps, 25, write_buf);
    TEST_ASSERT_EQUAL(0, ret);

    // Sync and close
    pstore_sync(ps);
    pstore_close(ps);

    // Reopen
    u32 num_pages = 0;
    ps = pstore_open(TEST_DB_REOPEN, &num_pages);
    TEST_ASSERT_NOT_NULL(ps);
    TEST_ASSERT_EQUAL(30, num_pages);

    // Verify both pages
    u8 read_buf[PAGE_SIZE];
    ret = pstore_read(ps, 5, read_buf);
    TEST_ASSERT_EQUAL(0, ret);
    verify_page_pattern(read_buf, 5, 0x11);

    ret = pstore_read(ps, 25, read_buf);
    TEST_ASSERT_EQUAL(0, ret);
    verify_page_pattern(read_buf, 25, 0x22);

    pstore_close(ps);
    unlink(TEST_DB_REOPEN);
}

void test_file_sync(void) {
    unlink(TEST_DB_FILE);

    struct PageStore *ps = pstore_create(TEST_DB_FILE, 10);
    TEST_ASSERT_NOT_NULL(ps);

    u8 write_buf[PAGE_SIZE];
    fill_page_with_pattern(write_buf, 0, 0xDD);
    i32 ret = pstore_write(ps, 0, write_buf);
    TEST_ASSERT_EQUAL(0, ret);

    // Sync should succeed
    ret = pstore_sync(ps);
    TEST_ASSERT_EQUAL(0, ret);

    pstore_close(ps);
    unlink(TEST_DB_FILE);
}

// =============================================================================
// EDGE CASE & ERROR HANDLING TESTS
// =============================================================================

void test_read_out_of_bounds(void) {
    struct PageStore *ps = pstore_create(NULL, 10);
    TEST_ASSERT_NOT_NULL(ps);

    u8 read_buf[PAGE_SIZE];

    // Try to read page 10 (valid pages are 0-9)
    i32 ret = pstore_read(ps, 10, read_buf);
    TEST_ASSERT_NOT_EQUAL(0, ret);

    // Try to read page 100
    ret = pstore_read(ps, 100, read_buf);
    TEST_ASSERT_NOT_EQUAL(0, ret);

    pstore_close(ps);
}

void test_write_out_of_bounds(void) {
    struct PageStore *ps = pstore_create(NULL, 10);
    TEST_ASSERT_NOT_NULL(ps);

    u8 write_buf[PAGE_SIZE];
    memset(write_buf, 0xAA, PAGE_SIZE);

    // Try to write page 10 (valid pages are 0-9)
    i32 ret = pstore_write(ps, 10, write_buf);
    TEST_ASSERT_NOT_EQUAL(0, ret);

    // Try to write page 100
    ret = pstore_write(ps, 100, write_buf);
    TEST_ASSERT_NOT_EQUAL(0, ret);

    pstore_close(ps);
}

void test_create_with_zero_pages(void) {
    // Creating with 0 pages might fail or create empty store
    struct PageStore *ps = pstore_create(NULL, 0);

    if (ps != NULL) {
        // If it succeeds, store_size should be 0
        TEST_ASSERT_EQUAL(0, ps->store_size);
        pstore_close(ps);
    }
    // Either behavior (NULL or empty store) is acceptable
}

void test_grow_by_zero(void) {
    struct PageStore *ps = pstore_create(NULL, 10);
    TEST_ASSERT_NOT_NULL(ps);

    u64 original_size = ps->store_size;

    // Growing by 0 should either succeed with no change or fail
    i32 ret = pstore_grow(ps, 0);

    // Size should not change
    TEST_ASSERT_EQUAL(original_size, ps->store_size);

    pstore_close(ps);
}

void test_open_returns_null_for_inmem(void) {
    // pstore_open should return NULL for in-memory mode
    u32 num_pages = 0;
    struct PageStore *ps = pstore_open(NULL, &num_pages);
    TEST_ASSERT_NULL(ps);
}

// =============================================================================
// CONCURRENT ACCESS TESTS
// =============================================================================

struct concurrent_test_args {
    struct PageStore *ps;
    u32 start_page;
    u32 num_pages;
    u8 pattern;
    i32 result;
};

void *concurrent_writer(void *arg) {
    struct concurrent_test_args *args = (struct concurrent_test_args *) arg;
    u8 write_buf[PAGE_SIZE];

    for (u32 i = 0; i < args->num_pages; i++) {
        u32 page_num = args->start_page + i;
        fill_page_with_pattern(write_buf, page_num, args->pattern);
        i32 ret = pstore_write(args->ps, page_num, write_buf);
        if (ret != 0) {
            args->result = ret;
            return NULL;
        }
    }

    args->result = 0;
    return NULL;
}

void test_concurrent_writes_different_pages(void) {
    struct PageStore *ps = pstore_create(NULL, 100);
    TEST_ASSERT_NOT_NULL(ps);

    const i32 NUM_THREADS = 4;
    pthread_t threads[NUM_THREADS];
    struct concurrent_test_args args[NUM_THREADS];

    // Each thread writes to non-overlapping pages
    for (i32 i = 0; i < NUM_THREADS; i++) {
        args[i].ps = ps;
        args[i].start_page = i * 25;
        args[i].num_pages = 25;
        args[i].pattern = 0x10 + i;
        args[i].result = -1;
        pthread_create(&threads[i], NULL, concurrent_writer, &args[i]);
    }

    // Wait for all threads
    for (i32 i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
        TEST_ASSERT_EQUAL(0, args[i].result);
    }

    // Verify all pages
    u8 read_buf[PAGE_SIZE];
    for (i32 i = 0; i < NUM_THREADS; i++) {
        for (u32 j = 0; j < args[i].num_pages; j++) {
            u32 page_num = args[i].start_page + j;
            i32 ret = pstore_read(ps, page_num, read_buf);
            TEST_ASSERT_EQUAL(0, ret);
            verify_page_pattern(read_buf, page_num, args[i].pattern);
        }
    }

    pstore_close(ps);
}

void *concurrent_grower(void *arg) {
    struct concurrent_test_args *args = (struct concurrent_test_args *) arg;
    i32 ret = pstore_grow(args->ps, args->num_pages);
    args->result = ret;
    return NULL;
}

void test_concurrent_grows(void) {
    struct PageStore *ps = pstore_create(NULL, 10);
    TEST_ASSERT_NOT_NULL(ps);

    const i32 NUM_THREADS = 4;
    pthread_t threads[NUM_THREADS];
    struct concurrent_test_args args[NUM_THREADS];

    // Multiple threads try to grow simultaneously
    for (i32 i = 0; i < NUM_THREADS; i++) {
        args[i].ps = ps;
        args[i].num_pages = 10;
        args[i].result = -1;
        pthread_create(&threads[i], NULL, concurrent_grower, &args[i]);
    }

    // Wait for all threads
    for (i32 i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
        TEST_ASSERT_EQUAL(0, args[i].result);
    }

    // Final size should be 10 + 4*10 = 50 pages
    TEST_ASSERT_EQUAL(50 * PAGE_SIZE, ps->store_size);

    pstore_close(ps);
}

// =============================================================================
// MAIN
// =============================================================================

int suite_setUp(void) { return 0; }

int suite_tearDown(void) {
    // Clean up any remaining test files
    unlink(TEST_DB_FILE);
    unlink(TEST_DB_REOPEN);
    return 0;
}

void setUp(void) {}

void tearDown(void) {}

int main(void) {
    if (suite_setUp() != 0) {
        return -1;
    }

    UNITY_BEGIN();

    // In-memory tests
    RUN_TEST(test_inmem_create_and_close);
    RUN_TEST(test_inmem_write_and_read_single_page);
    RUN_TEST(test_inmem_write_and_read_multiple_pages);
    RUN_TEST(test_inmem_grow);
    RUN_TEST(test_inmem_grow_multiple_times);
    RUN_TEST(test_inmem_sync_is_noop);

    // File-backed tests
    RUN_TEST(test_file_create_and_close);
    RUN_TEST(test_file_write_and_read_single_page);
    RUN_TEST(test_file_write_and_read_multiple_pages);
    RUN_TEST(test_file_persistence_after_close);
    RUN_TEST(test_file_open_nonexistent);
    RUN_TEST(test_file_grow);
    RUN_TEST(test_file_grow_and_persist);
    RUN_TEST(test_file_sync);

    // Edge cases
    RUN_TEST(test_read_out_of_bounds);
    RUN_TEST(test_write_out_of_bounds);
    RUN_TEST(test_create_with_zero_pages);
    RUN_TEST(test_grow_by_zero);
    RUN_TEST(test_open_returns_null_for_inmem);

    // Concurrent access
    RUN_TEST(test_concurrent_writes_different_pages);
    RUN_TEST(test_concurrent_grows);

    return suite_tearDown() + UNITY_END();
}
