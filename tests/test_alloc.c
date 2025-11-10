#include "alloc.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "bufpool.h"
#include "pagestore.h"
#include "unity.h"
#include "utils.h"

#define TEST_DB_FILE "test_alloc.db"

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

static int compare_u32(const void *a, const void *b) {
    u32 ua = *(const u32 *) a;
    u32 ub = *(const u32 *) b;
    if (ua < ub)
        return -1;
    if (ua > ub)
        return 1;
    return 0;
}

static bool check_bit_set(struct PageAllocator *pa, u32 page_num) {
    u32 rpage = page_num - HEAD_OFFSET;
    u32 group = rpage / GROUP_SIZE;
    u32 pidx = rpage % GROUP_SIZE;
    u32 word_idx = pidx / 64;
    u32 bit = pidx % 64;

    u32 gstart = HEAD_OFFSET + group * GROUP_SIZE;
    u32 bitmap_page = (word_idx < BITMAPS_PER_PAGE) ? gstart : (gstart + 1);
    u32 local_word = (word_idx < BITMAPS_PER_PAGE) ? word_idx : (word_idx - BITMAPS_PER_PAGE);

    struct FrameHandle *h = bpool_fetch_page(pa->pool, bitmap_page);
    struct BitmapPage *bp = (struct BitmapPage *) handle_data(h);

    u64 mask = LOAD(&bp->bitmap[local_word], ACQUIRE);
    bool is_set = (mask & (1ULL << bit)) != 0;

    bpool_release_handle(pa->pool, h);
    return is_set;
}

// =============================================================================
// BASIC SINGLE-THREADED TESTS
// =============================================================================

void test_init_create(void) {

    // Create new database
    struct PageStore *ps = pstore_create(NULL, INITIAL_PAGES);
    TEST_ASSERT_NOT_NULL(ps);

    struct BufPool *bp = bpool_init(ps);
    TEST_ASSERT_NOT_NULL(bp);

    START_TIMING();

    struct PageAllocator *pa = allocator_init(bp, true);
    TEST_ASSERT_NOT_NULL(pa);

    // Verify superblock fields
    TEST_ASSERT_EQUAL_UINT32(MAGIC, pa->sb_cache->magic);
    TEST_ASSERT_EQUAL_UINT32(VERSION, pa->sb_cache->version);
    TEST_ASSERT_EQUAL_UINT32(PAGE_SIZE, pa->sb_cache->page_size);
    TEST_ASSERT_EQUAL_UINT32(INITIAL_PAGES, LOAD(&pa->sb_cache->total_pages, RELAXED));
    TEST_ASSERT_EQUAL_UINT32(1, LOAD(&pa->sb_cache->total_groups, RELAXED));

    // Verify group 0 descriptor
    struct GroupDesc *desc = &pa->gdt_cache[0].descriptors[0];
    TEST_ASSERT_EQUAL_UINT32(HEAD_OFFSET, desc->start);
    TEST_ASSERT_EQUAL_UINT32(GROUP_SIZE - GROUP_BITMAPS, LOAD(&desc->free_pages, RELAXED));

    // Cleanup
    allocator_destroy(pa);
    END_TIMING("test_init_create");

    bpool_destroy(bp);
    pstore_close(ps);
}

void test_alloc_single_page(void) {
    START_TIMING();

    struct PageStore *ps = pstore_create(NULL, INITIAL_PAGES);
    struct BufPool *bp = bpool_init(ps);
    struct PageAllocator *pa = allocator_init(bp, true);

    u32 initial_free = LOAD(&pa->gdt_cache[0].descriptors[0].free_pages, RELAXED);

    // Allocate a page
    u32 page1 = alloc_page(pa, 0);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, page1);
    TEST_ASSERT_TRUE(page1 >= HEAD_OFFSET + GROUP_BITMAPS); // Not metadata or bitmap

    // Verify free count decremented
    u32 free_after = LOAD(&pa->gdt_cache[0].descriptors[0].free_pages, RELAXED);
    TEST_ASSERT_EQUAL_UINT32(initial_free - 1, free_after);

    // Verify bit is set in bitmap
    TEST_ASSERT_TRUE(check_bit_set(pa, page1));

    // Allocate another page
    u32 page2 = alloc_page(pa, 0);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, page2);
    TEST_ASSERT_NOT_EQUAL(page1, page2); // Different pages

    free_after = LOAD(&pa->gdt_cache[0].descriptors[0].free_pages, RELAXED);
    TEST_ASSERT_EQUAL_UINT32(initial_free - 2, free_after);

    allocator_destroy(pa);
    bpool_destroy(bp);
    pstore_close(ps);

    END_TIMING("test_alloc_single_page");
}

void test_alloc_free_page(void) {
    START_TIMING();

    struct PageStore *ps = pstore_create(NULL, INITIAL_PAGES);
    struct BufPool *bp = bpool_init(ps);
    struct PageAllocator *pa = allocator_init(bp, true);

    u32 initial_free = LOAD(&pa->gdt_cache[0].descriptors[0].free_pages, RELAXED);

    // Allocate a page
    u32 page = alloc_page(pa, 0);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, page);
    TEST_ASSERT_TRUE(check_bit_set(pa, page));

    u32 free_after_alloc = LOAD(&pa->gdt_cache[0].descriptors[0].free_pages, RELAXED);
    TEST_ASSERT_EQUAL_UINT32(initial_free - 1, free_after_alloc);

    // Free the page
    free_page(pa, page);

    u32 free_after_free = LOAD(&pa->gdt_cache[0].descriptors[0].free_pages, RELAXED);
    TEST_ASSERT_EQUAL_UINT32(initial_free, free_after_free);

    // Verify bit is cleared
    TEST_ASSERT_FALSE(check_bit_set(pa, page));

    allocator_destroy(pa);
    bpool_destroy(bp);
    pstore_close(ps);

    END_TIMING("test_alloc_free_page");
}

void test_alloc_multiple_pages(void) {
    START_TIMING();

    struct PageStore *ps = pstore_create(NULL, INITIAL_PAGES);
    struct BufPool *bp = bpool_init(ps);
    struct PageAllocator *pa = allocator_init(bp, true);

    const u32 NUM_PAGES = 100;
    u32 pages[NUM_PAGES];

    // Allocate 100 pages
    for (u32 i = 0; i < NUM_PAGES; i++) {
        pages[i] = alloc_page(pa, 0);
        TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, pages[i]);
    }

    // Verify all pages are unique
    qsort(pages, NUM_PAGES, sizeof(u32), compare_u32);
    for (u32 i = 1; i < NUM_PAGES; i++) {
        TEST_ASSERT_NOT_EQUAL(pages[i], pages[i - 1]);
    }

    // Verify free count
    u32 expected_free = (GROUP_SIZE - GROUP_BITMAPS) - NUM_PAGES;
    u32 actual_free = LOAD(&pa->gdt_cache[0].descriptors[0].free_pages, RELAXED);
    TEST_ASSERT_EQUAL_UINT32(expected_free, actual_free);

    // Free all pages
    for (u32 i = 0; i < NUM_PAGES; i++) {
        free_page(pa, pages[i]);
    }

    // Verify free count restored
    actual_free = LOAD(&pa->gdt_cache[0].descriptors[0].free_pages, RELAXED);
    TEST_ASSERT_EQUAL_UINT32(GROUP_SIZE - GROUP_BITMAPS, actual_free);

    allocator_destroy(pa);
    bpool_destroy(bp);
    pstore_close(ps);

    END_TIMING("test_alloc_multiple_pages");
}

void test_alloc_with_hint(void) {
    START_TIMING();

    struct PageStore *ps = pstore_create(NULL, INITIAL_PAGES);
    struct BufPool *bp = bpool_init(ps);
    struct PageAllocator *pa = allocator_init(bp, true);

    // Allocate with hint in group 0
    u32 hint = HEAD_OFFSET + 1000;
    u32 page = alloc_page(pa, hint);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, page);

    // Verify page is in group 0
    u32 group = (page - HEAD_OFFSET) / GROUP_SIZE;
    TEST_ASSERT_EQUAL_UINT32(0, group);

    allocator_destroy(pa);
    bpool_destroy(bp);
    pstore_close(ps);

    END_TIMING("test_alloc_with_hint");
}

void test_free_invalid_pages(void) {
    START_TIMING();

    struct PageStore *ps = pstore_create(NULL, INITIAL_PAGES);
    struct BufPool *bp = bpool_init(ps);
    struct PageAllocator *pa = allocator_init(bp, true);

    // Free page beyond total_pages (should be no-op)
    u32 total_pages = LOAD(&pa->sb_cache->total_pages, RELAXED);
    free_page(pa, total_pages + 1000);

    // Should not crash
    TEST_ASSERT_TRUE(true);

    allocator_destroy(pa);
    bpool_destroy(bp);
    pstore_close(ps);

    END_TIMING("test_free_invalid_pages");
}

// =============================================================================
// DATABASE GROWTH TESTS
// =============================================================================

void test_grow_database(void) {
    START_TIMING();

    struct PageStore *ps = pstore_create(NULL, INITIAL_PAGES);
    struct BufPool *bp = bpool_init(ps);
    struct PageAllocator *pa = allocator_init(bp, true);

    // Allocate all pages in group 0
    u32 initial_free = GROUP_SIZE - GROUP_BITMAPS;
    u32 *pages = malloc(initial_free * sizeof(u32));
    TEST_ASSERT_NOT_NULL(pages);

    for (u32 i = 0; i < initial_free; i++) {
        pages[i] = alloc_page(pa, 0);
        TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, pages[i]);
    }

    // Verify group 0 is full
    u32 free_group0 = LOAD(&pa->gdt_cache[0].descriptors[0].free_pages, RELAXED);
    TEST_ASSERT_EQUAL_UINT32(0, free_group0);

    // Verify we have 1 group
    u32 groups_before = LOAD(&pa->sb_cache->total_groups, RELAXED);
    TEST_ASSERT_EQUAL_UINT32(1, groups_before);

    // Next allocation should trigger growth
    u32 page_in_group1 = alloc_page(pa, 0);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, page_in_group1);

    // Verify growth occurred
    u32 groups_after = LOAD(&pa->sb_cache->total_groups, RELAXED);
    TEST_ASSERT_EQUAL_UINT32(2, groups_after);

    // Verify page is in group 1
    u32 group = (page_in_group1 - HEAD_OFFSET) / GROUP_SIZE;
    TEST_ASSERT_EQUAL_UINT32(1, group);

    // Verify group 1 descriptor
    struct GroupDesc *desc1 = &pa->gdt_cache[0].descriptors[1];
    TEST_ASSERT_EQUAL_UINT32(HEAD_OFFSET + GROUP_SIZE, desc1->start);
    TEST_ASSERT_EQUAL_UINT32(GROUP_SIZE - GROUP_BITMAPS - 1, LOAD(&desc1->free_pages, RELAXED));

    // Verify total_pages updated
    u32 total_pages = LOAD(&pa->sb_cache->total_pages, RELAXED);
    TEST_ASSERT_EQUAL_UINT32(HEAD_OFFSET + 2 * GROUP_SIZE, total_pages);

    free(pages);
    allocator_destroy(pa);
    bpool_destroy(bp);
    pstore_close(ps);

    END_TIMING("test_grow_database");
}

// =============================================================================
// PERSISTENCE TESTS
// =============================================================================

void test_persist_and_reopen(void) {
    START_TIMING();

    u32 allocated_pages[10];
    const u32 NUM_ALLOC = 10;

    // Create and allocate
    {
        struct PageStore *ps = pstore_create(TEST_DB_FILE, INITIAL_PAGES);
        struct BufPool *bp = bpool_init(ps);
        struct PageAllocator *pa = allocator_init(bp, true);

        for (u32 i = 0; i < NUM_ALLOC; i++) {
            allocated_pages[i] = alloc_page(pa, 0);
            TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, allocated_pages[i]);
        }

        allocator_destroy(pa);
        bpool_destroy(bp);
        pstore_close(ps);
    }

    // Reopen and verify
    {
        u32 num_pages;
        struct PageStore *ps = pstore_open(TEST_DB_FILE, &num_pages);
        TEST_ASSERT_NOT_NULL(ps);
        TEST_ASSERT_EQUAL_UINT32(INITIAL_PAGES, num_pages);

        struct BufPool *bp = bpool_init(ps);
        struct PageAllocator *pa = allocator_init(bp, false);
        TEST_ASSERT_NOT_NULL(pa);

        // Verify metadata
        TEST_ASSERT_EQUAL_UINT32(MAGIC, pa->sb_cache->magic);
        TEST_ASSERT_EQUAL_UINT32(VERSION, pa->sb_cache->version);
        TEST_ASSERT_EQUAL_UINT32(1, LOAD(&pa->sb_cache->total_groups, RELAXED));

        // Verify free count
        u32 expected_free = (GROUP_SIZE - GROUP_BITMAPS) - NUM_ALLOC;
        u32 actual_free = LOAD(&pa->gdt_cache[0].descriptors[0].free_pages, RELAXED);
        TEST_ASSERT_EQUAL_UINT32(expected_free, actual_free);

        // Verify bits are still set
        for (u32 i = 0; i < NUM_ALLOC; i++) {
            TEST_ASSERT_TRUE(check_bit_set(pa, allocated_pages[i]));
        }

        allocator_destroy(pa);
        bpool_destroy(bp);
        pstore_close(ps);
    }

    unlink(TEST_DB_FILE);

    END_TIMING("test_persist_and_reopen");
}

void test_checksum_validation(void) {
    START_TIMING();

    // Create database
    {
        struct PageStore *ps = pstore_create(TEST_DB_FILE, INITIAL_PAGES);
        struct BufPool *bp = bpool_init(ps);
        struct PageAllocator *pa = allocator_init(bp, true);

        alloc_page(pa, 0);
        alloc_page(pa, 0);

        allocator_destroy(pa);
        bpool_destroy(bp);
        pstore_close(ps);
    }

    // Reopen - should pass checksum validation
    {
        u32 num_pages;
        struct PageStore *ps = pstore_open(TEST_DB_FILE, &num_pages);
        struct BufPool *bp = bpool_init(ps);
        struct PageAllocator *pa = allocator_init(bp, false);

        TEST_ASSERT_NOT_NULL(pa); // Should succeed

        allocator_destroy(pa);
        bpool_destroy(bp);
        pstore_close(ps);
    }

    unlink(TEST_DB_FILE);

    END_TIMING("test_checksum_validation");
}

void test_corrupt_superblock(void) {
    START_TIMING();

    // Create database
    {
        struct PageStore *ps = pstore_create(TEST_DB_FILE, INITIAL_PAGES);
        struct BufPool *bp = bpool_init(ps);
        struct PageAllocator *pa = allocator_init(bp, true);

        allocator_destroy(pa);
        bpool_destroy(bp);
        pstore_close(ps);
    }

    // Corrupt superblock checksum
    {
        i32 fd = open_relative(TEST_DB_FILE, O_RDWR, 0);
        TEST_ASSERT_TRUE(fd >= 0);

        u8 page[PAGE_SIZE];
        ssize_t n = pread(fd, page, PAGE_SIZE, 0);
        TEST_ASSERT_EQUAL(PAGE_SIZE, n);

        struct SuperBlock *sb = (struct SuperBlock *) page;
        sb->sb_checksum = 0xDEADBEEF; // Corrupt checksum

        n = pwrite(fd, page, PAGE_SIZE, 0);
        TEST_ASSERT_EQUAL(PAGE_SIZE, n);

        close(fd);
    }

    // Try to open - should fail
    {
        u32 num_pages;
        struct PageStore *ps = pstore_open(TEST_DB_FILE, &num_pages);
        struct BufPool *bp = bpool_init(ps);
        struct PageAllocator *pa = allocator_init(bp, false);

        TEST_ASSERT_NULL(pa); // Should fail due to checksum mismatch

        bpool_destroy(bp);
        pstore_close(ps);
    }

    unlink(TEST_DB_FILE);

    END_TIMING("test_corrupt_superblock");
}

// =============================================================================
// CONCURRENT ALLOCATION TESTS
// =============================================================================

struct thread_alloc_args {
    struct PageAllocator *pa;
    u32 num_allocs;
    u32 *pages;
    volatile bool *start_flag;
    u32 thread_id;
};

static void *thread_allocator(void *arg) {
    struct thread_alloc_args *args = (struct thread_alloc_args *) arg;

    // Wait for start signal
    while (!*args->start_flag) {
        usleep(100);
    }

    // Allocate pages
    for (u32 i = 0; i < args->num_allocs; i++) {
        args->pages[i] = alloc_page(args->pa, 0);
    }

    return NULL;
}

void test_concurrent_allocation(void) {
    START_TIMING();

    struct PageStore *ps = pstore_create(NULL, INITIAL_PAGES);
    struct BufPool *bp = bpool_init(ps);
    struct PageAllocator *pa = allocator_init(bp, true);

    const u32 NUM_THREADS = 8;
    const u32 ALLOCS_PER_THREAD = 100;

    pthread_t threads[NUM_THREADS];
    struct thread_alloc_args args[NUM_THREADS];
    volatile bool start_flag = false;

    // Create threads
    for (u32 i = 0; i < NUM_THREADS; i++) {
        args[i].pa = pa;
        args[i].num_allocs = ALLOCS_PER_THREAD;
        args[i].pages = malloc(ALLOCS_PER_THREAD * sizeof(u32));
        TEST_ASSERT_NOT_NULL(args[i].pages);
        args[i].start_flag = &start_flag;
        args[i].thread_id = i;

        pthread_create(&threads[i], NULL, thread_allocator, &args[i]);
    }

    // Start all threads simultaneously
    start_flag = true;

    // Wait for completion
    for (u32 i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Collect all pages
    u32 total_pages = NUM_THREADS * ALLOCS_PER_THREAD;
    u32 *all_pages = malloc(total_pages * sizeof(u32));
    TEST_ASSERT_NOT_NULL(all_pages);

    for (u32 i = 0; i < NUM_THREADS; i++) {
        memcpy(all_pages + (i * ALLOCS_PER_THREAD), args[i].pages, ALLOCS_PER_THREAD * sizeof(u32));
        free(args[i].pages);
    }

    // Verify no duplicates
    qsort(all_pages, total_pages, sizeof(u32), compare_u32);

    for (u32 i = 1; i < total_pages; i++) {
        if (all_pages[i] == all_pages[i - 1]) {
            TEST_FAIL_MESSAGE("Duplicate page allocation detected!");
        }
    }

    // Verify all allocations succeeded
    for (u32 i = 0; i < total_pages; i++) {
        TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, all_pages[i]);
    }

    // Verify free count
    u32 expected_free = (GROUP_SIZE - GROUP_BITMAPS) - total_pages;
    u32 actual_free = LOAD(&pa->gdt_cache[0].descriptors[0].free_pages, RELAXED);
    TEST_ASSERT_EQUAL_UINT32(expected_free, actual_free);

    free(all_pages);
    allocator_destroy(pa);
    bpool_destroy(bp);
    pstore_close(ps);

    END_TIMING("test_concurrent_allocation");
}

void test_concurrent_alloc_and_free(void) {
    START_TIMING();

    struct PageStore *ps = pstore_create(NULL, INITIAL_PAGES);
    struct BufPool *bp = bpool_init(ps);
    struct PageAllocator *pa = allocator_init(bp, true);

    const u32 NUM_THREADS = 4;
    const u32 OPS_PER_THREAD = 200;

    pthread_t threads[NUM_THREADS];
    struct thread_alloc_args args[NUM_THREADS];
    volatile bool start_flag = false;

    // Pre-allocate some pages to free
    u32 *pages_to_free = malloc(NUM_THREADS * OPS_PER_THREAD * sizeof(u32));
    for (u32 i = 0; i < NUM_THREADS * OPS_PER_THREAD / 2; i++) {
        pages_to_free[i] = alloc_page(pa, 0);
    }

    // Create threads - each will allocate and free
    for (u32 i = 0; i < NUM_THREADS; i++) {
        args[i].pa = pa;
        args[i].num_allocs = OPS_PER_THREAD;
        args[i].pages = malloc(OPS_PER_THREAD * sizeof(u32));
        args[i].start_flag = &start_flag;
        args[i].thread_id = i;

        pthread_create(&threads[i], NULL, thread_allocator, &args[i]);
    }

    start_flag = true;

    // Meanwhile, free some pages from main thread
    for (u32 i = 0; i < NUM_THREADS * OPS_PER_THREAD / 4; i++) {
        free_page(pa, pages_to_free[i]);
        usleep(10); // Small delay to interleave with allocations
    }

    // Wait for threads
    for (u32 i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Verify all allocations succeeded
    for (u32 i = 0; i < NUM_THREADS; i++) {
        for (u32 j = 0; j < OPS_PER_THREAD; j++) {
            TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, args[i].pages[j]);
        }
        free(args[i].pages);
    }

    free(pages_to_free);
    allocator_destroy(pa);
    bpool_destroy(bp);
    pstore_close(ps);

    END_TIMING("test_concurrent_alloc_and_free");
}

void test_concurrent_growth(void) {
    START_TIMING();

    struct PageStore *ps = pstore_create(NULL, INITIAL_PAGES);
    struct BufPool *bp = bpool_init(ps);
    struct PageAllocator *pa = allocator_init(bp, true);

    // Pre-allocate most of group 0, leaving ~100 pages
    u32 to_alloc = (GROUP_SIZE - GROUP_BITMAPS) - 100;
    for (u32 i = 0; i < to_alloc; i++) {
        u32 page = alloc_page(pa, 0);
        TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, page);
    }

    // Now have 8 threads each allocate 50 pages
    // This should trigger growth mid-way
    const u32 NUM_THREADS = 8;
    const u32 ALLOCS_PER_THREAD = 50;

    pthread_t threads[NUM_THREADS];
    struct thread_alloc_args args[NUM_THREADS];
    volatile bool start_flag = false;

    for (u32 i = 0; i < NUM_THREADS; i++) {
        args[i].pa = pa;
        args[i].num_allocs = ALLOCS_PER_THREAD;
        args[i].pages = malloc(ALLOCS_PER_THREAD * sizeof(u32));
        args[i].start_flag = &start_flag;
        args[i].thread_id = i;

        pthread_create(&threads[i], NULL, thread_allocator, &args[i]);
    }

    start_flag = true;

    for (u32 i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Verify growth occurred
    u32 total_groups = LOAD(&pa->sb_cache->total_groups, RELAXED);
    TEST_ASSERT_TRUE(total_groups >= 2);

    // Verify all allocations succeeded
    for (u32 i = 0; i < NUM_THREADS; i++) {
        for (u32 j = 0; j < ALLOCS_PER_THREAD; j++) {
            TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, args[i].pages[j]);
        }
        free(args[i].pages);
    }

    allocator_destroy(pa);
    bpool_destroy(bp);
    pstore_close(ps);

    END_TIMING("test_concurrent_growth");
}

// =============================================================================
// STRESS TESTS
// =============================================================================

void test_stress_many_allocations(void) {
    START_TIMING();

    struct PageStore *ps = pstore_create(NULL, INITIAL_PAGES);
    struct BufPool *bp = bpool_init(ps);
    struct PageAllocator *pa = allocator_init(bp, true);

    const u32 NUM_PAGES = 5000;
    u32 *pages = malloc(NUM_PAGES * sizeof(u32));
    TEST_ASSERT_NOT_NULL(pages);

    // Allocate many pages
    for (u32 i = 0; i < NUM_PAGES; i++) {
        pages[i] = alloc_page(pa, 0);
        TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, pages[i]);
    }

    // Verify no duplicates
    qsort(pages, NUM_PAGES, sizeof(u32), compare_u32);
    for (u32 i = 1; i < NUM_PAGES; i++) {
        TEST_ASSERT_NOT_EQUAL(pages[i], pages[i - 1]);
    }

    // Free all pages
    for (u32 i = 0; i < NUM_PAGES; i++) {
        free_page(pa, pages[i]);
    }

    // Verify free count restored (may span multiple groups)
    u32 total_groups = LOAD(&pa->sb_cache->total_groups, RELAXED);
    u32 total_free = 0;
    for (u32 g = 0; g < total_groups; g++) {
        total_free += LOAD(&pa->gdt_cache[g / GDT_DESCRIPTORS].descriptors[g % GDT_DESCRIPTORS].free_pages, RELAXED);
    }
    u32 expected_total = total_groups * (GROUP_SIZE - GROUP_BITMAPS);
    TEST_ASSERT_EQUAL_UINT32(expected_total, total_free);

    free(pages);
    allocator_destroy(pa);
    bpool_destroy(bp);
    pstore_close(ps);

    END_TIMING("test_stress_many_allocations");
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

    // Basic tests
    RUN_TEST(test_init_create);
    RUN_TEST(test_alloc_single_page);
    RUN_TEST(test_alloc_free_page);
    RUN_TEST(test_alloc_multiple_pages);
    RUN_TEST(test_alloc_with_hint);
    RUN_TEST(test_free_invalid_pages);

    // Growth tests
    RUN_TEST(test_grow_database);

    // Persistence tests
    RUN_TEST(test_persist_and_reopen);
    RUN_TEST(test_checksum_validation);
    RUN_TEST(test_corrupt_superblock);

    // Concurrent tests
    RUN_TEST(test_concurrent_allocation);
    RUN_TEST(test_concurrent_alloc_and_free);
    RUN_TEST(test_concurrent_growth);

    // Stress tests
    RUN_TEST(test_stress_many_allocations);

    return suite_tearDown() + UNITY_END();
}
