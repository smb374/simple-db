#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "alloc.h"
#include "bufpool.h"
#include "catalog.h"
#include "pagestore.h"
#include "unity.h"
#include "utils.h"

#define TEST_DB_PATH "test_catalog.db"
#define TEST_DB_PAGES 1000

static struct PageStore *store;
static struct BufPool *pool;
static struct PageAllocator *alloc;
static struct Catalog *catalog;

int suite_setUp(void) { return 0; }

int suite_tearDown(void) { return 0; }

void setUp(void) {
    // Remove existing test database
    unlink(TEST_DB_PATH);

    // Create fresh database components
    store = pstore_create(TEST_DB_PATH, TEST_DB_PAGES);
    TEST_ASSERT_NOT_NULL(store);

    pool = bpool_init(store);
    TEST_ASSERT_NOT_NULL(pool);

    alloc = allocator_init(pool, true);
    TEST_ASSERT_NOT_NULL(alloc);

    catalog = catalog_init(alloc);
    TEST_ASSERT_NOT_NULL(catalog);
}

void tearDown(void) {
    if (catalog) {
        catalog_close(catalog);
        catalog = NULL;
    }
    if (alloc) {
        allocator_destroy(alloc);
        alloc = NULL;
    }
    if (pool) {
        bpool_destroy(pool);
        pool = NULL;
    }
    if (store) {
        pstore_close(store);
        store = NULL;
    }
    unlink(TEST_DB_PATH);
}

// Test catalog initialization
void test_catalog_init(void) {
    TEST_ASSERT_NOT_NULL(catalog);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, catalog->page.fsm_head);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, catalog->page.kfsm_head);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, catalog->page.schema_root);

    // FSM heads should be different pages
    TEST_ASSERT_NOT_EQUAL(catalog->page.fsm_head, catalog->page.kfsm_head);
}

// Test catalog persistence (open existing)
void test_catalog_persistence(void) {
    u32 fsm_head = catalog->page.fsm_head;
    u32 kfsm_head = catalog->page.kfsm_head;
    u32 schema_root = catalog->page.schema_root;

    // Close and reopen
    catalog_close(catalog);
    catalog = NULL;

    catalog = catalog_open(alloc);
    TEST_ASSERT_NOT_NULL(catalog);

    // Verify same values restored
    TEST_ASSERT_EQUAL_UINT32(fsm_head, catalog->page.fsm_head);
    TEST_ASSERT_EQUAL_UINT32(kfsm_head, catalog->page.kfsm_head);
    TEST_ASSERT_EQUAL_UINT32(schema_root, catalog->page.schema_root);
}

// Test writing and reading small data
void test_catalog_write_read_small_data(void) {
    const char *test_data = "Hello, Catalog!";
    u16 data_len = strlen(test_data) + 1;

    // Write data
    struct VPtr ptr = catalog_write_data(catalog, (const u8 *) test_data, data_len);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptr.page_num);

    // Read back
    char read_buffer[256] = {0};
    i32 result = catalog_read(catalog, &ptr, (u8 *) read_buffer, false);
    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_STRING(test_data, read_buffer);
}

// Test writing and reading multiple small data items
void test_catalog_write_read_multiple_small(void) {
    const int NUM_ITEMS = 100;
    struct VPtr ptrs[NUM_ITEMS];
    char write_data[NUM_ITEMS][64];

    // Write multiple items
    for (int i = 0; i < NUM_ITEMS; i++) {
        snprintf(write_data[i], sizeof(write_data[i]), "Data item %d", i);
        ptrs[i] = catalog_write_data(catalog, (const u8 *) write_data[i], strlen(write_data[i]) + 1);
        TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptrs[i].page_num);
    }

    // Read back and verify
    for (int i = 0; i < NUM_ITEMS; i++) {
        char read_buffer[64] = {0};
        i32 result = catalog_read(catalog, &ptrs[i], (u8 *) read_buffer, false);
        TEST_ASSERT_EQUAL_INT32(0, result);
        TEST_ASSERT_EQUAL_STRING(write_data[i], read_buffer);
    }
}

// Test writing and reading key data
void test_catalog_write_read_key(void) {
    const char *test_key = "test_key_overflow_data";
    u16 key_len = strlen(test_key);

    // Write key
    struct VPtr ptr = catalog_write_key(catalog, (const u8 *) test_key, key_len);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptr.page_num);
    TEST_ASSERT_EQUAL_UINT8(1, ptr.slot_info.is_key);

    // Read back
    char read_buffer[256] = {0};
    i32 result = catalog_read(catalog, &ptr, (u8 *) read_buffer, false);
    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_MEMORY(test_key, read_buffer, key_len);
}

// Test freeing data
void test_catalog_free_data(void) {
    const char *test_data = "Data to be freed";
    u16 data_len = strlen(test_data) + 1;

    // Write data
    struct VPtr ptr = catalog_write_data(catalog, (const u8 *) test_data, data_len);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptr.page_num);

    // Free data
    i32 result = catalog_free(catalog, &ptr, false);
    TEST_ASSERT_EQUAL_INT32(0, result);

    // FSM should be updated (freed space should be available for reuse)
    // Write new data that should reuse the freed slot
    const char *new_data = "New data";
    struct VPtr new_ptr = catalog_write_data(catalog, (const u8 *) new_data, strlen(new_data) + 1);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, new_ptr.page_num);
}

// Test writing large data (triggers chain pages)
void test_catalog_write_read_huge_data(void) {
    // Create data larger than NORMAL_DATA_LIMIT
    const u32 large_size = 8192; // Should trigger chain pages
    u8 *large_data = malloc(large_size);
    TEST_ASSERT_NOT_NULL(large_data);

    // Fill with pattern
    for (u32 i = 0; i < large_size; i++) {
        large_data[i] = (u8) (i % 256);
    }

    // Write large data
    struct VPtr ptr = catalog_write_data(catalog, large_data, large_size);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptr.page_num);
    TEST_ASSERT_EQUAL_UINT32(large_size, ptr.size);

    // Read back
    u8 *read_buffer = malloc(large_size);
    TEST_ASSERT_NOT_NULL(read_buffer);

    i32 result = catalog_read(catalog, &ptr, read_buffer, true);
    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_MEMORY(large_data, read_buffer, large_size);

    free(large_data);
    free(read_buffer);
}

// Test freeing huge data (chain pages)
void test_catalog_free_huge_data(void) {
    const u32 large_size = 16384;
    u8 *large_data = malloc(large_size);
    TEST_ASSERT_NOT_NULL(large_data);
    memset(large_data, 0xAB, large_size);

    // Write large data
    struct VPtr ptr = catalog_write_data(catalog, large_data, large_size);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptr.page_num);

    // Free large data
    i32 result = catalog_free(catalog, &ptr, true);
    TEST_ASSERT_EQUAL_INT32(0, result);

    free(large_data);
}

// Test FSM behavior - filling up a page
void test_catalog_fsm_page_fill(void) {
    // Allocate many small items to fill up slot pages
    const int NUM_ITEMS = 200;
    struct VPtr ptrs[NUM_ITEMS];

    for (int i = 0; i < NUM_ITEMS; i++) {
        char data[32];
        snprintf(data, sizeof(data), "Item %d", i);
        ptrs[i] = catalog_write_data(catalog, (const u8 *) data, strlen(data) + 1);
        TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptrs[i].page_num);
    }

    // Should have allocated multiple slot pages
    // Verify we can still read all items
    for (int i = 0; i < NUM_ITEMS; i++) {
        char expected[32];
        char read_buffer[32] = {0};
        snprintf(expected, sizeof(expected), "Item %d", i);

        i32 result = catalog_read(catalog, &ptrs[i], (u8 *) read_buffer, false);
        TEST_ASSERT_EQUAL_INT32(0, result);
        TEST_ASSERT_EQUAL_STRING(expected, read_buffer);
    }
}

// Test FSM behavior - free and reuse pattern
void test_catalog_fsm_free_reuse(void) {
    const int NUM_ITEMS = 50;
    struct VPtr ptrs[NUM_ITEMS];

    // Write items
    for (int i = 0; i < NUM_ITEMS; i++) {
        char data[64];
        snprintf(data, sizeof(data), "Original item %d", i);
        ptrs[i] = catalog_write_data(catalog, (const u8 *) data, strlen(data) + 1);
        TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptrs[i].page_num);
    }

    // Free every other item
    for (int i = 0; i < NUM_ITEMS; i += 2) {
        i32 result = catalog_free(catalog, &ptrs[i], false);
        TEST_ASSERT_EQUAL_INT32(0, result);
    }

    // Write new items (should reuse freed space)
    struct VPtr new_ptrs[NUM_ITEMS / 2];
    for (int i = 0; i < NUM_ITEMS / 2; i++) {
        char data[64];
        snprintf(data, sizeof(data), "New item %d", i);
        new_ptrs[i] = catalog_write_data(catalog, (const u8 *) data, strlen(data) + 1);
        TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, new_ptrs[i].page_num);
    }

    // Verify remaining original items
    for (int i = 1; i < NUM_ITEMS; i += 2) {
        char expected[64];
        char read_buffer[64] = {0};
        snprintf(expected, sizeof(expected), "Original item %d", i);

        i32 result = catalog_read(catalog, &ptrs[i], (u8 *) read_buffer, false);
        TEST_ASSERT_EQUAL_INT32(0, result);
        TEST_ASSERT_EQUAL_STRING(expected, read_buffer);
    }

    // Verify new items
    for (int i = 0; i < NUM_ITEMS / 2; i++) {
        char expected[64];
        char read_buffer[64] = {0};
        snprintf(expected, sizeof(expected), "New item %d", i);

        i32 result = catalog_read(catalog, &new_ptrs[i], (u8 *) read_buffer, false);
        TEST_ASSERT_EQUAL_INT32(0, result);
        TEST_ASSERT_EQUAL_STRING(expected, read_buffer);
    }
}

// Test mixed data and key storage
void test_catalog_mixed_data_key(void) {
    const int NUM_ITEMS = 30;
    struct VPtr data_ptrs[NUM_ITEMS];
    struct VPtr key_ptrs[NUM_ITEMS];

    // Write alternating data and keys
    for (int i = 0; i < NUM_ITEMS; i++) {
        char data[64];
        char key[64];
        snprintf(data, sizeof(data), "Data %d", i);
        snprintf(key, sizeof(key), "Key %d", i);

        data_ptrs[i] = catalog_write_data(catalog, (const u8 *) data, strlen(data) + 1);
        key_ptrs[i] = catalog_write_key(catalog, (const u8 *) key, strlen(key));

        TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, data_ptrs[i].page_num);
        TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, key_ptrs[i].page_num);
        TEST_ASSERT_EQUAL_UINT8(0, data_ptrs[i].slot_info.is_key);
        TEST_ASSERT_EQUAL_UINT8(1, key_ptrs[i].slot_info.is_key);
    }

    // Verify all data
    for (int i = 0; i < NUM_ITEMS; i++) {
        char expected_data[64];
        char expected_key[64];
        char read_buffer[64] = {0};

        snprintf(expected_data, sizeof(expected_data), "Data %d", i);
        snprintf(expected_key, sizeof(expected_key), "Key %d", i);

        // Read data
        i32 result = catalog_read(catalog, &data_ptrs[i], (u8 *) read_buffer, false);
        TEST_ASSERT_EQUAL_INT32(0, result);
        TEST_ASSERT_EQUAL_STRING(expected_data, read_buffer);

        // Read key
        memset(read_buffer, 0, sizeof(read_buffer));
        result = catalog_read(catalog, &key_ptrs[i], (u8 *) read_buffer, false);
        TEST_ASSERT_EQUAL_INT32(0, result);
        TEST_ASSERT_EQUAL_MEMORY(expected_key, read_buffer, strlen(expected_key));
    }
}

// Test boundary conditions - exactly at NORMAL_DATA_LIMIT
void test_catalog_boundary_normal_limit(void) {
    // Test data just under the limit (should use slot pages)
    u16 under_limit = NORMAL_DATA_LIMIT - 1;
    u8 *data_under = malloc(under_limit);
    TEST_ASSERT_NOT_NULL(data_under);
    memset(data_under, 0xCC, under_limit);

    struct VPtr ptr_under = catalog_write_data(catalog, data_under, under_limit);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptr_under.page_num);

    // Test data at the limit (should use chain pages)
    u32 at_limit = NORMAL_DATA_LIMIT + 1;
    u8 *data_at = malloc(at_limit);
    TEST_ASSERT_NOT_NULL(data_at);
    memset(data_at, 0xDD, at_limit);

    struct VPtr ptr_at = catalog_write_data(catalog, data_at, at_limit);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptr_at.page_num);
    TEST_ASSERT_EQUAL_UINT32(at_limit, ptr_at.size);

    // Verify both can be read back
    u8 *read_under = malloc(under_limit);
    u8 *read_at = malloc(at_limit);
    TEST_ASSERT_NOT_NULL(read_under);
    TEST_ASSERT_NOT_NULL(read_at);

    TEST_ASSERT_EQUAL_INT32(0, catalog_read(catalog, &ptr_under, read_under, false));
    TEST_ASSERT_EQUAL_MEMORY(data_under, read_under, under_limit);

    TEST_ASSERT_EQUAL_INT32(0, catalog_read(catalog, &ptr_at, read_at, true));
    TEST_ASSERT_EQUAL_MEMORY(data_at, read_at, at_limit);

    free(data_under);
    free(data_at);
    free(read_under);
    free(read_at);
}

// Test error handling - invalid VPtr
void test_catalog_read_invalid_ptr(void) {
    struct VPtr invalid_ptr = {.page_num = INVALID_PAGE, .slot_info = {0, 0}};

    u8 buffer[256];
    i32 result = catalog_read(catalog, &invalid_ptr, buffer, false);
    TEST_ASSERT_EQUAL_INT32(-1, result);
}

// Test catalog with persistence across close/open
void test_catalog_persistence_with_data(void) {
    const char *test_data = "Persistent data";
    u16 data_len = strlen(test_data) + 1;

    // Write data
    struct VPtr ptr = catalog_write_data(catalog, (const u8 *) test_data, data_len);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptr.page_num);

    u32 saved_page = ptr.page_num;
    u16 saved_slot = ptr.slot_info.slot;

    // Close and reopen
    catalog_close(catalog);
    catalog = catalog_open(alloc);
    TEST_ASSERT_NOT_NULL(catalog);

    // Reconstruct VPtr and read
    struct VPtr restored_ptr = {.page_num = saved_page, .slot_info = {saved_slot, 0}};

    char read_buffer[256] = {0};
    i32 result = catalog_read(catalog, &restored_ptr, (u8 *) read_buffer, false);
    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_STRING(test_data, read_buffer);
}

int main(void) {
    if (suite_setUp() != 0) {
        return -1;
    }

    UNITY_BEGIN();

    // Basic tests
    RUN_TEST(test_catalog_init);
    RUN_TEST(test_catalog_persistence);

    // Small data tests
    RUN_TEST(test_catalog_write_read_small_data);
    RUN_TEST(test_catalog_write_read_multiple_small);
    RUN_TEST(test_catalog_write_read_key);
    RUN_TEST(test_catalog_free_data);

    // Large data tests
    RUN_TEST(test_catalog_write_read_huge_data);
    RUN_TEST(test_catalog_free_huge_data);

    // FSM tests
    RUN_TEST(test_catalog_fsm_page_fill);
    RUN_TEST(test_catalog_fsm_free_reuse);

    // Mixed tests
    RUN_TEST(test_catalog_mixed_data_key);

    // Boundary and error tests
    RUN_TEST(test_catalog_boundary_normal_limit);
    RUN_TEST(test_catalog_read_invalid_ptr);

    // Persistence test
    RUN_TEST(test_catalog_persistence_with_data);

    return suite_tearDown() + UNITY_END();
}
