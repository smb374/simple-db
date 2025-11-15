#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "alloc.h"
#include "bufpool.h"
#include "catalog.h"
#include "pagestore.h"
#include "record.h"
#include "unity.h"
#include "utils.h"

#define TEST_DB_PATH "test_record.db"
#define TEST_DB_PAGES 1000

static struct PageStore *store;
static struct BufPool *pool;
static struct PageAllocator *alloc;
static struct Catalog *catalog;

int suite_setUp(void) { return 0; }

int suite_tearDown(void) { return 0; }

void setUp(void) {
    unlink(TEST_DB_PATH);

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

// Test integer encoding produces correct memcmp ordering
void test_key_encode_int_ordering(void) {
    i64 values[] = {-9223372036854775807LL, -1000, -1, 0, 1, 1000, 9223372036854775807LL};
    u8 encoded[7][8];

    // Encode all values
    for (int i = 0; i < 7; i++) {
        key_encode_int(encoded[i], values[i]);
    }

    // Verify ordering: encoded[i] < encoded[i+1]
    for (int i = 0; i < 6; i++) {
        int cmp = memcmp(encoded[i], encoded[i + 1], 8);
        TEST_ASSERT_LESS_THAN(0, cmp);
    }
}

// Test double encoding produces correct memcmp ordering
void test_key_encode_double_ordering(void) {
    double values[] = {-INFINITY, -1e10, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 1e10, INFINITY};
    u8 encoded[11][8];

    // Encode all values
    for (int i = 0; i < 11; i++) {
        key_encode_double(encoded[i], values[i]);
    }

    // Verify ordering
    for (int i = 0; i < 10; i++) {
        int cmp = memcmp(encoded[i], encoded[i + 1], 8);
        TEST_ASSERT_LESS_THAN(0, cmp);
    }
}

// Test creating inline key (≤24 bytes)
void test_create_key_inline(void) {
    const char *key_data = "short_key";
    u16 key_len = strlen(key_data);

    struct KeyRef kr;
    i32 result = create_key(catalog, &kr, (const u8 *) key_data, key_len);

    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_UINT16(key_len, kr.size);
    TEST_ASSERT_NOT_EQUAL(0, kr.key_hash);
    TEST_ASSERT_EQUAL_MEMORY(key_data, kr.full, key_len);
}

// Test creating overflow key (>24 bytes)
void test_create_key_overflow(void) {
    const char *key_data = "this_is_a_very_long_key_that_exceeds_24_bytes_threshold";
    u16 key_len = strlen(key_data);

    struct KeyRef kr;
    i32 result = create_key(catalog, &kr, (const u8 *) key_data, key_len);

    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_UINT16(key_len, kr.size);
    TEST_ASSERT_NOT_EQUAL(0, kr.key_hash);

    // Verify prefix is stored
    TEST_ASSERT_EQUAL_MEMORY(key_data, kr.partial.prefix, KEY_PREFIX_SIZE);

    // Verify overflow pointer is valid
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, kr.partial.data_ptr.page_num);
}

// Test reading inline key
void test_read_key_inline(void) {
    const char *key_data = "inline_test";
    u16 key_len = strlen(key_data);

    struct KeyRef kr;
    create_key(catalog, &kr, (const u8 *) key_data, key_len);

    u8 read_buffer[256] = {0};
    i32 result = read_key(catalog, &kr, read_buffer);

    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_MEMORY(key_data, read_buffer, key_len);
}

// Test reading overflow key
void test_read_key_overflow(void) {
    const char *key_data = "overflow_key_data_that_is_definitely_longer_than_24_bytes";
    u16 key_len = strlen(key_data);

    struct KeyRef kr;
    create_key(catalog, &kr, (const u8 *) key_data, key_len);

    u8 read_buffer[256] = {0};
    i32 result = read_key(catalog, &kr, read_buffer);

    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_MEMORY(key_data, read_buffer, key_len);
}

// Test freeing inline key (should be no-op)
void test_free_key_inline(void) {
    const char *key_data = "inline_key";
    u16 key_len = strlen(key_data);

    struct KeyRef kr;
    create_key(catalog, &kr, (const u8 *) key_data, key_len);

    i32 result = free_key(catalog, &kr);
    TEST_ASSERT_EQUAL_INT32(0, result);
}

// Test freeing overflow key
void test_free_key_overflow(void) {
    const char *key_data = "overflow_key_that_needs_to_be_freed_from_catalog_storage";
    u16 key_len = strlen(key_data);

    struct KeyRef kr;
    create_key(catalog, &kr, (const u8 *) key_data, key_len);

    i32 result = free_key(catalog, &kr);
    TEST_ASSERT_EQUAL_INT32(0, result);
}

// Test fast compare: both inline, equal
void test_fast_compare_both_inline_equal(void) {
    const char *key_data = "equal_key";
    u16 key_len = strlen(key_data);

    struct KeyRef k1, k2;
    create_key(catalog, &k1, (const u8 *) key_data, key_len);
    create_key(catalog, &k2, (const u8 *) key_data, key_len);

    bool need_full;
    i32 cmp = fast_compare_key(&k1, &k2, &need_full);

    TEST_ASSERT_FALSE(need_full);
    TEST_ASSERT_EQUAL_INT32(0, cmp);
}

// Test fast compare: both inline, different
void test_fast_compare_both_inline_different(void) {
    struct KeyRef k1, k2;
    create_key(catalog, &k1, (const u8 *) "apple", 5);
    create_key(catalog, &k2, (const u8 *) "banana", 6);

    bool need_full;
    i32 cmp = fast_compare_key(&k1, &k2, &need_full);

    TEST_ASSERT_FALSE(need_full);
    TEST_ASSERT_LESS_THAN(0, cmp); // "apple" < "banana"
}

// Test fast compare: both inline, different sizes
void test_fast_compare_both_inline_different_sizes(void) {
    struct KeyRef k1, k2;
    create_key(catalog, &k1, (const u8 *) "short", 5);
    create_key(catalog, &k2, (const u8 *) "short_longer", 12);

    bool need_full;
    i32 cmp = fast_compare_key(&k1, &k2, &need_full);

    TEST_ASSERT_FALSE(need_full);
    TEST_ASSERT_LESS_THAN(0, cmp); // shorter < longer with same prefix
}

// Test fast compare: both overflow, prefix differs
void test_fast_compare_both_overflow_prefix_differs(void) {
    struct KeyRef k1, k2;
    create_key(catalog, &k1, (const u8 *) "aaaaaaaaaaaaaaaa_overflow_key_1", 32);
    create_key(catalog, &k2, (const u8 *) "bbbbbbbbbbbbbbbb_overflow_key_2", 32);

    bool need_full;
    i32 cmp = fast_compare_key(&k1, &k2, &need_full);

    TEST_ASSERT_TRUE(need_full); // Overflow keys set this flag
    TEST_ASSERT_LESS_THAN(0, cmp); // But prefix comparison should work
}

// Test fast compare: both overflow, prefix same
void test_fast_compare_both_overflow_prefix_same(void) {
    struct KeyRef k1, k2;
    create_key(catalog, &k1, (const u8 *) "same_prefix_1234_suffix_aaaa", 28);
    create_key(catalog, &k2, (const u8 *) "same_prefix_1234_suffix_bbbb", 28);

    bool need_full;
    i32 cmp = fast_compare_key(&k1, &k2, &need_full);

    TEST_ASSERT_TRUE(need_full);
    TEST_ASSERT_EQUAL_INT32(0, cmp); // Prefixes match
}

// Test full compare: overflow keys with same prefix
void test_full_compare_overflow_same_prefix(void) {
    struct KeyRef k1, k2;
    create_key(catalog, &k1, (const u8 *) "same_prefix_1234_suffix_aaaa", 28);
    create_key(catalog, &k2, (const u8 *) "same_prefix_1234_suffix_bbbb", 28);

    i32 cmp_result;
    i32 status = full_compare_key(catalog, &k1, &k2, &cmp_result);

    TEST_ASSERT_EQUAL_INT32(0, status);
    TEST_ASSERT_LESS_THAN(0, cmp_result); // "aaaa" < "bbbb"
}

// Test full compare: identical overflow keys
void test_full_compare_overflow_identical(void) {
    const char *key_data = "identical_overflow_key_data_1234567890";

    struct KeyRef k1, k2;
    create_key(catalog, &k1, (const u8 *) key_data, strlen(key_data));
    create_key(catalog, &k2, (const u8 *) key_data, strlen(key_data));

    i32 cmp_result;
    i32 status = full_compare_key(catalog, &k1, &k2, &cmp_result);

    TEST_ASSERT_EQUAL_INT32(0, status);
    TEST_ASSERT_EQUAL_INT32(0, cmp_result);
}

// Test fast_compare_key_ext: inline vs raw data
void test_fast_compare_ext_inline(void) {
    struct KeyRef kr;
    create_key(catalog, &kr, (const u8 *) "test_key", 8);

    bool need_full;
    i32 cmp1 = fast_compare_key_ext(&kr, (const u8 *) "test_key", 8, &need_full);
    TEST_ASSERT_FALSE(need_full);
    TEST_ASSERT_EQUAL_INT32(0, cmp1);

    i32 cmp2 = fast_compare_key_ext(&kr, (const u8 *) "different", 9, &need_full);
    TEST_ASSERT_FALSE(need_full);
    TEST_ASSERT_NOT_EQUAL(0, cmp2);
}

// Test fast_compare_key_ext: overflow vs raw data
void test_fast_compare_ext_overflow(void) {
    struct KeyRef kr;
    create_key(catalog, &kr, (const u8 *) "overflow_key_1234567890_data", 28);

    bool need_full;
    i32 cmp = fast_compare_key_ext(&kr, (const u8 *) "overflow_key_1234567890_data", 28, &need_full);
    TEST_ASSERT_TRUE(need_full);
    TEST_ASSERT_EQUAL_INT32(0, cmp); // Prefix matches
}

// Test full_compare_key_ext: overflow vs raw data
void test_full_compare_ext_overflow(void) {
    const char *key_data = "full_overflow_comparison_test_1234567890";
    struct KeyRef kr;
    create_key(catalog, &kr, (const u8 *) key_data, strlen(key_data));

    i32 cmp_result;
    i32 status = full_compare_key_ext(catalog, &kr, (const u8 *) key_data, strlen(key_data), &cmp_result);

    TEST_ASSERT_EQUAL_INT32(0, status);
    TEST_ASSERT_EQUAL_INT32(0, cmp_result);

    // Different data
    const char *diff_data = "full_overflow_comparison_test_different";
    status = full_compare_key_ext(catalog, &kr, (const u8 *) diff_data, strlen(diff_data), &cmp_result);
    TEST_ASSERT_EQUAL_INT32(0, status);
    TEST_ASSERT_NOT_EQUAL(0, cmp_result);
}

// Test key with maximum inline size (24 bytes)
void test_create_key_boundary_24_bytes(void) {
    u8 key_data[24];
    memset(key_data, 'A', 24);

    struct KeyRef kr;
    i32 result = create_key(catalog, &kr, key_data, 24);

    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_UINT16(24, kr.size);
    TEST_ASSERT_EQUAL_MEMORY(key_data, kr.full, 24);
}

// Test key just over inline size (25 bytes)
void test_create_key_boundary_25_bytes(void) {
    u8 key_data[25];
    memset(key_data, 'B', 25);

    struct KeyRef kr;
    i32 result = create_key(catalog, &kr, key_data, 25);

    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_UINT16(25, kr.size);
    TEST_ASSERT_EQUAL_MEMORY(key_data, kr.partial.prefix, KEY_PREFIX_SIZE);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, kr.partial.data_ptr.page_num);
}

// Test composite key: integer + string
void test_composite_key_int_string(void) {
    u8 key_data[128];
    u8 *ptr = key_data;

    // Encode integer
    i64 id = 12345;
    key_encode_int(ptr, id);
    ptr += 8;

    // Append string
    const char *name = "john_doe";
    memcpy(ptr, name, strlen(name));
    ptr += strlen(name);

    u16 total_len = ptr - key_data;

    struct KeyRef kr;
    i32 result = create_key(catalog, &kr, key_data, total_len);
    TEST_ASSERT_EQUAL_INT32(0, result);

    // Read back and verify
    u8 read_buffer[128];
    read_key(catalog, &kr, read_buffer);
    TEST_ASSERT_EQUAL_MEMORY(key_data, read_buffer, total_len);
}

// Test many keys for ordering consistency
void test_key_ordering_consistency(void) {
    const char *keys[] = {"aaa", "aab", "aba", "abc", "bbb", "zzz"};
    int num_keys = 6;

    struct KeyRef krs[6];
    for (int i = 0; i < num_keys; i++) {
        create_key(catalog, &krs[i], (const u8 *) keys[i], strlen(keys[i]));
    }

    // Verify ordering: krs[i] < krs[i+1]
    for (int i = 0; i < num_keys - 1; i++) {
        bool need_full;
        i32 cmp = fast_compare_key(&krs[i], &krs[i + 1], &need_full);
        TEST_ASSERT_LESS_THAN(0, cmp);
    }
}

// Test empty key
void test_create_key_empty(void) {
    struct KeyRef kr;
    i32 result = create_key(catalog, &kr, (const u8 *) "", 0);

    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_UINT16(0, kr.size);
}

// Test very large overflow key
void test_create_key_very_large(void) {
    const u32 large_size = 5000;
    u8 *large_key = malloc(large_size);
    TEST_ASSERT_NOT_NULL(large_key);

    memset(large_key, 'X', large_size);

    struct KeyRef kr;
    i32 result = create_key(catalog, &kr, large_key, large_size);
    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_UINT16(large_size, kr.size);

    // Read back
    u8 *read_buffer = malloc(large_size);
    TEST_ASSERT_NOT_NULL(read_buffer);

    result = read_key(catalog, &kr, read_buffer);
    TEST_ASSERT_EQUAL_INT32(0, result);
    TEST_ASSERT_EQUAL_MEMORY(large_key, read_buffer, large_size);

    // Free
    free_key(catalog, &kr);

    free(large_key);
    free(read_buffer);
}

// Test hash consistency
void test_key_hash_consistency(void) {
    const char *key_data = "hash_test_key";

    struct KeyRef kr1, kr2;
    create_key(catalog, &kr1, (const u8 *) key_data, strlen(key_data));
    create_key(catalog, &kr2, (const u8 *) key_data, strlen(key_data));

    // Same key should produce same hash
    TEST_ASSERT_EQUAL_UINT32(kr1.key_hash, kr2.key_hash);
}

// Test hash inequality detection
void test_key_hash_inequality(void) {
    struct KeyRef k1, k2;
    create_key(catalog, &k1, (const u8 *) "key1", 4);
    create_key(catalog, &k2, (const u8 *) "key2", 4);

    // Different keys should (very likely) have different hashes
    TEST_ASSERT_NOT_EQUAL(k1.key_hash, k2.key_hash);
}

int main(void) {
    if (suite_setUp() != 0) {
        return -1;
    }

    UNITY_BEGIN();

    // Integer and double encoding tests
    RUN_TEST(test_key_encode_int_ordering);
    RUN_TEST(test_key_encode_double_ordering);

    // Key creation tests
    RUN_TEST(test_create_key_inline);
    RUN_TEST(test_create_key_overflow);

    // Key read tests
    RUN_TEST(test_read_key_inline);
    RUN_TEST(test_read_key_overflow);

    // Key free tests
    RUN_TEST(test_free_key_inline);
    RUN_TEST(test_free_key_overflow);

    // Fast compare tests (KeyRef vs KeyRef)
    RUN_TEST(test_fast_compare_both_inline_equal);
    RUN_TEST(test_fast_compare_both_inline_different);
    RUN_TEST(test_fast_compare_both_inline_different_sizes);
    RUN_TEST(test_fast_compare_both_overflow_prefix_differs);
    RUN_TEST(test_fast_compare_both_overflow_prefix_same);

    // Full compare tests
    RUN_TEST(test_full_compare_overflow_same_prefix);
    RUN_TEST(test_full_compare_overflow_identical);

    // External compare tests (KeyRef vs raw data)
    RUN_TEST(test_fast_compare_ext_inline);
    RUN_TEST(test_fast_compare_ext_overflow);
    RUN_TEST(test_full_compare_ext_overflow);

    // Boundary tests
    RUN_TEST(test_create_key_boundary_24_bytes);
    RUN_TEST(test_create_key_boundary_25_bytes);

    // Composite and complex tests
    RUN_TEST(test_composite_key_int_string);
    RUN_TEST(test_key_ordering_consistency);

    // Edge cases
    RUN_TEST(test_create_key_empty);
    RUN_TEST(test_create_key_very_large);

    // Hash tests
    RUN_TEST(test_key_hash_consistency);
    RUN_TEST(test_key_hash_inequality);

    return suite_tearDown() + UNITY_END();
}
