#include "btree.h"

#include <string.h>

#include "page.h"
#include "unity.h"

static struct BTree tree;

// Helper to create a zero-padded, MAX_KEY length key
static void make_key(u8 *key_buf, const char *key_str) {
    memset(key_buf, 0, MAX_KEY);
    strcpy((char *) key_buf, key_str);
}

void setUp(void) {
    // Use in-memory database for all tests
    btree_create(&tree, -1);
}

void tearDown(void) { btree_close(&tree); }

void test_insert_and_search_single_key(void) {
    u8 key[MAX_KEY];
    make_key(key, "test_key");
    const char *val = "test_value";
    u32 len = strlen(val) + 1;

    i32 ret = btree_insert(&tree, key, val, len);
    TEST_ASSERT_EQUAL(0, ret);

    char read_buf[64];
    u32 read_len;
    ret = btree_search(&tree, key, read_buf, &read_len);

    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_EQUAL(len, read_len);
    TEST_ASSERT_EQUAL_STRING(val, read_buf);
}

void test_search_non_existent_key(void) {
    u8 key[MAX_KEY];
    make_key(key, "non_existent");
    char read_buf[64];
    u32 read_len;

    i32 ret = btree_search(&tree, key, read_buf, &read_len);
    TEST_ASSERT_EQUAL(-1, ret);
}

void test_update_existing_key(void) {
    u8 key[MAX_KEY];
    make_key(key, "update_key");
    const char *val1 = "initial_value";
    const char *val2 = "updated_value";
    u32 len1 = strlen(val1) + 1;
    u32 len2 = strlen(val2) + 1;

    // Insert initial value
    i32 ret = btree_insert(&tree, key, val1, len1);
    TEST_ASSERT_EQUAL(0, ret);

    // Update with new value
    ret = btree_insert(&tree, key, val2, len2);
    TEST_ASSERT_EQUAL(0, ret);

    char read_buf[64];
    u32 read_len;
    ret = btree_search(&tree, key, read_buf, &read_len);

    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_EQUAL(len2, read_len);
    TEST_ASSERT_EQUAL_STRING(val2, read_buf);
}

// This test requires MAX_NODE_ENTS to be 30 as per the design doc
void test_insertion_causes_leaf_split(void) {
    // MAX_NODE_ENTS is 30
    for (int i = 0; i <= 30; i++) {
        u8 key[MAX_KEY];
        char val[16];
        char key_str[16];
        sprintf(key_str, "key_%02d", i);
        make_key(key, key_str);

        sprintf(val, "val_%02d", i);
        i32 ret = btree_insert(&tree, key, val, strlen(val) + 1);
        TEST_ASSERT_EQUAL(0, ret);

        struct Superblock *sb = get_superblock(&tree.bank);
        struct NodeHeader *root_header = get_page(&tree.bank, sb->root_page);
        if (root_header->type == BNODE_LEAF) {
            TEST_ASSERT_EQUAL_HEX32(INVALID_PAGE, root_header->next_page);
        }
    }

    // Verify all keys can be found
    for (int i = 0; i <= 30; i++) {
        u8 key[MAX_KEY];
        char val[16];
        char key_str[16];
        char read_buf[16];
        u32 read_len;

        sprintf(key_str, "key_%02d", i);
        make_key(key, key_str);
        sprintf(val, "val_%02d", i);

        i32 ret = btree_search(&tree, key, read_buf, &read_len);
        TEST_ASSERT_EQUAL(0, ret);
        TEST_ASSERT_EQUAL_STRING(val, read_buf);
    }

    // Check that root is now an internal node
    struct Superblock *sb = get_superblock(&tree.bank);
    struct NodeHeader *root_header = get_page(&tree.bank, sb->root_page);
    TEST_ASSERT_EQUAL(BNODE_INT, root_header->type);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_insert_and_search_single_key);
    RUN_TEST(test_search_non_existent_key);
    RUN_TEST(test_update_existing_key);
    RUN_TEST(test_insertion_causes_leaf_split);
    // More complex tests for internal node splits can be added here later
    return UNITY_END();
}

