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

void test_delete_simple(void) {
    u8 key1[MAX_KEY], key2[MAX_KEY], key3[MAX_KEY];
    make_key(key1, "key1");
    make_key(key2, "key2");
    make_key(key3, "key3");
    const char *val = "value";
    u32 len = strlen(val) + 1;

    btree_insert(&tree, key1, val, len);
    btree_insert(&tree, key2, val, len);
    btree_insert(&tree, key3, val, len);

    // Delete key2
    i32 ret = btree_delete(&tree, key2);
    TEST_ASSERT_EQUAL(0, ret);

    // Verify key2 is gone
    char read_buf[16];
    u32 read_len;
    ret = btree_search(&tree, key2, read_buf, &read_len);
    TEST_ASSERT_EQUAL(-1, ret);

    // Verify key1 and key3 are still there
    ret = btree_search(&tree, key1, read_buf, &read_len);
    TEST_ASSERT_EQUAL(0, ret);
    ret = btree_search(&tree, key3, read_buf, &read_len);
    TEST_ASSERT_EQUAL(0, ret);
}

void test_delete_cause_redistribute(void) {
    // Setup: Create two leaf nodes.
    // A split occurs on the 31st insertion (i=30).
    // lleaf gets 15 keys ("key_00" to "key_14") -> MIN_NODE_ENTS
    // rleaf gets 16 keys ("key_15" to "key_30") -> MIN_NODE_ENTS + 1
    for (int i = 0; i <= 30; i++) {
        u8 key[MAX_KEY];
        char val[16];
        char key_str[16];
        sprintf(key_str, "key_%02d", i);
        make_key(key, key_str);
        sprintf(val, "val_%02d", i);
        btree_insert(&tree, key, val, strlen(val) + 1);
    }

    // Delete a key from the left leaf to make it underfull.
    u8 key_to_delete[MAX_KEY];
    make_key(key_to_delete, "key_05");
    i32 ret = btree_delete(&tree, key_to_delete);
    TEST_ASSERT_EQUAL(0, ret);

    // Verification:
    // The left leaf should have borrowed "key_15" from the right leaf.
    // The parent separator key should have been updated from "key_15" to "key_16".

    // 1. Check the deleted key is gone.
    ret = btree_search(&tree, key_to_delete, NULL, NULL);
    TEST_ASSERT_EQUAL(-1, ret);

    // 2. Check a key that was NOT moved.
    u8 unoved_key[MAX_KEY];
    make_key(unoved_key, "key_03");
    ret = btree_search(&tree, unoved_key, NULL, NULL);
    TEST_ASSERT_EQUAL(0, ret);

    // 3. Check that the borrowed key ("key_15") is now findable via search
    //    (it will be in the left leaf now).
    u8 borrowed_key[MAX_KEY];
    make_key(borrowed_key, "key_15");
    ret = btree_search(&tree, borrowed_key, NULL, NULL);
    TEST_ASSERT_EQUAL(0, ret);

    // 4. Check that a key from the right leaf is still there.
    u8 rleaf_key[MAX_KEY];
    make_key(rleaf_key, "key_20");
    ret = btree_search(&tree, rleaf_key, NULL, NULL);
    TEST_ASSERT_EQUAL(0, ret);

    // 5. Check the parent (root) separator key is now "key_16"
    struct Superblock *sb = get_superblock(&tree.bank);
    struct IntNode *root = get_page(&tree.bank, sb->root_page);
    TEST_ASSERT_EQUAL(BNODE_INT, root->header.type);
    TEST_ASSERT_EQUAL(1, root->header.nkeys);
    u8 new_sep_key[MAX_KEY];
    make_key(new_sep_key, "key_16");
    TEST_ASSERT_EQUAL(0, memcmp(root->entries[0].key, new_sep_key, MAX_KEY));
}

void test_delete_cause_merge_and_root_shrink(void) {
    // Setup: Create two leaf nodes, both with MIN_NODE_ENTS keys.
    // 1. Insert 31 keys to cause a split.
    //    lleaf has 15 keys, rleaf has 16 keys.
    for (int i = 0; i <= 30; i++) {
        u8 key[MAX_KEY];
        char val[16];
        char key_str[16];
        sprintf(key_str, "key_%02d", i);
        make_key(key, key_str);
        sprintf(val, "val_%02d", i);
        btree_insert(&tree, key, val, strlen(val) + 1);
    }

    // 2. Delete one from the right leaf to bring it to 15 keys.
    u8 key_to_trim[MAX_KEY];
    make_key(key_to_trim, "key_30");
    btree_delete(&tree, key_to_trim);

    // At this point, both leaves have 15 keys (MIN_NODE_ENTS).
    // The root is an internal node with one separator key ("key_15").

    // 3. Delete a key from the left leaf, causing it to be underfull.
    //    It cannot redistribute, so it must merge with the right leaf.
    u8 key_to_delete[MAX_KEY];
    make_key(key_to_delete, "key_00");
    i32 ret = btree_delete(&tree, key_to_delete);
    TEST_ASSERT_EQUAL(0, ret);

    // Verification:
    // 1. The root should now be a LEAF node again (tree height shrunk).
    struct Superblock *sb = get_superblock(&tree.bank);
    struct NodeHeader *root_header = get_page(&tree.bank, sb->root_page);
    TEST_ASSERT_EQUAL(BNODE_LEAF, root_header->type);

    // 2. The new root leaf should have 14 + 15 = 29 keys.
    TEST_ASSERT_EQUAL(29, root_header->nkeys);

    // 3. Check that the deleted key is gone.
    ret = btree_search(&tree, key_to_delete, NULL, NULL);
    TEST_ASSERT_EQUAL(-1, ret);

    // 4. Check that a key from the original left leaf is still there.
    u8 l_key[MAX_KEY];
    make_key(l_key, "key_01");
    ret = btree_search(&tree, l_key, NULL, NULL);
    TEST_ASSERT_EQUAL(0, ret);

    // 5. Check that a key from the original right leaf is still there.
    u8 r_key[MAX_KEY];
    make_key(r_key, "key_20");
    ret = btree_search(&tree, r_key, NULL, NULL);
    TEST_ASSERT_EQUAL(0, ret);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_insert_and_search_single_key);
    RUN_TEST(test_search_non_existent_key);
    RUN_TEST(test_update_existing_key);
    RUN_TEST(test_insertion_causes_leaf_split);
    // New delete tests
    RUN_TEST(test_delete_simple);
    RUN_TEST(test_delete_cause_redistribute);
    RUN_TEST(test_delete_cause_merge_and_root_shrink);
    return UNITY_END();
}

