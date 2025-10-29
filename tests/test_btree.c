#include "btree.h"

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "page.h"
#include "unity.h"

static struct BTree tree;

static inline i32 key_cmp(const u8 *k1, const u8 *k2) { return memcmp(k1, k2, MAX_KEY); }

static u8 binary_search(const u8 *keys, u8 num_keys, const u8 *key, size_t stride, bool *exact_match) {
    u8 left = 0, right = num_keys;

    while (left < right) {
        u8 mid = left + (right - left) / 2;
        const u8 *mid_key = keys + mid * stride;

        i32 res = key_cmp(mid_key, key);
        if (res < 0) {
            left = mid + 1;
        } else {
            if (!res && exact_match) {
                *exact_match = true;
            }
            right = mid;
        }
    }

    return left;
}

// Find slot in leaf node
static u8 leaf_find_slot(struct LeafNode *leaf, const u8 *key, bool *exact_match) {
    return binary_search((const u8 *) leaf->entries, leaf->header.nkeys, key, sizeof(struct LeafEnt), exact_match);
}

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

void test_value_storage_types(void) {
    // Test inline value (≤63 bytes)
    u8 key1[MAX_KEY];
    make_key(key1, "inline_key");
    char inline_val[50] = "This is an inline value";
    btree_insert(&tree, key1, inline_val, strlen(inline_val) + 1);

    struct Superblock *sb = get_superblock(&tree.bank);
    struct LeafNode *root = get_page(&tree.bank, sb->root_page);

    bool exact_match = false;
    u8 slot = leaf_find_slot(root, key1, &exact_match);
    TEST_ASSERT_TRUE(exact_match);
    TEST_ASSERT_EQUAL(DATA_INLINE, root->entries[slot].val_type);
    TEST_ASSERT_EQUAL(strlen(inline_val) + 1, root->entries[slot].ival.len);

    // Test normal value (64-4000 bytes)
    u8 key2[MAX_KEY];
    make_key(key2, "normal_key");
    char normal_val[100];
    memset(normal_val, 'A', 99);
    normal_val[99] = '\0';
    btree_insert(&tree, key2, normal_val, 100);

    root = get_page(&tree.bank, sb->root_page);
    exact_match = false;
    slot = leaf_find_slot(root, key2, &exact_match);
    TEST_ASSERT_TRUE(exact_match);
    TEST_ASSERT_EQUAL(DATA_NORMAL, root->entries[slot].val_type);

    // Test huge value (>4000 bytes)
    u8 key3[MAX_KEY];
    make_key(key3, "zzz_huge_key");
    char huge_val[5000];
    memset(huge_val, 'B', 4999);
    huge_val[4999] = '\0';
    btree_insert(&tree, key3, huge_val, 5000);

    root = get_page(&tree.bank, sb->root_page);
    exact_match = false;
    slot = leaf_find_slot(root, key3, &exact_match);
    TEST_ASSERT_TRUE(exact_match);
    TEST_ASSERT_EQUAL(DATA_HUGE, root->entries[slot].val_type);

    // Verify all values can be read back correctly
    char read_buf[5000];
    u32 read_len;

    btree_search(&tree, key1, read_buf, &read_len);
    TEST_ASSERT_EQUAL_STRING(inline_val, read_buf);

    btree_search(&tree, key2, read_buf, &read_len);
    TEST_ASSERT_EQUAL_MEMORY(normal_val, read_buf, 100);

    btree_search(&tree, key3, read_buf, &read_len);
    TEST_ASSERT_EQUAL_MEMORY(huge_val, read_buf, 5000);
}

void test_value_size_boundaries(void) {
    u8 key[MAX_KEY];
    char val[5000];

    struct Superblock *sb = get_superblock(&tree.bank);
    struct LeafNode *root;
    bool exact_match;
    u8 slot;

    // Test at boundary: exactly 63 bytes (should be inline)
    make_key(key, "bound_00_63b");
    memset(val, 'X', 63);
    btree_insert(&tree, key, val, 63);

    root = get_page(&tree.bank, sb->root_page);
    exact_match = false;
    slot = leaf_find_slot(root, key, &exact_match);
    TEST_ASSERT_TRUE(exact_match);
    TEST_ASSERT_EQUAL(DATA_INLINE, root->entries[slot].val_type);

    // Test at boundary: 64 bytes (should be normal)
    make_key(key, "bound_01_64b");
    memset(val, 'Y', 64);
    btree_insert(&tree, key, val, 64);

    root = get_page(&tree.bank, sb->root_page);
    exact_match = false;
    slot = leaf_find_slot(root, key, &exact_match);
    TEST_ASSERT_TRUE(exact_match);
    TEST_ASSERT_EQUAL(DATA_NORMAL, root->entries[slot].val_type);

    // Test at boundary: 4000 bytes (should be normal)
    make_key(key, "bound_02_4kb");
    memset(val, 'Z', 4000);
    btree_insert(&tree, key, val, 4000);

    root = get_page(&tree.bank, sb->root_page);
    exact_match = false;
    slot = leaf_find_slot(root, key, &exact_match);
    TEST_ASSERT_TRUE(exact_match);
    TEST_ASSERT_EQUAL(DATA_NORMAL, root->entries[slot].val_type);

    // Test at boundary: 4001 bytes (should be huge)
    make_key(key, "bound_03_4kb");
    memset(val, 'W', 4001);
    btree_insert(&tree, key, val, 4001);

    root = get_page(&tree.bank, sb->root_page);
    exact_match = false;
    slot = leaf_find_slot(root, key, &exact_match);
    TEST_ASSERT_TRUE(exact_match);
    TEST_ASSERT_EQUAL(DATA_HUGE, root->entries[slot].val_type);

    // Verify all can be read back
    make_key(key, "bound_00_63b");
    char read_buf[5000];
    u32 read_len;
    TEST_ASSERT_EQUAL(0, btree_search(&tree, key, read_buf, &read_len));
    TEST_ASSERT_EQUAL(63, read_len);
}

void test_update_changes_value_type(void) {
    u8 key[MAX_KEY];
    make_key(key, "update_test");

    struct Superblock *sb = get_superblock(&tree.bank);
    struct LeafNode *root;
    bool exact_match;
    u8 slot;

    // Start with inline value
    char small_val[20] = "small";
    btree_insert(&tree, key, small_val, strlen(small_val) + 1);

    root = get_page(&tree.bank, sb->root_page);
    exact_match = false;
    slot = leaf_find_slot(root, key, &exact_match);
    TEST_ASSERT_TRUE(exact_match);
    TEST_ASSERT_EQUAL(DATA_INLINE, root->entries[slot].val_type);

    // Update to normal value
    char medium_val[100];
    memset(medium_val, 'M', 99);
    medium_val[99] = '\0';
    btree_insert(&tree, key, medium_val, 100);

    root = get_page(&tree.bank, sb->root_page);
    exact_match = false;
    slot = leaf_find_slot(root, key, &exact_match);
    TEST_ASSERT_TRUE(exact_match);
    TEST_ASSERT_EQUAL(DATA_NORMAL, root->entries[slot].val_type);

    // Update to huge value
    char large_val[5000];
    memset(large_val, 'L', 4999);
    large_val[4999] = '\0';
    btree_insert(&tree, key, large_val, 5000);

    root = get_page(&tree.bank, sb->root_page);
    exact_match = false;
    slot = leaf_find_slot(root, key, &exact_match);
    TEST_ASSERT_TRUE(exact_match);
    TEST_ASSERT_EQUAL(DATA_HUGE, root->entries[slot].val_type);

    // Verify final value
    char read_buf[5000];
    u32 read_len;
    btree_search(&tree, key, read_buf, &read_len);
    TEST_ASSERT_EQUAL_MEMORY(large_val, read_buf, 5000);
}

void test_internal_node_split(void) {
    // Insert enough keys to trigger internal node split
    // With MAX_NODE_ENTS=30, we need roughly 30*30 = 900 keys

    for (int i = 0; i < 1000; i++) {
        u8 key[MAX_KEY];
        char key_str[16];
        sprintf(key_str, "k_%04d", i);
        make_key(key, key_str);

        char val[16];
        sprintf(val, "v_%04d", i);
        btree_insert(&tree, key, val, strlen(val) + 1);
    }

    // Verify all keys are searchable
    for (int i = 0; i < 1000; i++) {
        u8 key[MAX_KEY];
        char key_str[16];
        sprintf(key_str, "k_%04d", i);
        make_key(key, key_str);

        char read_buf[16];
        u32 read_len;
        i32 ret = btree_search(&tree, key, read_buf, &read_len);
        TEST_ASSERT_EQUAL(0, ret);

        char expected_val[16];
        sprintf(expected_val, "v_%04d", i);
        TEST_ASSERT_EQUAL_STRING(expected_val, read_buf);
    }

    // Root should be internal node with height > 1
    struct Superblock *sb = get_superblock(&tree.bank);
    struct NodeHeader *root_header = get_page(&tree.bank, sb->root_page);
    TEST_ASSERT_EQUAL(BNODE_INT, root_header->type);
}

void test_delete_internal_node_merge(void) {
    // Insert many keys to create multi-level tree
    for (int i = 0; i < 500; i++) {
        u8 key[MAX_KEY];
        char key_str[16];
        sprintf(key_str, "k_%04d", i);
        make_key(key, key_str);

        char val[16];
        sprintf(val, "v_%04d", i);
        btree_insert(&tree, key, val, strlen(val) + 1);
    }

    // Delete many keys to trigger internal node merges
    for (int i = 0; i < 400; i++) {
        u8 key[MAX_KEY];
        char key_str[16];
        sprintf(key_str, "k_%04d", i);
        make_key(key, key_str);

        i32 ret = btree_delete(&tree, key);
        TEST_ASSERT_EQUAL(0, ret);
    }

    // Verify remaining keys are still searchable
    for (int i = 400; i < 500; i++) {
        u8 key[MAX_KEY];
        char key_str[16];
        sprintf(key_str, "k_%04d", i);
        make_key(key, key_str);

        char read_buf[16];
        u32 read_len;
        i32 ret = btree_search(&tree, key, read_buf, &read_len);
        TEST_ASSERT_EQUAL(0, ret);

        char expected_val[16];
        sprintf(expected_val, "v_%04d", i);
        TEST_ASSERT_EQUAL_STRING(expected_val, read_buf);
    }

    // Verify deleted keys are gone
    for (int i = 0; i < 400; i++) {
        u8 key[MAX_KEY];
        char key_str[16];
        sprintf(key_str, "k_%04d", i);
        make_key(key, key_str);

        i32 ret = btree_search(&tree, key, NULL, NULL);
        TEST_ASSERT_EQUAL(-1, ret);
    }
}

void test_random_operations(void) {
#define NUM_KEYS 1000
    int inserted[NUM_KEYS] = {0};

    srand(42); // Fixed seed for reproducibility

    // Random inserts
    for (int i = 0; i < NUM_KEYS; i++) {
        int idx = rand() % NUM_KEYS;
        u8 key[MAX_KEY];
        char key_str[16];
        sprintf(key_str, "k_%04d", idx);
        make_key(key, key_str);

        char val[16];
        sprintf(val, "v_%04d", idx);
        btree_insert(&tree, key, val, strlen(val) + 1);
        inserted[idx] = 1;
    }

    // Verify all inserted keys exist
    for (int i = 0; i < NUM_KEYS; i++) {
        if (inserted[i]) {
            u8 key[MAX_KEY];
            char key_str[16];
            sprintf(key_str, "k_%04d", i);
            make_key(key, key_str);

            i32 ret = btree_search(&tree, key, NULL, NULL);
            TEST_ASSERT_EQUAL(0, ret);
        }
    }

    // Random deletes
    for (int i = 0; i < NUM_KEYS / 2; i++) {
        int idx = rand() % NUM_KEYS;
        if (inserted[idx]) {
            u8 key[MAX_KEY];
            char key_str[16];
            sprintf(key_str, "k_%04d", idx);
            make_key(key, key_str);

            btree_delete(&tree, key);
            inserted[idx] = 0;
        }
    }

    // Verify correct keys exist/don't exist
    for (int i = 0; i < NUM_KEYS; i++) {
        u8 key[MAX_KEY];
        char key_str[16];
        sprintf(key_str, "k_%04d", i);
        make_key(key, key_str);

        i32 ret = btree_search(&tree, key, NULL, NULL);
        if (inserted[i]) {
            TEST_ASSERT_EQUAL(0, ret);
        } else {
            TEST_ASSERT_EQUAL(-1, ret);
        }
    }
}

void test_empty_tree_operations(void) {
    u8 key[MAX_KEY];
    make_key(key, "test");

    // Search in empty tree
    i32 ret = btree_search(&tree, key, NULL, NULL);
    TEST_ASSERT_EQUAL(-1, ret);

    // Delete from empty tree
    ret = btree_delete(&tree, key);
    TEST_ASSERT_EQUAL(-1, ret);
}

void test_file_persistence(void) {
    const char *test_file = "test_btree.db";

    // Create and populate tree
    struct BTree tree1;
    int fd = open(test_file, O_RDWR | O_CREAT | O_TRUNC, 0644);
    btree_create(&tree1, fd);

    for (int i = 0; i < 100; i++) {
        u8 key[MAX_KEY];
        char key_str[16];
        sprintf(key_str, "k_%03d", i);
        make_key(key, key_str);

        char val[16];
        sprintf(val, "v_%03d", i);
        btree_insert(&tree1, key, val, strlen(val) + 1);
    }

    btree_close(&tree1);

    // Reopen and verify
    struct BTree tree2;
    btree_open(&tree2, test_file);

    for (int i = 0; i < 100; i++) {
        u8 key[MAX_KEY];
        char key_str[16];
        sprintf(key_str, "k_%03d", i);
        make_key(key, key_str);

        char read_buf[16];
        u32 read_len;
        i32 ret = btree_search(&tree2, key, read_buf, &read_len);
        TEST_ASSERT_EQUAL(0, ret);

        char expected_val[16];
        sprintf(expected_val, "v_%03d", i);
        TEST_ASSERT_EQUAL_STRING(expected_val, read_buf);
    }

    btree_close(&tree2);
    unlink(test_file);
}

int main(void) {
    UNITY_BEGIN();
    // Insert & Search tests
    RUN_TEST(test_insert_and_search_single_key);
    RUN_TEST(test_search_non_existent_key);
    RUN_TEST(test_update_existing_key);
    RUN_TEST(test_insertion_causes_leaf_split);
    // Delete tests
    RUN_TEST(test_delete_simple);
    RUN_TEST(test_delete_cause_redistribute);
    RUN_TEST(test_delete_cause_merge_and_root_shrink);
    // Other tests
    RUN_TEST(test_value_storage_types);
    RUN_TEST(test_value_size_boundaries);
    RUN_TEST(test_update_changes_value_type);
    RUN_TEST(test_internal_node_split);
    RUN_TEST(test_delete_internal_node_merge);
    RUN_TEST(test_random_operations);
    RUN_TEST(test_empty_tree_operations);
    RUN_TEST(test_file_persistence);

    return UNITY_END();
}
