#include "dblock.h"

#include <stdlib.h>
#include <string.h>

#include "page.h"
#include "unity.h"
#include "utils.h"

static struct PageBank bank;

void setUp(void) {
    // Use in-memory database for speed
    bank_create(&bank, -1);
}

void tearDown(void) { bank_close(&bank); }

// =============================================================================
// HUGE DATA TESTS
// =============================================================================

void test_write_read_huge_single_page(void) {
    u32 len = DATA_HUGE_SPACE / 2;
    u8 *data = malloc(len);
    for (u32 i = 0; i < len; i++) {
        data[i] = i % 256;
    }

    struct VPtr ptr = write_huge_data(&bank, data, len);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptr.page);
    TEST_ASSERT_EQUAL(len, VPTR_GET_HUGE_LEN(ptr));

    u8 *read_buf = malloc(len);
    i32 ret = read_huge_data(&bank, read_buf, ptr);
    TEST_ASSERT_EQUAL(0, ret);

    TEST_ASSERT_EQUAL_MEMORY(data, read_buf, len);

    free(data);
    free(read_buf);
}

void test_write_read_huge_multi_page(void) {
    u32 len = DATA_HUGE_SPACE * 2 + 123;
    u8 *data = malloc(len);
    for (u32 i = 0; i < len; i++) {
        data[i] = i % 256;
    }

    struct VPtr ptr = write_huge_data(&bank, data, len);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptr.page);
    TEST_ASSERT_EQUAL(len, VPTR_GET_HUGE_LEN(ptr));

    u8 *read_buf = malloc(len);
    i32 ret = read_huge_data(&bank, read_buf, ptr);
    TEST_ASSERT_EQUAL(0, ret);

    TEST_ASSERT_EQUAL_MEMORY(data, read_buf, len);

    free(data);
    free(read_buf);
}

void test_delete_huge_data(void) {
    u32 len = DATA_HUGE_SPACE * 3 + 456;
    u8 *data = malloc(len);
    // No need to fill data for this test

    struct VPtr ptr = write_huge_data(&bank, data, len);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptr.page);

    u32 npage = len / DATA_HUGE_SPACE + (len % DATA_HUGE_SPACE != 0);
    u32 *pages = malloc(npage * sizeof(u32));
    u32 curr = ptr.page;
    for (u32 i = 0; i < npage; i++) {
        pages[i] = curr;
        TEST_ASSERT_TRUE(is_page_set(&bank, curr));
        struct DataBlockHuge *blk = get_page(&bank, curr);
        curr = blk->meta.next_page;
    }
    TEST_ASSERT_EQUAL(INVALID_PAGE, curr);

    delete_huge_data(&bank, ptr);

    for (u32 i = 0; i < npage; i++) {
        TEST_ASSERT_FALSE(is_page_set(&bank, pages[i]));
    }

    free(data);
    free(pages);
}

// =============================================================================
// NORMAL DATA TESTS
// =============================================================================

void test_write_read_normal_data(void) {
    char *test_str = "Hello, world!";
    u16 len = strlen(test_str) + 1;

    struct VPtr ptr = write_normal_data(&bank, test_str, len);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, ptr.page);

    char *read_buf = malloc(len);
    i32 ret = read_normal_data(&bank, read_buf, ptr);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_EQUAL_STRING(test_str, read_buf);

    free(read_buf);
}

void test_delete_normal_data_updates_frag_bytes(void) {
    char *test_str = "This will be deleted";
    u16 len = strlen(test_str) + 1;

    struct VPtr ptr = write_normal_data(&bank, test_str, len);
    struct DataBlockNormal *blk = get_page(&bank, ptr.page);
    TEST_ASSERT_EQUAL(0, blk->frag_bytes);

    delete_normal_data(&bank, ptr);

    u16 expected_frag = sizeof(struct Cell) + len;
    TEST_ASSERT_EQUAL(expected_frag, blk->frag_bytes);
}

void test_write_reuses_slots(void) {
    char *str1 = "first";
    char *str2 = "second";
    char *str3 = "third";

    struct VPtr ptr1 = write_normal_data(&bank, str1, strlen(str1) + 1);
    write_normal_data(&bank, str2, strlen(str2) + 1);

    struct DataBlockNormal *blk = get_page(&bank, ptr1.page);
    TEST_ASSERT_EQUAL(2, blk->num_slots);

    // Delete the first entry
    delete_normal_data(&bank, ptr1);
    TEST_ASSERT_EQUAL(0, blk->slots[0]);

    // Write a third entry, it should reuse the first slot
    struct VPtr ptr3 = write_normal_data(&bank, str3, strlen(str3) + 1);

    // num_slots should still be 2, and the new ptr should be in slot 0
    TEST_ASSERT_EQUAL(2, blk->num_slots);
    TEST_ASSERT_EQUAL(0, VPTR_GET_SLOT(ptr3));
    TEST_ASSERT_NOT_EQUAL(0, blk->slots[0]);
}

void test_defrag_normal_page(void) {
    // 1. Setup: Write 3 records, ensure they are in order
    char *str1 = "record one";
    char *str3 = "record three";
    u16 len1 = strlen(str1) + 1;
    u16 len3 = strlen(str3) + 1;

    // Make a record to delete that is large enough to trigger defrag
    u16 len2 = (PAGE_SIZE / 4) + 1;
    char *str2 = malloc(len2);
    memset(str2, 'X', len2 - 1);
    str2[len2 - 1] = '\0';

    struct VPtr ptr2 = write_normal_data(&bank, str2, len2);
    struct VPtr ptr1 = write_normal_data(&bank, str1, len1);
    struct VPtr ptr3 = write_normal_data(&bank, str3, len3);

    struct DataBlockNormal *blk = get_page(&bank, ptr1.page);
    u16 slot1_off_before = blk->slots[VPTR_GET_SLOT(ptr1)];
    u16 slot3_off_before = blk->slots[VPTR_GET_SLOT(ptr3)];

    // 2. Create fragmentation by deleting the middle record
    delete_normal_data(&bank, ptr2);
    TEST_ASSERT_EQUAL(sizeof(struct Cell) + len2, blk->frag_bytes);

    // 3. Trigger defragmentation by writing another small record
    write_normal_data(&bank, "trigger", 8);

    // 4. Assertions
    TEST_ASSERT_EQUAL(0, blk->frag_bytes); // Defrag should reset this

    u16 slot1_off_after = blk->slots[VPTR_GET_SLOT(ptr1)];
    u16 slot3_off_after = blk->slots[VPTR_GET_SLOT(ptr3)];

    // After compaction, cell offsets should have moved (increased)
    TEST_ASSERT_GREATER_THAN(slot1_off_before, slot1_off_after);
    TEST_ASSERT_GREATER_THAN(slot3_off_before, slot3_off_after);

    // Data should still be intact
    char *buf1 = malloc(len1);
    char *buf3 = malloc(len3);
    TEST_ASSERT_EQUAL(0, read_normal_data(&bank, buf1, ptr1));
    TEST_ASSERT_EQUAL(0, read_normal_data(&bank, buf3, ptr3));
    TEST_ASSERT_EQUAL_STRING(str1, buf1);
    TEST_ASSERT_EQUAL_STRING(str3, buf3);

    free(str2);
    free(buf1);
    free(buf3);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_write_read_huge_single_page);
    RUN_TEST(test_write_read_huge_multi_page);
    RUN_TEST(test_delete_huge_data);
    RUN_TEST(test_write_read_normal_data);
    RUN_TEST(test_delete_normal_data_updates_frag_bytes);
    RUN_TEST(test_write_reuses_slots);
    RUN_TEST(test_defrag_normal_page);
    return UNITY_END();
}
