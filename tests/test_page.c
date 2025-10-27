#include "page.h"

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "unity.h"
#include "utils.h"

static const char *TEST_DB_PATH = "test_page.db";

void setUp(void) {
    // Executed before each test case
    remove(TEST_DB_PATH);
}

void tearDown(void) {
    // Executed after each test case
    remove(TEST_DB_PATH);
}

void test_bank_create_in_memory(void) {
    struct PageBank bank;
    int ret = bank_create(&bank, -1);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_EQUAL(-1, bank.fd);
    TEST_ASSERT_NOT_NULL(bank.pages);
    TEST_ASSERT_EQUAL(PAGE_SIZE * INITIAL_PAGES, bank.size);

    struct Superblock *sb = get_superblock(&bank);
    TEST_ASSERT_EQUAL(MAGIC, sb->magic);
    TEST_ASSERT_EQUAL(VERSION, sb->version);
    TEST_ASSERT_EQUAL(MAX_BITMAP_PAGES, sb->bitmap_pages);
    TEST_ASSERT_EQUAL(1 + MAX_BITMAP_PAGES, sb->fst_free_page);
    TEST_ASSERT_EQUAL(INITIAL_PAGES, sb->total_pages);
    TEST_ASSERT_EQUAL(PAGE_SIZE, sb->page_size);

    // Check if reserved pages are marked in bitmap
    for (u32 i = 0; i <= sb->fst_free_page; i++) {
        TEST_ASSERT_TRUE(is_page_set(&bank, i));
    }

    bank_close(&bank);
}

void test_bank_create_file(void) {
    struct PageBank bank;
    int fd = open(TEST_DB_PATH, O_RDWR | O_CREAT | O_TRUNC, 0644);
    TEST_ASSERT_GREATER_OR_EQUAL(0, fd);

    int ret = bank_create(&bank, fd);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_NOT_EQUAL(-1, bank.fd);
    TEST_ASSERT_NOT_NULL(bank.pages);

    struct Superblock *sb = get_superblock(&bank);
    TEST_ASSERT_EQUAL(MAGIC, sb->magic);
    TEST_ASSERT_EQUAL(INITIAL_PAGES, sb->total_pages);

    bank_close(&bank);

    // Verify file size
    struct stat file_stat;
    stat(TEST_DB_PATH, &file_stat);
    TEST_ASSERT_EQUAL(PAGE_SIZE * INITIAL_PAGES, file_stat.st_size);
}

void test_bank_open_existing(void) {
    struct PageBank bank;
    // Create a new db file first
    int ret = bank_open(&bank, TEST_DB_PATH);
    TEST_ASSERT_EQUAL(0, ret);
    bank.curr_dblk = 123;
    bank_close(&bank);

    // Now open the existing file
    struct PageBank bank2;
    ret = bank_open(&bank2, TEST_DB_PATH);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_NOT_EQUAL(-1, bank2.fd);
    TEST_ASSERT_NOT_NULL(bank2.pages);

    struct Superblock *sb = get_superblock(&bank2);
    TEST_ASSERT_EQUAL(MAGIC, sb->magic);
    TEST_ASSERT_EQUAL(VERSION, sb->version);
    TEST_ASSERT_EQUAL(PAGE_SIZE, sb->page_size);
    TEST_ASSERT_EQUAL(INITIAL_PAGES, sb->total_pages);
    TEST_ASSERT_EQUAL(123, bank2.curr_dblk); // Check if value was persisted

    bank_close(&bank2);
}

void test_alloc_page_finds_first_free_page(void) {
    struct PageBank bank;
    bank_create(&bank, -1);

    struct Superblock *sb = get_superblock(&bank);
    u32 expected_page = sb->fst_free_page + 1;

    u32 page_num = alloc_page(&bank);
    TEST_ASSERT_EQUAL(expected_page, page_num);
    TEST_ASSERT_TRUE(is_page_set(&bank, page_num));

    bank_close(&bank);
}

void test_alloc_page_expands_bank(void) {
    struct PageBank bank;
    bank_create(&bank, -1);

    struct Superblock *sb = get_superblock(&bank);
    u32 initial_total_pages = sb->total_pages;
    u64 initial_size = bank.size;

    // Now, allocate one more page, which should trigger expansion
    u32 new_page = alloc_page(&bank);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, new_page);
    TEST_ASSERT_TRUE(is_page_set(&bank, new_page));

    sb = get_superblock(&bank); // Re-fetch after potential realloc
    TEST_ASSERT_GREATER_THAN(initial_total_pages, sb->total_pages); // total_pages should have increased
    TEST_ASSERT_GREATER_THAN(initial_size, bank.size); // bank.size should have increased

    bank_close(&bank);
}

void test_resize_in_memory(void) {
    struct PageBank bank;
    bank_create(&bank, -1);
    u64 old_size = bank.size;
    u64 new_size = old_size + 10 * PAGE_SIZE;

    int ret = resize(&bank, new_size);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_EQUAL(new_size, bank.size);

    // Test writing to the new memory region
    char *last_byte = (char *) bank.pages + new_size - 1;
    *last_byte = 'a';
    TEST_ASSERT_EQUAL('a', *last_byte);

    bank_close(&bank);
}

void test_bitmap_functions(void) {
    struct PageBank bank;
    bank_create(&bank, -1);

    u32 page_num = 100;
    TEST_ASSERT_FALSE(is_page_set(&bank, page_num));

    set_page(&bank, page_num);
    TEST_ASSERT_TRUE(is_page_set(&bank, page_num));

    unset_page(&bank, page_num);
    TEST_ASSERT_FALSE(is_page_set(&bank, page_num));

    bank_close(&bank);
}


int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_bank_create_in_memory);
    RUN_TEST(test_bank_create_file);
    RUN_TEST(test_bank_open_existing);
    RUN_TEST(test_alloc_page_finds_first_free_page);
    RUN_TEST(test_alloc_page_expands_bank);
    RUN_TEST(test_resize_in_memory);
    RUN_TEST(test_bitmap_functions);
    return UNITY_END();
}
