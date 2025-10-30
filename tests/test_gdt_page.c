#include "gdt_page.h"

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "unity.h"
#include "utils.h"

static const char *TEST_DB_PATH = "test_gdt_page.db";

void setUp(void) {
    // Executed before each test case
    remove(TEST_DB_PATH);
}

void tearDown(void) {
    // Executed after each test case
    remove(TEST_DB_PATH);
}

void test_gdt_bank_create_in_memory(void) {
    struct GdtPageBank bank;
    int ret = gdt_bank_create(&bank, -1);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_EQUAL(-1, bank.fd);
    TEST_ASSERT_NOT_NULL(bank.pages);
    TEST_ASSERT_EQUAL(PAGE_SIZE * INITIAL_PAGES, bank.size);

    struct GdtSuperblock *sb = gdt_get_superblock(&bank);
    TEST_ASSERT_EQUAL(MAGIC, sb->magic);
    TEST_ASSERT_EQUAL(VERSION, sb->version);
    TEST_ASSERT_EQUAL(MAX_GDTS, sb->gdt_pages);
    TEST_ASSERT_EQUAL(INITIAL_PAGES, sb->total_pages);
    TEST_ASSERT_EQUAL(1, sb->total_groups);
    TEST_ASSERT_EQUAL(PAGE_SIZE, sb->page_size);

    struct GroupDescriptor *gdt = get_gdt(&bank);
    TEST_ASSERT_EQUAL(HEAD_OFFSET, gdt[0].group_start);
    TEST_ASSERT_EQUAL(GROUP_SIZE - GROUP_BITMAPS, gdt[0].free_pages);
    TEST_ASSERT_EQUAL(INVALID_PAGE, gdt[1].group_start);

    // Check if the group's bitmap pages are marked as used in the bitmap
    TEST_ASSERT_TRUE(gdt_is_page_set(&bank, HEAD_OFFSET));
    TEST_ASSERT_TRUE(gdt_is_page_set(&bank, HEAD_OFFSET + 1));

    gdt_bank_close(&bank);
}

void test_gdt_bank_create_file(void) {
    struct GdtPageBank bank;
    int fd = open(TEST_DB_PATH, O_RDWR | O_CREAT | O_TRUNC, 0644);
    TEST_ASSERT_GREATER_OR_EQUAL(0, fd);

    int ret = gdt_bank_create(&bank, fd);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_NOT_EQUAL(-1, bank.fd);
    TEST_ASSERT_NOT_NULL(bank.pages);

    struct GdtSuperblock *sb = gdt_get_superblock(&bank);
    TEST_ASSERT_EQUAL(MAGIC, sb->magic);
    TEST_ASSERT_EQUAL(INITIAL_PAGES, sb->total_pages);

    gdt_bank_close(&bank);

    // Verify file size
    struct stat file_stat;
    stat(TEST_DB_PATH, &file_stat);
    TEST_ASSERT_EQUAL(PAGE_SIZE * INITIAL_PAGES, file_stat.st_size);
}

void test_gdt_bank_open_existing(void) {
    struct GdtPageBank bank;
    // Create a new db file first
    int ret = gdt_bank_open(&bank, TEST_DB_PATH);
    TEST_ASSERT_EQUAL(0, ret);
    gdt_bank_close(&bank);

    // Now open the existing file
    struct GdtPageBank bank2;
    ret = gdt_bank_open(&bank2, TEST_DB_PATH);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_NOT_EQUAL(-1, bank2.fd);
    TEST_ASSERT_NOT_NULL(bank2.pages);

    struct GdtSuperblock *sb = gdt_get_superblock(&bank2);
    TEST_ASSERT_EQUAL(MAGIC, sb->magic);
    TEST_ASSERT_EQUAL(VERSION, sb->version);
    TEST_ASSERT_EQUAL(PAGE_SIZE, sb->page_size);
    TEST_ASSERT_EQUAL(INITIAL_PAGES, sb->total_pages);
    TEST_ASSERT_EQUAL(1, sb->total_groups);

    gdt_bank_close(&bank2);
}

void test_gdt_alloc_page_finds_first_free_page(void) {
    struct GdtPageBank bank;
    gdt_bank_create(&bank, -1);

    // The first free page is after the superblock, GDT, and the group's bitmap pages.
    u32 expected_page = HEAD_OFFSET + GROUP_BITMAPS;

    u32 page_num = gdt_alloc_page(&bank, INVALID_PAGE);
    TEST_ASSERT_EQUAL(expected_page, page_num);
    TEST_ASSERT_TRUE(gdt_is_page_set(&bank, page_num));

    struct GroupDescriptor *gdt = get_gdt(&bank);
    TEST_ASSERT_EQUAL(GROUP_SIZE - GROUP_BITMAPS - 1, gdt[0].free_pages);

    gdt_bank_close(&bank);
}

void test_gdt_bitmap_functions(void) {
    struct GdtPageBank bank;
    gdt_bank_create(&bank, -1);

    u32 page_num = HEAD_OFFSET + 100; // A random data page
    struct GroupDescriptor *gdt = get_gdt(&bank);
    u16 initial_free = gdt[0].free_pages;

    TEST_ASSERT_FALSE(gdt_is_page_set(&bank, page_num));

    gdt_set_page(&bank, page_num);
    TEST_ASSERT_TRUE(gdt_is_page_set(&bank, page_num));
    TEST_ASSERT_EQUAL(initial_free - 1, gdt[0].free_pages);

    gdt_unset_page(&bank, page_num);
    TEST_ASSERT_FALSE(gdt_is_page_set(&bank, page_num));
    TEST_ASSERT_EQUAL(initial_free, gdt[0].free_pages);

    gdt_bank_close(&bank);
}

void test_gdt_alloc_page_expands_bank(void) {
    struct GdtPageBank bank;
    gdt_bank_create(&bank, -1);

    u32 initial_total_pages = gdt_get_superblock(&bank)->total_pages;
    u64 initial_size = bank.size;
    u32 initial_groups = gdt_get_superblock(&bank)->total_groups;

    // Allocate all pages in the first group to force a grow
    for (int i = 0; i < GROUP_SIZE - GROUP_BITMAPS; i++) {
        u32 page = gdt_alloc_page(&bank, INVALID_PAGE);
        TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, page);
    }

    // The next allocation must trigger a grow()
    u32 new_page = gdt_alloc_page(&bank, INVALID_PAGE);
    TEST_ASSERT_NOT_EQUAL(INVALID_PAGE, new_page);
    TEST_ASSERT_TRUE(gdt_is_page_set(&bank, new_page));

    struct GdtSuperblock *sb = gdt_get_superblock(&bank); // Re-fetch after potential realloc
    TEST_ASSERT_EQUAL(initial_groups + 1, sb->total_groups);
    TEST_ASSERT_EQUAL(initial_total_pages + GROUP_SIZE, sb->total_pages);
    TEST_ASSERT_EQUAL(initial_size + (GROUP_SIZE * PAGE_SIZE), bank.size);

    // Check that the new page is the first allocatable page in the new group
    u32 expected_page = HEAD_OFFSET + GROUP_SIZE + GROUP_BITMAPS;
    TEST_ASSERT_EQUAL(expected_page, new_page);

    gdt_bank_close(&bank);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_gdt_bank_create_in_memory);
    RUN_TEST(test_gdt_bank_create_file);
    RUN_TEST(test_gdt_bank_open_existing);
    RUN_TEST(test_gdt_alloc_page_finds_first_free_page);
    RUN_TEST(test_gdt_bitmap_functions);
    RUN_TEST(test_gdt_alloc_page_expands_bank);
    return UNITY_END();
}
