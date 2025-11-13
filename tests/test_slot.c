#include "unity.h"
#include "slot.h"
#include "pagestore.h"
#include <string.h>

// Test page buffer
static u8 page[PAGE_SIZE];
static struct SlotHeap *sh;

int suite_setUp(void) {
    return 0;
}

int suite_tearDown(void) {
    return 0;
}

void setUp(void) {
    memset(page, 0, PAGE_SIZE);
    sh = (struct SlotHeap *)page;
    slot_init(sh, 0);
}

void tearDown(void) {}

// Test basic initialization
void test_slot_init(void) {
    TEST_ASSERT_EQUAL_UINT16(0, sh->start);
    TEST_ASSERT_EQUAL_UINT16(0, sh->nslots);
    TEST_ASSERT_EQUAL_UINT16(PAGE_SIZE, sh->free_offset);
    TEST_ASSERT_EQUAL_UINT16(0, sh->frag_bytes);
}

// Test single allocation
void test_slot_alloc_single(void) {
    u16 slot = slot_alloc(sh, 100);

    TEST_ASSERT_NOT_EQUAL(INVALID_SLOT, slot);
    TEST_ASSERT_EQUAL_UINT16(0, slot);
    TEST_ASSERT_EQUAL_UINT16(1, sh->nslots);

    struct Cell *cell = slot_get(sh, slot);
    TEST_ASSERT_NOT_NULL(cell);
    TEST_ASSERT_EQUAL_UINT16(100, cell->size);
}

// Test multiple allocations
void test_slot_alloc_multiple(void) {
    u16 slot0 = slot_alloc(sh, 50);
    u16 slot1 = slot_alloc(sh, 100);
    u16 slot2 = slot_alloc(sh, 150);

    TEST_ASSERT_EQUAL_UINT16(0, slot0);
    TEST_ASSERT_EQUAL_UINT16(1, slot1);
    TEST_ASSERT_EQUAL_UINT16(2, slot2);
    TEST_ASSERT_EQUAL_UINT16(3, sh->nslots);

    struct Cell *cell0 = slot_get(sh, slot0);
    struct Cell *cell1 = slot_get(sh, slot1);
    struct Cell *cell2 = slot_get(sh, slot2);

    TEST_ASSERT_EQUAL_UINT16(50, cell0->size);
    TEST_ASSERT_EQUAL_UINT16(100, cell1->size);
    TEST_ASSERT_EQUAL_UINT16(150, cell2->size);
}

// Test writing and reading data
void test_slot_data_roundtrip(void) {
    const char *test_data = "Hello, SlotHeap!";
    u16 data_len = strlen(test_data);

    u16 slot = slot_alloc(sh, data_len);
    TEST_ASSERT_NOT_EQUAL(INVALID_SLOT, slot);

    struct Cell *cell = slot_get(sh, slot);
    TEST_ASSERT_NOT_NULL(cell);

    memcpy(cell->data, test_data, data_len);

    // Read back
    struct Cell *read_cell = slot_get(sh, slot);
    TEST_ASSERT_EQUAL_MEMORY(test_data, read_cell->data, data_len);
}

// Test slot_free
void test_slot_free_single(void) {
    u16 slot = slot_alloc(sh, 100);
    TEST_ASSERT_NOT_EQUAL(INVALID_SLOT, slot);

    slot_free(sh, slot);

    // Slot should be marked invalid
    TEST_ASSERT_EQUAL_UINT16(INVALID_SLOT, sh->slots[slot]);
    TEST_ASSERT_EQUAL_UINT16(102, sh->frag_bytes); // size + 2 bytes for Cell header

    // Getting freed slot should return NULL
    struct Cell *cell = slot_get(sh, slot);
    TEST_ASSERT_NULL(cell);
}

// Test slot reuse after free
void test_slot_reuse_after_free(void) {
    u16 slot0 = slot_alloc(sh, 50);
    u16 slot1 = slot_alloc(sh, 100);
    u16 slot2 = slot_alloc(sh, 75);

    TEST_ASSERT_EQUAL_UINT16(3, sh->nslots);

    // Free slot 1
    slot_free(sh, slot1);

    // Allocate new slot - should reuse slot 1's index
    u16 slot3 = slot_alloc(sh, 80);
    TEST_ASSERT_EQUAL_UINT16(1, slot3); // Reused slot 1's index
    TEST_ASSERT_EQUAL_UINT16(3, sh->nslots); // nslots doesn't increase

    // Verify all slots
    TEST_ASSERT_NOT_NULL(slot_get(sh, slot0));
    TEST_ASSERT_NOT_NULL(slot_get(sh, slot3));
    TEST_ASSERT_NOT_NULL(slot_get(sh, slot2));
}

// Test defragmentation
void test_slot_defrag(void) {
    // Allocate several slots
    u16 slot0 = slot_alloc(sh, 100);
    u16 slot1 = slot_alloc(sh, 200);
    u16 slot2 = slot_alloc(sh, 150);

    // Write data
    struct Cell *cell0 = slot_get(sh, slot0);
    memset(cell0->data, 'A', 100);

    struct Cell *cell1 = slot_get(sh, slot1);
    memset(cell1->data, 'B', 200);

    struct Cell *cell2 = slot_get(sh, slot2);
    memset(cell2->data, 'C', 150);

    // Free middle slot
    slot_free(sh, slot1);
    TEST_ASSERT_EQUAL_UINT16(202, sh->frag_bytes);

    // Trigger defrag
    slot_defrag(sh);

    // After defrag, fragmentation should be cleared
    TEST_ASSERT_EQUAL_UINT16(0, sh->frag_bytes);

    // Data should still be intact
    cell0 = slot_get(sh, slot0);
    cell2 = slot_get(sh, slot2);

    TEST_ASSERT_NOT_NULL(cell0);
    TEST_ASSERT_NOT_NULL(cell2);
    TEST_ASSERT_EQUAL_UINT16(100, cell0->size);
    TEST_ASSERT_EQUAL_UINT16(150, cell2->size);

    // Verify data content
    u8 expected_a[100];
    u8 expected_c[150];
    memset(expected_a, 'A', 100);
    memset(expected_c, 'C', 150);

    TEST_ASSERT_EQUAL_MEMORY(expected_a, cell0->data, 100);
    TEST_ASSERT_EQUAL_MEMORY(expected_c, cell2->data, 150);

    // Slot 1 should still be invalid
    TEST_ASSERT_NULL(slot_get(sh, slot1));
}

// Test automatic defrag trigger
void test_auto_defrag_on_fragmentation(void) {
    // Fill page with many small allocations
    u16 slots[20];
    for (i32 i = 0; i < 20; i++) {
        slots[i] = slot_alloc(sh, 100);
        TEST_ASSERT_NOT_EQUAL(INVALID_SLOT, slots[i]);
    }

    // Free every other slot to create fragmentation
    for (i32 i = 0; i < 20; i += 2) {
        slot_free(sh, slots[i]);
    }

    // frag_bytes should be significant
    u16 frag_before = sh->frag_bytes;
    TEST_ASSERT_GREATER_THAN(0, frag_before);

    // If fragmentation > PAGE_SIZE/4, next alloc should trigger defrag
    if (frag_before >= PAGE_SIZE / 4) {
        u16 new_slot = slot_alloc(sh, 50);
        TEST_ASSERT_NOT_EQUAL(INVALID_SLOT, new_slot);
        TEST_ASSERT_EQUAL_UINT16(0, sh->frag_bytes); // Should be defragged
    }
}

// Test allocation failure when full
void test_slot_alloc_full_page(void) {
    // Try to allocate most of the page
    u16 large_size = PAGE_SIZE - sizeof(struct SlotHeap) - 10;
    u16 slot = slot_alloc(sh, large_size);

    // First allocation should succeed
    TEST_ASSERT_NOT_EQUAL(INVALID_SLOT, slot);

    // Second allocation should fail (not enough space)
    u16 slot2 = slot_alloc(sh, 100);
    TEST_ASSERT_EQUAL(INVALID_SLOT, slot2);
}

// Test edge case: zero-size allocation
void test_slot_alloc_zero_size(void) {
    u16 slot = slot_alloc(sh, 0);

    TEST_ASSERT_NOT_EQUAL(INVALID_SLOT, slot);

    struct Cell *cell = slot_get(sh, slot);
    TEST_ASSERT_NOT_NULL(cell);
    TEST_ASSERT_EQUAL_UINT16(0, cell->size);
}

// Test slot_get with invalid slot index
void test_slot_get_invalid(void) {
    // Get non-existent slot
    struct Cell *cell = slot_get(sh, 999);
    TEST_ASSERT_NULL(cell);

    // Allocate one slot
    u16 slot = slot_alloc(sh, 50);

    // Get beyond allocated slots
    cell = slot_get(sh, slot + 10);
    TEST_ASSERT_NULL(cell);
}

// Test slot_free with invalid slot index
void test_slot_free_invalid(void) {
    // Free non-existent slot (should not crash)
    slot_free(sh, 999);

    // State should be unchanged
    TEST_ASSERT_EQUAL_UINT16(0, sh->nslots);
    TEST_ASSERT_EQUAL_UINT16(0, sh->frag_bytes);
}

// Test SlotHeap with non-zero start offset
void test_slot_with_nonzero_start(void) {
    // Simulate page with 16-byte header
    u8 page_with_header[PAGE_SIZE];
    memset(page_with_header, 0, PAGE_SIZE);

    u16 header_size = 16;
    struct SlotHeap *sh_offset = (struct SlotHeap *)(page_with_header + header_size);
    slot_init(sh_offset, header_size);

    TEST_ASSERT_EQUAL_UINT16(header_size, sh_offset->start);

    // Allocate and verify
    u16 slot = slot_alloc(sh_offset, 100);
    TEST_ASSERT_NOT_EQUAL(INVALID_SLOT, slot);

    struct Cell *cell = slot_get(sh_offset, slot);
    TEST_ASSERT_NOT_NULL(cell);
    TEST_ASSERT_EQUAL_UINT16(100, cell->size);

    // Verify cell is within page bounds
    u8 *cell_ptr = (u8 *)cell;
    TEST_ASSERT_GREATER_OR_EQUAL(page_with_header, cell_ptr);
    TEST_ASSERT_LESS_THAN(page_with_header + PAGE_SIZE, cell_ptr);
}

// Test many allocations and frees
void test_slot_stress_alloc_free(void) {
    u16 slots[50];

    // Allocate 50 slots
    for (i32 i = 0; i < 50; i++) {
        slots[i] = slot_alloc(sh, 20 + i);
        if (slots[i] == INVALID_SLOT) {
            break; // Page full
        }
    }

    u16 allocated_count = sh->nslots;
    TEST_ASSERT_GREATER_THAN(0, allocated_count);

    // Free all slots
    for (i32 i = 0; i < allocated_count; i++) {
        slot_free(sh, slots[i]);
    }

    // All should be freed
    for (i32 i = 0; i < allocated_count; i++) {
        TEST_ASSERT_NULL(slot_get(sh, slots[i]));
    }

    // Defrag to compact
    slot_defrag(sh);
    TEST_ASSERT_EQUAL_UINT16(0, sh->frag_bytes);

    // Should be able to allocate again
    u16 new_slot = slot_alloc(sh, 100);
    TEST_ASSERT_NOT_EQUAL(INVALID_SLOT, new_slot);
}

int main(void) {
    if (suite_setUp() != 0) {
        return -1;
    }

    UNITY_BEGIN();

    // Basic operations
    RUN_TEST(test_slot_init);
    RUN_TEST(test_slot_alloc_single);
    RUN_TEST(test_slot_alloc_multiple);
    RUN_TEST(test_slot_data_roundtrip);

    // Free and reuse
    RUN_TEST(test_slot_free_single);
    RUN_TEST(test_slot_reuse_after_free);

    // Defragmentation
    RUN_TEST(test_slot_defrag);
    RUN_TEST(test_auto_defrag_on_fragmentation);

    // Edge cases
    RUN_TEST(test_slot_alloc_full_page);
    RUN_TEST(test_slot_alloc_zero_size);
    RUN_TEST(test_slot_get_invalid);
    RUN_TEST(test_slot_free_invalid);

    // Advanced scenarios
    RUN_TEST(test_slot_with_nonzero_start);
    RUN_TEST(test_slot_stress_alloc_free);

    return suite_tearDown() + UNITY_END();
}
