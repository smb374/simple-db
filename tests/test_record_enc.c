#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "alloc.h"
#include "bufpool.h"
#include "catalog.h"
#include "pagestore.h"
#include "record.h"
#include "schema.h"
#include "unity.h"
#include "utils.h"

#define TEST_DB_PATH "test_record_enc.db"
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

// Helper to create a simple schema
static struct MemSchema *create_test_schema(u32 schema_id, u8 ncols) {
    struct MemSchema *schema = calloc(1, sizeof(struct MemSchema));
    schema->header.id = schema_id;
    schema->header.ncols = ncols;
    schema->header.version = 1;
    schema->defs = calloc(ncols, sizeof(struct ColumnDef));
    return schema;
}

static void free_test_schema(struct MemSchema *schema) {
    if (schema) {
        free(schema->defs);
        free(schema);
    }
}

// Test encode/decode with small fixed-size columns
void test_encode_decode_fixed_cols(void) {
    struct MemSchema *schema = create_test_schema(1, 3);
    schema->defs[0].tag = (FLAG_NONE << 4) | TYPE_INTEGER;
    schema->defs[0].size = 8;
    schema->defs[1].tag = (FLAG_NONE << 4) | TYPE_INTEGER;
    schema->defs[1].size = 8;
    schema->defs[2].tag = (FLAG_NONE << 4) | TYPE_REAL;
    schema->defs[2].size = 8;

    // Create record
    struct MemRecord rec = {0};
    rec.header.schema_id = 1;
    rec.header.version = 1;
    rec.header.ncols = 3;
    rec.schema = schema;

    i64 val1 = 42;
    i64 val2 = -100;
    double val3 = 3.14159;

    rec.cols[0] = &val1;
    rec.col_size[0] = 8;
    rec.cols[1] = &val2;
    rec.col_size[1] = 8;
    rec.cols[2] = &val3;
    rec.col_size[2] = 8;

    // Encode
    u8 buf[8192];
    u32 encoded_size;
    i32 ret = record_encode(&rec, buf, 8192, &encoded_size);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_GREATER_THAN(0, encoded_size);

    // Decode
    struct MemRecord *decoded = record_decode(schema, &rec.header, buf, encoded_size);
    TEST_ASSERT_NOT_NULL(decoded);
    TEST_ASSERT_EQUAL_UINT32(1, decoded->header.schema_id);
    TEST_ASSERT_EQUAL_UINT8(3, decoded->header.ncols);

    // Verify values
    TEST_ASSERT_EQUAL_MEMORY(&val1, decoded->cols[0], 8);
    TEST_ASSERT_EQUAL_MEMORY(&val2, decoded->cols[1], 8);
    TEST_ASSERT_EQUAL_MEMORY(&val3, decoded->cols[2], 8);

    free_record(decoded);
    free_test_schema(schema);
}

// Test encode/decode with small variable columns
void test_encode_decode_variable_cols(void) {
    struct MemSchema *schema = create_test_schema(2, 2);
    schema->defs[0].tag = (FLAG_NONE << 4) | TYPE_INTEGER;
    schema->defs[0].size = 8;
    schema->defs[1].tag = (FLAG_NONE << 4) | TYPE_BLOB;
    schema->defs[1].size = 0;

    struct MemRecord rec = {0};
    rec.header.schema_id = 2;
    rec.header.version = 1;
    rec.header.ncols = 2;
    rec.schema = schema;

    i64 id = 123;
    const char *name = "Alice";
    u16 name_len = strlen(name) + 1;

    rec.cols[0] = &id;
    rec.col_size[0] = 8;
    rec.cols[1] = (void *) name;
    rec.col_size[1] = name_len;

    // Encode
    u8 buf[4096];
    u32 encoded_size;
    i32 ret = record_encode(&rec, buf, 4096, &encoded_size);
    TEST_ASSERT_EQUAL(0, ret);

    // Decode
    struct MemRecord *decoded = record_decode(schema, &rec.header, buf, encoded_size);
    TEST_ASSERT_NOT_NULL(decoded);
    TEST_ASSERT_EQUAL_UINT8(2, decoded->header.ncols);

    TEST_ASSERT_EQUAL_MEMORY(&id, decoded->cols[0], 8);
    TEST_ASSERT_EQUAL_STRING(name, (const char *) decoded->cols[1]);

    free_record(decoded);
    free_test_schema(schema);
}

// Test NULL values in columns
void test_encode_decode_with_nulls(void) {
    struct MemSchema *schema = create_test_schema(3, 4);
    schema->defs[0].tag = (FLAG_NONE << 4) | TYPE_INTEGER;
    schema->defs[0].size = 8;
    schema->defs[1].tag = (FLAG_NULL << 4) | TYPE_INTEGER;
    schema->defs[1].size = 8;
    schema->defs[2].tag = (FLAG_NONE << 4) | TYPE_BLOB;
    schema->defs[2].size = 0;
    schema->defs[3].tag = (FLAG_NULL << 4) | TYPE_REAL;
    schema->defs[3].size = 8;

    struct MemRecord rec = {0};
    rec.header.schema_id = 3;
    rec.header.version = 1;
    rec.header.ncols = 4;
    rec.schema = schema;

    i64 val0 = 100;
    const char *val2 = "test";
    u16 val2_len = strlen(val2) + 1;

    rec.cols[0] = &val0;
    rec.col_size[0] = 8;
    rec.cols[1] = NULL; // NULL column
    rec.col_size[1] = 0;
    rec.cols[2] = (void *) val2;
    rec.col_size[2] = val2_len;
    rec.cols[3] = NULL; // NULL column
    rec.col_size[3] = 0;

    // Set null bitmap
    rec.header.null_bitmap[0] = (1 << 1) | (1 << 3); // columns 1 and 3 are NULL

    // Encode
    u8 buf[4096];
    u32 encoded_size;
    i32 ret = record_encode(&rec, buf, 4096, &encoded_size);
    TEST_ASSERT_EQUAL(0, ret);

    // Decode
    struct MemRecord *decoded = record_decode(schema, &rec.header, buf, encoded_size);
    TEST_ASSERT_NOT_NULL(decoded);

    TEST_ASSERT_EQUAL_MEMORY(&val0, decoded->cols[0], 8);
    TEST_ASSERT_NULL(decoded->cols[1]);
    TEST_ASSERT_EQUAL_UINT16(0, decoded->col_size[1]);
    TEST_ASSERT_EQUAL_STRING(val2, (const char *) decoded->cols[2]);
    TEST_ASSERT_NULL(decoded->cols[3]);
    TEST_ASSERT_EQUAL_UINT16(0, decoded->col_size[3]);

    free_record(decoded);
    free_test_schema(schema);
}

// Test overflow column handling
void test_encode_decode_overflow_cols(void) {
    struct MemSchema *schema = create_test_schema(4, 2);
    schema->defs[0].tag = (FLAG_NONE << 4) | TYPE_INTEGER;
    schema->defs[0].size = 8;
    schema->defs[1].tag = (FLAG_NONE << 4) | TYPE_BLOB;
    schema->defs[1].size = 0;

    struct MemRecord rec = {0};
    rec.header.schema_id = 4;
    rec.header.version = 1;
    rec.header.ncols = 2;
    rec.schema = schema;

    i64 id = 999;

    // Create large data that exceeds COL_OVERFLOW_THRES
    u16 large_size = 2048;
    u8 *large_data = malloc(large_size);
    for (u16 i = 0; i < large_size; i++) {
        large_data[i] = (u8) (i % 256);
    }

    // Save a copy for later comparison (overflow destroys original)
    u8 *large_data_copy = malloc(large_size);
    memcpy(large_data_copy, large_data, large_size);

    rec.cols[0] = &id;
    rec.col_size[0] = 8;
    rec.cols[1] = large_data;
    rec.col_size[1] = large_size;

    // Overflow the large column (modifies large_data in-place)
    i32 ret = record_overflow_cols(catalog, &rec);
    TEST_ASSERT_EQUAL(0, ret);

    // Column 1 col_size stays at original size (doesn't change)
    TEST_ASSERT_EQUAL_UINT16(large_size, rec.col_size[1]);

    // Encode
    u8 buf[4096];
    u32 encoded_size;
    ret = record_encode(&rec, buf, 4096, &encoded_size);
    TEST_ASSERT_EQUAL(0, ret);

    // Decode
    struct MemRecord *decoded = record_decode(schema, &rec.header, buf, encoded_size);
    TEST_ASSERT_NOT_NULL(decoded);

    // Column 1 col_size reflects original size (buffer has prefix + VPtr)
    TEST_ASSERT_EQUAL_UINT16(large_size, decoded->col_size[1]);
    TEST_ASSERT_EQUAL_MEMORY(rec.cols[1], decoded->cols[1], COL_PREFIX_SIZE);

    // Recover overflow columns
    ret = record_recover_cols(catalog, decoded);
    TEST_ASSERT_EQUAL(0, ret);

    // col_size unchanged, buffer now has full data
    TEST_ASSERT_EQUAL_UINT16(large_size, decoded->col_size[1]);
    TEST_ASSERT_EQUAL_MEMORY(large_data_copy, decoded->cols[1], large_size);

    free(large_data);
    free(large_data_copy);
    free_record(decoded);
    free_test_schema(schema);
}

// Test multiple overflow columns
void test_multiple_overflow_cols(void) {
    struct MemSchema *schema = create_test_schema(5, 3);
    schema->defs[0].tag = (FLAG_NONE << 4) | TYPE_BLOB;
    schema->defs[0].size = 0;
    schema->defs[1].tag = (FLAG_NONE << 4) | TYPE_INTEGER;
    schema->defs[1].size = 8;
    schema->defs[2].tag = (FLAG_NONE << 4) | TYPE_BLOB;
    schema->defs[2].size = 0;

    struct MemRecord rec = {0};
    rec.header.schema_id = 5;
    rec.header.version = 1;
    rec.header.ncols = 3;
    rec.schema = schema;

    u16 size0 = 1500;
    u8 *data0 = malloc(size0);
    memset(data0, 'A', size0);

    i64 val1 = 42;

    u16 size2 = 3000;
    u8 *data2 = malloc(size2);
    memset(data2, 'B', size2);

    // Save copies for later comparison (overflow destroys originals)
    u8 *data0_copy = malloc(size0);
    memcpy(data0_copy, data0, size0);
    u8 *data2_copy = malloc(size2);
    memcpy(data2_copy, data2, size2);

    rec.cols[0] = data0;
    rec.col_size[0] = size0;
    rec.cols[1] = &val1;
    rec.col_size[1] = 8;
    rec.cols[2] = data2;
    rec.col_size[2] = size2;

    // Overflow (modifies data0 and data2 in-place)
    i32 ret = record_overflow_cols(catalog, &rec);
    TEST_ASSERT_EQUAL(0, ret);

    // col_size stays at original sizes (doesn't change after overflow)
    TEST_ASSERT_EQUAL_UINT16(size0, rec.col_size[0]);
    TEST_ASSERT_EQUAL_UINT16(size2, rec.col_size[2]);

    // Encode
    u8 buf[4096];
    u32 encoded_size;
    ret = record_encode(&rec, buf, 4096, &encoded_size);
    TEST_ASSERT_EQUAL(0, ret);

    // Decode
    struct MemRecord *decoded = record_decode(schema, &rec.header, buf, encoded_size);
    TEST_ASSERT_NOT_NULL(decoded);

    // Recover
    ret = record_recover_cols(catalog, decoded);
    TEST_ASSERT_EQUAL(0, ret);

    TEST_ASSERT_EQUAL_UINT16(size0, decoded->col_size[0]);
    TEST_ASSERT_EQUAL_MEMORY(data0_copy, decoded->cols[0], size0);
    TEST_ASSERT_EQUAL_MEMORY(&val1, decoded->cols[1], 8);
    TEST_ASSERT_EQUAL_UINT16(size2, decoded->col_size[2]);
    TEST_ASSERT_EQUAL_MEMORY(data2_copy, decoded->cols[2], size2);

    free(data0);
    free(data0_copy);
    free(data2);
    free(data2_copy);
    free_record(decoded);
    free_test_schema(schema);
}

// Test edge case: exactly at overflow threshold
void test_overflow_boundary(void) {
    struct MemSchema *schema = create_test_schema(6, 1);
    schema->defs[0].tag = (FLAG_NONE << 4) | TYPE_BLOB;
    schema->defs[0].size = 0;

    struct MemRecord rec = {0};
    rec.header.schema_id = 6;
    rec.header.version = 1;
    rec.header.ncols = 1;
    rec.schema = schema;

    // Exactly at threshold - should NOT overflow
    u16 size = COL_OVERFLOW_THRES;
    u8 *data = malloc(size);
    memset(data, 'X', size);

    rec.cols[0] = data;
    rec.col_size[0] = size;

    i32 ret = record_overflow_cols(catalog, &rec);
    TEST_ASSERT_EQUAL(0, ret);

    // Should not be overflowed
    TEST_ASSERT_EQUAL_UINT16(size, rec.col_size[0]);

    // Encode and decode
    u8 buf[4096];
    u32 encoded_size;
    ret = record_encode(&rec, buf, 4096, &encoded_size);
    TEST_ASSERT_EQUAL(0, ret);

    struct MemRecord *decoded = record_decode(schema, &rec.header, buf, encoded_size);
    TEST_ASSERT_NOT_NULL(decoded);

    TEST_ASSERT_EQUAL_UINT16(size, decoded->col_size[0]);
    TEST_ASSERT_EQUAL_MEMORY(data, decoded->cols[0], size);

    free(data);
    free_record(decoded);
    free_test_schema(schema);
}

// Test edge case: one byte over threshold
void test_overflow_one_over(void) {
    struct MemSchema *schema = create_test_schema(7, 1);
    schema->defs[0].tag = (FLAG_NONE << 4) | TYPE_BLOB;
    schema->defs[0].size = 0;

    struct MemRecord rec = {0};
    rec.header.schema_id = 7;
    rec.header.version = 1;
    rec.header.ncols = 1;
    rec.schema = schema;

    // One byte over - should overflow
    u16 size = COL_OVERFLOW_THRES + 1;
    u8 *data = malloc(size);
    for (u16 i = 0; i < size; i++) {
        data[i] = (u8) (i % 256);
    }

    // Save a copy for later comparison (overflow destroys original)
    u8 *data_copy = malloc(size);
    memcpy(data_copy, data, size);

    rec.cols[0] = data;
    rec.col_size[0] = size;

    i32 ret = record_overflow_cols(catalog, &rec);
    TEST_ASSERT_EQUAL(0, ret);

    // col_size stays at original size (doesn't change after overflow)
    TEST_ASSERT_EQUAL_UINT16(size, rec.col_size[0]);

    // Encode and decode
    u8 buf[4096];
    u32 encoded_size;
    ret = record_encode(&rec, buf, 4096, &encoded_size);
    TEST_ASSERT_EQUAL(0, ret);

    struct MemRecord *decoded = record_decode(schema, &rec.header, buf, encoded_size);
    TEST_ASSERT_NOT_NULL(decoded);

    // Recover
    ret = record_recover_cols(catalog, decoded);
    TEST_ASSERT_EQUAL(0, ret);

    TEST_ASSERT_EQUAL_UINT16(size, decoded->col_size[0]);
    TEST_ASSERT_EQUAL_MEMORY(data_copy, decoded->cols[0], size);

    free(data);
    free(data_copy);
    free_record(decoded);
    free_test_schema(schema);
}

// Test insufficient buffer for encode
void test_encode_insufficient_buffer(void) {
    struct MemSchema *schema = create_test_schema(8, 1);
    schema->defs[0].tag = (FLAG_NONE << 4) | TYPE_BLOB;
    schema->defs[0].size = 0;

    struct MemRecord rec = {0};
    rec.header.schema_id = 8;
    rec.header.version = 1;
    rec.header.ncols = 1;
    rec.schema = schema;

    u16 size = 500;
    u8 *data = malloc(size);
    memset(data, 'Z', size);

    rec.cols[0] = data;
    rec.col_size[0] = size;

    // Try to encode with tiny buffer
    u8 buf[32];
    u32 encoded_size;
    i32 ret = record_encode(&rec, buf, 32, &encoded_size);
    TEST_ASSERT_EQUAL(-1, ret);

    free(data);
    free_test_schema(schema);
}

// Test empty record (0 columns)
void test_encode_decode_empty_record(void) {
    struct MemSchema *schema = create_test_schema(9, 0);

    struct MemRecord rec = {0};
    rec.header.schema_id = 9;
    rec.header.version = 1;
    rec.header.ncols = 0;
    rec.schema = schema;

    // Encode
    u8 buf[4096];
    u32 encoded_size;
    i32 ret = record_encode(&rec, buf, 4096, &encoded_size);
    TEST_ASSERT_EQUAL(0, ret);
    TEST_ASSERT_EQUAL(0, encoded_size);

    // Decode
    struct MemRecord *decoded = record_decode(schema, &rec.header, buf, encoded_size);
    TEST_ASSERT_NOT_NULL(decoded);
    TEST_ASSERT_EQUAL_UINT8(0, decoded->header.ncols);

    free_record(decoded);
    free_test_schema(schema);
}

// Test many columns
void test_encode_decode_many_columns(void) {
    u8 ncols = 64;
    struct MemSchema *schema = create_test_schema(10, ncols);

    for (u8 i = 0; i < ncols; i++) {
        schema->defs[i].tag = (FLAG_NONE << 4) | TYPE_INTEGER;
        schema->defs[i].size = 8;
    }

    struct MemRecord rec = {0};
    rec.header.schema_id = 10;
    rec.header.version = 1;
    rec.header.ncols = ncols;
    rec.schema = schema;

    i64 values[64];
    for (u8 i = 0; i < ncols; i++) {
        values[i] = i * 100;
        rec.cols[i] = &values[i];
        rec.col_size[i] = 8;
    }

    // Encode
    u8 buf[8192];
    u32 encoded_size;
    i32 ret = record_encode(&rec, buf, 8192, &encoded_size);
    TEST_ASSERT_EQUAL(0, ret);

    // Decode
    struct MemRecord *decoded = record_decode(schema, &rec.header, buf, encoded_size);
    TEST_ASSERT_NOT_NULL(decoded);
    TEST_ASSERT_EQUAL_UINT8(ncols, decoded->header.ncols);

    for (u8 i = 0; i < ncols; i++) {
        TEST_ASSERT_EQUAL_MEMORY(&values[i], decoded->cols[i], 8);
    }

    free_record(decoded);
    free_test_schema(schema);
}

// Test round-trip with persistence
void test_roundtrip_with_persistence(void) {
    struct MemSchema *schema = create_test_schema(11, 2);
    schema->defs[0].tag = (FLAG_NONE << 4) | TYPE_INTEGER;
    schema->defs[0].size = 8;
    schema->defs[1].tag = (FLAG_NONE << 4) | TYPE_BLOB;
    schema->defs[1].size = 0;

    struct MemRecord rec = {0};
    rec.header.schema_id = 11;
    rec.header.version = 1;
    rec.header.ncols = 2;
    rec.schema = schema;

    i64 id = 777;
    u16 data_size = 2000;
    u8 *data = malloc(data_size);
    for (u16 i = 0; i < data_size; i++) {
        data[i] = (u8) ((i * 7) % 256);
    }

    // Save a copy for later comparison (overflow destroys original)
    u8 *data_copy = malloc(data_size);
    memcpy(data_copy, data, data_size);

    rec.cols[0] = &id;
    rec.col_size[0] = 8;
    rec.cols[1] = data;
    rec.col_size[1] = data_size;

    // Overflow (modifies data in-place)
    i32 ret = record_overflow_cols(catalog, &rec);
    TEST_ASSERT_EQUAL(0, ret);

    // Encode
    u8 buf[4096];
    u32 encoded_size;
    ret = record_encode(&rec, buf, 4096, &encoded_size);
    TEST_ASSERT_EQUAL(0, ret);

    // Close and reopen catalog
    catalog_close(catalog);
    catalog = catalog_open(alloc);
    TEST_ASSERT_NOT_NULL(catalog);

    // Decode
    struct MemRecord *decoded = record_decode(schema, &rec.header, buf, encoded_size);
    TEST_ASSERT_NOT_NULL(decoded);

    // Recover - should work after reopen
    ret = record_recover_cols(catalog, decoded);
    TEST_ASSERT_EQUAL(0, ret);

    TEST_ASSERT_EQUAL_UINT16(data_size, decoded->col_size[1]);
    TEST_ASSERT_EQUAL_MEMORY(data_copy, decoded->cols[1], data_size);

    free(data);
    free(data_copy);
    free_record(decoded);
    free_test_schema(schema);
}

int main(void) {
    if (suite_setUp() != 0) {
        return -1;
    }

    UNITY_BEGIN();
    RUN_TEST(test_encode_decode_fixed_cols);
    RUN_TEST(test_encode_decode_variable_cols);
    RUN_TEST(test_encode_decode_with_nulls);
    RUN_TEST(test_encode_decode_overflow_cols);
    RUN_TEST(test_multiple_overflow_cols);
    RUN_TEST(test_overflow_boundary);
    RUN_TEST(test_overflow_one_over);
    RUN_TEST(test_encode_insufficient_buffer);
    RUN_TEST(test_encode_decode_empty_record);
    RUN_TEST(test_encode_decode_many_columns);
    RUN_TEST(test_roundtrip_with_persistence);
    return suite_tearDown() + UNITY_END();
}
