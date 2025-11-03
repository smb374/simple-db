#include "schema.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <threads.h>

#include "btree.h"
#include "gdt_page.h"
#include "type.h"
#include "utils.h"

i32 ssb_init(struct StaticSchemaBuilder *builder, u8 type) {
    switch (type) {
        case S_TABLE:
        case S_INDEX:
            builder->type = type;
            break;
        default:
            return SSB_UNKNOWN_SCHEMA_TYPE;
    }
    return 0;
}

i32 ssb_add_column(struct StaticSchemaBuilder *builder, const char *name, u8 type, u8 uniq, u8 size) {
    u8 idx = builder->num_cols;
    if (idx >= MAX_COLUMN) {
        return SSB_TOO_MUCH_COLS;
    }

    u8 name_len = strnlen(name, MAX_NAME + 16);
    if (name_len > MAX_NAME) {
        return SSB_COL_NAME_TOO_LONG;
    }

    switch (type) {
        case TYPE_INTEGER:
        case TYPE_REAL:
            size = 8;
        case TYPE_BLOB:
        case TYPE_TEXT:
            break;
        default:
            return SSB_COL_UNKNOWN_TYPE;
    }

    switch (uniq) {
        case KEY_NONE:
        case KEY_UNIQ:
            break;
        case KEY_PRIM:
            if (size == 0) {
                return SSB_KEY_SIZE_REQUIRED;
            }
            if (size > MAX_KEY) {
                return SSB_KEY_TOO_LONG;
            }
            break;
        default:
            return SSB_COL_UNKNOWN_UNIQ;
    }

    memset(&builder->cols[idx], 0, sizeof(struct ColumnDef));
    memcpy(builder->cols[idx].name, name, name_len);
    builder->cols[idx].type = type;
    builder->cols[idx].uniq = uniq;
    builder->cols[idx].size_hint = size;

    builder->num_cols++;

    return SSB_OK;
}

i32 ssb_finalize(struct StaticSchema *s, const struct StaticSchemaBuilder *builder) {
    if (!builder->name) {
        return SSB_NO_NAME;
    }
    if (!builder->table_name) {
        return SSB_NO_TABLE_NAME;
    }
    u8 name_len = strnlen(builder->name, MAX_NAME + 16);
    u8 table_name_len = strnlen(builder->table_name, MAX_NAME + 16);
    if (name_len > MAX_NAME) {
        return SSB_NAME_TOO_LONG;
    }
    if (table_name_len > MAX_NAME) {
        return SSB_TABLE_NAME_TOO_LONG;
    }

    memset(s, 0, sizeof(struct StaticSchema));
    s->type = builder->type;
    s->num_cols = builder->num_cols;
    s->prim_col = 0xFF;
    memcpy(s->name, builder->name, name_len);
    memcpy(s->table_name, builder->table_name, table_name_len);

    for (u8 i = 0; i < s->num_cols; i++) {
        memcpy(&s->cols[i], &builder->cols[i], sizeof(struct ColumnDef));
        if (s->cols[i].uniq == KEY_PRIM) {
            if (s->prim_col != 0xFF) {
                return SSB_DUPLICATE_PRIM;
            } else {
                s->prim_col = i;
            }
        }
    }

    if (s->prim_col == 0xFF) {
        return SSB_NO_PRIM;
    }

    return SSB_OK;
}

i32 init_schema_tree(struct GdtPageBank *bank) { return btree_create_known_root(NULL, bank, SCHEMA_PAGE); }

i32 create_tree(struct IndexHandle *handle, struct GdtPageBank *bank, const struct StaticSchema *s) {
    if (!handle || !bank || !s) {
        return -1;
    }

    handle->schema = calloc(1, sizeof(struct StaticSchema));
    if (!handle->schema) {
        return -1;
    }

    // Try to allocate a new node in bank and saves the root_page in handle
    if (btree_create_root(&handle->th, bank) < 0) {
        return -1;
    }

    memcpy(handle->schema, s, sizeof(struct StaticSchema));
    handle->schema->root_page = handle->th.root_page;

    // Try to insert static schema to the schema tree
    struct BTreeHandle shandle = {bank, SCHEMA_PAGE};
    if (btree_insert(&shandle, (const u8 *) handle->schema->name, handle->schema, sizeof(struct StaticSchema)) < 0) {
        gdt_unset_page(bank, handle->th.root_page);
        handle->th.root_page = INVALID_PAGE;
        free(handle->schema);
        handle->schema = NULL;
        return -1;
    }

    handle->type = s->type;
    handle->key_idx = s->prim_col;
    handle->sversion = 0;
    handle->spage = INVALID_PAGE;
    handle->cache_valid = false;

    return 0;
}

i32 open_tree(struct IndexHandle *handle, struct GdtPageBank *bank, const char *name) {
    if (!handle || !bank || !name) {
        return -1;
    }

    memset(handle, 0, sizeof(struct IndexHandle));
    handle->th.bank = bank;

    // Allocate schema storage
    handle->schema = calloc(1, sizeof(struct StaticSchema));
    if (!handle->schema) {
        return -1;
    }

    // Look up schema by name
    u8 key[MAX_KEY];
    encode_blob_key(key, (u8 *) name, strnlen(name, MAX_KEY));

    struct BTreeHandle shandle = {bank, SCHEMA_PAGE};
    u32 parent_stack[MAX_HEIGHT];
    u16 stack_top = 0;
    u32 leaf_page = btree_find_leaf(&shandle, SCHEMA_PAGE, key, parent_stack, &stack_top);

    if (leaf_page == INVALID_PAGE) {
        free(handle->schema);
        handle->schema = NULL;
        return -1;
    }

    struct TreeNode *leaf = gdt_get_page(bank, leaf_page);
    bool exact_match = false;
    u8 slot = slotted_binary_search(leaf, key, &exact_match);

    if (!exact_match || slot >= leaf->nkeys) {
        free(handle->schema);
        handle->schema = NULL;
        return -1;
    }

    // Read schema
    struct LeafEnt *ent = TN_GET_LENT(leaf, slot);
    u32 len;
    if (read_leafval(&shandle, &ent->val, handle->schema, &len) < 0) {
        free(handle->schema);
        handle->schema = NULL;
        return -1;
    }

    // Initialize handle
    handle->type = handle->schema->type;
    handle->key_idx = handle->schema->prim_col;
    handle->sversion = leaf->version;
    handle->spage = leaf_page;
    handle->cache_valid = true;
    handle->th.root_page = handle->schema->root_page;

    return 0;
}

void close_tree(struct IndexHandle *handle) {
    if (handle && handle->schema) {
        free(handle->schema);
        handle->schema = NULL;
    }
    handle->cache_valid = false;
}

// Validate cache (fast path)
i32 validate_tree_cache(struct IndexHandle *handle) {
    if (!handle || !handle->cache_valid) {
        return -1;
    }

    if (handle->spage == INVALID_PAGE) {
        // Never had a valid cache
        return -1;
    }

    struct TreeNode *leaf = gdt_get_page(handle->th.bank, handle->spage);
    if (!leaf) {
        handle->cache_valid = false;
        return -1;
    }

    if (leaf->version == handle->sversion) {
        return 0; // Cache still valid
    }

    // Try fast path: search within same leaf
    u8 key[MAX_KEY];
    encode_blob_key(key, (const u8 *) handle->schema->name, strnlen(handle->schema->name, MAX_KEY));

    bool exact_match = false;
    u8 slot = slotted_binary_search(leaf, key, &exact_match);

    if (exact_match && slot < leaf->nkeys) {
        // Found in same leaf, update cache
        struct BTreeHandle shandle = {handle->th.bank, SCHEMA_PAGE};
        struct LeafEnt *ent = TN_GET_LENT(leaf, slot);
        u32 len;
        if (read_leafval(&shandle, &ent->val, handle->schema, &len) < 0) {
            handle->cache_valid = false;
            return -1;
        }
        handle->sversion = leaf->version;
        handle->th.root_page = handle->schema->root_page;
        return 0;
    }

    // Not in same leaf - need full refresh
    handle->cache_valid = false;
    return -1;
}

// Full refresh (slow path)
i32 refresh_tree_cache(struct IndexHandle *handle) {
    if (!handle || !handle->schema) {
        return -1;
    }

    // Do full lookup
    u8 key[MAX_KEY];
    encode_blob_key(key, (const u8 *) handle->schema->name, strnlen(handle->schema->name, MAX_KEY));

    struct BTreeHandle shandle = {handle->th.bank, SCHEMA_PAGE};
    u32 parent_stack[MAX_HEIGHT];
    u16 stack_top = 0;
    u32 leaf_page = btree_find_leaf(&shandle, SCHEMA_PAGE, key, parent_stack, &stack_top);

    if (leaf_page == INVALID_PAGE) {
        return -1;
    }

    struct TreeNode *leaf = gdt_get_page(handle->th.bank, leaf_page);
    bool exact_match = false;
    u8 slot = slotted_binary_search(leaf, key, &exact_match);

    if (!exact_match || slot >= leaf->nkeys) {
        return -1;
    }

    // Read schema
    struct LeafEnt *ent = TN_GET_LENT(leaf, slot);
    u32 len;
    if (read_leafval(&shandle, &ent->val, handle->schema, &len) < 0) {
        return -1;
    }

    // Update cache info
    handle->sversion = leaf->version;
    handle->spage = leaf_page;
    handle->cache_valid = true;
    handle->th.root_page = handle->schema->root_page;

    return 0;
}

i32 encode_record(struct RecordEnc *enc, const struct RecordColumn *cols, const u8 n_cols) {
    if (!enc || !enc->handle || !cols) {
        return ENC_REC_INVALID_ARGS;
    }

    enc->prim_col = 0xFF;

    if (!enc->buf) {
        enc->buf = calloc(65536, 1);
        enc->buf_capacity = 65536;
        enc->managed = true;
    }

    i32 ret;
    const struct StaticSchema *s = enc->handle->schema;
    u32 offset = 0;

    u8 var_cols[MAX_COLUMN];
    u8 n_var_cols = 0;

    memset(&enc->meta, 0, sizeof(struct RecordMetadata));
    enc->meta.num_cols = s->num_cols;
    memset(enc->meta.null_bitmap, 0xFF, NULL_BITMAPS);

    u32 *cot = (u32 *) enc->buf;
    offset += s->num_cols * 4;
    memset(cot, 0, offset);

    for (u8 i = 0; i < n_cols; i++) {
        u8 col_idx = cols[i].col_idx;
        if (col_idx >= s->num_cols) {
            ret = ENC_REC_COL_INVALID_IDX;
            goto CLEANUP;
        }
        if (cols[i].type != s->cols[col_idx].type) {
            ret = ENC_REC_COL_TYPE_UNMATCH;
            goto CLEANUP;
        }
        // Check if already set (bit cleared means already processed)
        if (!(enc->meta.null_bitmap[col_idx / 8] & (1U << (col_idx % 8)))) {
            ret = ENC_REC_COL_DUPLICATED;
            goto CLEANUP;
        }
        if (col_idx == enc->handle->key_idx) {
            enc->prim_col = i;
        }

        u64 fixed_buf = 0;
        switch (cols[i].type) {
            case TYPE_INTEGER:
                memcpy(&fixed_buf, &cols[i].ival, 8);
                break;
            case TYPE_REAL:
                memcpy(&fixed_buf, &cols[i].dval, 8);
                break;
            default:
                var_cols[n_var_cols++] = i;
                continue;
        }

        if (offset > enc->buf_capacity - 8) {
            if (enc->managed) {
                u8 *nbuf = realloc(enc->buf, enc->buf_capacity + 1024);
                if (!nbuf) {
                    ret = ENC_REC_ALLOC_FAILED;
                    goto CLEANUP;
                }
                enc->buf = nbuf;
                enc->buf_capacity += 1024;
                cot = (u32 *) enc->buf; // Recalculate after realloc
            } else {
                ret = ENC_REC_BUF_TOO_SMALL;
                goto CLEANUP;
            }
        }

        cot[col_idx] = offset;
        enc->meta.null_bitmap[col_idx / 8] &= ~(1U << (col_idx % 8));
        store64be(fixed_buf, enc->buf + offset);
        offset += 8;
    }

    for (u8 i = 0; i < n_var_cols; i++) {
        u8 idx = var_cols[i];
        u8 col_idx = cols[idx].col_idx;

        u32 size = cols[idx].blob.size;

        if (size > enc->buf_capacity || offset > enc->buf_capacity - size - 4) {
            if (enc->managed) {
                u32 needed = offset + 4 + size + 1024;
                u8 *nbuf = realloc(enc->buf, needed);
                if (!nbuf) {
                    ret = ENC_REC_ALLOC_FAILED;
                    goto CLEANUP;
                }
                enc->buf = nbuf;
                enc->buf_capacity = needed;
                cot = (u32 *) enc->buf; // Recalculate after realloc
            } else {
                ret = ENC_REC_BUF_TOO_SMALL;
                goto CLEANUP;
            }
        }

        if (col_idx == enc->handle->key_idx) {
            enc->prim_col = idx;
        }

        cot[col_idx] = offset;
        enc->meta.null_bitmap[col_idx / 8] &= ~(1U << (col_idx % 8));
        memcpy(enc->buf + offset, &size, 4);
        memcpy(enc->buf + offset + 4, cols[idx].blob.dat, size);
        offset += 4 + size;
    }

    if (enc->prim_col == 0xFF) {
        ret = ENC_REC_NO_PRIM;
        goto CLEANUP;
    }

    enc->buf_len = offset;
    enc->meta.size = offset;

    return ENC_REC_OK;

CLEANUP:
    if (enc->managed) {
        free(enc->buf);
        enc->buf = NULL;
        enc->buf_capacity = 0;
    }
    return ret;
}

i32 decode_record(struct RecordDec *dec, const u8 *buf, u32 buf_len) {
    if (!dec || !buf || !dec->meta || !dec->handle) {
        return DEC_REC_INVALID_ARGS;
    }
    const struct StaticSchema *s = dec->handle->schema;
    if (dec->meta->num_cols != s->num_cols) {
        return DEC_REC_INVALID_METADATA;
    }

    i32 ret;
    dec->cols = calloc(s->num_cols, sizeof(struct RecordColumn));
    if (!dec->cols) {
        return DEC_REC_ALLOC_FAILED;
    }
    dec->n_cols = 0;

    const u32 *cot = (const u32 *) buf;
    for (u8 i = 0; i < s->num_cols; i++) {
        // Check null_bitmap consistency
        if ((dec->meta->null_bitmap[i / 8] & (1 << (i % 8))) != 0) {
            if (cot[i]) {
                ret = DEC_REC_INVALID_METADATA;
                goto CLEANUP;
            }
            continue;
        }

        u32 offset = cot[i];
        if (!offset || offset >= buf_len) {
            ret = DEC_REC_INVALID_RECORD;
            goto CLEANUP;
        }

        struct RecordColumn *col = &dec->cols[dec->n_cols++];
        col->col_idx = i;
        col->type = s->cols[i].type;

        u64 fixed_buf = 0;
        switch (col->type) {
            case TYPE_INTEGER:
                if (offset + 8 > buf_len) {
                    ret = DEC_REC_INVALID_RECORD;
                    goto CLEANUP;
                }
                fixed_buf = load64be(buf + offset);
                memcpy(&col->ival, &fixed_buf, 8);
                break;
            case TYPE_REAL:
                if (offset + 8 > buf_len) {
                    ret = DEC_REC_INVALID_RECORD;
                    goto CLEANUP;
                }
                fixed_buf = load64be(buf + offset);
                memcpy(&col->dval, &fixed_buf, 8);
                break;
            case TYPE_TEXT:
            case TYPE_BLOB:
            default:
                if (offset + 4 > buf_len) {
                    ret = DEC_REC_INVALID_RECORD;
                    goto CLEANUP;
                }
                memcpy(&col->blob.size, buf + offset, 4);
                if (offset + 4 + col->blob.size > buf_len) {
                    ret = DEC_REC_INVALID_RECORD;
                    goto CLEANUP;
                }
                col->blob.dat = buf + offset + 4;
                break;
        }
    }

    return DEC_REC_OK;

CLEANUP:
    free(dec->cols);
    dec->cols = NULL;
    dec->n_cols = 0;
    return ret;
}
