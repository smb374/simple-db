#include "schema.h"

#include <string.h>

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

    memset(&builder->cols[idx], 0, sizeof(struct Column));
    memcpy(builder->cols[idx].name, name, name_len);
    builder->cols[idx].type = type;
    builder->cols[idx].uniq = uniq;
    builder->cols[idx].size = size;

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
        memcpy(&s->cols[i], &builder->cols[i], sizeof(struct Column));
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

i32 create_tree(struct BTreeHandle *handle, struct GdtPageBank *bank, struct StaticSchema *s) {
    if (!handle || !bank) {
        return -1;
    }
    // Try to allocate a new node in bank and saves the root_page in handle
    if (btree_create_root(handle, bank) < 0) {
        return -1;
    }
    s->root_page = handle->root_page;
    // Try to insert static schema to the schema tree
    struct BTreeHandle shandle = {bank, SCHEMA_PAGE};
    if (btree_insert(&shandle, (const u8 *) s->name, s, sizeof(struct StaticSchema)) < 0) {
        gdt_unset_page(bank, handle->root_page);
        handle->root_page = INVALID_PAGE;
        return -1;
    }

    return 0;
}
