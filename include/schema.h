#ifndef SCHEMA_H
#define SCHEMA_H

#include "gdt_page.h"
#include "utils.h"

enum {
    TYPE_BYTES = 0,
};

enum {
    KEY_NONE = 0,
    KEY_PRIM = 1,
    KEY_UNIQ = 2,
};

static inline struct Schema *get_schema_root(struct GdtPageBank *b) {
    return (struct Schema *) ((u8 *) b->pages + SCHEMA_PAGE * PAGE_SIZE);
}

#endif /* ifndef SCHEMA_H */
