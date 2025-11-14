#ifndef CATALOG_H
#define CATALOG_H

#include <stdbool.h>

#include "alloc.h"
#include "bufpool.h"
#include "page.h"
#include "pagestore.h"
#include "utils.h"

#define FSM_SLOTS 800
#define NORMAL_DATA_LIMIT 3072

struct VPtr {
    u32 page_num;
    union {
        u32 size;
        struct {
            u16 slot;
            bool is_key;
        } slot_info;
    };
};

struct CatalogPage {
    struct PageHeader header;
    u32 schema_root;
    u32 fsm_head;
    u32 kfsm_head;

    u8 _pad[PAGE_SIZE - sizeof(u32) * 3 - sizeof(struct PageHeader)];
};

struct ChainPage {
    struct PageHeader header;
    u32 next_page;

    u8 data[];
};
#define CHAIN_PAGE_DATA_SIZE (PAGE_SIZE - sizeof(struct ChainPage))

#define FSM_INDEX_PADDING (PAGE_SIZE - (4 * 2 + sizeof(struct PageHeader) + FSM_SLOTS * 5))
struct FSMIndexPage {
    struct PageHeader header;
    u32 next_page;
    u16 nslots;
    u16 _pad1;

    u8 free_space[FSM_SLOTS];
    u32 data_page[FSM_SLOTS];
    u8 _pad2[FSM_INDEX_PADDING];
};

struct Catalog {
    struct PageAllocator *alloc;
    struct BufPool *pool;
    struct PageStore *store;
    struct CatalogPage page;
};

struct Catalog *catalog_init(struct PageAllocator *alloc);
struct Catalog *catalog_open(struct PageAllocator *alloc);
i32 catalog_close(struct Catalog *c);

struct VPtr catalog_write_data(struct Catalog *c, const u8 *data, u32 len);
struct VPtr catalog_write_key(struct Catalog *c, const u8 *key_data, u16 len);
i32 catalog_read(struct Catalog *c, const struct VPtr *ptr, u8 *data, bool chained);
i32 catalog_free(struct Catalog *c, const struct VPtr *ptr, bool chained);

struct FrameHandle *catalog_get_slot_page(struct Catalog *c, const struct VPtr *ptr);

#endif /* ifndef CATALOG_H */
