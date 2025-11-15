#include "catalog.h"

#include <stdlib.h>
#include <string.h>

#include "alloc.h"
#include "bufpool.h"
#include "page.h"
#include "pagestore.h"
#include "rwsxlock.h"
#include "slot.h"
#include "utils.h"

static void fsm_index_init(struct BufPool *pool, u32 page_num) {
    struct FrameHandle *fh = bpool_acquire_page(pool, page_num, LATCH_NONE);

    memset(fh->fdata->data, 0, PAGE_SIZE);
    struct FSMIndexPage *fp = (void *) fh->fdata->data;

    fp->nslots = 0;
    fp->next_page = INVALID_PAGE;
    memset(fp->free_space, 0xFF, FSM_SLOTS);
    memset(fp->data_page, 0xFF, FSM_SLOTS * sizeof(u32));
    compute_checksum(&fp->header);

    bpool_release_page(pool, fh, true, LATCH_NONE);
}

struct Catalog *catalog_init(struct PageAllocator *alloc) {
    if (!alloc)
        return NULL;

    struct Catalog *c = calloc(1, sizeof(struct Catalog));

    c->alloc = alloc;
    c->pool = alloc->pool;
    c->store = alloc->store;
    c->page.fsm_head = alloc_page(alloc, INVALID_PAGE);
    c->page.kfsm_head = alloc_page(alloc, c->page.fsm_head);
    c->page.schema_root = alloc_page(alloc, INVALID_PAGE);

    if (c->page.fsm_head == INVALID_PAGE || c->page.kfsm_head == INVALID_PAGE || c->page.schema_root == INVALID_PAGE) {
        goto error;
    }

    compute_checksum(&c->page.header);

    if (pstore_write(c->store, CATALOG_PAGE, &c->page) < 0) {
        goto error;
    }

    fsm_index_init(c->pool, c->page.fsm_head);
    fsm_index_init(c->pool, c->page.kfsm_head);

    return c;

error:
    free_page(alloc, c->page.schema_root);
    free_page(alloc, c->page.fsm_head);
    free_page(alloc, c->page.kfsm_head);
    free(c);
    return NULL;
}

struct Catalog *catalog_open(struct PageAllocator *alloc) {
    if (!alloc)
        return NULL;

    struct Catalog *c = calloc(1, sizeof(struct Catalog));

    c->alloc = alloc;
    c->pool = alloc->pool;
    c->store = alloc->store;

    if (pstore_read(c->store, CATALOG_PAGE, &c->page) < 0) {
        goto error;
    }

    if (!verify_checksum(&c->page.header)) {
        goto error;
    }

    return c;
error:
    free(c);
    return NULL;
}

i32 catalog_close(struct Catalog *c) {
    compute_checksum(&c->page.header);

    if (pstore_write(c->store, CATALOG_PAGE, &c->page) < 0) {
        return -1;
    }

    free(c);
    return 0;
}

static i32 catalog_read_huge_data(struct Catalog *c, const struct VPtr *ptr, u8 *data) {
    u32 page = ptr->page_num;
    const u32 size = ptr->size;

    u32 start = 0;
    while (start < size) {
        u32 to_read = MIN(size - start, CHAIN_PAGE_DATA_SIZE);

        if (page == INVALID_PAGE) {
            return -1;
        }
        struct FrameHandle *h = bpool_acquire_page(c->pool, page, LATCH_NONE);
        struct ChainPage *p = (void *) h->fdata->data;

        if (!verify_checksum(&p->header)) {
            bpool_release_handle(c->pool, h);
            return -1;
        }
        memcpy(data + start, p->data, to_read);

        page = p->next_page;

        bpool_release_page(c->pool, h, false, LATCH_NONE);

        start += to_read;
    }

    return 0;
}

static void catalog_free_huge_data(struct Catalog *c, const struct VPtr *ptr) {
    u32 page = ptr->page_num;

    while (page != INVALID_PAGE) {
        struct FrameHandle *h = bpool_acquire_page(c->pool, page, LATCH_NONE);
        struct ChainPage *p = (void *) h->fdata->data;

        free_page(c->alloc, page);

        page = p->next_page;

        bpool_release_page(c->pool, h, false, LATCH_NONE);
    }
}

static struct VPtr catalog_write_huge_data(struct Catalog *c, const u8 *data, const u32 size) {
    struct VPtr res;
    u32 chnks = size / CHAIN_PAGE_DATA_SIZE + (size % CHAIN_PAGE_DATA_SIZE != 0);
    u32 *pages = calloc(chnks, sizeof(u32));

    u32 top;
    for (top = 0; top < chnks; top++) {
        pages[top] = alloc_page(c->alloc, !top ? INVALID_PAGE : pages[0]);
        if (pages[top] == INVALID_PAGE) {
            goto error;
        }
    }

    u32 start = 0;
    for (u32 i = 0; i < top; i++) {
        u32 to_write = MIN(size - start, CHAIN_PAGE_DATA_SIZE);
        struct FrameHandle *h = bpool_acquire_page(c->pool, pages[i], LATCH_NONE);

        struct ChainPage *p = (void *) h->fdata->data;
        p->next_page = (i != top - 1) ? pages[i + 1] : INVALID_PAGE;

        memcpy(p->data, data + start, to_write);
        compute_checksum(&p->header);

        bpool_release_page(c->pool, h, true, LATCH_NONE);
        start += to_write;
    }

    res.page_num = pages[0];
    res.size = size;
    free(pages);
    return res;

error:
    res.page_num = INVALID_PAGE;

    for (u32 i = 0; i < top; i++) {
        free_page(c->alloc, pages[i]);
    }

    return res;
}

static i32 catalog_read_normal_data(struct Catalog *c, const struct VPtr *ptr, u8 *data) {
    const u32 page = ptr->page_num;
    const u16 slot = ptr->slot_info.slot;

    if (page == INVALID_PAGE)
        return -1;


    struct FrameHandle *h = bpool_acquire_page(c->pool, page, LATCH_SHARED);
    if (!h)
        return -1;

    struct SlotPage *sh = slot_open(h);
    if (!sh) {
        goto error;
    }

    const struct Cell *cell = slot_get(sh, slot);
    if (!cell) {
        goto error;
    }
    memcpy(data, cell->data, cell->size);

    bpool_release_page(c->pool, h, false, LATCH_SHARED);

    return 0;

error:
    bpool_release_page(c->pool, h, false, LATCH_SHARED);
    return -1;
}

static i32 catalog_free_normal_data(struct Catalog *c, const struct VPtr *ptr) {
    const u32 page = ptr->page_num;
    const u16 slot = ptr->slot_info.slot;

    if (page == INVALID_PAGE)
        return -1;


    struct FrameHandle *h = bpool_acquire_page(c->pool, page, LATCH_EXCLUSIVE);
    if (!h)
        return -1;

    struct SlotPage *sh = slot_open(h);
    if (!sh) {
        goto error;
    }

    u32 fsm_index = sh->fsm_index;
    u16 fsm_slot = sh->fsm_slot;
    const u16 size = slot_get(sh, slot)->size;
    slot_free(sh, slot);
    slot_update_checksum(sh);

    bpool_release_page(c->pool, h, true, LATCH_EXCLUSIVE);

    struct FrameHandle *fh = bpool_acquire_page(c->pool, fsm_index, LATCH_EXCLUSIVE);

    struct FSMIndexPage *fp = (void *) fh->fdata->data;
    u16 size_scaled = ((u64) size * 0xFF / MAX_SLOT_PAGE_SIZE) + (((u64) size * 0xFF % MAX_SLOT_PAGE_SIZE) > 0);
    fp->free_space[fsm_slot] = MIN((u16) fp->free_space[fsm_slot] + size_scaled, 0xFF);
    compute_checksum(&fp->header);

    bpool_release_page(c->pool, fh, true, LATCH_EXCLUSIVE);

    return 0;

error:
    bpool_release_page(c->pool, h, false, LATCH_EXCLUSIVE);
    return -1;
}

static struct VPtr catalog_write_normal_data(struct Catalog *c, const u8 *data, const u16 len, bool is_key) {
    u32 page = is_key ? c->page.kfsm_head : c->page.fsm_head;
    u8 size_scaled = ((u64) len * 0xFF / MAX_SLOT_PAGE_SIZE) + (((u64) len * 0xFF % MAX_SLOT_PAGE_SIZE) > 0);

    while (page != INVALID_PAGE) {
        struct FrameHandle *fh = bpool_acquire_page(c->pool, page, LATCH_SHARED_EXCLUSIVE);

        struct FSMIndexPage *fp = (void *) fh->fdata->data;
        for (u16 i = 0; i < fp->nslots; i++) {
            if (fp->free_space[i] >= size_scaled) {
                // Case 1. found a page with enough space.
                u32 spage = fp->data_page[i];
                struct FrameHandle *h = bpool_acquire_page(c->pool, spage, LATCH_EXCLUSIVE);

                struct SlotPage *sh = slot_open(h);
                u16 slot = slot_alloc(sh, len);
                if (slot != INVALID_SLOT) {
                    // Alloc success, write in, update free_space, return
                    struct Cell *cell = slot_get(sh, slot);
                    memcpy(cell->data, data, len);
                    cell->size = len;
                    struct VPtr ptr = {
                            .page_num = spage,
                            .slot_info = {slot, is_key},
                    };
                    slot_update_checksum(sh);
                    bpool_release_page(c->pool, h, true, LATCH_EXCLUSIVE);

                    rwsx_upgrade_sx(&fh->fdata->latch);
                    fp->free_space[i] -= size_scaled;
                    compute_checksum(&fp->header);

                    bpool_release_page(c->pool, fh, true, LATCH_EXCLUSIVE);

                    return ptr;
                }
                bpool_release_page(c->pool, h, false, LATCH_EXCLUSIVE);
            }
        }
        // Case 2. add a new page (FSM is not full)
        if (fp->nslots < FSM_SLOTS) {
            u32 index = fp->nslots++;
            u32 dat_page = alloc_page(c->alloc, INVALID_PAGE);
            if (dat_page != INVALID_PAGE) {
                struct FrameHandle *h = bpool_acquire_page(c->pool, dat_page, LATCH_NONE);

                struct SlotPage *sh = slot_init(h, page, index);
                u16 slot = slot_alloc(sh, len);
                struct Cell *cell = slot_get(sh, slot);
                memcpy(cell->data, data, len);
                cell->size = len;
                struct VPtr ptr = {
                        .page_num = dat_page,
                        .slot_info = {slot, is_key},
                };

                slot_update_checksum(sh);
                bpool_release_page(c->pool, h, true, LATCH_NONE);

                rwsx_upgrade_sx(&fh->fdata->latch);
                fp->free_space[index] = 0xFF - size_scaled;
                fp->data_page[index] = dat_page;
                compute_checksum(&fp->header);

                bpool_release_page(c->pool, fh, true, LATCH_EXCLUSIVE);
                return ptr;
            }
        }

        if (fp->next_page == INVALID_PAGE) {
            fp->next_page = alloc_page(c->alloc, page);
            if (fp->next_page != INVALID_PAGE) {
                fsm_index_init(c->pool, fp->next_page);
            }
        }
        page = fp->next_page;

        bpool_release_page(c->pool, fh, false, LATCH_SHARED_EXCLUSIVE);
    }

    return (struct VPtr) {.page_num = INVALID_PAGE, .size = INVALID_PAGE};
}

struct FrameHandle *catalog_get_slot_page(struct Catalog *c, const struct VPtr *ptr) {
    const u32 page = ptr->page_num;

    if (page == INVALID_PAGE)
        return NULL;


    return bpool_fetch_page(c->pool, page);
}

struct VPtr catalog_write_data(struct Catalog *c, const u8 *data, u32 len) {
    if (len > NORMAL_DATA_LIMIT) {
        return catalog_write_huge_data(c, data, len);
    } else {
        return catalog_write_normal_data(c, data, len, false);
    }
}

struct VPtr catalog_write_key(struct Catalog *c, const u8 *key_data, u16 len) {
    if (len > NORMAL_DATA_LIMIT) {
        return catalog_write_huge_data(c, key_data, len);
    } else {
        return catalog_write_normal_data(c, key_data, len, true);
    }
}

i32 catalog_read(struct Catalog *c, const struct VPtr *ptr, u8 *data, bool chained) {
    if (chained) {
        return catalog_read_huge_data(c, ptr, data);
    } else {
        return catalog_read_normal_data(c, ptr, data);
    }
}

i32 catalog_free(struct Catalog *c, const struct VPtr *ptr, bool chained) {
    if (chained) {
        catalog_free_huge_data(c, ptr);
        return 0;
    } else {
        return catalog_free_normal_data(c, ptr);
    }
}
