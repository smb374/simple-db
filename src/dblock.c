#include "dblock.h"

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "page.h"
#include "utils.h"

static u32 alloc_hdblk(struct PageBank *b) {
    u32 page_num = alloc_page(b);
    if (page_num == INVALID_PAGE)
        return INVALID_PAGE;
    struct DataBlockHuge *blk = get_page(b, page_num);
    blk->meta.block_type = DATA_HUGE;
    blk->meta.next_page = INVALID_PAGE;

    return page_num;
}

static u32 alloc_ndblk(struct PageBank *b) {
    u32 page_num = alloc_page(b);
    if (page_num == INVALID_PAGE)
        return INVALID_PAGE;
    struct DataBlockNormal *blk = get_page(b, page_num);
    blk->meta.block_type = DATA_NORMAL;
    blk->num_slots = 0;
    blk->cell_off = PAGE_SIZE;
    blk->frag_bytes = 0;
    blk->meta.next_page = INVALID_PAGE;
    blk->prev_page = INVALID_PAGE;

    struct Superblock *sb = get_superblock(b);
    blk->meta.next_page = sb->head_dblk;
    if (sb->head_dblk != INVALID_PAGE) {
        struct DataBlockNormal *ohead = get_page(b, sb->head_dblk);
        ohead->prev_page = page_num;
    }
    sb->head_dblk = page_num;

    return page_num;
}

static u32 ndblk_free_space(struct DataBlockNormal *blk) {
    u32 slots_size = sizeof(struct DataBlockNormal) + blk->num_slots * sizeof(u16);
    u32 used_cell_space = PAGE_SIZE - blk->cell_off;
    u32 total_used = slots_size + used_cell_space;
    return PAGE_SIZE - total_used - blk->frag_bytes;
}

static bool ndblk_has_space(struct DataBlockNormal *blk, u32 len) {
    u32 space_needed = sizeof(u16) + sizeof(u16) + len;
    return ndblk_free_space(blk) >= space_needed;
}

static void ndblk_defrag(struct DataBlockNormal *blk) {
    u8 tmp[PAGE_SIZE];
    memcpy(tmp, blk, PAGE_SIZE);

    struct DataBlockNormal *src = (struct DataBlockNormal *) tmp;

    blk->cell_off = PAGE_SIZE;
    blk->frag_bytes = 0;

    for (u16 i = 0; i < blk->num_slots; i++) {
        if (!src->slots[i])
            continue;

        struct Cell *cell = (struct Cell *) ((u8 *) src + src->slots[i]);
        u32 cell_size = sizeof(u16) + cell->size;
        blk->cell_off -= cell_size;
        memcpy((u8 *) blk + blk->cell_off, cell, cell_size);
        blk->slots[i] = blk->cell_off;
    }
}

struct VPtr write_huge_data(struct PageBank *b, void *data, u32 len) {
    // Create array to hold page numbers for pre-allocation
    u32 npage = len / DATA_HUGE_SPACE + (len % DATA_HUGE_SPACE != 0);
    u32 *pages = calloc(npage, sizeof(u32));
    // Alloc all required pages, reject when failed.
    u32 top;
    for (top = 0; top < npage; top++) {
        pages[top] = alloc_hdblk(b);
        if (pages[top] == INVALID_PAGE) {
            goto CLEANUP;
        }
    }
    // Write data to pages.
    u32 off = 0, left = len;
    for (u32 i = 0; i < npage; i++) {
        struct DataBlockHuge *blk = get_page(b, pages[i]);
        blk->meta.next_page = i < npage - 1 ? pages[i + 1] : INVALID_PAGE;

        u32 size = MIN(left, DATA_HUGE_SPACE);
        memcpy(blk->data, (u8 *) data + off, size);
        off += size;
        left -= size;
    }

    struct VPtr res = VPTR_MAKE_HUGE(pages[0], len);
    free(pages);
    return res;

CLEANUP:
    for (u32 i = 0; i < top; i++) {
        unset_page(b, pages[i]);
    }
    free(pages);
    return VPTR_INVALID;
}

i32 read_huge_data(struct PageBank *b, void *data, struct VPtr ptr) {
    u32 curr = ptr.page;
    u32 left = VPTR_GET_HUGE_LEN(ptr);
    u32 off = 0;
    while (left) {
        struct DataBlockHuge *blk = get_page(b, curr);
        u32 size = MIN(left, DATA_HUGE_SPACE);

        memcpy((u8 *) data + off, blk->data, size);

        off += size;
        left -= size;
        curr = blk->meta.next_page;
        if (left && curr == INVALID_PAGE) {
            return -1;
        }
    }

    return 0;
}

void delete_huge_data(struct PageBank *b, struct VPtr ptr) {
    u32 curr = ptr.page;
    while (curr != INVALID_PAGE) {
        struct DataBlockHuge *blk = get_page(b, curr);
        u32 next = blk->meta.next_page;
        unset_page(b, curr);
        curr = next;
    }
}

struct VPtr write_normal_data(struct PageBank *b, void *data, const u16 len) {
    u32 page_num = b->curr_dblk;
    if (page_num == INVALID_PAGE) {
        page_num = alloc_ndblk(b);
        if (page_num == INVALID_PAGE) {
            return VPTR_INVALID;
        }
        b->curr_dblk = page_num;
    }

    struct DataBlockNormal *blk = get_page(b, page_num);
    if (blk->frag_bytes > PAGE_SIZE / 4) {
        ndblk_defrag(blk);
    }

    if (!ndblk_has_space(blk, len)) {
        struct Superblock *sb = get_superblock(b);
        u32 curr = sb->head_dblk;
        i32 cnt = 0;

        while (curr != INVALID_PAGE && cnt < 8) {
            blk = get_page(b, curr);

            if (blk->frag_bytes > PAGE_SIZE / 4) {
                ndblk_defrag(blk);
            }

            if (ndblk_has_space(blk, len)) {
                page_num = curr;
                b->curr_dblk = page_num;
                break;
            }

            curr = blk->meta.next_page;
            cnt++;
        }

        if (!ndblk_has_space(blk, len)) {
            page_num = alloc_ndblk(b);
            if (page_num == INVALID_PAGE) {
                return VPTR_INVALID;
            }
            b->curr_dblk = page_num;
            blk = get_page(b, page_num);
        }
    }

    u16 slot_idx = (u16) -1;
    for (u16 i = 0; i < blk->num_slots; i++) {
        if (!blk->slots[i]) {
            slot_idx = i;
            break;
        }
    }
    if (slot_idx == (u16) -1) {
        slot_idx = blk->num_slots++;
    }

    u16 cell_size = sizeof(u16) + len;
    blk->cell_off -= cell_size;
    blk->slots[slot_idx] = blk->cell_off;

    struct Cell *cell = (struct Cell *) ((u8 *) blk + blk->cell_off);
    cell->size = len;
    memcpy(cell->data, data, len);

    return VPTR_MAKE_NORMAL(page_num, slot_idx, len);
}

i32 read_normal_data(struct PageBank *b, void *data, struct VPtr ptr) {
    u32 page_num = ptr.page;
    u16 slot_idx = VPTR_GET_SLOT(ptr);

    struct DataBlockNormal *blk = get_page(b, page_num);
    if (slot_idx >= blk->num_slots || !blk->slots[slot_idx]) {
        return -1;
    }

    struct Cell *cell = (struct Cell *) ((u8 *) blk + blk->slots[slot_idx]);
    memcpy(data, cell->data, cell->size);
    return 0;
}

void delete_normal_data(struct PageBank *b, struct VPtr ptr) {
    u32 page_num = ptr.page;
    u16 slot_idx = VPTR_GET_SLOT(ptr);

    struct DataBlockNormal *blk = get_page(b, page_num);
    if (slot_idx >= blk->num_slots || !blk->slots[slot_idx]) {
        return;
    }

    blk->frag_bytes += sizeof(u16) + VPTR_GET_LEN(ptr);
    blk->slots[slot_idx] = 0;
}
