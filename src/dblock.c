#include "dblock.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "page.h"
#include "utils.h"

static u32 alloc_hdblock(struct PageBank *b) {
    u32 page_num = alloc_page(b);
    if (page_num == INVALID_PAGE)
        return INVALID_PAGE;
    struct DataBlockHuge *blk = get_page(b, page_num);
    blk->meta.block_type = DATA_HUGE;
    blk->meta.next_page = INVALID_PAGE;

    return page_num;
}

struct VPtr write_huge_data(struct PageBank *b, void *data, u32 len) {
    // Create array to hold page numbers for pre-allocation
    u32 npage = len / DATA_HUGE_SPACE + (len % DATA_HUGE_SPACE != 0);
    u32 *pages = calloc(npage, sizeof(u32));
    // Alloc all required pages, reject when failed.
    u32 top;
    for (top = 0; top < npage; top++) {
        pages[top] = alloc_hdblock(b);
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
