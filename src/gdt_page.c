#include "gdt_page.h"

#include <assert.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "utils.h"

void gdt_sync(struct GdtPageBank *b) {
    if (b->fd != -1) {
        msync(b->pages, b->size, MS_SYNC);
    }
}

void *gdt_get_page(struct GdtPageBank *b, u32 page_num) {
    if (page_num * PAGE_SIZE >= b->size)
        return NULL;
    return (u8 *) b->pages + page_num * PAGE_SIZE;
}

static inline i32 ffz(u32 mask) { return ffs(~mask) - 1; }

// Returns the relative index of the page in group gidx.
static u32 find_free_page_group(struct GdtPageBank *b, u32 gidx) {
    assert(gidx < GDT_SIZE_PER_PAGE * MAX_GDTS);
    struct GroupDescriptor *gdt = get_gdt(b);
    u32 gpage = gdt[gidx].group_start;
    if (gpage == INVALID_PAGE || !gdt[gidx].free_pages) {
        return INVALID_PAGE;
    }
    u16 last_used_mask = gdt[gidx].last_set_mask;
    u32 *bitmap = (u32 *) ((u8 *) b->pages + PAGE_SIZE * gpage);

    for (u32 i = last_used_mask; i < 2 * MASKS_PER_PAGE; i++) {
        if (bitmap[i] == 0xFFFFFFFF) {
            continue;
        }
        i32 bit = ffz(bitmap[i]);
        if (bit >= 0) {
            u32 pidx = i * 32 + bit;
            return pidx;
        }
    }
    for (u32 i = 0; i < last_used_mask; i++) {
        if (bitmap[i] == 0xFFFFFFFF) {
            continue;
        }
        i32 bit = ffz(bitmap[i]);
        if (bit >= 0) {
            u32 pidx = i * 32 + bit;
            return pidx;
        }
    }

    return INVALID_PAGE;
}

static u32 find_free_page(struct GdtPageBank *b, u32 hint) {
    struct GdtSuperblock *sb = gdt_get_superblock(b);
    u32 total_groups = sb->total_groups;
    if (hint != INVALID_PAGE) {
        // If we have a hint, search the same group & 2 groups after it
        u32 hgroup = (hint - HEAD_OFFSET) / GROUP_SIZE;
        for (int i = 0; i < 3; i++) {
            u32 gidx = hgroup + i;
            if (gidx >= total_groups) {
                break;
            }
            u32 pidx = find_free_page_group(b, gidx);
            if (pidx == INVALID_PAGE) {
                continue;
            }
            u32 page_num = HEAD_OFFSET + gidx * GROUP_SIZE + pidx;
            return page_num;
        }
        for (u32 i = hgroup + 3; i < total_groups; i++) {
            u32 pidx = find_free_page_group(b, i);
            if (pidx == INVALID_PAGE) {
                continue;
            }
            u32 page_num = HEAD_OFFSET + i * GROUP_SIZE + pidx;
            return page_num;
        }
        for (u32 i = 0; i < hgroup; i++) {
            u32 pidx = find_free_page_group(b, i);
            if (pidx == INVALID_PAGE) {
                continue;
            }
            u32 page_num = HEAD_OFFSET + i * GROUP_SIZE + pidx;
            return page_num;
        }
    } else {
        for (u32 i = 0; i < total_groups; i++) {
            u32 pidx = find_free_page_group(b, i);
            if (pidx == INVALID_PAGE) {
                continue;
            }
            u32 page_num = HEAD_OFFSET + i * GROUP_SIZE + pidx;
            return page_num;
        }
    }
    return INVALID_PAGE;
}

static i32 grow(struct GdtPageBank *b) {
    struct GdtSuperblock *sb = gdt_get_superblock(b);
    u32 new_size = (sb->total_pages + GROUP_SIZE) * PAGE_SIZE;
    if (new_size > b->size) {
        if (b->fd == -1) {
            void *new_addr = realloc(b->pages, new_size);
            if (!new_addr) {
                perror("realloc()");
                return -1;
            }
            b->pages = new_addr;
        } else {
            if (ftruncate(b->fd, new_size) < 0) {
                perror("ftruncate()");
                return -1;
            }
            void *new_addr = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, b->fd, 0);
            if (new_addr == MAP_FAILED) {
                perror("mmap()");
                return -1;
            }
            munmap(b->pages, b->size);
            b->pages = new_addr;
        }
        b->size = new_size;
        sb = gdt_get_superblock(b);
    }
    // Update group metadata
    u32 ngidx = sb->total_groups;
    struct GroupDescriptor *gdt = get_gdt(b);
    gdt[ngidx].group_start = sb->total_pages;
    gdt[ngidx].free_pages = GROUP_SIZE - GROUP_BITMAPS;
    // Update group bitmap.
    u32 *group_bitmap = gdt_get_page(b, sb->total_pages);
    group_bitmap[0] = 0x3;
    // Update superblock
    sb->total_groups++;
    sb->total_pages += GROUP_SIZE;
    return 0;
}

u32 gdt_alloc_page(struct GdtPageBank *b, u32 hint) {
    u32 page_num = find_free_page(b, hint);
    if (page_num == INVALID_PAGE) {
        u32 ngroup = gdt_get_superblock(b)->total_groups;

        if (grow(b) < 0) {
            return INVALID_PAGE;
        }

        u32 pidx = find_free_page_group(b, ngroup);
        page_num = HEAD_OFFSET + ngroup * GROUP_SIZE + pidx;
    }

    gdt_set_page(b, page_num);
    void *page = gdt_get_page(b, page_num);
    memset(page, 0, PAGE_SIZE);
    return page_num;
}

i32 gdt_bank_create(struct GdtPageBank *b, i32 fd) {
    b->fd = fd;
    b->size = INITIAL_PAGES * PAGE_SIZE;
    if (fd == -1) {
        b->pages = calloc(1, b->size);
        if (!b->pages) {
            perror("calloc()");
            return -1;
        }
    } else {
        if (ftruncate(b->fd, b->size) < 0) {
            perror("ftruncate()");
            return -1;
        }
        b->pages = mmap(NULL, b->size, PROT_READ | PROT_WRITE, MAP_SHARED, b->fd, 0);
        if (b->pages == MAP_FAILED) {
            perror("mmap()");
            close(b->fd);
            return -1;
        }
    }

    // Initialize GdtSuperblock
    struct GdtSuperblock *sb = gdt_get_superblock(b);
    memset(sb, 0, sizeof(struct GdtSuperblock));
    sb->magic = MAGIC;
    sb->version = VERSION;
    sb->page_size = PAGE_SIZE;
    sb->gdt_pages = MAX_GDTS;
    sb->total_pages = INITIAL_PAGES;
    sb->total_groups = 1;
    sb->_root_page = INVALID_PAGE;
    b->curr_dblk = sb->curr_dblk = INVALID_PAGE;
    sb->head_dblk = INVALID_PAGE;

    // Initialize GDT
    struct GroupDescriptor *gdt = get_gdt(b);
    for (u32 i = 0; i < GDT_SIZE_PER_PAGE * MAX_GDTS; i++) {
        gdt[i].group_start = INVALID_PAGE;
        gdt[i].free_pages = 0;
        gdt[i].last_set_mask = 0;
    }

    // Initialize the first group
    gdt[0].group_start = HEAD_OFFSET;
    gdt[0].free_pages = GROUP_SIZE - GROUP_BITMAPS;

    // Initialize the first group's bitmap
    u32 *bitmap = gdt_get_page(b, gdt[0].group_start);
    bitmap[0] = 0x3; // Mark first two pages (the bitmap itself) as used

    return 0;
}

i32 gdt_bank_open(struct GdtPageBank *b, const char *path) {
    if (!path) {
        return gdt_bank_create(b, -1);
    }
    i32 fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        perror("open()");
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        perror("fstat()");
        close(fd);
        return -1;
    }

    if (st.st_size == 0) {
        return gdt_bank_create(b, fd);
    }

    if (st.st_size % PAGE_SIZE != 0) {
        fprintf(stderr, "Error: File size is not a multiple of page size.\n");
        close(fd);
        return -1;
    }

    b->fd = fd;
    b->size = st.st_size;
    b->pages = mmap(NULL, b->size, PROT_READ | PROT_WRITE, MAP_SHARED, b->fd, 0);
    if (b->pages == MAP_FAILED) {
        perror("mmap()");
        close(b->fd);
        return -1;
    }

    struct GdtSuperblock *s = gdt_get_superblock(b);
    if (s->magic != MAGIC || s->version != VERSION || s->page_size != PAGE_SIZE || s->gdt_pages != MAX_GDTS ||
        b->size < s->total_pages * PAGE_SIZE) {
        fprintf(stderr, "Error: Invalid database file format or size mismatch.\n");
        munmap(b->pages, b->size);
        close(b->fd);
        return -1;
    }

    return 0;
}

void gdt_bank_close(struct GdtPageBank *b) {
    if (b->pages == NULL)
        return;
    if (b->fd != -1) {
        msync(b->pages, b->size, MS_SYNC);
        munmap(b->pages, b->size);
        close(b->fd);
    } else {
        free(b->pages);
    }
    b->pages = NULL;
    b->size = 0;
    b->fd = -1;
}
