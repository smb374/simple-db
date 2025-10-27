#include "page.h"

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

void *get_page(struct PageBank *b, u32 page_num) {
    if (page_num * PAGE_SIZE >= b->size)
        return NULL;
    return (char *) b->pages + page_num * PAGE_SIZE;
}

static inline int ffz(u32 mask) { return ffs(~mask) - 1; }

u32 find_free_page(struct PageBank *b) {
    struct Superblock *sb = (struct Superblock *) b->pages;
    u32 *bitmap = get_bitmap(b);

    u32 start_page = sb->fst_free_page;
    u32 start_mask = start_page / 32;
    u32 max_masks = sb->bitmap_pages * MASKS_PER_PAGE;

    for (u32 i = start_mask; i < max_masks; i++) {
        if (bitmap[i] == 0xFFFFFFFF)
            continue;

        int bit = ffz(bitmap[i]);
        if (bit >= 0) {
            u32 page = i * 32 + bit;
            if (page < start_page)
                continue;
            if (page >= sb->total_pages)
                break;
            return page;
        }
    }

    return INVALID_PAGE; // No free pages
}

int resize(struct PageBank *b, u64 new_size) {
    new_size = (new_size + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);
    if (b->fd == -1) {
        void *old_addr = b->pages;
        b->pages = realloc(b->pages, new_size);
        if (!b->pages) {
            perror("realloc()");
            b->pages = old_addr;
            return -1;
        }
    } else {
        if (ftruncate(b->fd, new_size) < 0) {
            perror("ftruncate()");
            return -1;
        }
        void *old_addr = b->pages;
        b->pages = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, b->fd, 0);
        if (b->pages == MAP_FAILED) {
            perror("mmap()");
            b->pages = old_addr;
            return -1;
        }
        munmap(old_addr, b->size);
    }
    b->size = new_size;
    return 0;
}

u32 alloc_page(struct PageBank *b) {
    struct Superblock *sb = (struct Superblock *) b->pages;

    u32 page_num = find_free_page(b);
    if (page_num == INVALID_PAGE) {
        page_num = sb->total_pages;

        u32 max_pages = sb->bitmap_pages * MASKS_PER_PAGE * 32;
        if (page_num >= max_pages) {
            return INVALID_PAGE;
        }

        if ((page_num + 1) * PAGE_SIZE > b->size) {
            u64 new_size = b->size + 256 * PAGE_SIZE;
            if (resize(b, new_size) < 0) {
                return INVALID_PAGE;
            }
            sb = (struct Superblock *) b->pages;
        }
        sb->total_pages = page_num + 1;
    }

    set_page(b, page_num);
    void *page = get_page(b, page_num);
    memset(page, 0, PAGE_SIZE);
    return page_num;
}

int bank_create(struct PageBank *b, int fd) {
    b->fd = fd;
    b->size = PAGE_SIZE * INITIAL_PAGES;
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
    memset(b->pages, 0, PAGE_SIZE * INITIAL_PAGES);

    struct Superblock *sb = (struct Superblock *) b->pages;
    sb->magic = MAGIC;
    sb->version = VERSION;
    sb->bitmap_pages = MAX_BITMAP_PAGES;
    sb->fst_free_page = 1 + MAX_BITMAP_PAGES;
    sb->total_pages = INITIAL_PAGES;
    sb->page_size = PAGE_SIZE;
    sb->root_offset = sb->fst_free_page * PAGE_SIZE;
    b->curr_dblk = sb->curr_dblk = INVALID_PAGE;
    sb->head_dblk = 0;

    for (u32 i = 0; i <= sb->fst_free_page; i++) {
        set_page(b, i);
    }

    return 0;
}

int bank_open(struct PageBank *b, const char *path) {
    if (!path) {
        return bank_create(b, -1);
    }
    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0)
        return -1;

    struct stat sb;
    fstat(fd, &sb);
    if (sb.st_size % PAGE_SIZE != 0) {
        return -1;
    }
    if (!sb.st_size) {
        return bank_create(b, fd);
    }
    if (sb.st_size < PAGE_SIZE) {
        return -1;
    }
    b->fd = fd;
    b->size = sb.st_size;

    b->pages = mmap(NULL, b->size, PROT_READ | PROT_WRITE, MAP_SHARED, b->fd, 0);
    if (b->pages == MAP_FAILED) {
        perror("mmap()");
        close(b->fd);

        return -1;
    }

    struct Superblock *s = (struct Superblock *) b->pages;
    if (s->magic != MAGIC || s->version != VERSION || s->page_size != PAGE_SIZE ||
        b->size < s->total_pages * PAGE_SIZE) {
        munmap(b->pages, b->size);
        close(b->fd);
        return -1;
    }
    b->curr_dblk = s->curr_dblk;

    return 0;
}

void bank_close(struct PageBank *b) {
    struct Superblock *sb = (struct Superblock *) b->pages;
    sb->curr_dblk = b->curr_dblk;
    if (b->fd != -1) {
        msync(b->pages, b->size, MS_SYNC);
        munmap(b->pages, b->size);
        close(b->fd);
    } else {
        free(b->pages);
    }
}
