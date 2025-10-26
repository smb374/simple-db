#include "btree.h"

#include <stddef.h>
#include <stdio.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "utils.h"

static inline void *btree_get_node(struct BTree *t, u32 off) {
    if (off >= t->size || off % PAGE_SIZE != 0)
        return NULL;

    return (char *) t->mapped + off;
}

static inline int ffz(u32 byt) { return ffs(~byt) - 1; }

static inline u32 *get_bitmap(struct BTree *t) { return (u32 *) ((u8 *) t->mapped + PAGE_SIZE); }

static inline int is_page_set(struct BTree *t, u32 page_num) {
    u32 *bitmap = get_bitmap(t);
    u32 mask_idx = page_num / 32;
    u32 bit_idx = page_num % 32;
    return (bitmap[mask_idx] >> bit_idx) & 1;
}

static inline void set_page(struct BTree *t, u32 page_num) {
    u32 *bitmap = get_bitmap(t);
    u32 mask_idx = page_num / 32;
    u32 bit_idx = page_num % 32;
    bitmap[mask_idx] |= (1 << bit_idx);
}

static inline void unset_page(struct BTree *t, u32 page_num) {
    u32 *bitmap = get_bitmap(t);
    u32 mask_idx = page_num / 32;
    u32 bit_idx = page_num % 32;
    bitmap[mask_idx] &= ~(1 << bit_idx);
}

static u32 find_free_page(struct BTree *t) {
    struct Superblock *sb = (struct Superblock *) t->mapped;
    u32 *bitmap = get_bitmap(t);

    u32 start_page = sb->first_data_page;
    u32 start_mask = start_page / 32;
    u32 max_masks = sb->bitmap_pages * MASKS_PER_PAGE;

    for (u32 i = start_mask; i < max_masks; i++) {
        int bit = ffz(bitmap[i]);
        if (bit >= 0) {
            u32 page = i * 32 + bit;
            if (page >= start_page && page < sb->total_pages) {
                return page;
            }
        }
    }

    return 0; // No free pages
}

static int grow_bitmap(struct BTree *t) {
    struct Superblock *sb = (struct Superblock *) t->mapped;
    u32 new_bitmap_page = sb->bitmap_pages++;
    if (sb->bitmap_pages >= MAX_BITMAP_PAGES) {
        return -1; // Too big for single file.
    }
    sb->first_data_page = 1 + sb->bitmap_pages;
    set_page(t, new_bitmap_page);
    return 0;
}

// Use fd=-1 for in-memory BTree for testing.
int btree_open(struct BTree *t, int fd) {
    u32 initial_pages = 1 + MAX_BITMAP_PAGES + 16;
    if (fd == -1) {
        t->fd = -1;
        t->size = PAGE_SIZE * initial_pages;
        t->mapped = mmap(NULL, t->size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    } else {
        t->fd = fd;

        struct stat sb;
        fstat(fd, &sb);
        t->size = sb.st_size;
        if (!t->size) {
            t->size = PAGE_SIZE * initial_pages;
            ftruncate(fd, t->size);
        }

        t->mapped = mmap(NULL, t->size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, t->fd, 0);
    }

    if (t->mapped == MAP_FAILED) {
        perror("mmap()");
        if (t->fd != -1) {
            close(t->fd);
        }

        return -1;
    }

    struct Superblock *sb = (struct Superblock *) t->mapped;
    sb->magic = MAGIC;
    sb->version = VERSION;
    sb->bitmap_pages = MAX_BITMAP_PAGES;
    sb->first_data_page = 1 + MAX_BITMAP_PAGES;
    sb->total_pages = initial_pages;
    sb->page_size = PAGE_SIZE;
    sb->root_offset = sb->first_data_page * PAGE_SIZE;

    u32 *bitmaps = get_bitmap(t);
    for (u32 i = 0; i <= sb->first_data_page; i++) {
        u32 mask_idx = i / 32;
        u32 bit_idx = i % 32;
        bitmaps[mask_idx] |= (1 << bit_idx);
    }

    LeafNode *root = (LeafNode *) ((char *) t->mapped + sb->root_offset);
    root->header.type = BNODE_LEAF;
    root->header.nkeys = 0;
    root->header.poff = 0;
    root->header.prev = 0;
    root->header.next = 0;

    if (t->fd != -1) {
        msync(t->mapped, t->size, MS_SYNC);
    }

    return 0;
}

void btree_close(struct BTree *t) {
    if (t->fd != -1) {
        msync(t->mapped, t->size, MS_SYNC);
        munmap(t->mapped, t->size);
        close(t->fd);
    } else {
        munmap(t->mapped, t->size);
    }
}
