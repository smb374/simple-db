#ifndef PAGE_H
#define PAGE_H

#include <stdbool.h>
#include <stddef.h>

#include "utils.h"

#define PAGE_SIZE 4096
#define MAGIC 0x42545245 // "BTRE"
#define VERSION 1
#define MAX_BITMAP_PAGES 64
#define MASKS_PER_PAGE (PAGE_SIZE / 4) // 1 page has PAGE_SIZE / 4 32-bit masks
#define INITIAL_PAGES (1 + MAX_BITMAP_PAGES + 1)
#define INVALID_PAGE ((u32) - 1)

struct PageBank {
    i32 fd; // Backend, -1 if in memory, else should be a valid fd to a file.
    u64 size; // Total size of the pagebank
    u32 curr_dblk; // Cache current DATA_NORMAL block page number.
    void *pages; // Pages, either by calloc/realloc (in-mem) or mmap (file-backed)
};

struct Superblock {
    u32 magic; // Magic number
    u32 version; // Version number
    u32 root_page; // Offset to root node, should be first free page.
    u32 total_pages; // Total pages in file
    u32 bitmap_pages; // Number of pages used for bitmap
    u32 page_size; // Should be PAGE_SIZE (4096)
    u32 fst_free_page; // First free page after metadata pages
    u32 curr_dblk; // Cache current DATA_NORMAL block page number.
    u32 head_dblk; // Head of the DATA_NORMAL block list by page number.
    u8 _pad[PAGE_SIZE - 36];
};
_Static_assert(sizeof(struct Superblock) == PAGE_SIZE, "Superblock should be PAGE_SIZE long");

static inline u32 *get_bitmap(struct PageBank *b) { return (u32 *) ((u8 *) b->pages + PAGE_SIZE); }

static inline bool is_page_set(struct PageBank *b, u32 page_num) {
    u32 *bitmap = get_bitmap(b);
    u32 mask_idx = page_num / 32;
    u32 bit_idx = page_num % 32;
    return (bitmap[mask_idx] >> bit_idx) & 1;
}

static inline void set_page(struct PageBank *b, u32 page_num) {
    u32 *bitmap = get_bitmap(b);
    u32 mask_idx = page_num / 32;
    u32 bit_idx = page_num % 32;
    bitmap[mask_idx] |= (1 << bit_idx);
}

static inline void unset_page(struct PageBank *b, u32 page_num) {
    u32 *bitmap = get_bitmap(b);
    u32 mask_idx = page_num / 32;
    u32 bit_idx = page_num % 32;
    bitmap[mask_idx] &= ~(1 << bit_idx);
}

static inline struct Superblock *get_superblock(struct PageBank *b) { return (struct Superblock *) b->pages; }

void *get_page(struct PageBank *b, u32 page_num);
u32 find_free_page(struct PageBank *b);
i32 resize(struct PageBank *b, u64 new_size);
u32 alloc_page(struct PageBank *b);
i32 bank_create(struct PageBank *b, i32 fd);
i32 bank_open(struct PageBank *b, const char *path);
void bank_close(struct PageBank *b);

#endif /* ifndef PAGE_H */
