#ifndef GDT_PAGE_H
#define GDT_PAGE_H

#include <stdbool.h>

#include "utils.h"

#define PAGE_SIZE 4096
#define MAGIC 0x42545245 // "BTRE"
#define VERSION 1
#define MAX_GDTS 64
#define GROUP_BITMAPS 2
#define GROUP_SIZE 65536
#define SCHEMA_PAGE 1
#define GDT_START 2
#define HEAD_OFFSET (GDT_START + MAX_GDTS) // 1 for SB, 1 for schema, 1 for index catalog
#define MASKS_PER_PAGE (PAGE_SIZE / 4) // 1 page has PAGE_SIZE / 4 32-bit masks
#define INITIAL_PAGES (HEAD_OFFSET + GROUP_SIZE)
#define INVALID_PAGE ((u32) - 1)

struct GdtPageBank {
    i32 fd; // Backend, -1 if in memory, else should be a valid fd to a file.
    u32 size; // Total size of the pagebank
    u32 curr_dblk; // Cache current DATA_NORMAL block page number.
    void *pages; // Pages, either by calloc/realloc (in-mem) or mmap (file-backed)
};

struct GdtSuperblock {
    u32 magic; // Magic number
    u32 version; // Version number
    u32 page_size; // Should be PAGE_SIZE (4096)
    u32 gdt_pages; // Number of GDTs, should be MAX_GDTS
    u32 total_pages; // Total pages in file
    u32 total_groups; // Total groups in file
    u32 _root_page;
    u32 curr_dblk; // Cache current DATA_NORMAL block page number.
    u32 head_dblk; // Head of the DATA_NORMAL block list by page number.
    u8 _pad[PAGE_SIZE - 36];
};
_Static_assert(sizeof(struct GdtSuperblock) == PAGE_SIZE, "Superblock should be PAGE_SIZE long");

// 1 Group is 65536 pages, with first 2 pages used for bitmap
struct GroupDescriptor {
    u32 group_start; // Start page of the group
    u16 free_pages; // Number of free pages, < 65536
    u16 last_set_mask; // Last set/unset mask in the bitmaps
    u8 _pad[8];
};
_Static_assert(sizeof(struct GroupDescriptor) == 16, "GroupDescriptor should be 16-byte long");
#define GDT_SIZE_PER_PAGE (PAGE_SIZE / sizeof(struct GroupDescriptor))

static inline struct GdtSuperblock *gdt_get_superblock(struct GdtPageBank *b) {
    return (struct GdtSuperblock *) b->pages;
}

static inline struct GroupDescriptor *get_gdt(struct GdtPageBank *b) {
    return (struct GroupDescriptor *) ((u8 *) b->pages + GDT_START * PAGE_SIZE);
}

static inline bool gdt_is_page_set(struct GdtPageBank *b, const u32 page) {
    u32 rpage = page - HEAD_OFFSET;
    u32 gidx = rpage / GROUP_SIZE;
    u32 pidx = rpage % GROUP_SIZE;

    struct GroupDescriptor *gdt = get_gdt(b);
    u32 gpage = gdt[gidx].group_start;
    u32 *bitmap = (u32 *) ((u8 *) b->pages + PAGE_SIZE * gpage);
    u32 mask_idx = pidx / 32;
    u32 bit_idx = pidx % 32;

    return (bitmap[mask_idx] >> bit_idx) & 1;
}

static inline void gdt_set_page(struct GdtPageBank *b, const u32 page) {
    u32 rpage = page - HEAD_OFFSET;
    u32 gidx = rpage / GROUP_SIZE;
    u32 pidx = rpage % GROUP_SIZE;

    struct GroupDescriptor *gdt = get_gdt(b);
    u32 gpage = gdt[gidx].group_start;
    u32 *bitmap = (u32 *) ((u8 *) b->pages + PAGE_SIZE * gpage);
    u32 mask_idx = pidx / 32;
    u32 bit_idx = pidx % 32;
    gdt[gidx].last_set_mask = mask_idx;

    bitmap[mask_idx] |= (1 << bit_idx);
    gdt[gidx].free_pages--;
}

static inline void gdt_unset_page(struct GdtPageBank *b, const u32 page) {
    u32 rpage = page - HEAD_OFFSET;
    u32 gidx = rpage / GROUP_SIZE;
    u32 pidx = rpage % GROUP_SIZE;

    struct GroupDescriptor *gdt = get_gdt(b);
    u32 gpage = gdt[gidx].group_start;
    u32 *bitmap = (u32 *) ((u8 *) b->pages + PAGE_SIZE * gpage);
    u32 mask_idx = pidx / 32;
    u32 bit_idx = pidx % 32;
    gdt[gidx].last_set_mask = mask_idx;

    bitmap[mask_idx] &= ~(1 << bit_idx);
    gdt[gidx].free_pages++;
}

void *gdt_get_page(struct GdtPageBank *b, u32 page_num);
i32 gdt_bank_create(struct GdtPageBank *b, i32 fd);
i32 gdt_bank_open(struct GdtPageBank *b, const char *path);
void gdt_bank_close(struct GdtPageBank *b);
u32 gdt_alloc_page(struct GdtPageBank *b, u32 hint);

#endif /* ifndef GDT_PAGE_H */
