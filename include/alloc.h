#ifndef ALLOC_H
#define ALLOC_H

#include <stdbool.h>

#include "bufpool.h"
#include "pagestore.h"
#include "rwsxlock.h"
#include "utils.h"

#define MAGIC 0x53494D44
#define VERSION 1
#define SB_PAGE 0
#define GDT_START 1
#define GDT_PAGES 64
#define CATALOG_PAGE 65
#define HEAD_OFFSET 66

#define GROUP_SIZE 65536
#define GROUP_BITMAPS 2

#define INITIAL_PAGES (HEAD_OFFSET + GROUP_SIZE)

struct GroupDesc {
    u32 start;
    atomic_u16 free_pages;
    atomic_u16 last_set;
    u8 _pad[8]; // reserved space
};
_Static_assert(sizeof(struct GroupDesc) == 16, "GroupDesc should be 16-bytes long");

#define GDT_DESCRIPTORS (PAGE_SIZE / sizeof(struct GroupDesc))

struct SuperBlock {
    u32 magic;
    u32 version;
    u32 page_size;

    atomic_u32 total_pages;
    atomic_u32 total_groups;

    u32 gdt_start;
    u32 gdt_pages;
    u32 catalog_page;

    // Checksums using CRC-32C
    u32 gdt_checksum[GDT_PAGES];
    u32 sb_checksum;
    u32 catalog_checksum;

    u8 _pad[PAGE_SIZE - (32 + 264)];
};
_Static_assert(sizeof(struct SuperBlock) == PAGE_SIZE, "SuperBlock should be PAGE_SIZE long");
#define SB_CHKSUM_OFF 32

struct GDTPage {
    struct GroupDesc descriptors[GDT_DESCRIPTORS];
};
_Static_assert(sizeof(struct GDTPage) == PAGE_SIZE, "GDTPage should be PAGE_SIZE long");

#define BITMAPS_PER_PAGE (PAGE_SIZE / sizeof(u64))
#define BITS_PER_PAGE (BITMAPS_PER_PAGE * 64)
struct BitmapPage {
    atomic_u64 bitmap[BITMAPS_PER_PAGE];
};
_Static_assert(sizeof(struct BitmapPage) == PAGE_SIZE, "BitmapPage should be PAGE_SIZE long");

struct PageAllocator {
    struct SuperBlock *sb_cache;
    struct GDTPage *gdt_cache;

    struct BufPool *pool;
    struct PageStore *store;

    atomic_u32 last_group;
    struct RWSXLock latch;
};

struct PageAllocator *allocator_init(struct BufPool *pool, bool create);
void allocator_destroy(struct PageAllocator *pa);
u32 alloc_page(struct PageAllocator *pa, u32 hint);
void free_page(struct PageAllocator *pa, u32 page_num);

#endif /* ifndef ALLOC_H */
