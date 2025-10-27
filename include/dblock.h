#ifndef DBLOCK_H
#define DBLOCK_H

#include "page.h"
#include "utils.h"

#define MAX_INLINE 63
#define MAX_NORMAL 4000

enum {
    DATA_INLINE = 0, // Value is inlined inside the leaf node.
    DATA_NORMAL = 1,
    DATA_HUGE = 2,
};

struct VPtr {
    u32 page; // data page
    // For DATA_NORMAL block: | 16-bit slot | 16-bit len |
    // For DATA_HUGE block:   | 32-bit len               |
    u32 info;
};
typedef struct VPtr VPtr;

// Helper macros for VPtr encoding
#define VPTR_MAKE_NORMAL(page, slot, len) ((struct VPtr) {(page), ((u32) (slot) << 16) | (len)})
#define VPTR_MAKE_HUGE(page, len) ((struct VPtr) {(page), (len)})
#define VPTR_INVALID ((struct VPtr) {INVALID_PAGE, INVALID_PAGE})
#define VPTR_GET_SLOT(vptr) ((u16) ((vptr).info >> 16))
#define VPTR_GET_LEN(vptr) ((u16) ((vptr).info & 0xFFFF))
#define VPTR_GET_HUGE_LEN(vptr) ((vptr).info)

struct DataBlockMeta {
    u8 block_type;
    u8 _pad[3];
    u32 next_page;
}; // 8 bytes

struct DataBlockNormal {
    struct DataBlockMeta meta;
    u32 prev_page;
    u16 num_slots;
    u16 cell_off;
    u16 frag_bytes;
    u16 _pad;

    u16 slots[];
};

struct Cell {
    u16 size;
    u8 data[];
};

struct DataBlockHuge {
    struct DataBlockMeta meta;

    u8 data[];
};
#define DATA_HUGE_SPACE (PAGE_SIZE - sizeof(struct DataBlockHuge))

struct VPtr write_huge_data(struct PageBank *b, void *data, u32 len);

#endif /* ifndef DBLOCK_H */
