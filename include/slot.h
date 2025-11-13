#ifndef SLOT_H
#define SLOT_H

#include "utils.h"

#define INVALID_SLOT 0xFFFF

struct SlotHeap {
    u16 start;
    u16 nslots;
    u16 free_offset;
    u16 frag_bytes;

    u16 slots[];
};

struct Cell {
    u16 size;
    u8 data[];
};

void slot_init(struct SlotHeap *sh, u16 start);
u16 slot_alloc(struct SlotHeap *sh, u16 size);
void slot_free(struct SlotHeap *sh, u16 slot);
struct Cell *slot_get(struct SlotHeap *sh, u16 slot);
void slot_defrag(struct SlotHeap *sh);

#endif /* ifndef SLOT_H */
