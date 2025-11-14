#ifndef SLOT_H
#define SLOT_H

#include "bufpool.h"
#include "page.h"
#include "utils.h"

#define INVALID_SLOT 0xFFFF

struct SlotPage {
    struct PageHeader header; // Generic header
    u32 fsm_index; // FSM index page for updating free_space.
    u16 fsm_slot; // FSM slot
    u16 nslots;
    u16 free_offset;
    u16 frag_bytes;

    u16 slots[];
};
#define MAX_SLOT_PAGE_SIZE (PAGE_SIZE - sizeof(struct SlotPage))

struct Cell {
    u16 size;
    u8 data[];
};

struct SlotPage *slot_init(struct FrameHandle *h, u32 fsm_index, u16 fsm_slot);
struct SlotPage *slot_open(struct FrameHandle *h);
void slot_update_checksum(struct SlotPage *sh);
u16 slot_alloc(struct SlotPage *sh, u16 size);
void slot_free(struct SlotPage *sh, u16 slot);
struct Cell *slot_get(struct SlotPage *sh, u16 slot);
void slot_defrag(struct SlotPage *sh);

#endif /* ifndef SLOT_H */
