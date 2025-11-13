#include "slot.h"

#include <string.h>

#include "pagestore.h"
#include "utils.h"

static u16 slot_free_space(struct SlotHeap *sh) {
    return sh->free_offset - (sizeof(struct SlotHeap) + sh->nslots * 2 + sh->frag_bytes);
}

void slot_init(struct SlotHeap *sh, u16 start) {
    sh->start = start;
    sh->nslots = 0;
    sh->frag_bytes = 0;
    sh->free_offset = PAGE_SIZE;
}

u16 slot_alloc(struct SlotHeap *sh, u16 size) {
    if (sh->frag_bytes >= PAGE_SIZE / 4) {
        slot_defrag(sh);
    }

    if (slot_free_space(sh) < size + 4) {
        return INVALID_SLOT;
    }

    u16 slot = INVALID_SLOT;
    for (u16 i = 0; i < sh->nslots; i++) {
        if (sh->slots[i] == INVALID_SLOT) {
            slot = i;
        }
    }
    if (slot == INVALID_SLOT) {
        slot = sh->nslots++;
    }

    sh->free_offset -= size + 2;
    sh->slots[slot] = sh->free_offset;

    struct Cell *cell = (struct Cell *) ((u8 *) sh - sh->start + sh->slots[slot]);
    cell->size = size;

    return slot;
}

void slot_free(struct SlotHeap *sh, u16 slot) {
    if (slot >= sh->nslots)
        return;

    struct Cell *cell = (struct Cell *) ((u8 *) sh - sh->start + sh->slots[slot]);
    sh->slots[slot] = INVALID_SLOT;
    sh->frag_bytes += cell->size + 2;
}

struct Cell *slot_get(struct SlotHeap *sh, u16 slot) {
    if (slot >= sh->nslots || sh->slots[slot] == INVALID_SLOT)
        return NULL;

    return (struct Cell *) ((u8 *) sh - sh->start + sh->slots[slot]);
}

void slot_defrag(struct SlotHeap *sh) {
    u8 tmp[PAGE_SIZE];
    memcpy(&tmp[sh->start], sh, PAGE_SIZE - sh->start);

    const struct SlotHeap *src = (void *) &tmp[sh->start];
    sh->frag_bytes = 0;
    sh->free_offset = PAGE_SIZE;

    for (u16 i = 0; i < src->nslots; i++) {
        if (src->slots[i] == INVALID_SLOT) {
            continue;
        }

        struct Cell *scell = (struct Cell *) ((u8 *) src - src->start + src->slots[i]);
        u16 size = 2 + scell->size;
        sh->free_offset -= size;
        sh->slots[i] = sh->free_offset;
        struct Cell *cell = (struct Cell *) ((u8 *) sh - sh->start + sh->slots[i]);
        cell->size = scell->size;
        memcpy(cell->data, scell->data, cell->size);
    }
}
