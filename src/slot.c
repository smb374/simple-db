#include "slot.h"

#include <string.h>

#include "bufpool.h"
#include "page.h"
#include "pagestore.h"
#include "utils.h"

static u16 slot_free_space(struct SlotPage *sh) { return sh->free_offset - (sizeof(struct SlotPage) + sh->nslots * 2); }

struct SlotPage *slot_init(struct FrameHandle *h, u32 fsm_index, u16 fsm_slot) {
    if (!h)
        return NULL;
    struct SlotPage *sh = (void *) h->fdata->data;
    sh->nslots = 0;
    sh->frag_bytes = 0;
    sh->free_offset = PAGE_SIZE;
    sh->fsm_index = fsm_index;
    sh->fsm_slot = fsm_slot;
    compute_checksum(&sh->header);

    return sh;
}

struct SlotPage *slot_open(struct FrameHandle *h, u32 fsm_index, u16 fsm_slot) {
    if (!h)
        return NULL;
    struct SlotPage *sh = (void *) h->fdata->data;
    if (!verify_checksum(&sh->header) || sh->fsm_index != fsm_index || sh->fsm_slot != fsm_slot) {
        return NULL;
    }

    return sh;
}

void slot_update_checksum(struct SlotPage *sh) { compute_checksum(&sh->header); }

u16 slot_alloc(struct SlotPage *sh, u16 size) {
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

    struct Cell *cell = (struct Cell *) ((u8 *) sh + sh->slots[slot]);
    cell->size = size;

    return slot;
}

void slot_free(struct SlotPage *sh, u16 slot) {
    if (slot >= sh->nslots)
        return;

    struct Cell *cell = (struct Cell *) ((u8 *) sh + sh->slots[slot]);
    sh->slots[slot] = INVALID_SLOT;
    sh->frag_bytes += cell->size + 2;
}

struct Cell *slot_get(struct SlotPage *sh, u16 slot) {
    if (slot >= sh->nslots || sh->slots[slot] == INVALID_SLOT)
        return NULL;

    return (struct Cell *) ((u8 *) sh + sh->slots[slot]);
}

void slot_defrag(struct SlotPage *sh) {
    u8 tmp[PAGE_SIZE];
    memcpy(tmp, sh, PAGE_SIZE);

    const struct SlotPage *src = (void *) tmp;
    sh->frag_bytes = 0;
    sh->free_offset = PAGE_SIZE;

    for (u16 i = 0; i < src->nslots; i++) {
        if (src->slots[i] == INVALID_SLOT) {
            continue;
        }

        struct Cell *scell = (struct Cell *) ((u8 *) src + src->slots[i]);
        u16 size = 2 + scell->size;
        sh->free_offset -= size;
        sh->slots[i] = sh->free_offset;
        struct Cell *cell = (struct Cell *) ((u8 *) sh + sh->slots[i]);
        cell->size = scell->size;
        memcpy(cell->data, scell->data, cell->size);
    }
}
