#ifndef CATALOG_H
#define CATALOG_H

#include "page.h"
#include "utils.h"

#define INVALID_SLOT 0xFFFF
#define FSM_SLOTS 800

struct FSMIndexPage {
    struct PageHeader header;
    u32 next_page;
    u16 nslots;

    u8 free_space[FSM_SLOTS];
    u32 data_page[FSM_SLOTS];
};

#endif /* ifndef CATALOG_H */
