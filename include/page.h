#ifndef PAGE_H
#define PAGE_H

#include <stdbool.h>

#include "pagestore.h"
#include "utils.h"

// Embed in page
struct PageHeader {
    u32 checksum; // CRC-32C
};

static inline void compute_checksum(struct PageHeader *header) {
    u8 *page_data = (u8 *) header + sizeof(u32);
    u32 checksum = crc32c(page_data, PAGE_SIZE - sizeof(u32));
    header->checksum = checksum;
}

static inline bool verify_checksum(struct PageHeader *header) {
    u8 *page_data = (u8 *) header + sizeof(u32);
    u32 checksum = crc32c(page_data, PAGE_SIZE - sizeof(u32));
    return header->checksum == checksum;
}

#endif /* ifndef PAGE_H */
