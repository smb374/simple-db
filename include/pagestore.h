#ifndef PAGESTORE_H
#define PAGESTORE_H

#include <pthread.h>
#include <stdbool.h>

#include "utils.h"

#define PAGE_SIZE 4096
#define INVALID_PAGE 0xFFFFFFFF
#define MAX_STORE_SIZE (4ULL << 40)

struct PageStore {
    i32 fd; // -1 for in-mem
    u64 store_size; // File size in bytes
    void *mmap_addr; // NULL for file IO
    pthread_mutex_t grow_lock; // Grow lock
};

// path = NULL -> in-mem mode.
struct PageStore *pstore_create(const char *path, u32 num_pages);
// return NULL when path is NULL
struct PageStore *pstore_open(const char *path, u32 *num_pages);

i32 pstore_read(struct PageStore *ps, u32 page_num, void *buf);
i32 pstore_write(struct PageStore *ps, u32 page_num, const void *buf);
i32 pstore_sync(struct PageStore *ps);
i32 pstore_grow(struct PageStore *ps, u32 num_pages);
void pstore_close(struct PageStore *ps);

#endif /* ifndef PAGESTORE_H */
