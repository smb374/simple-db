#include "pagestore.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "utils.h"

static struct PageStore *pstore_create_mem(u32 num_pages) {
    // mmap a 4TB virtual address with PROT_NONE
    // Shouldn't use actual memory
    errno = 0;
    void *addr_start = mmap(NULL, MAX_STORE_SIZE, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (addr_start == MAP_FAILED) {
        logger(stderr, "ERROR", "[pstore_create_mem] mmap failed: %s\n", strerror(errno));
        return NULL;
    }

    u64 size = (u64) num_pages * PAGE_SIZE;

    errno = 0;
    if (mprotect(addr_start, size, PROT_READ | PROT_WRITE) < 0) {
        logger(stderr, "ERROR", "[pstore_create_mem] initial mprotect failed: %s\n", strerror(errno));
        munmap(addr_start, MAX_STORE_SIZE); // Clean up on failure
        return NULL;
    }

    struct PageStore *ps = calloc(1, sizeof(struct PageStore));
    ps->fd = -1;
    ps->store_size = size;
    ps->mmap_addr = addr_start;
    pthread_mutex_init(&ps->grow_lock, NULL);

    return ps;
}

static i32 pstore_grow_mem(struct PageStore *ps, u32 num_pages) {
    u64 size = (u64) num_pages * PAGE_SIZE;

    pthread_mutex_lock(&ps->grow_lock);
    if (ps->store_size > MAX_STORE_SIZE - size) {
        pthread_mutex_unlock(&ps->grow_lock);
        return -1;
    }

    errno = 0;
    if (mprotect((u8 *) ps->mmap_addr + ps->store_size, size, PROT_READ | PROT_WRITE) < 0) {
        pthread_mutex_unlock(&ps->grow_lock);
        logger(stderr, "ERROR", "[pstore_grow_mem] mprotect failed: %s\n", strerror(errno));
        return -1;
    }

    ps->store_size += size;
    pthread_mutex_unlock(&ps->grow_lock);
    return 0;
}

static struct PageStore *pstore_create_file(const char *path, u32 num_pages) {
    u64 size = (u64) num_pages * PAGE_SIZE;
    i32 fd;

    errno = 0;
    if ((fd = open_relative(path, O_RDWR | O_CREAT, 0644)) < 0) {
        goto FAIL;
    }
    if (ftruncate(fd, (off_t) size) < 0) {
        logger(stderr, "ERROR", "[pstore_create_file] Failed to truncate file at path '%s': %s\n", path,
               strerror(errno));
        goto C_FD;
    }

    struct PageStore *ps = calloc(1, sizeof(struct PageStore));
    ps->fd = fd;
    ps->store_size = size;
    ps->mmap_addr = NULL;
    pthread_mutex_init(&ps->grow_lock, NULL);

    return ps;

C_FD:
    close(fd);
FAIL:
    return NULL;
}

static i32 pstore_grow_file(struct PageStore *ps, u32 num_pages) {
    pthread_mutex_lock(&ps->grow_lock);
    u64 nsize = ps->store_size + (u64) num_pages * PAGE_SIZE;

    if (ftruncate(ps->fd, (off_t) nsize) < 0) {
        pthread_mutex_unlock(&ps->grow_lock);
        logger(stderr, "ERROR", "[pstore_create_file] Failed to truncate file: %s\n", strerror(errno));
        return -1;
    }

    ps->store_size = nsize;
    pthread_mutex_unlock(&ps->grow_lock);
    return 0;
}

struct PageStore *pstore_create(const char *path, u32 num_pages) {
    if (!path) {
        return pstore_create_mem(num_pages);
    } else {
        return pstore_create_file(path, num_pages);
    }
}

struct PageStore *pstore_open(const char *path, u32 *num_pages) {
    if (!path || !num_pages)
        return NULL;

    struct stat sb;
    i32 fd;

    errno = 0;
    if ((fd = open_relative(path, O_RDWR, 0644)) < 0) {
        goto FAIL;
    }
    if (fstat(fd, &sb) < 0) {
        logger(stderr, "ERROR", "[pstore_open] Failed to get file stat: %s\n", strerror(errno));
        goto C_FD;
    }

    *num_pages = sb.st_size / PAGE_SIZE;

    struct PageStore *ps = calloc(1, sizeof(struct PageStore));
    ps->fd = fd;
    ps->store_size = sb.st_size;
    ps->mmap_addr = NULL;
    pthread_mutex_init(&ps->grow_lock, NULL);

    return ps;

C_FD:
    close(fd);
FAIL:
    return NULL;
}

i32 pstore_sync(struct PageStore *ps) {
    if (ps->fd != -1) {
        for (;;) {
            errno = 0;
            if (fdatasync(ps->fd) < 0) {
                if (errno == EINTR) {
                    continue;
                }
                logger(stderr, "ERROR", "[pstore_sync] Failed to run fdatasync: %s\n", strerror(errno));
                return -1;
            }
            break;
        }
    }
    return 0;
}

i32 pstore_read(struct PageStore *ps, u32 page_num, void *buf) {
    u64 start = (u64) page_num * PAGE_SIZE;
    if (start >= ps->store_size) {
        return -1;
    }

    if (ps->fd != -1) {
        for (;;) {
            errno = 0;
            if (pread(ps->fd, buf, PAGE_SIZE, (off_t) start) < 0) {
                if (errno == EINTR) {
                    continue;
                }
                logger(stderr, "ERROR", "[pstore_read] Failed to read data with pread: %s\n", strerror(errno));
                return -1;
            }
            break;
        }
    } else {
        memcpy(buf, (u8 *) ps->mmap_addr + start, PAGE_SIZE);
    }
    return 0;
}

i32 pstore_write(struct PageStore *ps, u32 page_num, const void *buf) {
    u64 start = (u64) page_num * PAGE_SIZE;
    if (start >= ps->store_size) {
        return -1;
    }

    if (ps->fd != -1) {
        for (;;) {
            errno = 0;
            if (pwrite(ps->fd, buf, PAGE_SIZE, (off_t) start) < 0) {
                if (errno == EINTR) {
                    continue;
                }
                logger(stderr, "ERROR", "[pstore_write] Failed to write data with pwrite: %s\n", strerror(errno));
                return -1;
            }
            break;
        }
    } else {
        memcpy((u8 *) ps->mmap_addr + start, buf, PAGE_SIZE);
    }
    return 0;
}

i32 pstore_grow(struct PageStore *ps, u32 num_pages) {
    if (!ps)
        return -1;
    if (!num_pages)
        return 0;

    if (ps->fd != -1) {
        return pstore_grow_file(ps, num_pages);
    } else {
        return pstore_grow_mem(ps, num_pages);
    }
}

void pstore_close(struct PageStore *ps) {
    if (ps->fd != -1) {
        pstore_sync(ps);
        close(ps->fd);
    } else {
        assert(ps->mmap_addr != NULL);
        munmap(ps->mmap_addr, MAX_STORE_SIZE);
    }
    pthread_mutex_destroy(&ps->grow_lock);
    free(ps);
}
