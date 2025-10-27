#include "btree.h"

#include <fcntl.h>
#include <stddef.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "page.h"

// Use fd=-1 to create in-memory BTree for testing.
int btree_create(struct BTree *t, int fd) {
    struct PageBank *b = &t->bank;
    bank_create(b, fd);
    struct Superblock *sb = get_superblock(b);

    LeafNode *root = (LeafNode *) ((char *) b->pages + sb->root_offset);
    root->header.type = BNODE_LEAF;
    root->header.nkeys = 0;
    root->header.poff = 0;
    root->header.prev = 0;
    root->header.next = 0;

    if (b->fd != -1) {
        msync(b->pages, b->size, MS_SYNC);
    }

    return 0;
}

int btree_open(struct BTree *t, const char *path) {
    bank_open(&t->bank, path);

    return 0;
}

void btree_close(struct BTree *t) { bank_close(&t->bank); }
