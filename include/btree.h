#ifndef BTREE_H
#define BTREE_H

// Short-hand sized integers defined here
#include "utils.h"

#define PAGE_SIZE 4096
#define MAX_KEY 128
#define HEADER_SIZE 64
#define MAGIC 0x42545245 // "BTRE"
#define VERSION 1
#define MAX_BITMAP_PAGES 64
#define MASKS_PER_PAGE (PAGE_SIZE / 4) // 1 page has PAGE_SIZE / 4 32-bit masks

enum { BNODE_INT = 0, BNODE_LEAF = 1 };

struct VPtr {
    u32 off, len;
};
typedef struct VPtr VPtr;

struct LeafEnt {
    char key[MAX_KEY];
    struct VPtr val_ptr;
};
typedef struct LeafEnt LeafEnt;

struct IntEnt {
    char key[MAX_KEY];
    u32 coff;
};
typedef struct IntEnt IntEnt;

struct NodeHeader {
    u8 type;
    u8 nkeys;
    u32 poff;
    u32 prev;
    u32 next;
    u8 _pad[50]; // Pad header to 64-bytes
};
typedef struct NodeHeader NodeHeader;

#define MAX_NODE_ENTS ((PAGE_SIZE - HEADER_SIZE) / sizeof(struct LeafEnt))

// Nodes are all 1 page sized
#define LEAF_PADDING (PAGE_SIZE - (HEADER_SIZE + sizeof(struct LeafEnt) * MAX_NODE_ENTS))
struct LeafNode {
    struct NodeHeader header;
    struct LeafEnt data[MAX_NODE_ENTS];
    u8 _pad[LEAF_PADDING];
};
typedef struct LeafNode LeafNode;

// We can technically stuff in 2 more internal entries
// but to keep things simple we just use as much as leaf uses
#define INT_PADDING (PAGE_SIZE - (HEADER_SIZE + sizeof(struct IntEnt) * MAX_NODE_ENTS + sizeof(u32)))
struct IntNode {
    struct NodeHeader header;
    struct IntEnt key_ptrs[MAX_NODE_ENTS];
    u32 tail_coff;
    u8 _pad[INT_PADDING];
};
typedef struct IntNode IntNode;

typedef u8 RawNode[PAGE_SIZE];

#define NODE_TYPE(node) (((NodeHeader *) (node))->type)
#define NODE_NKEYS(node) (((NodeHeader *) (node))->nkeys)
#define IS_LEAF(node) (NODE_TYPE(node) == BNODE_LEAF)
#define IS_FULL(node) (NODE_NKEYS(node) >= MAX_NODE_ENTS)

struct Superblock {
    u32 magic;
    u32 version;
    u32 root_offset; // Offset to root node (0 = empty tree)
    u32 total_pages; // Total pages in file
    u32 bitmap_pages; // Number of pages used for bitmap
    u32 page_size; // Should be PAGE_SIZE (4096)
    u32 first_data_page; // First page after superblock + bitmap
    u8 _pad[4096 - 28];
};

struct BTree {
    int fd;
    u64 size;
    void *mapped;
};

int btree_open(struct BTree *t, int fd);
void btree_close(struct BTree *t);

#endif /* ifndef BTREE_H */
