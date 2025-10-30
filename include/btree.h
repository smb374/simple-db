#ifndef BTREE_H
#define BTREE_H

// Short-hand sized integers defined here
#include "dblock.h"
#include "gdt_page.h"
#include "utils.h"

#define MAX_KEY 64
#define MAX_INLINE 63
#define MAX_NORMAL 4000
#define NODE_HEADER_SIZE 64

enum {
    BNODE_INT = 0,
    BNODE_LEAF = 1,
};

struct LeafEnt {
    u8 key[MAX_KEY];
    u8 val_type;
    u8 _pad[3];
    union {
        struct {
            u8 len;
            u8 data[MAX_INLINE];
        } ival;
        struct VPtr ptr;
    };
};
typedef struct LeafEnt LeafEnt;

struct IntEnt {
    u8 key[MAX_KEY];
    u32 cpage;
};
typedef struct IntEnt IntEnt;

struct NodeHeader {
    u8 type;
    u8 nkeys;
    u16 _pad1;
    u32 parent_page;
    u32 prev_page;
    u32 next_page;
    u8 _pad2[NODE_HEADER_SIZE - 16]; // Pad header to 64-bytes
};
typedef struct NodeHeader NodeHeader;
_Static_assert(sizeof(struct NodeHeader) == NODE_HEADER_SIZE, "NoddeHeeader should be NODE_HEADER_SIZE long");

#define MAX_NODE_ENTS ((PAGE_SIZE - NODE_HEADER_SIZE) / sizeof(struct LeafEnt))
#define MIN_NODE_ENTS (MAX_NODE_ENTS / 2)

// Nodes are all 1 page sized
#define LEAF_PADDING (PAGE_SIZE - (NODE_HEADER_SIZE + sizeof(struct LeafEnt) * MAX_NODE_ENTS))
struct LeafNode {
    struct NodeHeader header;
    struct LeafEnt entries[MAX_NODE_ENTS];
    u8 _pad[LEAF_PADDING];
};
typedef struct LeafNode LeafNode;
_Static_assert(sizeof(struct LeafNode) == PAGE_SIZE, "LeafNode should be PAGE_SIZE long");

// We can technically stuff in 2 more internal entries
// but to keep things simple we just use as much as leaf uses
#define INT_PADDING (PAGE_SIZE - (NODE_HEADER_SIZE + sizeof(struct IntEnt) * MAX_NODE_ENTS + sizeof(u32)))
struct IntNode {
    struct NodeHeader header;
    u32 head_page;
    struct IntEnt entries[MAX_NODE_ENTS];
    u8 _pad[INT_PADDING];
};
typedef struct IntNode IntNode;
_Static_assert(sizeof(struct IntNode) == PAGE_SIZE, "IntNode should be PAGE_SIZE long");

#define NODE_TYPE(node) (((NodeHeader *) (node))->type)
#define NODE_NKEYS(node) (((NodeHeader *) (node))->nkeys)
#define IS_LEAF(node) (NODE_TYPE(node) == BNODE_LEAF)
#define IS_FULL(node) (NODE_NKEYS(node) >= MAX_NODE_ENTS)

struct BTree {
    struct GdtPageBank bank;
};

int btree_create(struct BTree *tree, i32 fd);
int btree_open(struct BTree *tree, const char *path);
void btree_close(struct BTree *tree);

i32 btree_search(struct BTree *tree, const u8 *key, void *value_out, u32 *len_out);
i32 btree_insert(struct BTree *tree, const u8 *key, const void *val, u32 len);
i32 btree_delete(struct BTree *tree, const u8 *key);

#endif /* ifndef BTREE_H */
