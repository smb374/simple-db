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

struct LeafVal {
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

struct LeafEnt {
    u8 key[MAX_KEY];
    struct LeafVal val;
};
typedef struct LeafEnt LeafEnt;

struct IntEnt {
    u8 key[MAX_KEY];
    u32 cpage;
};
typedef struct IntEnt IntEnt;

struct TreeNode {
    u8 type; // BNODE_INT or BNODE_LEAF
    u8 nkeys; // Number of keys, <= MAX_TN_ENTS
    u16 frag_bytes; // Fragmented bytes
    u16 ent_off; // entry offset
    u16 _pad;
    // Pointers
    u32 parent_page;
    u32 prev_page;
    u32 next_page;
    u32 head_page; // smallest branch in BNODE_INT, unused in BNODE_LEAF

    u16 slots[];
};
#define MAX_TN_ENTS ((PAGE_SIZE - sizeof(struct TreeNode)) / (2 + sizeof(struct LeafEnt))) // 30
#define MIN_TN_ENTS (MAX_TN_ENTS / 2) // 15
#define MAX_LEAF_FRAG (MIN_TN_ENTS * sizeof(struct LeafEnt))
#define MAX_INT_FRAG (MIN_TN_ENTS * sizeof(struct IntEnt))
#define TN_GET_ENT(n, s) ((void *) ((u8 *) (n) + (n)->slots[(s)]))
#define TN_GET_LENT(n, s) ((struct LeafEnt *) ((u8 *) (n) + (n)->slots[(s)]))
#define TN_GET_IENT(n, s) ((struct IntEnt *) ((u8 *) (n) + (n)->slots[(s)]))

struct BTree {
    struct GdtPageBank bank;
    u32 root_page;
};

struct BTreeHandle {
    struct GdtPageBank *bank;
    u32 root_page;
};

u32 alloc_node(struct GdtPageBank *b, u8 type, u32 hint);

// Legacy API for 1 tree per bank, currently used in test.

i32 btree_create(struct BTree *tree, i32 fd);
i32 btree_open(struct BTree *tree, const char *path);
void btree_close(struct BTree *tree);
void btree_make_handle(struct BTree *tree, struct BTreeHandle *h);

// Handle Based API

i32 btree_create_root(struct BTreeHandle *handle, struct GdtPageBank *bank);
i32 btree_create_known_root(struct BTreeHandle *handle, struct GdtPageBank *bank, u32 page);
i32 btree_search(struct BTreeHandle *handle, const u8 *key, void *value_out, u32 *len_out);
i32 btree_insert(struct BTreeHandle *handle, const u8 *key, const void *val, u32 len);
i32 btree_delete(struct BTreeHandle *handle, const u8 *key);

#endif /* ifndef BTREE_H */
