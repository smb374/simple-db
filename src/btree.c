#include "btree.h"

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <sys/mman.h>

#include "dblock.h"
#include "page.h"
#include "stdio.h"
#include "utils.h"

static inline i32 key_cmp(const u8 *k1, const u8 *k2) { return memcmp(k1, k2, MAX_KEY); }

static u8 binary_search(const u8 *keys, u8 num_keys, const u8 *key, size_t stride, bool *exact_match) {
    u8 left = 0, right = num_keys;

    while (left < right) {
        u8 mid = left + (right - left) / 2;
        const u8 *mid_key = keys + mid * stride;

        i32 res = key_cmp(mid_key, key);
        if (res < 0) {
            left = mid + 1;
        } else {
            if (!res && exact_match) {
                *exact_match = true;
            }
            right = mid;
        }
    }

    return left;
}

// Find slot in leaf node
static u8 leaf_find_slot(struct LeafNode *leaf, const u8 *key, bool *exact_match) {
    return binary_search((const u8 *) leaf->entries, leaf->header.nkeys, key, sizeof(struct LeafEnt), exact_match);
}

// Find child in internal node
static u8 internal_find_child(struct IntNode *node, const u8 *key, bool *exact_match) {
    return binary_search((const u8 *) node->entries, node->header.nkeys, key, sizeof(struct IntEnt), exact_match);
}

static i32 read_entry_value(struct BTree *tree, struct LeafEnt *entry, void *value_out, u32 *len_out) {
    i32 ret;
    u8 len;
    switch (entry->val_type) {
        case DATA_INLINE:
            len = entry->ival.len;
            if (len_out)
                *len_out = len;
            if (value_out)
                memcpy(value_out, entry->ival.data, len);
            break;
        case DATA_NORMAL:
            ret = read_normal_data(&tree->bank, value_out, entry->ptr);
            if (ret < 0)
                return -1;
            if (len_out)
                *len_out = VPTR_GET_LEN(entry->ptr);
            break;
        default:
            ret = read_huge_data(&tree->bank, value_out, entry->ptr);
            if (ret < 0)
                return -1;
            if (len_out)
                *len_out = VPTR_GET_HUGE_LEN(entry->ptr);
            break;
    }
    return 0;
}

static i32 write_entry_value(struct BTree *tree, struct LeafEnt *entry, const void *value, u32 len) {
    if (len <= MAX_INLINE) {
        entry->val_type = DATA_INLINE;
        entry->ival.len = len;
        memcpy(entry->ival.data, value, len);
        return 0;
    } else if (len <= MAX_NORMAL) {
        entry->val_type = DATA_NORMAL;
        entry->ptr = write_normal_data(&tree->bank, (void *) value, len);
        if (entry->ptr.page == INVALID_PAGE)
            return -1;
        return 0;
    } else {
        entry->val_type = DATA_HUGE;
        entry->ptr = write_huge_data(&tree->bank, (void *) value, len);
        if (entry->ptr.page == INVALID_PAGE)
            return -1;
        return 0;
    }
}

static void delete_entry_value(struct BTree *tree, struct LeafEnt *entry) {
    switch (entry->val_type) {
        case DATA_INLINE:
            memset(&entry->ival, 0, MAX_INLINE + 1);
            break;
        case DATA_NORMAL:
            delete_normal_data(&tree->bank, entry->ptr);
            break;
        default:
            delete_huge_data(&tree->bank, entry->ptr);
            break;
    }
}

static u32 alloc_node(struct PageBank *b, u8 type) {
    u32 page = alloc_page(b);
    struct NodeHeader *header = get_page(b, page);
    header->type = type;
    header->nkeys = 0;
    header->parent_page = INVALID_PAGE;
    header->prev_page = INVALID_PAGE;
    header->next_page = INVALID_PAGE;

    return page;
}

// Use fd=-1 to create in-memory BTree for testing.
i32 btree_create(struct BTree *tree, i32 fd) {
    struct PageBank *b = &tree->bank;
    bank_create(b, fd);
    struct Superblock *sb = get_superblock(b);

    struct LeafNode *root = get_page(b, sb->root_page);
    memset(root, 0, sizeof(struct LeafNode));
    root->header.type = BNODE_LEAF;
    root->header.nkeys = 0;
    root->header.parent_page = INVALID_PAGE;
    root->header.prev_page = INVALID_PAGE;
    root->header.next_page = INVALID_PAGE;

    if (b->fd != -1) {
        msync(b->pages, b->size, MS_SYNC);
    }

    return 0;
}

i32 btree_open(struct BTree *tree, const char *path) {
    bank_open(&tree->bank, path);

    return 0;
}

void btree_close(struct BTree *tree) { bank_close(&tree->bank); }

i32 btree_search(struct BTree *tree, const u8 *key, void *value_out, u32 *len_out) {
    struct Superblock *sb = get_superblock(&tree->bank);
    u32 page_num = sb->root_page;

    if (page_num == INVALID_PAGE) {
        return -1; // Empty tree
    }

    // Traverse down to leaf
    for (;;) {
        void *page = get_page(&tree->bank, page_num);
        if (!page)
            return -1;

        struct NodeHeader *header = (struct NodeHeader *) page;

        if (header->type == BNODE_LEAF) {
            // Found leaf, search for key
            struct LeafNode *leaf = page;
            bool exact_match = false;
            u8 slot = leaf_find_slot(leaf, key, &exact_match);

            if (slot >= leaf->header.nkeys || !exact_match) {
                return -1; // Key not found
            }
            return read_entry_value(tree, &leaf->entries[slot], value_out, len_out);

        } else { // BNODE_INTERNAL
            struct IntNode *node = page;
            bool exact_match = false;
            u8 cidx = internal_find_child(node, key, &exact_match);

            if (cidx < node->header.nkeys && exact_match) {
                cidx++;
            }

            if (cidx < node->header.nkeys) {
                page_num = node->entries[cidx].cpage;
            } else {
                page_num = node->tail_page;
            }
        }
    }
}

// Insert
static i32 split_leaf(struct BTree *tree, u32 lpage, const u8 *key, const void *val, u32 len, u8 *pkey_out,
                      u32 *npage_out);
static i32 split_internal(struct BTree *tree, u32 ipage, const u8 *key, u32 lpage, u32 rpage, u8 *pkey_out,
                          u32 *npage_out);
static i32 insert_into_parent(struct BTree *tree, const u8 *key, u32 lpage, u32 rpage);

// key should be put in a MAX_KEY buf before passing in.
i32 btree_insert(struct BTree *tree, const u8 *key, const void *val, u32 len) {
    struct Superblock *sb = get_superblock(&tree->bank);

    // Find leaf node for insertion
    u32 page_num = sb->root_page;
    u32 parent_stack[32]; // Stack to track path (max height ~32)
    u16 stack_size = 0;

    for (;;) {
        void *page = get_page(&tree->bank, page_num);
        if (!page)
            return -1;

        struct NodeHeader *header = page;
        if (header->type == BNODE_LEAF)
            break;

        parent_stack[stack_size++] = page_num;
        struct IntNode *node = page;
        bool exact_match = false;
        u8 cidx = internal_find_child(node, key, &exact_match);

        if (cidx < node->header.nkeys && exact_match) {
            cidx++;
        }

        if (cidx < node->header.nkeys) {
            page_num = node->entries[cidx].cpage;
        } else {
            page_num = node->tail_page;
        }
    }

    struct LeafNode *leaf = get_page(&tree->bank, page_num);
    bool exact_match = false;
    u8 slot = leaf_find_slot(leaf, key, &exact_match);

    if (slot < leaf->header.nkeys && exact_match) {
        // Key exists - update value
        delete_entry_value(tree, &leaf->entries[slot]);
        return write_entry_value(tree, &leaf->entries[slot], val, len);
    }

    if (leaf->header.nkeys < MAX_NODE_ENTS) {
        // Insert into leaf (shift entries to make space)
        memmove(&leaf->entries[slot + 1], &leaf->entries[slot], (leaf->header.nkeys - slot) * sizeof(struct LeafEnt));
        memcpy(leaf->entries[slot].key, key, MAX_KEY);
        i32 ret = write_entry_value(tree, &leaf->entries[slot], val, len);
        if (ret < 0)
            return ret;

        leaf->header.nkeys++;
        return 0;
    }

    u8 pkey[MAX_KEY];
    u32 new_page;

    if (split_leaf(tree, page_num, key, val, len, pkey, &new_page) < 0) {
        return -1;
    }

    return insert_into_parent(tree, pkey, page_num, new_page);
}

static i32 split_leaf(struct BTree *tree, u32 lpage, const u8 *key, const void *val, const u32 len, u8 *pkey_out,
                      u32 *npage_out) {
    struct LeafEnt tmp[MAX_NODE_ENTS + 1];

    struct LeafNode *lleaf = get_page(&tree->bank, lpage);
    u8 slot = leaf_find_slot(lleaf, key, NULL);

    memcpy(tmp, lleaf->entries, slot * sizeof(struct LeafEnt));
    memcpy(&tmp[slot].key, key, MAX_KEY);
    if (write_entry_value(tree, &tmp[slot], val, len) < 0) {
        return -1;
    }
    if (MAX_NODE_ENTS - slot) {
        memcpy(&tmp[slot + 1], &lleaf->entries[slot], (MAX_NODE_ENTS - slot) * sizeof(struct LeafEnt));
    }

    u32 rpage = alloc_node(&tree->bank, BNODE_LEAF);
    if (rpage == INVALID_PAGE) {
        return -1;
    }
    // Reload lleaf's addr since the base address of the pages can change after a resize
    lleaf = get_page(&tree->bank, lpage);
    *npage_out = rpage;
    struct LeafNode *rleaf = get_page(&tree->bank, rpage);

    u8 mid = (MAX_NODE_ENTS + 1) / 2;

    memcpy(lleaf->entries, tmp, mid * sizeof(struct LeafEnt));
    memcpy(rleaf->entries, &tmp[mid], (MAX_NODE_ENTS + 1 - mid) * sizeof(struct LeafEnt));

    lleaf->header.nkeys = mid;
    rleaf->header.nkeys = MAX_NODE_ENTS + 1 - mid;
    rleaf->header.next_page = lleaf->header.next_page;
    if (rleaf->header.next_page != INVALID_PAGE) {
        struct LeafNode *sib = get_page(&tree->bank, rleaf->header.next_page);
        sib->header.prev_page = rpage;
    }
    rleaf->header.prev_page = lpage;
    lleaf->header.next_page = rpage;

    memcpy(pkey_out, rleaf->entries[0].key, MAX_KEY);
    return 0;
}

static i32 split_internal(struct BTree *tree, u32 ipage, const u8 *key, u32 lpage, u32 rpage, u8 *pkey_out,
                          u32 *npage_out) {
    struct IntEnt tmp[MAX_NODE_ENTS + 1];

    struct IntNode *inode = get_page(&tree->bank, ipage);
    u32 otail = inode->tail_page;
    u8 slot = internal_find_child(inode, key, NULL);

    memcpy(tmp, inode->entries, slot * sizeof(struct IntEnt));
    memcpy(&tmp[slot].key, key, MAX_KEY);
    tmp[slot].cpage = lpage;
    if (MAX_NODE_ENTS - slot) {
        memcpy(&tmp[slot + 1], &inode->entries[slot], (MAX_NODE_ENTS - slot) * sizeof(struct IntEnt));
        assert(tmp[slot + 1].cpage == lpage);
        tmp[slot + 1].cpage = rpage;
    } else {
        assert(otail == lpage);
        otail = rpage;
    }

    u32 npage = alloc_node(&tree->bank, BNODE_INT);
    if (npage == INVALID_PAGE) {
        return -1;
    }
    inode = get_page(&tree->bank, ipage);
    *npage_out = npage;
    struct IntNode *nnode = get_page(&tree->bank, npage);

    u8 mid = (MAX_NODE_ENTS + 1) / 2;

    memcpy(inode->entries, tmp, mid * sizeof(struct IntEnt));
    memcpy(nnode->entries, &tmp[mid + 1], (MAX_NODE_ENTS - mid) * sizeof(struct IntEnt));
    memcpy(pkey_out, tmp[mid].key, MAX_KEY);

    inode->header.nkeys = mid;
    inode->tail_page = tmp[mid].cpage;
    nnode->header.nkeys = MAX_NODE_ENTS - mid;
    nnode->tail_page = otail;

    for (u8 i = 0; i < nnode->header.nkeys; i++) {
        struct NodeHeader *ch = get_page(&tree->bank, nnode->entries[i].cpage);
        ch->parent_page = npage;
    }
    struct NodeHeader *th = get_page(&tree->bank, nnode->tail_page);
    th->parent_page = npage;

    return 0;
}

static i32 insert_into_parent(struct BTree *tree, const u8 *key, u32 lpage, u32 rpage) {
    struct NodeHeader *lh = get_page(&tree->bank, lpage);

    if (lh->parent_page == INVALID_PAGE) {
        // lpage was root.
        u32 nr_page = alloc_node(&tree->bank, BNODE_INT);
        if (nr_page == INVALID_PAGE) {
            return -1;
        }
        lh = get_page(&tree->bank, lpage);
        struct IntNode *nroot = get_page(&tree->bank, nr_page);

        memcpy(nroot->entries[0].key, key, MAX_KEY);
        nroot->entries[0].cpage = lpage;
        nroot->tail_page = rpage;
        nroot->header.nkeys = 1;

        struct NodeHeader *rh = get_page(&tree->bank, rpage);
        lh->parent_page = nr_page;
        rh->parent_page = nr_page;

        struct Superblock *sb = get_superblock(&tree->bank);
        sb->root_page = nr_page;
        return 0;
    }

    u32 ppage = lh->parent_page;
    struct IntNode *pnode = get_page(&tree->bank, ppage);
    if (pnode->header.nkeys != MAX_NODE_ENTS) {
        u8 cidx = internal_find_child(pnode, key, NULL);
        if (cidx < pnode->header.nkeys) {
            memmove(&pnode->entries[cidx + 1], &pnode->entries[cidx],
                    (pnode->header.nkeys - cidx) * sizeof(struct IntEnt));
            memcpy(pnode->entries[cidx].key, key, MAX_KEY);
            pnode->entries[cidx].cpage = lpage;

            assert(pnode->entries[cidx + 1].cpage == lpage);
            pnode->entries[cidx + 1].cpage = rpage;
        } else {
            memcpy(pnode->entries[cidx].key, key, MAX_KEY);
            pnode->entries[cidx].cpage = lpage;

            assert(pnode->tail_page == lpage);
            pnode->tail_page = rpage;
        }
        struct NodeHeader *rh = get_page(&tree->bank, rpage);
        rh->parent_page = ppage;
        pnode->header.nkeys++;
        return 0;
    }

    u8 pkey[MAX_KEY];
    u32 new_page;

    if (split_internal(tree, ppage, key, lpage, rpage, pkey, &new_page) < 0) {
        return -1;
    }

    return insert_into_parent(tree, pkey, ppage, new_page);
}
