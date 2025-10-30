#include "btree.h"

#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>

#include "dblock.h"
#include "gdt_page.h"
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

static i32 read_entry_value(struct BTreeHandle *tree, struct LeafEnt *entry, void *value_out, u32 *len_out) {
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
            ret = read_normal_data(tree->bank, value_out, entry->ptr);
            if (ret < 0)
                return -1;
            if (len_out)
                *len_out = VPTR_GET_LEN(entry->ptr);
            break;
        default:
            ret = read_huge_data(tree->bank, value_out, entry->ptr);
            if (ret < 0)
                return -1;
            if (len_out)
                *len_out = VPTR_GET_HUGE_LEN(entry->ptr);
            break;
    }
    return 0;
}

static i32 write_entry_value(struct BTreeHandle *tree, u32 hint, struct LeafEnt *entry, const void *value, u32 len) {
    if (len <= MAX_INLINE) {
        entry->val_type = DATA_INLINE;
        entry->ival.len = len;
        memcpy(entry->ival.data, value, len);
        return 0;
    } else if (len <= MAX_NORMAL) {
        entry->val_type = DATA_NORMAL;
        entry->ptr = write_normal_data(tree->bank, hint, value, len);
        if (entry->ptr.page == INVALID_PAGE)
            return -1;
        return 0;
    } else {
        entry->val_type = DATA_HUGE;
        entry->ptr = write_huge_data(tree->bank, value, len);
        if (entry->ptr.page == INVALID_PAGE)
            return -1;
        return 0;
    }
}

static void delete_entry_value(struct BTreeHandle *tree, struct LeafEnt *entry) {
    switch (entry->val_type) {
        case DATA_INLINE:
            memset(&entry->ival, 0, MAX_INLINE + 1);
            break;
        case DATA_NORMAL:
            delete_normal_data(tree->bank, entry->ptr);
            break;
        default:
            delete_huge_data(tree->bank, entry->ptr);
            break;
    }
}

u32 alloc_node(struct GdtPageBank *b, u8 type, u32 hint) {
    u32 page = gdt_alloc_page(b, hint);
    struct NodeHeader *header = gdt_get_page(b, page);
    header->type = type;
    header->nkeys = 0;
    header->parent_page = INVALID_PAGE;
    header->prev_page = INVALID_PAGE;
    header->next_page = INVALID_PAGE;

    return page;
}

static u32 btree_find_leaf(struct BTreeHandle *tree, u32 start_page, const u8 *key, u32 *parent_stack, u16 *stack_top) {
    if (start_page == INVALID_PAGE) {
        return -1;
    }
    u32 page_num = start_page;
    for (;;) {
        void *page = gdt_get_page(tree->bank, page_num);
        if (!page)
            return INVALID_PAGE;

        struct NodeHeader *header = page;
        if (header->type == BNODE_LEAF)
            return page_num;

        parent_stack[*stack_top] = page_num;
        *stack_top += 1;
        struct IntNode *node = page;
        bool exact_match = false;
        u8 cidx = internal_find_child(node, key, &exact_match);

        if (cidx == 0 && !exact_match) {
            page_num = node->head_page;
        } else {
            u8 entry_idx = exact_match ? cidx : cidx - 1;
            page_num = node->entries[entry_idx].cpage;
        }
    }
    return INVALID_PAGE;
}

// Use fd=-1 to create in-memory BTree for testing.
i32 btree_create(struct BTree *tree, i32 fd) {
    struct GdtPageBank *b = &tree->bank;
    gdt_bank_create(b, fd);

    u32 root_page = gdt_alloc_page(b, INVALID_PAGE);
    struct GdtSuperblock *sb = gdt_get_superblock(b);
    sb->_root_page = root_page;
    tree->root_page = root_page;
    struct LeafNode *root = gdt_get_page(b, root_page);
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
    gdt_bank_open(&tree->bank, path);
    tree->root_page = gdt_get_superblock(&tree->bank)->_root_page;

    return 0;
}

void btree_close(struct BTree *tree) { gdt_bank_close(&tree->bank); }

void btree_make_handle(struct BTree *tree, struct BTreeHandle *h) {
    h->bank = &tree->bank;
    h->root_page = tree->root_page;
}

i32 btree_search(struct BTreeHandle *tree, const u8 *key, void *value_out, u32 *len_out) {
    u32 parent_stack[32]; // Stack to track path (max height ~32)
    u16 stack_top = 0;
    u32 page_num = btree_find_leaf(tree, tree->root_page, key, parent_stack, &stack_top);
    if (page_num == INVALID_PAGE) {
        return -1;
    }
    struct LeafNode *leaf = gdt_get_page(tree->bank, page_num);
    bool exact_match = false;
    u8 slot = leaf_find_slot(leaf, key, &exact_match);

    if (slot >= leaf->header.nkeys || !exact_match) {
        return -1; // Key not found
    }
    return read_entry_value(tree, &leaf->entries[slot], value_out, len_out);
}

// Insert
static i32 split_leaf(struct BTreeHandle *tree, u32 lpage, const u8 *key, const void *val, u32 len, u8 *pkey_out,
                      u32 *npage_out);
static i32 split_internal(struct BTreeHandle *tree, u32 ipage, const u8 *key, u32 rpage, u8 *pkey_out, u32 *npage_out);

// key should be put in a MAX_KEY buf before passing in.
i32 btree_insert(struct BTreeHandle *tree, const u8 *key, const void *val, u32 len) {
    u32 parent_stack[32]; // Stack to track path (max height ~32)
    u16 stack_top = 0;
    u32 page_num = btree_find_leaf(tree, tree->root_page, key, parent_stack, &stack_top);
    if (page_num == INVALID_PAGE) {
        return -1;
    }

    struct LeafNode *leaf = gdt_get_page(tree->bank, page_num);
    bool exact_match = false;
    u8 slot = leaf_find_slot(leaf, key, &exact_match);

    if (slot < leaf->header.nkeys && exact_match) {
        // Key exists - update value
        delete_entry_value(tree, &leaf->entries[slot]);
        return write_entry_value(tree, page_num, &leaf->entries[slot], val, len);
    }

    if (leaf->header.nkeys < MAX_NODE_ENTS) {
        // Insert into leaf (shift entries to make space)
        memmove(&leaf->entries[slot + 1], &leaf->entries[slot], (leaf->header.nkeys - slot) * sizeof(struct LeafEnt));
        memcpy(leaf->entries[slot].key, key, MAX_KEY);
        i32 ret = write_entry_value(tree, page_num, &leaf->entries[slot], val, len);
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

    u32 lpage = page_num, rpage = new_page;

    while (stack_top) {
        u32 ppage = parent_stack[--stack_top];
        struct IntNode *pnode = gdt_get_page(tree->bank, ppage);
        if (pnode->header.nkeys < MAX_NODE_ENTS) {
            u8 cidx = internal_find_child(pnode, pkey, NULL);
            memmove(&pnode->entries[cidx + 1], &pnode->entries[cidx],
                    (pnode->header.nkeys - cidx) * sizeof(struct IntEnt));
            memcpy(pnode->entries[cidx].key, pkey, MAX_KEY);
            pnode->entries[cidx].cpage = rpage;
            struct NodeHeader *rh = gdt_get_page(tree->bank, rpage);
            rh->parent_page = ppage;
            pnode->header.nkeys++;
            return 0;
        }

        u8 new_pkey[MAX_KEY];

        if (split_internal(tree, ppage, pkey, rpage, new_pkey, &new_page) < 0) {
            return -1;
        }

        memcpy(pkey, new_pkey, MAX_KEY);
        lpage = ppage;
        rpage = new_page;
    }
    assert(lpage == tree->root_page);
    void *root_page_ptr = gdt_get_page(tree->bank, tree->root_page);
    struct NodeHeader *root_header = root_page_ptr;

    u32 nlpage = alloc_node(tree->bank, root_header->type, rpage);
    if (nlpage == INVALID_PAGE) {
        return -1;
    }
    void *nlpage_ptr = gdt_get_page(tree->bank, nlpage);

    memcpy(nlpage_ptr, root_page_ptr, PAGE_SIZE);
    lpage = nlpage;

    if (root_header->type == BNODE_INT) {
        struct IntNode *nlnode = nlpage_ptr;
        struct NodeHeader *ch = gdt_get_page(tree->bank, nlnode->head_page);
        ch->parent_page = nlpage;
        for (u8 i = 0; i < nlnode->header.nkeys; i++) {
            ch = gdt_get_page(tree->bank, nlnode->entries[i].cpage);
            ch->parent_page = nlpage;
        }
    }

    root_header->type = BNODE_INT;
    root_header->nkeys = 1;
    root_header->parent_page = INVALID_PAGE;
    root_header->next_page = INVALID_PAGE;
    root_header->prev_page = INVALID_PAGE;

    struct IntNode *root = (struct IntNode *) root_header;
    memcpy(root->entries[0].key, pkey, MAX_KEY);
    root->head_page = lpage;
    root->entries[0].cpage = rpage;

    struct NodeHeader *lh = gdt_get_page(tree->bank, lpage);
    struct NodeHeader *rh = gdt_get_page(tree->bank, rpage);
    lh->parent_page = tree->root_page;
    rh->parent_page = tree->root_page;

    return 0;
}

static i32 split_leaf(struct BTreeHandle *tree, u32 lpage, const u8 *key, const void *val, const u32 len, u8 *pkey_out,
                      u32 *npage_out) {
    struct LeafEnt tmp[MAX_NODE_ENTS + 1];

    struct LeafNode *lleaf = gdt_get_page(tree->bank, lpage);
    u8 slot = leaf_find_slot(lleaf, key, NULL);

    memcpy(tmp, lleaf->entries, slot * sizeof(struct LeafEnt));
    memcpy(&tmp[slot].key, key, MAX_KEY);
    if (write_entry_value(tree, lpage, &tmp[slot], val, len) < 0) {
        return -1;
    }
    if (MAX_NODE_ENTS - slot) {
        memcpy(&tmp[slot + 1], &lleaf->entries[slot], (MAX_NODE_ENTS - slot) * sizeof(struct LeafEnt));
    }

    u32 rpage = alloc_node(tree->bank, BNODE_LEAF, lpage);
    if (rpage == INVALID_PAGE) {
        return -1;
    }
    // Reload lleaf's addr since the base address of the pages can change after a resize
    lleaf = gdt_get_page(tree->bank, lpage);
    *npage_out = rpage;
    struct LeafNode *rleaf = gdt_get_page(tree->bank, rpage);

    u8 mid = (MAX_NODE_ENTS + 1) / 2;

    memcpy(lleaf->entries, tmp, mid * sizeof(struct LeafEnt));
    memcpy(rleaf->entries, &tmp[mid], (MAX_NODE_ENTS + 1 - mid) * sizeof(struct LeafEnt));

    lleaf->header.nkeys = mid;
    rleaf->header.nkeys = MAX_NODE_ENTS + 1 - mid;
    rleaf->header.next_page = lleaf->header.next_page;
    if (rleaf->header.next_page != INVALID_PAGE) {
        struct LeafNode *sib = gdt_get_page(tree->bank, rleaf->header.next_page);
        sib->header.prev_page = rpage;
    }
    rleaf->header.prev_page = lpage;
    lleaf->header.next_page = rpage;

    memcpy(pkey_out, rleaf->entries[0].key, MAX_KEY);
    return 0;
}

static i32 split_internal(struct BTreeHandle *tree, u32 ipage, const u8 *key, u32 rpage, u8 *pkey_out, u32 *npage_out) {
    struct IntEnt tmp[MAX_NODE_ENTS + 1];

    struct IntNode *inode = gdt_get_page(tree->bank, ipage);
    u8 slot = internal_find_child(inode, key, NULL);

    memcpy(tmp, inode->entries, slot * sizeof(struct IntEnt));
    memcpy(&tmp[slot].key, key, MAX_KEY);
    tmp[slot].cpage = rpage;
    memcpy(&tmp[slot + 1], &inode->entries[slot], (MAX_NODE_ENTS - slot) * sizeof(struct IntEnt));

    u32 npage = alloc_node(tree->bank, BNODE_INT, ipage);
    if (npage == INVALID_PAGE) {
        return -1;
    }
    inode = gdt_get_page(tree->bank, ipage);
    *npage_out = npage;
    struct IntNode *nnode = gdt_get_page(tree->bank, npage);

    u8 mid = (MAX_NODE_ENTS + 1) / 2;
    memcpy(pkey_out, tmp[mid].key, MAX_KEY); // This key is promoted.

    memcpy(inode->entries, tmp, mid * sizeof(struct IntEnt));
    inode->header.nkeys = mid;

    nnode->head_page = tmp[mid].cpage;
    memcpy(nnode->entries, &tmp[mid + 1], (MAX_NODE_ENTS - mid) * sizeof(struct IntEnt));

    nnode->header.nkeys = MAX_NODE_ENTS - mid;
    nnode->header.next_page = inode->header.next_page;
    if (inode->header.next_page != INVALID_PAGE) {
        struct IntNode *sib = gdt_get_page(tree->bank, inode->header.next_page);
        sib->header.prev_page = npage;
    }
    nnode->header.prev_page = ipage;
    inode->header.next_page = npage;

    struct NodeHeader *head_child = gdt_get_page(tree->bank, nnode->head_page);
    head_child->parent_page = npage;
    for (u8 i = 0; i < nnode->header.nkeys; i++) {
        struct NodeHeader *child = gdt_get_page(tree->bank, nnode->entries[i].cpage);
        child->parent_page = npage;
    }

    return 0;
}

static bool delete_internal_entry(struct BTreeHandle *tree, u32 page, const u8 *key, u32 dpage);
static i32 redistribute_leaf(struct BTreeHandle *tree, u32 page);
static i32 redistribute_internal(struct BTreeHandle *tree, u32 page);
static i32 merge_leaf(struct BTreeHandle *tree, u32 page, u8 *skey_out, u32 *dpage_out);
static i32 merge_node(struct BTreeHandle *tree, u32 page, u8 *skey_out, u32 *dpage_out);

i32 btree_delete(struct BTreeHandle *tree, const u8 *key) {
    u32 parent_stack[32]; // Stack to track path (max height ~32)
    u16 stack_top = 0;
    u32 page_num = btree_find_leaf(tree, tree->root_page, key, parent_stack, &stack_top);
    if (page_num == INVALID_PAGE) {
        return -1;
    }

    struct LeafNode *leaf = gdt_get_page(tree->bank, page_num);
    bool exact_match = false;
    u8 slot = leaf_find_slot(leaf, key, &exact_match);

    if (!exact_match) {
        return -1;
    }

    delete_entry_value(tree, &leaf->entries[slot]);
    if (slot < leaf->header.nkeys) {
        memmove(&leaf->entries[slot], &leaf->entries[slot + 1],
                (leaf->header.nkeys - slot - 1) * sizeof(struct LeafEnt));
    }
    leaf->header.nkeys--;

    // Return on enough entries or being root node itself.
    if (leaf->header.nkeys >= MIN_NODE_ENTS || !stack_top) {
        return 0;
    }
    if (!redistribute_leaf(tree, page_num)) {
        return 0;
    }

    u8 skey[MAX_KEY];
    u32 dpage;
    if (merge_leaf(tree, page_num, skey, &dpage) < 0) {
        return -1;
    }

    u32 ppage;
    while (stack_top > 1) {
        ppage = parent_stack[--stack_top];
        if (delete_internal_entry(tree, ppage, skey, dpage)) {
            return 0;
        }
        if (!redistribute_internal(tree, ppage)) {
            return 0;
        }

        u8 new_skey[MAX_KEY];
        if (merge_node(tree, ppage, new_skey, &dpage) < 0) {
            return -1;
        }

        memcpy(skey, new_skey, MAX_KEY);
    }
    u32 root_page = parent_stack[0];
    void *root_page_ptr = gdt_get_page(tree->bank, root_page);
    struct NodeHeader *root_header = root_page_ptr;
    delete_internal_entry(tree, root_page, skey, dpage);
    if (!root_header->nkeys) {
        u32 nr_page = ((struct IntNode *) root_page_ptr)->head_page;
        void *nr_page_ptr = gdt_get_page(tree->bank, nr_page);
        memcpy(root_page_ptr, nr_page_ptr, PAGE_SIZE);
        if (root_header->type == BNODE_INT) {
            struct IntNode *root = root_page_ptr;
            struct NodeHeader *ch = gdt_get_page(tree->bank, root->head_page);
            ch->parent_page = root_page;
            for (u8 i = 0; i < root->header.nkeys; i++) {
                ch = gdt_get_page(tree->bank, root->entries[i].cpage);
                ch->parent_page = root_page;
            }
        }
        gdt_unset_page(tree->bank, nr_page);
    }
    return 0;
}

static bool delete_internal_entry(struct BTreeHandle *tree, u32 page, const u8 *key, const u32 dpage) {
    struct IntNode *node = gdt_get_page(tree->bank, page);
    bool exact_match = false;
    u8 cidx = internal_find_child(node, key, &exact_match);
    if (exact_match) {
        assert(node->entries[cidx].cpage == dpage);
        memmove(&node->entries[cidx], &node->entries[cidx + 1],
                (node->header.nkeys - cidx - 1) * sizeof(struct IntEnt));
    } else {
        assert(cidx > 0);
        memmove(&node->entries[cidx - 1], &node->entries[cidx], (node->header.nkeys - cidx) * sizeof(struct IntEnt));
    }
    node->header.nkeys--;
    return node->header.nkeys >= MIN_NODE_ENTS;
}

static i32 redistribute_leaf(struct BTreeHandle *tree, const u32 page) {
    struct LeafNode *leaf = gdt_get_page(tree->bank, page);

    u32 lpage = leaf->header.prev_page, rpage = leaf->header.next_page, ppage = leaf->header.parent_page;
    assert(ppage != INVALID_PAGE); // Should only happens on root
    assert(lpage != INVALID_PAGE || rpage != INVALID_PAGE); // Should be handled already

    if (rpage != INVALID_PAGE) {
        struct LeafNode *rleaf = gdt_get_page(tree->bank, rpage);
        if (rleaf->header.parent_page == ppage && rleaf->header.nkeys > MIN_NODE_ENTS) {
            // The key separating `leaf` and `rleaf` is the smallest key in `rleaf`.
            // Save it so we can find it in the parent.
            u8 old_sep[MAX_KEY];
            memcpy(old_sep, rleaf->entries[0].key, MAX_KEY);

            // Perform the borrow: move first entry from rleaf to the end of leaf.
            memcpy(&leaf->entries[leaf->header.nkeys], &rleaf->entries[0], sizeof(struct LeafEnt));
            memmove(&rleaf->entries[0], &rleaf->entries[1], (rleaf->header.nkeys - 1) * sizeof(struct LeafEnt));
            leaf->header.nkeys++;
            rleaf->header.nkeys--;

            // Update the parent's separator key.
            struct IntNode *pnode = gdt_get_page(tree->bank, ppage);
            bool exact_match = false;
            u8 cidx = internal_find_child(pnode, old_sep, &exact_match);

            if (exact_match) {
                memcpy(pnode->entries[cidx].key, rleaf->entries[0].key, MAX_KEY);
            } else {
                assert(cidx > 0);
                memcpy(pnode->entries[cidx - 1].key, rleaf->entries[0].key, MAX_KEY);
            }
            return 0; // Success
        }
    }

    if (lpage != INVALID_PAGE) {
        struct LeafNode *lleaf = gdt_get_page(tree->bank, lpage);
        if (lleaf->header.parent_page == ppage && lleaf->header.nkeys > MIN_NODE_ENTS) {
            // This is the key that separates lleaf and leaf in the parent
            u8 old_sep[MAX_KEY];
            memcpy(old_sep, leaf->entries[0].key, MAX_KEY);

            // Perform the borrow
            memmove(&leaf->entries[1], &leaf->entries[0], leaf->header.nkeys * sizeof(struct LeafEnt));
            memcpy(&leaf->entries[0], &lleaf->entries[lleaf->header.nkeys - 1], sizeof(struct LeafEnt));
            leaf->header.nkeys++;
            lleaf->header.nkeys--;

            // Find the position of the old separator and update it to the new one
            struct IntNode *pnode = gdt_get_page(tree->bank, ppage);
            bool exact_match = false;
            u8 cidx = internal_find_child(pnode, old_sep, &exact_match);

            if (exact_match) {
                // The separator was an exact match, update it to the new smallest key.
                memcpy(pnode->entries[cidx].key, leaf->entries[0].key, MAX_KEY);
            } else {
                // The separator was smaller. The correct key to update is the one before cidx.
                assert(cidx > 0);
                memcpy(pnode->entries[cidx - 1].key, leaf->entries[0].key, MAX_KEY);
            }
            return 0; // Success
        }
    }

    return -1;
}

static i32 redistribute_internal(struct BTreeHandle *tree, u32 page) {
    struct IntNode *node = gdt_get_page(tree->bank, page);

    u32 lpage = node->header.prev_page, rpage = node->header.next_page, ppage = node->header.parent_page;
    assert(ppage != INVALID_PAGE);
    assert(lpage != INVALID_PAGE || rpage != INVALID_PAGE);

    struct IntNode *pnode = gdt_get_page(tree->bank, ppage);

    // ## Case 1: Borrow from the RIGHT sibling
    if (rpage != INVALID_PAGE) {
        struct IntNode *rnode = gdt_get_page(tree->bank, rpage);
        if (rnode->header.parent_page == ppage && rnode->header.nkeys > MIN_NODE_ENTS) {
            bool exact_match = false;
            u8 cidx = internal_find_child(pnode, rnode->entries[0].key, &exact_match);
            if (!exact_match) {
                assert(cidx > 0);
                cidx--;
            }

            memcpy(node->entries[node->header.nkeys].key, pnode->entries[cidx].key, MAX_KEY);
            node->entries[node->header.nkeys].cpage = rnode->head_page;
            node->header.nkeys++;

            struct NodeHeader *moved = gdt_get_page(tree->bank, rnode->head_page);
            moved->parent_page = page;

            memcpy(pnode->entries[cidx].key, rnode->entries[0].key, MAX_KEY);
            rnode->head_page = rnode->entries[0].cpage;

            memmove(&rnode->entries[0], &rnode->entries[1], (rnode->header.nkeys - 1) * sizeof(struct IntEnt));
            rnode->header.nkeys--;

            return 0; // Success
        }
    }

    // ## Case 2: Borrow from the LEFT sibling
    if (lpage != INVALID_PAGE) {
        struct IntNode *lnode = gdt_get_page(tree->bank, lpage);
        if (lnode->header.parent_page == ppage && lnode->header.nkeys > MIN_NODE_ENTS) {
            bool exact_match = false;
            u8 cidx = internal_find_child(pnode, node->entries[0].key, &exact_match);
            if (!exact_match) {
                assert(cidx > 0);
                cidx--;
            }

            memmove(&node->entries[1], &node->entries[0], node->header.nkeys * sizeof(struct IntEnt));
            memcpy(node->entries[0].key, pnode->entries[cidx].key, MAX_KEY);
            node->entries[0].cpage = node->head_page;

            node->head_page = lnode->entries[lnode->header.nkeys - 1].cpage;
            node->header.nkeys++;

            struct NodeHeader *moved = gdt_get_page(tree->bank, node->head_page);
            moved->parent_page = page;

            memcpy(pnode->entries[cidx].key, lnode->entries[lnode->header.nkeys - 1].key, MAX_KEY);
            lnode->header.nkeys--;

            return 0;
        }
    }

    return -1; // Could not redistribute
}

static void merge_leaf_helper(struct BTreeHandle *tree, const u32 lpage, const u32 rpage, u8 *skey_out, u32 *dpage) {
    struct LeafNode *lleaf = gdt_get_page(tree->bank, lpage);
    struct LeafNode *rleaf = gdt_get_page(tree->bank, rpage);
    assert(lleaf->header.nkeys <= MIN_NODE_ENTS && rleaf->header.nkeys <= MIN_NODE_ENTS);
    memcpy(skey_out, rleaf->entries[0].key, MAX_KEY);
    memcpy(&lleaf->entries[lleaf->header.nkeys], rleaf->entries, rleaf->header.nkeys * sizeof(struct LeafEnt));
    lleaf->header.nkeys += rleaf->header.nkeys;
    rleaf->header.nkeys = 0;

    lleaf->header.next_page = rleaf->header.next_page;
    if (rleaf->header.next_page != INVALID_PAGE) {
        struct LeafNode *sib = gdt_get_page(tree->bank, rleaf->header.next_page);
        sib->header.prev_page = lpage;
    }
    gdt_unset_page(tree->bank, rpage);
    *dpage = rpage;
}

static i32 merge_leaf(struct BTreeHandle *tree, u32 page, u8 *skey_out, u32 *dpage) {
    struct LeafNode *leaf = gdt_get_page(tree->bank, page);

    u32 lpage = leaf->header.prev_page, rpage = leaf->header.next_page, ppage = leaf->header.parent_page;
    assert(ppage != INVALID_PAGE); // Should only happens on root
    assert(lpage != INVALID_PAGE || rpage != INVALID_PAGE); // Should be handled already

    if (lpage != INVALID_PAGE) {
        struct LeafNode *lleaf = gdt_get_page(tree->bank, lpage);
        if (lleaf->header.parent_page == ppage) {
            merge_leaf_helper(tree, lpage, page, skey_out, dpage);
            return 0;
        }
    }

    if (rpage != INVALID_PAGE) {
        struct LeafNode *rleaf = gdt_get_page(tree->bank, rpage);
        if (rleaf->header.parent_page == ppage) {
            merge_leaf_helper(tree, page, rpage, skey_out, dpage);
            return 0;
        }
    }

    return -1;
}

static void merge_node_helper(struct BTreeHandle *tree, const u32 ppage, const u32 lpage, const u32 rpage, u8 *skey_out,
                              u32 *dpage) {
    struct IntNode *pnode = gdt_get_page(tree->bank, ppage);
    struct IntNode *lnode = gdt_get_page(tree->bank, lpage);
    struct IntNode *rnode = gdt_get_page(tree->bank, rpage);

    bool exact_match = false;
    u8 cidx = internal_find_child(pnode, rnode->entries[0].key, &exact_match);
    if (!exact_match) {
        assert(cidx > 0);
        cidx--;
    }

    memcpy(skey_out, pnode->entries[cidx].key, MAX_KEY);
    memcpy(lnode->entries[lnode->header.nkeys].key, pnode->entries[cidx].key, MAX_KEY);
    lnode->entries[lnode->header.nkeys].cpage = rnode->head_page;
    lnode->header.nkeys++;

    memcpy(&lnode->entries[lnode->header.nkeys], rnode->entries, rnode->header.nkeys * sizeof(struct IntEnt));
    lnode->header.nkeys += rnode->header.nkeys;

    struct NodeHeader *moved = gdt_get_page(tree->bank, rnode->head_page);
    moved->parent_page = lpage;
    for (u8 i = 0; i < rnode->header.nkeys; i++) {
        moved = gdt_get_page(tree->bank, rnode->entries[i].cpage);
        moved->parent_page = lpage;
    }

    gdt_unset_page(tree->bank, rpage);
    *dpage = rpage;
}

static i32 merge_node(struct BTreeHandle *tree, u32 page, u8 *skey_out, u32 *dpage) {
    struct IntNode *node = gdt_get_page(tree->bank, page);

    u32 lpage = node->header.prev_page, rpage = node->header.next_page, ppage = node->header.parent_page;
    assert(ppage != INVALID_PAGE);
    assert(lpage != INVALID_PAGE || rpage != INVALID_PAGE);

    // ## Case 1: Merge with the LEFT sibling (preferred)
    if (lpage != INVALID_PAGE) {
        struct IntNode *lnode = gdt_get_page(tree->bank, lpage);
        if (lnode->header.parent_page == ppage) {
            merge_node_helper(tree, ppage, lpage, page, skey_out, dpage);
            return 0; // Success
        }
    }

    // ## Case 2: Merge with the RIGHT sibling
    if (rpage != INVALID_PAGE) {
        struct IntNode *rnode = gdt_get_page(tree->bank, rpage);
        if (rnode->header.parent_page == ppage) {
            merge_node_helper(tree, ppage, page, rpage, skey_out, dpage);
            return 0; // Success
        }
    }

    return -1;
}
