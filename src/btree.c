#include "btree.h"

#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#include "dblock.h"
#include "gdt_page.h"
#include "utils.h"

#define MAX_HEIGHT 32
#define TXN_PAGE_MASK 0x80000000
#define TXN_TLB_DIRTY 0xDEADBEEF

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

static i32 read_leafval(struct BTreeHandle *handle, struct LeafVal *val, void *value_out, u32 *len_out) {
    i32 ret;
    u8 len;
    switch (val->val_type) {
        case DATA_INLINE:
            len = val->ival.len;
            if (len_out)
                *len_out = len;
            if (value_out)
                memcpy(value_out, val->ival.data, len);
            break;
        case DATA_NORMAL:
            ret = read_normal_data(handle->bank, value_out, val->ptr);
            if (ret < 0)
                return -1;
            if (len_out)
                *len_out = VPTR_GET_LEN(val->ptr);
            break;
        default:
            ret = read_huge_data(handle->bank, value_out, val->ptr);
            if (ret < 0)
                return -1;
            if (len_out)
                *len_out = VPTR_GET_HUGE_LEN(val->ptr);
            break;
    }
    return 0;
}

static i32 write_leafval(struct BTreeHandle *handle, struct LeafVal *lval, const void *val, const u32 len) {
    if (len <= MAX_INLINE) {
        lval->val_type = DATA_INLINE;
        lval->ival.len = len;
        memcpy(lval->ival.data, val, len);
    } else if (len <= MAX_NORMAL) {
        lval->val_type = DATA_NORMAL;
        lval->ptr = write_normal_data(handle->bank, INVALID_PAGE, val, len);
        if (lval->ptr.page == INVALID_PAGE) {
            return -1;
        }
    } else {
        lval->val_type = DATA_HUGE;
        lval->ptr = write_huge_data(handle->bank, val, len);
        if (lval->ptr.page == INVALID_PAGE) {
            return -1;
        }
    }
    return 0;
}

static void delete_leafval(struct BTreeHandle *handle, struct LeafVal *val) {
    switch (val->val_type) {
        case DATA_INLINE:
            memset(&val->ival, 0, MAX_INLINE + 1);
            break;
        case DATA_NORMAL:
            delete_normal_data(handle->bank, val->ptr);
            break;
        default:
            delete_huge_data(handle->bank, val->ptr);
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

i32 btree_create_root(struct BTreeHandle *handle, struct GdtPageBank *bank) {
    return btree_create_known_root(handle, bank, gdt_alloc_page(bank, INVALID_PAGE));
}

i32 btree_create_known_root(struct BTreeHandle *handle, struct GdtPageBank *bank, u32 page) {
    if (page == INVALID_PAGE) {
        return -1;
    }
    void *root_page_ptr = gdt_get_page(bank, page);
    if (!root_page_ptr) {
        return -1;
    }
    if (handle) {
        handle->bank = bank;
        handle->root_page = page;
    }
    struct LeafNode *root = root_page_ptr;

    memset(root, 0, sizeof(struct LeafNode));
    root->header.type = BNODE_LEAF;
    root->header.nkeys = 0;
    root->header.parent_page = INVALID_PAGE;
    root->header.prev_page = INVALID_PAGE;
    root->header.next_page = INVALID_PAGE;

    gdt_sync(handle->bank);
    return 0;
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

    gdt_sync(b);
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

i32 btree_search(struct BTreeHandle *handle, const u8 *key, void *value_out, u32 *len_out) {
    u32 parent_stack[MAX_HEIGHT]; // Stack to track path (max height ~32)
    u16 stack_top = 0;
    u32 page_num = btree_find_leaf(handle, handle->root_page, key, parent_stack, &stack_top);
    if (page_num == INVALID_PAGE) {
        return -1;
    }
    struct LeafNode *leaf = gdt_get_page(handle->bank, page_num);
    bool exact_match = false;
    u8 slot = leaf_find_slot(leaf, key, &exact_match);

    if (slot >= leaf->header.nkeys || !exact_match) {
        return -1; // Key not found
    }
    return read_leafval(handle, &leaf->entries[slot].val, value_out, len_out);
}

// Transaction
struct BTreeTxn {
    struct BTreeHandle *handle;
    void *page_stack;
    u32 *tlb;
    u16 tlb_size;
    u16 height;
    u16 new_page_cnt;
    u32 new_pages[MAX_HEIGHT + 2];
};

static void *txn_get_page(struct BTreeTxn *txn, u32 txn_page_num) {
    if (txn_page_num == INVALID_PAGE || !(txn_page_num & TXN_PAGE_MASK)) {
        return NULL;
    }

    u16 idx = txn_page_num & ~TXN_PAGE_MASK;
    if (idx >= txn->tlb_size) {
        return NULL;
    }

    return (u8 *) txn->page_stack + idx * PAGE_SIZE;
}

// Only insert will use it.
static u32 txn_alloc_node(struct BTreeTxn *txn, u32 base_idx, u8 type) {
    if (base_idx >= txn->tlb_size || (base_idx & 1)) {
        return INVALID_PAGE;
    }
    u16 idx = base_idx + 1;
    void *page = (u8 *) txn->page_stack + idx * PAGE_SIZE;
    memset(page, 0, PAGE_SIZE);
    struct NodeHeader *header = page;
    header->type = type;
    header->nkeys = 0;
    header->next_page = INVALID_PAGE;
    header->prev_page = INVALID_PAGE;
    header->parent_page = INVALID_PAGE;
    txn->tlb[idx] = TXN_TLB_DIRTY;

    return (idx | TXN_PAGE_MASK);
}

static u32 txn_translate(struct BTreeTxn *txn, u32 txn_page_num) {
    if (txn_page_num == INVALID_PAGE || !(txn_page_num & TXN_PAGE_MASK)) {
        return INVALID_PAGE;
    }

    u16 idx = txn_page_num & ~TXN_PAGE_MASK;
    if (idx >= txn->tlb_size) {
        return INVALID_PAGE;
    }

    return txn->tlb[idx];
}

static i32 txn_pull_real_page(struct BTreeTxn *txn, u32 txn_page_num, u16 base_idx) {
    if (txn_page_num == INVALID_PAGE || !(txn_page_num & TXN_PAGE_MASK)) {
        return -1;
    }

    u16 idx = txn_page_num & ~TXN_PAGE_MASK;
    if (idx >= txn->tlb_size || idx <= base_idx) {
        return INVALID_PAGE;
    }

    u32 real_page = gdt_alloc_page(txn->handle->bank, txn->tlb[base_idx]);
    if (real_page == INVALID_PAGE) {
        return -1;
    }
    txn->new_pages[txn->new_page_cnt++] = real_page;
    txn->tlb[idx] = real_page;
    return 0;
}

// Insert
static i32 split_leaf_txn(struct BTreeTxn *txn, const u8 *key, const struct LeafVal *val, u8 *pkey_out, u32 *npage_out);
static i32 split_internal_txn(struct BTreeTxn *txn, u32 ipage, const u8 *key, u32 rpage, u8 *pkey_out, u32 *npage_out);
static i32 btree_insert_txn(struct BTreeTxn *txn, u8 slot, const u8 *key, const struct LeafVal *val);
static i32 btree_insert_txn_fix(struct BTreeTxn *txn);
#define SIDX_TO_TXN_PAGE(sidx) (((sidx) * 2) | TXN_PAGE_MASK)

// key should be put in a MAX_KEY buf before passing in.
i32 btree_insert(struct BTreeHandle *handle, const u8 *key, const void *val, u32 len) {
    struct LeafVal lval;
    if (write_leafval(handle, &lval, val, len) < 0) {
        return -1;
    }

    u32 parent_stack[MAX_HEIGHT]; // Stack to track path (max height ~32)
    u16 stack_top = 0;
    u32 page_num = btree_find_leaf(handle, handle->root_page, key, parent_stack, &stack_top);
    if (page_num == INVALID_PAGE) {
        return -1;
    }

    struct LeafNode *leaf = gdt_get_page(handle->bank, page_num);
    bool exact_match = false;
    u8 slot = leaf_find_slot(leaf, key, &exact_match);

    if (slot < leaf->header.nkeys && exact_match) {
        // Key exists - update value
        struct LeafVal oval;
        memcpy(&oval, &leaf->entries[slot].val, sizeof(struct LeafVal));

        memcpy(&leaf->entries[slot].val, &lval, sizeof(struct LeafVal));

        delete_leafval(handle, &oval);

        return 0;
    }
    // Copy stack to the Txn, with leaf added at stack top
    // Each copied page is at idx of height * 2
    // So split page can just use idx + 1
    // Special stack height: MAX_HEIGHT -> leaf, MAX_HEIGHT + 1 -> new root
    // leaf split will be at TLB[MAX_HEIGHT * 2 + 1]
    struct BTreeTxn txn;
    txn.handle = handle;
    txn.page_stack = calloc((MAX_HEIGHT + 2) * 2, PAGE_SIZE);
    txn.tlb = calloc((MAX_HEIGHT + 2) * 2, sizeof(u32));
    txn.tlb_size = (MAX_HEIGHT + 2) * 2;
    // Initialize TLB with INVALID_PAGE
    memset(txn.tlb, 0xFF, ((MAX_HEIGHT + 2) * 2) * sizeof(u32));
    txn.height = stack_top + 1;
    txn.new_page_cnt = 0;
    memset(txn.new_pages, 0xFF, (MAX_HEIGHT + 2) * sizeof(u32));
    u16 idx = 0;
    for (u16 i = 0; i < stack_top; i++) {
        void *node_ptr = gdt_get_page(handle->bank, parent_stack[i]);
        memcpy((u8 *) txn.page_stack + idx * PAGE_SIZE, node_ptr, PAGE_SIZE);
        txn.tlb[idx] = parent_stack[i];
        idx += 2;
    }
    memcpy((u8 *) txn.page_stack + idx * PAGE_SIZE, leaf, PAGE_SIZE);
    txn.tlb[idx] = page_num;

    if (btree_insert_txn(&txn, slot, key, &lval) < 0) {
        free(txn.page_stack);
        free(txn.tlb);
        return -1;
    }

    if (btree_insert_txn_fix(&txn) < 0) {
        for (u16 i = 0; i < txn.new_page_cnt; i++) {
            if (txn.new_pages[i] != INVALID_PAGE) {
                gdt_unset_page(handle->bank, txn.new_pages[i]);
            }
        }
        free(txn.page_stack);
        free(txn.tlb);
        return -1;
    }

    free(txn.page_stack);
    free(txn.tlb);
    return 0;
}

static void txn_fix_children(struct BTreeTxn *txn, struct IntNode *node, const u8 idx) {
    u32 tpage = txn_translate(txn, node->head_page);
    if (tpage != INVALID_PAGE) {
        struct NodeHeader *ch = txn_get_page(txn, node->head_page);
        ch->parent_page = txn->tlb[idx];
        node->head_page = tpage;
    } else {
        struct NodeHeader *ch = gdt_get_page(txn->handle->bank, node->head_page);
        ch->parent_page = txn->tlb[idx];
    }
    for (u16 j = 0; j < node->header.nkeys; j++) {
        tpage = txn_translate(txn, node->entries[j].cpage);
        if (tpage != INVALID_PAGE) {
            struct NodeHeader *ch = txn_get_page(txn, node->entries[j].cpage);
            ch->parent_page = txn->tlb[idx];
            node->entries[j].cpage = tpage;
        } else {
            struct NodeHeader *ch = gdt_get_page(txn->handle->bank, node->entries[j].cpage);
            ch->parent_page = txn->tlb[idx];
        }
    }
}

static i32 btree_insert_txn(struct BTreeTxn *txn, const u8 slot, const u8 *key, const struct LeafVal *val) {
    u32 leaf_page = SIDX_TO_TXN_PAGE(txn->height - 1);
    struct LeafNode *leaf = txn_get_page(txn, leaf_page);

    if (leaf->header.nkeys < MAX_NODE_ENTS) {
        // Insert into leaf (shift entries to make space)
        memmove(&leaf->entries[slot + 1], &leaf->entries[slot], (leaf->header.nkeys - slot) * sizeof(struct LeafEnt));
        memcpy(leaf->entries[slot].key, key, MAX_KEY);
        memcpy(&leaf->entries[slot].val, val, sizeof(struct LeafVal));
        leaf->header.nkeys++;
        return 0;
    }

    u8 pkey[MAX_KEY];
    u32 new_page;

    if (split_leaf_txn(txn, key, val, pkey, &new_page) < 0) {
        return -1;
    }

    u32 lpage = leaf_page, rpage = new_page;
    u16 stack_top = txn->height - 1;
    while (stack_top) {
        u32 ppage = SIDX_TO_TXN_PAGE(--stack_top);
        struct IntNode *pnode = txn_get_page(txn, ppage);
        if (pnode->header.nkeys < MAX_NODE_ENTS) {
            u8 cidx = internal_find_child(pnode, pkey, NULL);
            memmove(&pnode->entries[cidx + 1], &pnode->entries[cidx],
                    (pnode->header.nkeys - cidx) * sizeof(struct IntEnt));
            memcpy(pnode->entries[cidx].key, pkey, MAX_KEY);
            pnode->entries[cidx].cpage = rpage;
            struct NodeHeader *rh = txn_get_page(txn, rpage);
            rh->parent_page = ppage;
            pnode->header.nkeys++;
            return 0;
        }

        u8 new_pkey[MAX_KEY];

        if (split_internal_txn(txn, ppage, pkey, rpage, new_pkey, &new_page) < 0) {
            return -1;
        }

        memcpy(pkey, new_pkey, MAX_KEY);
        lpage = ppage;
        rpage = new_page;
    }
    // Root case: split root.
    assert(lpage == SIDX_TO_TXN_PAGE(0));
    void *root_page_ptr = txn_get_page(txn, SIDX_TO_TXN_PAGE(0));
    struct NodeHeader *root_header = root_page_ptr;
    const u16 nroot_idx = txn->height * 2;
    u32 nlpage = txn_alloc_node(txn, nroot_idx, root_header->type);
    if (nlpage == INVALID_PAGE) {
        return -1;
    }
    void *nlpage_ptr = txn_get_page(txn, nlpage);
    memcpy(nlpage_ptr, root_page_ptr, PAGE_SIZE);

    root_header->type = BNODE_INT;
    root_header->nkeys = 1;
    root_header->parent_page = INVALID_PAGE;
    root_header->next_page = INVALID_PAGE;
    root_header->prev_page = INVALID_PAGE;

    struct IntNode *root = (struct IntNode *) root_header;
    memcpy(root->entries[0].key, pkey, MAX_KEY);
    root->head_page = nlpage;
    root->entries[0].cpage = rpage;

    struct NodeHeader *lh = txn_get_page(txn, nlpage);
    struct NodeHeader *rh = txn_get_page(txn, rpage);
    lh->parent_page = SIDX_TO_TXN_PAGE(0);
    rh->parent_page = SIDX_TO_TXN_PAGE(0);

    return 0;
}

static i32 btree_insert_txn_fix(struct BTreeTxn *txn) {
    // Phase 1: Check split and pull real pages from page bank
    // 1.1. Check leaf split
    const u16 leaf_idx = (txn->height - 1) * 2;
    const u16 nroot_idx = txn->height * 2;
    if (txn->tlb[leaf_idx + 1] != TXN_TLB_DIRTY) {
        struct LeafNode *leaf = txn_get_page(txn, leaf_idx | TXN_PAGE_MASK);
        u32 leaf_real_page = txn->tlb[leaf_idx];
        void *leaf_real = gdt_get_page(txn->handle->bank, leaf_real_page);
        memcpy(leaf_real, leaf, PAGE_SIZE);
        gdt_sync(txn->handle->bank);
        return 0;
    }
    // 1.2. Pull real page for leaf split
    if (txn_pull_real_page(txn, (leaf_idx + 1) | TXN_PAGE_MASK, leaf_idx) < 0) {
        return -1;
    }
    // 1.3. Check stack split & pull real pages for them
    u16 split_height = txn->height - 1;
    while (split_height) {
        u16 sidx = split_height - 1;
        if (txn->tlb[sidx * 2 + 1] == TXN_TLB_DIRTY) {
            if (txn_pull_real_page(txn, (sidx * 2 + 1) | TXN_PAGE_MASK, sidx * 2) < 0) {
                return -1;
            }
        } else {
            break;
        }
        split_height--;
    }
    if (!split_height) {
        if (txn_pull_real_page(txn, (nroot_idx + 1) | TXN_PAGE_MASK, 0) < 0) {
            return -1;
        }
    }
    // Phase 2: Fix Pointers
    for (u16 i = txn->height; i > split_height + 1; i--) {
        u16 idx = (i - 1) * 2;
        struct NodeHeader *node = txn_get_page(txn, idx | TXN_PAGE_MASK);
        struct NodeHeader *node_split = txn_get_page(txn, (idx + 1) | TXN_PAGE_MASK);
        // [ Node ] <-> [ Node Split ]
        node->next_page = txn->tlb[idx + 1];
        node_split->prev_page = txn->tlb[idx];
        // [ Node Split ] <-> [ Sibling ]
        if (node_split->next_page != INVALID_PAGE) {
            struct NodeHeader *sib = gdt_get_page(txn->handle->bank, node_split->next_page);
            sib->prev_page = txn->tlb[idx + 1];
        }
        // [ Node ] <-> [ Child ]
        if (node->type == BNODE_INT) {
            txn_fix_children(txn, (struct IntNode *) node, idx);
        }
        // [ Node Split ] <-> [ Child ]
        if (node->type == BNODE_INT) {
            txn_fix_children(txn, (struct IntNode *) node_split, idx + 1);
        }
    }
    u16 idx, idx_r, pidx;
    if (!split_height) {
        // Root split
        pidx = 0;
        idx = nroot_idx + 1;
        idx_r = 1;
    } else {
        // Normal split
        pidx = (split_height - 1) * 2;
        idx = split_height * 2;
        idx_r = idx + 1;
    }
    struct IntNode *pnode = txn_get_page(txn, pidx | TXN_PAGE_MASK);
    struct NodeHeader *node = txn_get_page(txn, idx | TXN_PAGE_MASK);
    struct NodeHeader *node_split = txn_get_page(txn, idx_r | TXN_PAGE_MASK);
    // [ Node ] <-> [ Node Split ]
    node->next_page = txn->tlb[idx_r];
    node_split->prev_page = txn->tlb[idx];
    // [ Node Split ] <-> [ Sibling ]
    if (node_split->next_page != INVALID_PAGE) {
        struct NodeHeader *sib = gdt_get_page(txn->handle->bank, node_split->next_page);
        sib->prev_page = txn->tlb[idx + 1];
    }
    // [ Parent ] <-> [ Child ]
    txn_fix_children(txn, pnode, pidx);
    // [ Node ] <-> [ Child ]
    if (node->type == BNODE_INT) {
        txn_fix_children(txn, (struct IntNode *) node, idx);
    }
    // [ Node Split ] <-> [ Child ]
    if (node->type == BNODE_INT) {
        txn_fix_children(txn, (struct IntNode *) node_split, idx + 1);
    }

    for (u16 i = txn->height; i > split_height; i--) {
        u16 idx = (i - 1) * 2;
        void *txn_ptr = txn_get_page(txn, idx | TXN_PAGE_MASK);
        void *real_ptr = gdt_get_page(txn->handle->bank, txn->tlb[idx]);

        void *txn_split_ptr = txn_get_page(txn, (idx + 1) | TXN_PAGE_MASK);
        void *real_split_ptr = gdt_get_page(txn->handle->bank, txn->tlb[idx + 1]);

        memcpy(real_ptr, txn_ptr, PAGE_SIZE);
        memcpy(real_split_ptr, txn_split_ptr, PAGE_SIZE);
    }
    if (!split_height) {
        u16 idx = txn->height * 2 + 1;
        void *txn_ptr = txn_get_page(txn, idx | TXN_PAGE_MASK);
        void *real_ptr = gdt_get_page(txn->handle->bank, txn->tlb[idx]);
        memcpy(real_ptr, txn_ptr, PAGE_SIZE);
    } else {
        u16 idx = (split_height - 1) * 2;
        void *txn_ptr = txn_get_page(txn, idx | TXN_PAGE_MASK);
        void *real_ptr = gdt_get_page(txn->handle->bank, txn->tlb[idx]);
        memcpy(real_ptr, txn_ptr, PAGE_SIZE);
    }
    gdt_sync(txn->handle->bank);

    return 0;
}

static i32 split_leaf_txn(struct BTreeTxn *txn, const u8 *key, const struct LeafVal *val, u8 *pkey_out,
                          u32 *npage_out) {
    struct LeafEnt tmp[MAX_NODE_ENTS + 1];

    u32 leaf_sidx = txn->height - 1;
    u32 leaf_page = SIDX_TO_TXN_PAGE(leaf_sidx);
    struct LeafNode *lleaf = txn_get_page(txn, leaf_page);
    u8 slot = leaf_find_slot(lleaf, key, NULL);

    memcpy(tmp, lleaf->entries, slot * sizeof(struct LeafEnt));
    memcpy(&tmp[slot].key, key, MAX_KEY);
    memcpy(&tmp[slot].val, val, sizeof(struct LeafVal));
    if (MAX_NODE_ENTS - slot) {
        memcpy(&tmp[slot + 1], &lleaf->entries[slot], (MAX_NODE_ENTS - slot) * sizeof(struct LeafEnt));
    }

    u32 txn_rpage = txn_alloc_node(txn, leaf_sidx * 2, BNODE_LEAF);
    if (txn_rpage == INVALID_PAGE) {
        return -1;
    }
    *npage_out = txn_rpage;
    struct LeafNode *rleaf = txn_get_page(txn, txn_rpage);

    u8 mid = (MAX_NODE_ENTS + 1) / 2;

    memcpy(lleaf->entries, tmp, mid * sizeof(struct LeafEnt));
    memcpy(rleaf->entries, &tmp[mid], (MAX_NODE_ENTS + 1 - mid) * sizeof(struct LeafEnt));

    lleaf->header.nkeys = mid;
    rleaf->header.nkeys = MAX_NODE_ENTS + 1 - mid;
    rleaf->header.next_page = lleaf->header.next_page;
    // No need to update here sibling here, do it in txn commit stage.
    rleaf->header.prev_page = leaf_page;
    lleaf->header.next_page = txn_rpage;

    memcpy(pkey_out, rleaf->entries[0].key, MAX_KEY);
    return 0;
}

static i32 split_internal_txn(struct BTreeTxn *txn, u32 ipage, const u8 *key, u32 rpage, u8 *pkey_out, u32 *npage_out) {
    struct IntEnt tmp[MAX_NODE_ENTS + 1];

    struct IntNode *inode = txn_get_page(txn, ipage);
    u8 slot = internal_find_child(inode, key, NULL);

    memcpy(tmp, inode->entries, slot * sizeof(struct IntEnt));
    memcpy(&tmp[slot].key, key, MAX_KEY);
    tmp[slot].cpage = rpage;
    memcpy(&tmp[slot + 1], &inode->entries[slot], (MAX_NODE_ENTS - slot) * sizeof(struct IntEnt));

    u32 txn_npage = txn_alloc_node(txn, ipage & ~TXN_PAGE_MASK, BNODE_INT);
    if (txn_npage == INVALID_PAGE) {
        return -1;
    }
    *npage_out = txn_npage;
    struct IntNode *nnode = txn_get_page(txn, txn_npage);

    u8 mid = (MAX_NODE_ENTS + 1) / 2;
    memcpy(pkey_out, tmp[mid].key, MAX_KEY); // This key is promoted.

    memcpy(inode->entries, tmp, mid * sizeof(struct IntEnt));
    inode->header.nkeys = mid;

    nnode->head_page = tmp[mid].cpage;
    memcpy(nnode->entries, &tmp[mid + 1], (MAX_NODE_ENTS - mid) * sizeof(struct IntEnt));

    nnode->header.nkeys = MAX_NODE_ENTS - mid;
    nnode->header.next_page = inode->header.next_page;
    nnode->header.prev_page = ipage;
    inode->header.next_page = txn_npage;

    return 0;
}

static bool delete_internal_entry(struct BTreeHandle *handle, u32 page, const u8 *key, u32 dpage);
static i32 redistribute_leaf(struct BTreeHandle *handle, u32 page);
static i32 redistribute_internal(struct BTreeHandle *handle, u32 page);
static i32 merge_leaf(struct BTreeHandle *handle, u32 page, u8 *skey_out, u32 *dpage_out);
static i32 merge_node(struct BTreeHandle *handle, u32 page, u8 *skey_out, u32 *dpage_out);

i32 btree_delete(struct BTreeHandle *handle, const u8 *key) {
    u32 parent_stack[MAX_HEIGHT]; // Stack to track path (max height ~32)
    u16 stack_top = 0;
    u32 page_num = btree_find_leaf(handle, handle->root_page, key, parent_stack, &stack_top);
    if (page_num == INVALID_PAGE) {
        return -1;
    }

    struct LeafNode *leaf = gdt_get_page(handle->bank, page_num);
    bool exact_match = false;
    u8 slot = leaf_find_slot(leaf, key, &exact_match);

    if (!exact_match) {
        return -1;
    }

    delete_leafval(handle, &leaf->entries[slot].val);
    if (slot < leaf->header.nkeys) {
        memmove(&leaf->entries[slot], &leaf->entries[slot + 1],
                (leaf->header.nkeys - slot - 1) * sizeof(struct LeafEnt));
    }
    leaf->header.nkeys--;

    // Return on enough entries or being root node itself.
    if (leaf->header.nkeys >= MIN_NODE_ENTS || !stack_top) {
        return 0;
    }
    if (!redistribute_leaf(handle, page_num)) {
        return 0;
    }

    u8 skey[MAX_KEY];
    u32 dpage;
    if (merge_leaf(handle, page_num, skey, &dpage) < 0) {
        return -1;
    }

    u32 ppage;
    while (stack_top > 1) {
        ppage = parent_stack[--stack_top];
        if (delete_internal_entry(handle, ppage, skey, dpage)) {
            return 0;
        }
        if (!redistribute_internal(handle, ppage)) {
            return 0;
        }

        u8 new_skey[MAX_KEY];
        if (merge_node(handle, ppage, new_skey, &dpage) < 0) {
            return -1;
        }

        memcpy(skey, new_skey, MAX_KEY);
    }
    u32 root_page = parent_stack[0];
    void *root_page_ptr = gdt_get_page(handle->bank, root_page);
    struct NodeHeader *root_header = root_page_ptr;
    delete_internal_entry(handle, root_page, skey, dpage);
    if (!root_header->nkeys) {
        u32 nr_page = ((struct IntNode *) root_page_ptr)->head_page;
        void *nr_page_ptr = gdt_get_page(handle->bank, nr_page);
        memcpy(root_page_ptr, nr_page_ptr, PAGE_SIZE);
        if (root_header->type == BNODE_INT) {
            struct IntNode *root = root_page_ptr;
            struct NodeHeader *ch = gdt_get_page(handle->bank, root->head_page);
            ch->parent_page = root_page;
            for (u8 i = 0; i < root->header.nkeys; i++) {
                ch = gdt_get_page(handle->bank, root->entries[i].cpage);
                ch->parent_page = root_page;
            }
        }
        gdt_unset_page(handle->bank, nr_page);
    }
    return 0;
}

static bool delete_internal_entry(struct BTreeHandle *handle, u32 page, const u8 *key, const u32 dpage) {
    struct IntNode *node = gdt_get_page(handle->bank, page);
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

static i32 redistribute_leaf(struct BTreeHandle *handle, const u32 page) {
    struct LeafNode *leaf = gdt_get_page(handle->bank, page);

    u32 lpage = leaf->header.prev_page, rpage = leaf->header.next_page, ppage = leaf->header.parent_page;
    assert(ppage != INVALID_PAGE); // Should only happens on root
    assert(lpage != INVALID_PAGE || rpage != INVALID_PAGE); // Should be handled already

    if (rpage != INVALID_PAGE) {
        struct LeafNode *rleaf = gdt_get_page(handle->bank, rpage);
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
            struct IntNode *pnode = gdt_get_page(handle->bank, ppage);
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
        struct LeafNode *lleaf = gdt_get_page(handle->bank, lpage);
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
            struct IntNode *pnode = gdt_get_page(handle->bank, ppage);
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

static i32 redistribute_internal(struct BTreeHandle *handle, u32 page) {
    struct IntNode *node = gdt_get_page(handle->bank, page);

    u32 lpage = node->header.prev_page, rpage = node->header.next_page, ppage = node->header.parent_page;
    assert(ppage != INVALID_PAGE);
    assert(lpage != INVALID_PAGE || rpage != INVALID_PAGE);

    struct IntNode *pnode = gdt_get_page(handle->bank, ppage);

    // ## Case 1: Borrow from the RIGHT sibling
    if (rpage != INVALID_PAGE) {
        struct IntNode *rnode = gdt_get_page(handle->bank, rpage);
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

            struct NodeHeader *moved = gdt_get_page(handle->bank, rnode->head_page);
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
        struct IntNode *lnode = gdt_get_page(handle->bank, lpage);
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

            struct NodeHeader *moved = gdt_get_page(handle->bank, node->head_page);
            moved->parent_page = page;

            memcpy(pnode->entries[cidx].key, lnode->entries[lnode->header.nkeys - 1].key, MAX_KEY);
            lnode->header.nkeys--;

            return 0;
        }
    }

    return -1; // Could not redistribute
}

static void merge_leaf_helper(struct BTreeHandle *handle, const u32 lpage, const u32 rpage, u8 *skey_out, u32 *dpage) {
    struct LeafNode *lleaf = gdt_get_page(handle->bank, lpage);
    struct LeafNode *rleaf = gdt_get_page(handle->bank, rpage);
    assert(lleaf->header.nkeys <= MIN_NODE_ENTS && rleaf->header.nkeys <= MIN_NODE_ENTS);
    memcpy(skey_out, rleaf->entries[0].key, MAX_KEY);
    memcpy(&lleaf->entries[lleaf->header.nkeys], rleaf->entries, rleaf->header.nkeys * sizeof(struct LeafEnt));
    lleaf->header.nkeys += rleaf->header.nkeys;
    rleaf->header.nkeys = 0;

    lleaf->header.next_page = rleaf->header.next_page;
    if (rleaf->header.next_page != INVALID_PAGE) {
        struct LeafNode *sib = gdt_get_page(handle->bank, rleaf->header.next_page);
        sib->header.prev_page = lpage;
    }
    gdt_unset_page(handle->bank, rpage);
    *dpage = rpage;
}

static i32 merge_leaf(struct BTreeHandle *handle, u32 page, u8 *skey_out, u32 *dpage) {
    struct LeafNode *leaf = gdt_get_page(handle->bank, page);

    u32 lpage = leaf->header.prev_page, rpage = leaf->header.next_page, ppage = leaf->header.parent_page;
    assert(ppage != INVALID_PAGE); // Should only happens on root
    assert(lpage != INVALID_PAGE || rpage != INVALID_PAGE); // Should be handled already

    if (lpage != INVALID_PAGE) {
        struct LeafNode *lleaf = gdt_get_page(handle->bank, lpage);
        if (lleaf->header.parent_page == ppage) {
            merge_leaf_helper(handle, lpage, page, skey_out, dpage);
            return 0;
        }
    }

    if (rpage != INVALID_PAGE) {
        struct LeafNode *rleaf = gdt_get_page(handle->bank, rpage);
        if (rleaf->header.parent_page == ppage) {
            merge_leaf_helper(handle, page, rpage, skey_out, dpage);
            return 0;
        }
    }

    return -1;
}

static void merge_node_helper(struct BTreeHandle *handle, const u32 ppage, const u32 lpage, const u32 rpage,
                              u8 *skey_out, u32 *dpage) {
    struct IntNode *pnode = gdt_get_page(handle->bank, ppage);
    struct IntNode *lnode = gdt_get_page(handle->bank, lpage);
    struct IntNode *rnode = gdt_get_page(handle->bank, rpage);

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

    struct NodeHeader *moved = gdt_get_page(handle->bank, rnode->head_page);
    moved->parent_page = lpage;
    for (u8 i = 0; i < rnode->header.nkeys; i++) {
        moved = gdt_get_page(handle->bank, rnode->entries[i].cpage);
        moved->parent_page = lpage;
    }

    gdt_unset_page(handle->bank, rpage);
    *dpage = rpage;
}

static i32 merge_node(struct BTreeHandle *handle, u32 page, u8 *skey_out, u32 *dpage) {
    struct IntNode *node = gdt_get_page(handle->bank, page);

    u32 lpage = node->header.prev_page, rpage = node->header.next_page, ppage = node->header.parent_page;
    assert(ppage != INVALID_PAGE);
    assert(lpage != INVALID_PAGE || rpage != INVALID_PAGE);

    // ## Case 1: Merge with the LEFT sibling (preferred)
    if (lpage != INVALID_PAGE) {
        struct IntNode *lnode = gdt_get_page(handle->bank, lpage);
        if (lnode->header.parent_page == ppage) {
            merge_node_helper(handle, ppage, lpage, page, skey_out, dpage);
            return 0; // Success
        }
    }

    // ## Case 2: Merge with the RIGHT sibling
    if (rpage != INVALID_PAGE) {
        struct IntNode *rnode = gdt_get_page(handle->bank, rpage);
        if (rnode->header.parent_page == ppage) {
            merge_node_helper(handle, ppage, page, rpage, skey_out, dpage);
            return 0; // Success
        }
    }

    return -1;
}
