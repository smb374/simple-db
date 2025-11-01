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

static u8 slotted_binary_search(const struct TreeNode *node, const u8 *key, bool *exact_match) {
    u8 left = 0, right = node->nkeys;

    while (left < right) {
        u8 mid = left + (right - left) / 2;
        const u8 *mid_key;
        if (node->type == BNODE_INT) {
            mid_key = TN_GET_IENT(node, mid)->key;
        } else {
            mid_key = TN_GET_LENT(node, mid)->key;
        }

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

static void defrag_tree_node(struct TreeNode *node) {
    u8 tmp[PAGE_SIZE];
    memcpy(tmp, node, PAGE_SIZE);

    const size_t ent_size = (node->type == BNODE_INT) ? sizeof(struct IntEnt) : sizeof(struct LeafEnt);

    struct TreeNode *src = (struct TreeNode *) tmp;
    node->ent_off = PAGE_SIZE;
    node->frag_bytes = 0;

    for (u8 i = 0; i < node->nkeys; i++) {
        void *ent = TN_GET_ENT(src, i);
        node->ent_off -= ent_size;
        memcpy((u8 *) node + node->ent_off, ent, ent_size);
        node->slots[i] = node->ent_off;
    }
}

static struct LeafEnt *leaf_free_entry(struct TreeNode *node, const u8 *key) {
    assert(node->type == BNODE_LEAF);
    if (node->frag_bytes >= MAX_LEAF_FRAG) {
        defrag_tree_node(node);
    }

    bool exact_match = false;
    const u8 slot = slotted_binary_search(node, key, &exact_match);
    if (!exact_match) {
        return NULL;
    }

    struct LeafEnt *ent = TN_GET_LENT(node, slot);
    if (node->nkeys - slot - 1) {
        memmove(&node->slots[slot], &node->slots[slot + 1], (node->nkeys - slot - 1) * sizeof(u16));
    }
    node->nkeys--;
    node->frag_bytes += sizeof(struct LeafEnt);
    return ent;
}

static u8 internal_alloc_entry(struct TreeNode *node, const u8 *key, bool *exact_match) {
    assert(node->type == BNODE_INT);
    if (node->frag_bytes >= MAX_INT_FRAG) {
        defrag_tree_node(node);
    }

    const u8 slot = slotted_binary_search(node, key, exact_match);
    if (*exact_match) {
        return slot;
    }
    if (slot == MAX_TN_ENTS) {
        return MAX_TN_ENTS;
    }

    if (node->nkeys - slot) {
        memmove(&node->slots[slot + 1], &node->slots[slot], (node->nkeys - slot) * sizeof(u16));
    }
    node->nkeys++;
    node->ent_off -= sizeof(struct IntEnt);
    node->slots[slot] = node->ent_off;
    return slot;
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

static void init_node(struct TreeNode *node) {
    node->nkeys = 0;
    node->frag_bytes = 0;
    node->ent_off = PAGE_SIZE;
    node->parent_page = INVALID_PAGE;
    node->prev_page = INVALID_PAGE;
    node->next_page = INVALID_PAGE;
    node->head_page = INVALID_PAGE;
}

u32 alloc_node(struct GdtPageBank *b, u8 type, u32 hint) {
    u32 page = gdt_alloc_page(b, hint);
    struct TreeNode *node = gdt_get_page(b, page);
    init_node(node);
    node->type = type;

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

        struct TreeNode *node = page;
        if (node->type == BNODE_LEAF)
            return page_num;

        parent_stack[*stack_top] = page_num;
        *stack_top += 1;
        bool exact_match = false;
        u8 cidx = slotted_binary_search(node, key, &exact_match);

        if (!cidx && !exact_match) {
            page_num = node->head_page;
        } else {
            u8 entry_idx = exact_match ? cidx : cidx - 1;
            page_num = TN_GET_IENT(node, entry_idx)->cpage;
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
    struct TreeNode *root = root_page_ptr;
    memset(root, 0, PAGE_SIZE);
    init_node(root);
    root->type = BNODE_LEAF;

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
    struct TreeNode *root = gdt_get_page(b, root_page);
    memset(root, 0, PAGE_SIZE);
    init_node(root);
    root->type = BNODE_LEAF;

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
    struct TreeNode *leaf = gdt_get_page(handle->bank, page_num);
    bool exact_match = false;
    u8 slot = slotted_binary_search(leaf, key, &exact_match);
    if (slot >= leaf->nkeys || !exact_match) {
        return -1; // Key not found
    }
    return read_leafval(handle, &TN_GET_LENT(leaf, slot)->val, value_out, len_out);
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
    struct TreeNode *node = (struct TreeNode *) ((u8 *) txn->page_stack + idx * PAGE_SIZE);
    memset(node, 0, PAGE_SIZE);
    init_node(node);
    node->type = type;
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

    struct TreeNode *leaf = gdt_get_page(handle->bank, page_num);
    bool exact_match = false;
    u8 slot = slotted_binary_search(leaf, key, &exact_match);
    if (exact_match && slot < leaf->nkeys) {
        // Key exists - update value
        struct LeafVal oval;
        memcpy(&oval, &TN_GET_LENT(leaf, slot)->val, sizeof(struct LeafVal));
        memcpy(&TN_GET_LENT(leaf, slot)->val, &lval, sizeof(struct LeafVal));
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

static void txn_fix_children(struct BTreeTxn *txn, struct TreeNode *node, const u8 idx) {
    if (node->type == BNODE_LEAF) {
        return;
    }
    u32 tpage = txn_translate(txn, node->head_page);
    if (tpage != INVALID_PAGE) {
        struct TreeNode *ch = txn_get_page(txn, node->head_page);
        ch->parent_page = txn->tlb[idx];
        node->head_page = tpage;
    } else {
        struct TreeNode *ch = gdt_get_page(txn->handle->bank, node->head_page);
        ch->parent_page = txn->tlb[idx];
    }
    for (u16 j = 0; j < node->nkeys; j++) {
        tpage = txn_translate(txn, TN_GET_IENT(node, j)->cpage);
        if (tpage != INVALID_PAGE) {
            struct TreeNode *ch = txn_get_page(txn, TN_GET_IENT(node, j)->cpage);
            ch->parent_page = txn->tlb[idx];
            TN_GET_IENT(node, j)->cpage = tpage;
        } else {
            struct TreeNode *ch = gdt_get_page(txn->handle->bank, TN_GET_IENT(node, j)->cpage);
            ch->parent_page = txn->tlb[idx];
        }
    }
}

static i32 btree_insert_txn(struct BTreeTxn *txn, const u8 slot, const u8 *key, const struct LeafVal *val) {
    u32 leaf_page = SIDX_TO_TXN_PAGE(txn->height - 1);
    struct TreeNode *leaf = txn_get_page(txn, leaf_page);

    if (leaf->nkeys < MAX_TN_ENTS) {
        // Insert into leaf (shift entries to make space)
        if (leaf->nkeys - slot > 0) {
            memmove(&leaf->slots[slot + 1], &leaf->slots[slot], (leaf->nkeys - slot) * sizeof(u16));
        }
        leaf->ent_off -= sizeof(struct LeafEnt);
        leaf->slots[slot] = leaf->ent_off;
        leaf->nkeys++;

        // Now copy the data
        memcpy(TN_GET_LENT(leaf, slot)->key, key, MAX_KEY);
        memcpy(&TN_GET_LENT(leaf, slot)->val, val, sizeof(struct LeafVal));
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
        struct TreeNode *pnode = txn_get_page(txn, ppage);
        if (pnode->nkeys < MAX_TN_ENTS) {
            bool _exact_match = false;
            u8 cidx = internal_alloc_entry(pnode, pkey, &_exact_match);
            memcpy(TN_GET_IENT(pnode, cidx)->key, pkey, MAX_KEY);
            TN_GET_IENT(pnode, cidx)->cpage = rpage;
            struct TreeNode *rh = txn_get_page(txn, rpage);
            rh->parent_page = ppage;
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
    struct TreeNode *root = root_page_ptr;
    const u16 nroot_idx = txn->height * 2;
    u32 nlpage = txn_alloc_node(txn, nroot_idx, root->type);
    if (nlpage == INVALID_PAGE) {
        return -1;
    }
    void *nlpage_ptr = txn_get_page(txn, nlpage);
    memcpy(nlpage_ptr, root_page_ptr, PAGE_SIZE);

    init_node(root);
    root->type = BNODE_INT;
    root->nkeys = 1;
    root->ent_off = PAGE_SIZE - sizeof(struct IntEnt);
    root->slots[0] = root->ent_off;
    root->head_page = nlpage;

    memcpy(TN_GET_IENT(root, 0)->key, pkey, MAX_KEY);
    TN_GET_IENT(root, 0)->cpage = rpage;

    struct TreeNode *lh = txn_get_page(txn, nlpage);
    struct TreeNode *rh = txn_get_page(txn, rpage);
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
        void *leaf = txn_get_page(txn, leaf_idx | TXN_PAGE_MASK);
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
        struct TreeNode *node = txn_get_page(txn, idx | TXN_PAGE_MASK);
        struct TreeNode *node_split = txn_get_page(txn, (idx + 1) | TXN_PAGE_MASK);
        // [ Node ] <-> [ Node Split ]
        node->next_page = txn->tlb[idx + 1];
        node_split->prev_page = txn->tlb[idx];
        // [ Node Split ] <-> [ Sibling ]
        if (node_split->next_page != INVALID_PAGE) {
            struct TreeNode *sib = gdt_get_page(txn->handle->bank, node_split->next_page);
            sib->prev_page = txn->tlb[idx + 1];
        }
        // [ Node ] <-> [ Child ]
        if (node->type == BNODE_INT) {
            txn_fix_children(txn, node, idx);
        }
        // [ Node Split ] <-> [ Child ]
        if (node->type == BNODE_INT) {
            txn_fix_children(txn, node_split, idx + 1);
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
    struct TreeNode *pnode = txn_get_page(txn, pidx | TXN_PAGE_MASK);
    struct TreeNode *node = txn_get_page(txn, idx | TXN_PAGE_MASK);
    struct TreeNode *node_split = txn_get_page(txn, idx_r | TXN_PAGE_MASK);
    // [ Node ] <-> [ Node Split ]
    node->next_page = txn->tlb[idx_r];
    node_split->prev_page = txn->tlb[idx];
    // [ Node Split ] <-> [ Sibling ]
    if (node_split->next_page != INVALID_PAGE) {
        struct TreeNode *sib = gdt_get_page(txn->handle->bank, node_split->next_page);
        sib->prev_page = txn->tlb[idx + 1];
    }
    // [ Parent ] <-> [ Child ]
    txn_fix_children(txn, pnode, pidx);
    // [ Node ] <-> [ Child ]
    if (node->type == BNODE_INT) {
        txn_fix_children(txn, node, idx);
    }
    // [ Node Split ] <-> [ Child ]
    if (node->type == BNODE_INT) {
        txn_fix_children(txn, node_split, idx + 1);
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
    struct LeafEnt tmp[MAX_TN_ENTS + 1];

    u32 leaf_sidx = txn->height - 1;
    u32 leaf_page = SIDX_TO_TXN_PAGE(leaf_sidx);
    struct TreeNode *lleaf = txn_get_page(txn, leaf_page);
    u8 slot = slotted_binary_search(lleaf, key, NULL);

    for (u8 i = 0; i < slot; i++) {
        memcpy(&tmp[i], TN_GET_LENT(lleaf, i), sizeof(LeafEnt));
    }
    memcpy(&tmp[slot].key, key, MAX_KEY);
    memcpy(&tmp[slot].val, val, sizeof(struct LeafVal));

    if (MAX_TN_ENTS - slot) {
        for (u32 i = slot; i < MAX_TN_ENTS; i++) {
            memcpy(&tmp[i + 1], TN_GET_LENT(lleaf, i), sizeof(struct LeafEnt));
        }
    }

    u32 txn_rpage = txn_alloc_node(txn, leaf_sidx * 2, BNODE_LEAF);
    if (txn_rpage == INVALID_PAGE) {
        return -1;
    }
    *npage_out = txn_rpage;
    struct TreeNode *rleaf = txn_get_page(txn, txn_rpage);

    lleaf->frag_bytes = 0;
    lleaf->ent_off = PAGE_SIZE;

    u8 mid = (MAX_TN_ENTS + 1) / 2;
    memcpy(pkey_out, tmp[mid].key, MAX_KEY);

    for (u8 i = 0; i < mid; i++) {
        lleaf->ent_off -= sizeof(struct LeafEnt);
        lleaf->slots[i] = lleaf->ent_off;
        struct LeafEnt *ent = TN_GET_LENT(lleaf, i);
        memcpy(ent, &tmp[i], sizeof(struct LeafEnt));
    }
    for (u32 i = mid; i < MAX_TN_ENTS + 1; i++) {
        rleaf->ent_off -= sizeof(struct LeafEnt);
        rleaf->slots[i - mid] = rleaf->ent_off;
        struct LeafEnt *ent = TN_GET_LENT(rleaf, i - mid);
        memcpy(ent, &tmp[i], sizeof(struct LeafEnt));
    }
    lleaf->nkeys = mid;
    rleaf->nkeys = MAX_TN_ENTS + 1 - mid;
    rleaf->next_page = lleaf->next_page;
    // No need to update here sibling here, do it in txn commit stage.
    rleaf->prev_page = leaf_page;
    lleaf->next_page = txn_rpage;

    return 0;
}

static i32 split_internal_txn(struct BTreeTxn *txn, u32 ipage, const u8 *key, u32 rpage, u8 *pkey_out, u32 *npage_out) {
    struct IntEnt tmp[MAX_TN_ENTS + 1];

    struct TreeNode *inode = txn_get_page(txn, ipage);
    u8 slot = slotted_binary_search(inode, key, NULL);

    for (u8 i = 0; i < slot; i++) {
        memcpy(&tmp[i], TN_GET_IENT(inode, i), sizeof(IntEnt));
    }
    memcpy(&tmp[slot].key, key, MAX_KEY);
    tmp[slot].cpage = rpage;

    if (MAX_TN_ENTS - slot) {
        for (u32 i = slot; i < MAX_TN_ENTS; i++) {
            memcpy(&tmp[i + 1], TN_GET_IENT(inode, i), sizeof(struct IntEnt));
        }
    }

    u32 txn_npage = txn_alloc_node(txn, ipage & ~TXN_PAGE_MASK, BNODE_INT);
    if (txn_npage == INVALID_PAGE) {
        return -1;
    }
    *npage_out = txn_npage;
    struct TreeNode *nnode = txn_get_page(txn, txn_npage);

    inode->frag_bytes = 0;
    inode->ent_off = PAGE_SIZE;

    u8 mid = (MAX_TN_ENTS + 1) / 2;
    memcpy(pkey_out, tmp[mid].key, MAX_KEY); // This key is promoted.

    for (u8 i = 0; i < mid; i++) {
        inode->ent_off -= sizeof(struct IntEnt);
        inode->slots[i] = inode->ent_off;
        struct IntEnt *ent = TN_GET_IENT(inode, i);
        memcpy(ent, &tmp[i], sizeof(struct IntEnt));
    }
    nnode->head_page = tmp[mid].cpage;
    for (u32 i = mid + 1; i < MAX_TN_ENTS + 1; i++) {
        nnode->ent_off -= sizeof(struct IntEnt);
        nnode->slots[i - mid - 1] = nnode->ent_off;
        struct IntEnt *ent = TN_GET_IENT(nnode, i - mid - 1);
        memcpy(ent, &tmp[i], sizeof(struct IntEnt));
    }

    inode->nkeys = mid;
    nnode->nkeys = MAX_TN_ENTS - mid;
    nnode->next_page = inode->next_page;
    nnode->prev_page = ipage;
    inode->next_page = txn_npage;

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

    struct TreeNode *leaf = gdt_get_page(handle->bank, page_num);
    struct LeafEnt *ent = leaf_free_entry(leaf, key);

    if (!ent) {
        return -1;
    }
    delete_leafval(handle, &ent->val);

    // Return on enough entries or being root node itself.
    if (leaf->nkeys >= MIN_TN_ENTS || !stack_top) {
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
    struct TreeNode *root = root_page_ptr;
    delete_internal_entry(handle, root_page, skey, dpage);
    if (!root->nkeys) {
        u32 nr_page = root->head_page;
        struct TreeNode *nroot = gdt_get_page(handle->bank, nr_page);
        u8 nr_keys = nroot->nkeys;
        memcpy(root_page_ptr, nroot, PAGE_SIZE);
        root->parent_page = INVALID_PAGE;
        if (root->type == BNODE_INT) {
            struct TreeNode *ch = gdt_get_page(handle->bank, root->head_page);
            ch->parent_page = root_page;
            for (u8 i = 0; i < nr_keys; i++) {
                ch = gdt_get_page(handle->bank, TN_GET_IENT(root, i)->cpage);
                ch->parent_page = root_page;
            }
        }
        gdt_unset_page(handle->bank, nr_page);
    }
    return 0;
}
static bool delete_internal_entry(struct BTreeHandle *handle, u32 page, const u8 *key, const u32 dpage) {
    struct TreeNode *node = gdt_get_page(handle->bank, page);
    bool exact_match = false;
    u8 cidx = slotted_binary_search(node, key, &exact_match);

    if (exact_match) {
        // The key exists in the entries array
        assert(TN_GET_IENT(node, cidx)->cpage == dpage);
        // Remove this slot
        if (node->nkeys - cidx - 1 > 0) {
            memmove(&node->slots[cidx], &node->slots[cidx + 1], (node->nkeys - cidx - 1) * sizeof(u16));
        }
        node->nkeys--;
        node->frag_bytes += sizeof(struct IntEnt);
    } else {
        // The key would be before entries[cidx], so the deleted page must be head_page or entries[cidx-1]
        if (cidx == 0) {
            // The deleted page is head_page, replace it with entries[0].cpage
            assert(node->head_page == dpage);
            if (node->nkeys > 0) {
                node->head_page = TN_GET_IENT(node, 0)->cpage;
                // Remove entries[0]
                if (node->nkeys > 1) {
                    memmove(&node->slots[0], &node->slots[1], (node->nkeys - 1) * sizeof(u16));
                }
                node->nkeys--;
                node->frag_bytes += sizeof(struct IntEnt);
            }
        } else {
            // The deleted page is entries[cidx-1].cpage
            assert(cidx > 0);
            assert(TN_GET_IENT(node, cidx - 1)->cpage == dpage);
            // Remove slot at cidx-1
            if (node->nkeys - cidx > 0) {
                memmove(&node->slots[cidx - 1], &node->slots[cidx], (node->nkeys - cidx) * sizeof(u16));
            }
            node->nkeys--;
            node->frag_bytes += sizeof(struct IntEnt);
        }
    }

    return node->nkeys >= MIN_TN_ENTS;
}

static i32 redistribute_leaf(struct BTreeHandle *handle, const u32 page) {
    struct TreeNode *leaf = gdt_get_page(handle->bank, page);
    defrag_tree_node(leaf);

    u32 lpage = leaf->prev_page, rpage = leaf->next_page, ppage = leaf->parent_page;
    assert(ppage != INVALID_PAGE); // Should only happens on root
    assert(lpage != INVALID_PAGE || rpage != INVALID_PAGE); // Should be handled already

    if (rpage != INVALID_PAGE) {
        struct TreeNode *rleaf = gdt_get_page(handle->bank, rpage);
        if (rleaf->parent_page == ppage && rleaf->nkeys > MIN_TN_ENTS) {
            // The key separating `leaf` and `rleaf` is the smallest key in `rleaf`.
            // Save it so we can find it in the parent.
            u8 old_sep[MAX_KEY];
            // move leaf[nkeys] <- rleaf[0]
            struct LeafEnt *rent = TN_GET_LENT(rleaf, 0);
            memcpy(old_sep, rent->key, MAX_KEY);
            memmove(&rleaf->slots[0], &rleaf->slots[1], (rleaf->nkeys - 1) * sizeof(u16));
            rleaf->nkeys--;
            rleaf->frag_bytes += sizeof(struct LeafEnt);

            leaf->ent_off -= sizeof(struct LeafEnt);
            leaf->slots[leaf->nkeys] = leaf->ent_off;
            leaf->nkeys++;
            memcpy(TN_GET_LENT(leaf, leaf->nkeys - 1), rent, sizeof(struct LeafEnt));
            // Update the parent's separator key.
            struct TreeNode *pnode = gdt_get_page(handle->bank, ppage);
            bool exact_match = false;
            u8 cidx = slotted_binary_search(pnode, old_sep, &exact_match);

            assert(exact_match || cidx > 0);
            memcpy(TN_GET_IENT(pnode, cidx - !exact_match)->key, TN_GET_LENT(rleaf, 0)->key, MAX_KEY);
            return 0; // Success
        }
    }

    if (lpage != INVALID_PAGE) {
        struct TreeNode *lleaf = gdt_get_page(handle->bank, lpage);
        if (lleaf->parent_page == ppage && lleaf->nkeys > MIN_TN_ENTS) {
            // This is the key that separates lleaf and leaf in the parent
            u8 old_sep[MAX_KEY];
            memcpy(old_sep, TN_GET_LENT(leaf, 0)->key, MAX_KEY);
            // move lleaf[nkeys] -> leaf[0]
            struct LeafEnt *lent = TN_GET_LENT(lleaf, lleaf->nkeys - 1);
            lleaf->nkeys--;
            lleaf->frag_bytes += sizeof(struct LeafEnt);

            memmove(&leaf->slots[1], &leaf->slots[0], leaf->nkeys * sizeof(u16));
            leaf->ent_off -= sizeof(struct LeafEnt);
            leaf->slots[0] = leaf->ent_off;
            leaf->nkeys++;
            memcpy(TN_GET_LENT(leaf, 0), lent, sizeof(struct LeafEnt));

            struct TreeNode *pnode = gdt_get_page(handle->bank, ppage);
            bool exact_match = false;
            u8 cidx = slotted_binary_search(pnode, old_sep, &exact_match);

            assert(exact_match || cidx > 0);
            memcpy(TN_GET_IENT(pnode, cidx - !exact_match)->key, TN_GET_LENT(leaf, 0)->key, MAX_KEY);
            return 0; // Success
        }
    }

    return -1;
}

static i32 redistribute_internal(struct BTreeHandle *handle, u32 page) {
    struct TreeNode *node = gdt_get_page(handle->bank, page);
    defrag_tree_node(node);

    u32 lpage = node->prev_page, rpage = node->next_page, ppage = node->parent_page;
    assert(ppage != INVALID_PAGE);
    assert(lpage != INVALID_PAGE || rpage != INVALID_PAGE);

    struct TreeNode *pnode = gdt_get_page(handle->bank, ppage);

    // ## Case 1: Borrow from the RIGHT sibling
    if (rpage != INVALID_PAGE) {
        struct TreeNode *rnode = gdt_get_page(handle->bank, rpage);
        if (rnode->parent_page == ppage && rnode->nkeys > MIN_TN_ENTS) {
            bool exact_match = false;
            u8 cidx = slotted_binary_search(pnode, TN_GET_IENT(rnode, 0)->key, &exact_match);
            assert(exact_match || cidx > 0);
            cidx -= !exact_match;

            // Move node[nkeys-1] <- rnode[0]
            node->ent_off -= sizeof(struct IntEnt);
            node->slots[node->nkeys] = node->ent_off;
            node->nkeys++;
            memcpy(TN_GET_IENT(node, node->nkeys - 1)->key, TN_GET_IENT(pnode, cidx)->key, MAX_KEY);
            TN_GET_IENT(node, node->nkeys - 1)->cpage = rnode->head_page;

            struct TreeNode *moved = gdt_get_page(handle->bank, rnode->head_page);
            moved->parent_page = page;

            memcpy(TN_GET_IENT(pnode, cidx)->key, TN_GET_IENT(rnode, 0)->key, MAX_KEY);
            rnode->head_page = TN_GET_IENT(rnode, 0)->cpage;

            memmove(&rnode->slots[0], &rnode->slots[1], (rnode->nkeys - 1) * sizeof(u16));
            rnode->nkeys--;
            rnode->frag_bytes += sizeof(struct IntEnt);

            return 0; // Success
        }
    }

    // ## Case 2: Borrow from the LEFT sibling
    if (lpage != INVALID_PAGE) {
        struct TreeNode *lnode = gdt_get_page(handle->bank, lpage);
        if (lnode->parent_page == ppage && lnode->nkeys > MIN_TN_ENTS) {
            bool exact_match = false;
            u8 cidx = slotted_binary_search(pnode, TN_GET_IENT(node, 0)->key, &exact_match);
            assert(exact_match || cidx > 0);
            cidx -= !exact_match;

            // move lnode[nkeys-1] -> node[0]
            struct IntEnt *lent = TN_GET_IENT(lnode, lnode->nkeys - 1);

            memmove(&node->slots[1], &node->slots[0], node->nkeys * sizeof(u16));
            node->ent_off -= sizeof(struct IntEnt);
            node->slots[0] = node->ent_off;
            node->nkeys++;

            memcpy(TN_GET_IENT(node, 0)->key, TN_GET_IENT(pnode, cidx)->key, MAX_KEY);
            TN_GET_IENT(node, 0)->cpage = node->head_page;
            node->head_page = lent->cpage;

            struct TreeNode *moved = gdt_get_page(handle->bank, node->head_page);
            moved->parent_page = page;

            memcpy(TN_GET_IENT(pnode, cidx)->key, lent->key, MAX_KEY);
            lnode->nkeys--;
            lnode->frag_bytes += sizeof(struct IntEnt);

            return 0;
        }
    }

    return -1; // Could not redistribute
}

static void merge_leaf_helper(struct BTreeHandle *handle, const u32 lpage, const u32 rpage, u8 *skey_out, u32 *dpage) {
    struct TreeNode *lleaf = gdt_get_page(handle->bank, lpage);
    struct TreeNode *rleaf = gdt_get_page(handle->bank, rpage);
    assert(lleaf->nkeys <= MIN_TN_ENTS && rleaf->nkeys <= MIN_TN_ENTS);
    defrag_tree_node(lleaf);

    memcpy(skey_out, TN_GET_LENT(rleaf, 0)->key, MAX_KEY);
    for (u8 i = 0; i < rleaf->nkeys; i++) {
        u8 idx = lleaf->nkeys + i;
        lleaf->ent_off -= sizeof(struct LeafEnt);
        lleaf->slots[idx] = lleaf->ent_off;
        memcpy(TN_GET_LENT(lleaf, idx), TN_GET_LENT(rleaf, i), sizeof(struct LeafEnt));
    }
    lleaf->nkeys += rleaf->nkeys;
    rleaf->nkeys = 0;

    lleaf->next_page = rleaf->next_page;
    if (rleaf->next_page != INVALID_PAGE) {
        struct TreeNode *sib = gdt_get_page(handle->bank, rleaf->next_page);
        sib->prev_page = lpage;
    }
    gdt_unset_page(handle->bank, rpage);
    *dpage = rpage;
}

static i32 merge_leaf(struct BTreeHandle *handle, u32 page, u8 *skey_out, u32 *dpage) {
    struct TreeNode *leaf = gdt_get_page(handle->bank, page);

    u32 lpage = leaf->prev_page, rpage = leaf->next_page, ppage = leaf->parent_page;
    assert(ppage != INVALID_PAGE); // Should only happens on root
    assert(lpage != INVALID_PAGE || rpage != INVALID_PAGE); // Should be handled already

    if (lpage != INVALID_PAGE) {
        struct TreeNode *lleaf = gdt_get_page(handle->bank, lpage);
        if (lleaf->parent_page == ppage) {
            merge_leaf_helper(handle, lpage, page, skey_out, dpage);
            return 0;
        }
    }

    if (rpage != INVALID_PAGE) {
        struct TreeNode *rleaf = gdt_get_page(handle->bank, rpage);
        if (rleaf->parent_page == ppage) {
            merge_leaf_helper(handle, page, rpage, skey_out, dpage);
            return 0;
        }
    }

    return -1;
}

static void merge_node_helper(struct BTreeHandle *handle, const u32 ppage, const u32 lpage, const u32 rpage,
                              u8 *skey_out, u32 *dpage) {
    struct TreeNode *pnode = gdt_get_page(handle->bank, ppage);
    struct TreeNode *lnode = gdt_get_page(handle->bank, lpage);
    struct TreeNode *rnode = gdt_get_page(handle->bank, rpage);

    bool exact_match = false;
    u8 cidx = slotted_binary_search(pnode, TN_GET_IENT(rnode, 0)->key, &exact_match);
    assert(exact_match || cidx > 0);
    cidx -= !exact_match;

    memcpy(skey_out, TN_GET_IENT(pnode, cidx)->key, MAX_KEY);
    lnode->ent_off -= sizeof(struct IntEnt);
    lnode->slots[lnode->nkeys] = lnode->ent_off;
    lnode->nkeys++;
    memcpy(TN_GET_IENT(lnode, lnode->nkeys - 1)->key, TN_GET_IENT(pnode, cidx)->key, MAX_KEY);
    TN_GET_IENT(lnode, lnode->nkeys - 1)->cpage = rnode->head_page;

    struct TreeNode *moved = gdt_get_page(handle->bank, rnode->head_page);
    moved->parent_page = lpage;
    for (u8 i = 0; i < rnode->nkeys; i++) {
        u8 idx = lnode->nkeys + i;
        lnode->ent_off -= sizeof(struct IntEnt);
        lnode->slots[idx] = lnode->ent_off;
        memcpy(TN_GET_IENT(lnode, idx), TN_GET_IENT(rnode, i), sizeof(struct IntEnt));
        moved = gdt_get_page(handle->bank, TN_GET_IENT(rnode, i)->cpage);
        moved->parent_page = lpage;
    }

    lnode->nkeys += rnode->nkeys;
    rnode->nkeys = 0;
    gdt_unset_page(handle->bank, rpage);
    *dpage = rpage;
}

static i32 merge_node(struct BTreeHandle *handle, u32 page, u8 *skey_out, u32 *dpage) {
    struct TreeNode *node = gdt_get_page(handle->bank, page);

    u32 lpage = node->prev_page, rpage = node->next_page, ppage = node->parent_page;
    assert(ppage != INVALID_PAGE);
    assert(lpage != INVALID_PAGE || rpage != INVALID_PAGE);

    // ## Case 1: Merge with the LEFT sibling (preferred)
    if (lpage != INVALID_PAGE) {
        struct TreeNode *lnode = gdt_get_page(handle->bank, lpage);
        if (lnode->parent_page == ppage) {
            merge_node_helper(handle, ppage, lpage, page, skey_out, dpage);
            return 0; // Success
        }
    }

    // ## Case 2: Merge with the RIGHT sibling
    if (rpage != INVALID_PAGE) {
        struct TreeNode *rnode = gdt_get_page(handle->bank, rpage);
        if (rnode->parent_page == ppage) {
            merge_node_helper(handle, ppage, page, rpage, skey_out, dpage);
            return 0; // Success
        }
    }

    return -1;
}
