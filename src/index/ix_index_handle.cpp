/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_index_handle.h"

#include <algorithm>
#include <cstring>
#include <mutex>

#include "ix_scan.h"

static int ix_cmp_keys(const IxFileHdr *hdr, const char *a, const char *b) {
    return ix_compare(a, b, hdr->col_types_, hdr->col_lens_);
}

int IxNodeHandle::lower_bound(const char *target) const {
    int l = 0;
    int r = page_hdr->num_key;
    while (l < r) {
        int m = (l + r) / 2;
        if (ix_cmp_keys(file_hdr, get_key(m), target) < 0) {
            l = m + 1;
        } else {
            r = m;
        }
    }
    return l;
}

int IxNodeHandle::upper_bound(const char *target) const {
    if (page_hdr->is_leaf) {
        int l = 0;
        int r = page_hdr->num_key;
        while (l < r) {
            int m = (l + r) / 2;
            if (ix_cmp_keys(file_hdr, get_key(m), target) <= 0) {
                l = m + 1;
            } else {
                r = m;
            }
        }
        return l;
    }
    int l = 1;
    int r = page_hdr->num_key;
    while (l < r) {
        int m = (l + r) / 2;
        if (ix_cmp_keys(file_hdr, get_key(m), target) <= 0) {
            l = m + 1;
        } else {
            r = m;
        }
    }
    return l;
}

bool IxNodeHandle::leaf_lookup(const char *key, Rid **value) {
    int pos = lower_bound(key);
    if (pos >= page_hdr->num_key || ix_cmp_keys(file_hdr, get_key(pos), key) != 0) {
        return false;
    }
    *value = get_rid(pos);
    return true;
}

page_id_t IxNodeHandle::internal_lookup(const char *key) {
    if (page_hdr->num_key == 1) {
        return value_at(0);
    }
    int i = 1;
    for (; i < page_hdr->num_key; i++) {
        if (ix_cmp_keys(file_hdr, key, get_key(i)) < 0) {
            break;
        }
    }
    return value_at(i - 1);
}

void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
    if (n <= 0 || pos < 0 || pos > page_hdr->num_key) {
        return;
    }
    int nk = file_hdr->col_tot_len_;
    int old_sz = page_hdr->num_key;
    memmove(keys + (pos + n) * nk, keys + pos * nk, (old_sz - pos) * nk);
    memmove(rids + pos + n, rids + pos, (old_sz - pos) * sizeof(Rid));
    for (int i = 0; i < n; i++) {
        memcpy(keys + (pos + i) * nk, key + i * nk, nk);
        rids[pos + i] = rid[i];
    }
    page_hdr->num_key = old_sz + n;
}

int IxNodeHandle::insert(const char *key, const Rid &value) {
    int pos = lower_bound(key);
    if (pos < page_hdr->num_key && ix_cmp_keys(file_hdr, get_key(pos), key) == 0) {
        return page_hdr->num_key;
    }
    insert_pair(pos, key, value);
    return page_hdr->num_key;
}

void IxNodeHandle::erase_pair(int pos) {
    if (pos < 0 || pos >= page_hdr->num_key) {
        return;
    }
    int nk = file_hdr->col_tot_len_;
    int old_sz = page_hdr->num_key;
    memmove(keys + pos * nk, keys + (pos + 1) * nk, (old_sz - pos - 1) * nk);
    memmove(rids + pos, rids + pos + 1, (old_sz - pos - 1) * sizeof(Rid));
    page_hdr->num_key = old_sz - 1;
}

int IxNodeHandle::remove(const char *key) {
    int pos = lower_bound(key);
    if (pos >= page_hdr->num_key || ix_cmp_keys(file_hdr, get_key(pos), key) != 0) {
        return page_hdr->num_key;
    }
    erase_pair(pos);
    return page_hdr->num_key;
}

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    char *buf = new char[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(buf);
    delete[] buf;

    int now_page_no = disk_manager_->get_fd2pageno(fd);
    disk_manager_->set_fd2pageno(fd, now_page_no + 1);
}

std::pair<IxNodeHandle *, bool> IxIndexHandle::find_leaf_page(const char *key, Operation operation,
                                                              Transaction *transaction, bool find_first) {
    (void)operation;
    (void)transaction;
    (void)find_first;
    if (is_empty()) {
        return {nullptr, false};
    }
    page_id_t cur = file_hdr_->root_page_;
    IxNodeHandle *node = fetch_node(cur);
    while (!node->is_leaf_page()) {
        page_id_t nxt = node->internal_lookup(key);
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
        node = fetch_node(nxt);
    }
    return {node, false};
}

bool IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    (void)transaction;
    std::scoped_lock<std::mutex> lk(root_latch_);
    result->clear();
    if (is_empty()) {
        return false;
    }
    auto pr = find_leaf_page(key, Operation::FIND, nullptr, false);
    IxNodeHandle *leaf = pr.first;
    if (leaf == nullptr) {
        return false;
    }
    Rid *prid = nullptr;
    bool ok = leaf->leaf_lookup(key, &prid);
    if (ok) {
        result->push_back(*prid);
    }
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
    return ok;
}

IxNodeHandle *IxIndexHandle::split(IxNodeHandle *node) {
    int n = node->get_size();
    int mid = n / 2;
    IxNodeHandle *new_node = create_node();
    new_node->page_hdr->is_leaf = node->is_leaf_page();
    new_node->page_hdr->parent = node->get_parent_page_no();
    new_node->page_hdr->num_key = 0;
    new_node->page_hdr->prev_leaf = IX_NO_PAGE;
    new_node->page_hdr->next_leaf = IX_NO_PAGE;

    int right_cnt = n - mid;
    new_node->insert_pairs(0, node->get_key(mid), node->get_rid(mid), right_cnt);
    node->set_size(mid);

    if (node->is_leaf_page()) {
        page_id_t old_next = node->get_next_leaf();
        new_node->set_prev_leaf(node->get_page_no());
        new_node->set_next_leaf(old_next);
        node->set_next_leaf(new_node->get_page_no());
        IxNodeHandle *next_pg = fetch_node(old_next);
        next_pg->set_prev_leaf(new_node->get_page_no());
        buffer_pool_manager_->unpin_page(next_pg->get_page_id(), true);
        if (file_hdr_->last_leaf_ == node->get_page_no()) {
            file_hdr_->last_leaf_ = new_node->get_page_no();
        }
    } else {
        for (int i = 0; i < new_node->get_size(); i++) {
            maintain_child(new_node, i);
        }
    }
    return new_node;
}

void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                       Transaction *transaction) {
    (void)transaction;
    if (old_node->is_root_page()) {
        IxNodeHandle *new_root = create_node();
        new_root->page_hdr->is_leaf = false;
        new_root->page_hdr->parent = INVALID_PAGE_ID;
        new_root->page_hdr->num_key = 0;
        new_root->page_hdr->prev_leaf = IX_NO_PAGE;
        new_root->page_hdr->next_leaf = IX_NO_PAGE;

        new_root->insert_pair(0, old_node->get_key(0), Rid{old_node->get_page_no(), 0});
        new_root->insert_pair(1, key, Rid{new_node->get_page_no(), 0});

        old_node->set_parent_page_no(new_root->get_page_no());
        new_node->set_parent_page_no(new_root->get_page_no());

        update_root_page_no(new_root->get_page_no());
        buffer_pool_manager_->unpin_page(new_root->get_page_id(), true);
        buffer_pool_manager_->unpin_page(old_node->get_page_id(), true);
        buffer_pool_manager_->unpin_page(new_node->get_page_id(), true);
        return;
    }

    IxNodeHandle *parent = fetch_node(old_node->get_parent_page_no());
    int idx = parent->find_child(old_node);
    Rid new_rid{.page_no = new_node->get_page_no(), .slot_no = 0};
    parent->insert_pairs(idx + 1, key, &new_rid, 1);
    new_node->set_parent_page_no(parent->get_page_no());

    if (parent->get_size() > parent->get_max_size()) {
        IxNodeHandle *new_parent_sibling = split(parent);
        char *up_key = new_parent_sibling->get_key(0);
        insert_into_parent(parent, up_key, new_parent_sibling, transaction);
        buffer_pool_manager_->unpin_page(new_parent_sibling->get_page_id(), true);
    } else {
        buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
    }
    buffer_pool_manager_->unpin_page(old_node->get_page_id(), true);
    buffer_pool_manager_->unpin_page(new_node->get_page_id(), true);
}

page_id_t IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    (void)transaction;
    std::scoped_lock<std::mutex> lk(root_latch_);
    if (is_empty()) {
        return 0;
    }
    auto pr = find_leaf_page(key, Operation::INSERT, nullptr, false);
    IxNodeHandle *leaf = pr.first;
    if (leaf == nullptr) {
        return 0;
    }

    int before = leaf->get_size();
    page_id_t leaf_page = leaf->get_page_no();
    int after = leaf->insert(key, value);
    if (after == before) {
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
        return 0;
    }
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), true);

    leaf = fetch_node(leaf_page);
    if (leaf->get_size() > leaf->get_max_size()) {
        IxNodeHandle *new_leaf = split(leaf);
        char *up = new_leaf->get_key(0);
        insert_into_parent(leaf, up, new_leaf, transaction);
    } else {
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), true);
    }
    return leaf_page;
}

bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node) {
    if (old_root_node->is_leaf_page()) {
        if (old_root_node->get_size() == 0) {
            update_root_page_no(IX_NO_PAGE);
            file_hdr_->first_leaf_ = IX_INIT_ROOT_PAGE;
            file_hdr_->last_leaf_ = IX_INIT_ROOT_PAGE;
            return true;
        }
        return false;
    }
    if (old_root_node->get_size() == 1) {
        page_id_t child = old_root_node->remove_and_return_only_child();
        IxNodeHandle *ch = fetch_node(child);
        ch->set_parent_page_no(INVALID_PAGE_ID);
        buffer_pool_manager_->unpin_page(ch->get_page_id(), true);
        update_root_page_no(child);
        if (ch->is_leaf_page()) {
            file_hdr_->first_leaf_ = child;
            file_hdr_->last_leaf_ = child;
        }
        return true;
    }
    return false;
}

void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    if (node->is_leaf_page()) {
        if (index == 0) {
            node->insert_pair(node->get_size(), neighbor_node->get_key(0), *neighbor_node->get_rid(0));
            neighbor_node->erase_pair(0);
            parent->set_key(index + 1, neighbor_node->get_key(0));
        } else {
            int last = neighbor_node->get_size() - 1;
            node->insert_pairs(0, neighbor_node->get_key(last), neighbor_node->get_rid(last), 1);
            neighbor_node->erase_pair(last);
            parent->set_key(index, node->get_key(0));
        }
        maintain_parent(node);
        maintain_parent(neighbor_node);
        buffer_pool_manager_->unpin_page(node->get_page_id(), true);
        buffer_pool_manager_->unpin_page(neighbor_node->get_page_id(), true);
        buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
        return;
    }

    if (index == 0) {
        node->insert_pair(node->get_size(), parent->get_key(index + 1), *neighbor_node->get_rid(0));
        parent->set_key(index + 1, neighbor_node->get_key(0));
        neighbor_node->erase_pair(0);
        maintain_child(node, node->get_size() - 1);
    } else {
        int last = neighbor_node->get_size() - 1;
        Rid last_rid = *neighbor_node->get_rid(last);
        neighbor_node->erase_pair(last);
        node->insert_pairs(0, parent->get_key(index), &last_rid, 1);
        parent->set_key(index, neighbor_node->get_key(neighbor_node->get_size() - 1));
        maintain_child(node, 0);
    }
    maintain_parent(node);
    maintain_parent(neighbor_node);
    buffer_pool_manager_->unpin_page(node->get_page_id(), true);
    buffer_pool_manager_->unpin_page(neighbor_node->get_page_id(), true);
    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
}

bool IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                              Transaction *transaction, bool *root_is_latched) {
    (void)transaction;
    (void)root_is_latched;
    IxNodeHandle *nb = *neighbor_node;
    IxNodeHandle *nd = *node;
    IxNodeHandle *par = *parent;

    if (index == 0) {
        std::swap(nb, nd);
    }

    int nmove = nd->get_size();
    nb->insert_pairs(nb->get_size(), nd->get_key(0), nd->get_rid(0), nmove);
    if (!nb->is_leaf_page()) {
        for (int i = nb->get_size() - nmove; i < nb->get_size(); i++) {
            maintain_child(nb, i);
        }
    }

    if (nd->is_leaf_page()) {
        if (nd->get_page_no() == file_hdr_->last_leaf_) {
            file_hdr_->last_leaf_ = nb->get_page_no();
        }
        if (nd->get_page_no() == file_hdr_->first_leaf_) {
            file_hdr_->first_leaf_ = nb->get_page_no();
        }
        erase_leaf(nd);
    }

    int nd_idx = par->find_child(nd);
    par->erase_pair(nd_idx);

    release_node_handle(*nd);
    buffer_pool_manager_->unpin_page(nd->get_page_id(), true);
    buffer_pool_manager_->delete_page(nd->get_page_id());

    *neighbor_node = nb;
    *node = nullptr;
    *parent = par;

    if (par->is_root_page()) {
        if (adjust_root(par)) {
            buffer_pool_manager_->unpin_page(par->get_page_id(), true);
            buffer_pool_manager_->delete_page(par->get_page_id());
        } else {
            buffer_pool_manager_->unpin_page(par->get_page_id(), true);
        }
    } else if (par->get_size() < par->get_min_size()) {
        coalesce_or_redistribute(par, transaction, nullptr);
    } else {
        buffer_pool_manager_->unpin_page(par->get_page_id(), true);
    }

    maintain_parent(nb);
    buffer_pool_manager_->unpin_page(nb->get_page_id(), true);
    return false;
}

bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction, bool *root_is_latched) {
    (void)root_is_latched;
    if (node->is_root_page()) {
        bool removed = adjust_root(node);
        buffer_pool_manager_->unpin_page(node->get_page_id(), true);
        if (removed) {
            buffer_pool_manager_->delete_page(node->get_page_id());
        }
        return false;
    }

    int min_size = node->get_min_size();
    if (node->get_size() >= min_size) {
        buffer_pool_manager_->unpin_page(node->get_page_id(), true);
        return false;
    }

    IxNodeHandle *parent = fetch_node(node->get_parent_page_no());
    int index = parent->find_child(node);

    page_id_t nb_page = (index > 0) ? parent->value_at(index - 1) : parent->value_at(index + 1);
    IxNodeHandle *neighbor = fetch_node(nb_page);
    int comb = node->get_size() + neighbor->get_size();

    if (comb >= 2 * min_size) {
        redistribute(neighbor, node, parent, index);
        return false;
    }

    IxNodeHandle *nb_ptr = neighbor;
    IxNodeHandle *nd_ptr = node;
    IxNodeHandle *par_ptr = parent;
    coalesce(&nb_ptr, &nd_ptr, &par_ptr, index, transaction, root_is_latched);
    return false;
}

bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction) {
    (void)transaction;
    std::scoped_lock<std::mutex> lk(root_latch_);
    if (is_empty()) {
        return false;
    }
    auto pr = find_leaf_page(key, Operation::DELETE, nullptr, false);
    IxNodeHandle *leaf = pr.first;
    if (leaf == nullptr) {
        return false;
    }
    int before = leaf->get_size();
    int after = leaf->remove(key);
    if (after == before) {
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
        return false;
    }
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), true);

    leaf = fetch_node(leaf->get_page_no());
    maintain_parent(leaf);
    coalesce_or_redistribute(leaf, transaction, nullptr);
    return true;
}

Rid IxIndexHandle::get_rid(const Iid &iid) const {
    IxNodeHandle *node = fetch_node(iid.page_no);
    if (iid.slot_no >= node->get_size()) {
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
        throw IndexEntryNotFoundError();
    }
    Rid ret = *node->get_rid(iid.slot_no);
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);
    return ret;
}

Iid IxIndexHandle::lower_bound(const char *key) {
    std::scoped_lock<std::mutex> lk(root_latch_);
    if (is_empty()) {
        return leaf_end();
    }
    auto pr = find_leaf_page(key, Operation::FIND, nullptr, false);
    IxNodeHandle *leaf = pr.first;
    if (leaf == nullptr) {
        return leaf_end();
    }
    int pos = leaf->lower_bound(key);
    Iid ret{leaf->get_page_no(), pos};
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
    return ret;
}

Iid IxIndexHandle::upper_bound(const char *key) {
    std::scoped_lock<std::mutex> lk(root_latch_);
    if (is_empty()) {
        return leaf_end();
    }
    IxNodeHandle *leaf = find_leaf_page(key, Operation::FIND, nullptr, false).first;
    if (leaf == nullptr) {
        return leaf_end();
    }
    for (;;) {
        int pos = leaf->upper_bound(key);
        if (pos < leaf->get_size()) {
            Iid ret{leaf->get_page_no(), pos};
            buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
            return ret;
        }
        if (leaf->get_page_no() == file_hdr_->last_leaf_) {
            Iid ret{leaf->get_page_no(), leaf->get_size()};
            buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
            return ret;
        }
        page_id_t nxt = leaf->get_next_leaf();
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
        leaf = fetch_node(nxt);
    }
}

Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);
    return iid;
}

Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}

IxNodeHandle *IxIndexHandle::fetch_node(int page_no) const {
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    return new IxNodeHandle(file_hdr_, page);
}

IxNodeHandle *IxIndexHandle::create_node() {
    file_hdr_->num_pages_++;
    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    IxNodeHandle *node = new IxNodeHandle(file_hdr_, page);
    node->page_hdr->next_free_page_no = IX_NO_PAGE;
    node->page_hdr->parent = INVALID_PAGE_ID;
    node->page_hdr->num_key = 0;
    node->page_hdr->is_leaf = false;
    node->page_hdr->prev_leaf = IX_NO_PAGE;
    node->page_hdr->next_leaf = IX_NO_PAGE;
    return node;
}

void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
    IxNodeHandle *curr = node;
    while (curr->get_parent_page_no() != IX_NO_PAGE) {
        IxNodeHandle *parent = fetch_node(curr->get_parent_page_no());
        int rank = parent->find_child(curr);
        char *parent_key = parent->get_key(rank);
        char *child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) {
            assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);
        curr = parent;
        assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
    }
}

void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
    assert(leaf->is_leaf_page());

    IxNodeHandle *prev = fetch_node(leaf->get_prev_leaf());
    prev->set_next_leaf(leaf->get_next_leaf());
    buffer_pool_manager_->unpin_page(prev->get_page_id(), true);

    IxNodeHandle *next = fetch_node(leaf->get_next_leaf());
    next->set_prev_leaf(leaf->get_prev_leaf());
    buffer_pool_manager_->unpin_page(next->get_page_id(), true);
}

void IxIndexHandle::release_node_handle(IxNodeHandle &node) {
    (void)node;
    file_hdr_->num_pages_--;
}

void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    if (!node->is_leaf_page()) {
        int child_page_no = node->value_at(child_idx);
        IxNodeHandle *child = fetch_node(child_page_no);
        child->set_parent_page_no(node->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
    }
}
