#pragma once

#include <chrono>
#include <cmath>
#include <cstring>
#include <iostream>
#include <mutex>
#include <optional>
#include <ranges>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "../MemoryBlockManager.hpp"
#include "BTreeNode.hpp"
#include "ikr.h"
#include "sort.hpp"

namespace ConcurrentQuITBTree {
struct reset_stats {
    uint8_t fails;
    uint8_t threshold;

    explicit reset_stats(uint8_t t) {
        fails = 0;
        threshold = t;
    }

    void success() { fails = 0; }

    bool failure() {
        fails++;
        if (fails >= threshold) {
            return true;
        }
        return false;
    }

    void reset() { fails = 0; }
};

template <typename key_type, typename value_type,
          bool LEAF_APPENDS_ENABLED = false>
class BTree {
   public:
    using node_id_t = uint32_t;
    using BlockManager = InMemoryBlockManager<node_id_t>;
    using node_t =
        BTreeNode<node_id_t, key_type, value_type, BlockManager::block_size>;
    using step = node_id_t;
    using path_t = std::vector<step>;

    static constexpr const char *name = LEAF_APPENDS_ENABLED
                                            ? "ConcurrentQuitBTreeLeafAppends"
                                            : "ConcurrentQuITBTree";
    static constexpr const bool concurrent = false;
    friend std::ostream &operator<<(std::ostream &os, const BTree &tree) {
        os << tree.size << ", " << +tree.height << ", " << tree.internal << ", "
           << tree.leaves << ", " << tree.ctr_fast << ", "
           << tree.ctr_redistribute << ", " << tree.ctr_soft << ", "
           << tree.ctr_hard << ", " << tree.ctr_fast_fail << ", "
           << tree.ctr_sort;
        // os << ", " << tree.find_leaf_slot_time << ", " <<
        // tree.move_in_leaf_time
        //    << ", " << tree.sort_time;
        return os;
    }

    std::unordered_map<std::string, uint64_t> get_stats() const {
        return {{"size", size},
                {"height", height},
                {"internal", internal},
                {"leaves", leaves},
                {"fast_inserts", ctr_fast},
                {"redistribute", ctr_redistribute},
                {"soft_resets", ctr_soft},
                {"hard_resets", ctr_hard},
                {"fast_inserts_fail", ctr_fast_fail},
                {"sort", ctr_sort}};
    }

    std::unordered_map<std::string, uint64_t> get_profiling_times() {
        std::unordered_map<std::string, uint64_t> times;
        times["find_leaf_slot_time"] = find_leaf_slot_time;
        times["move_in_leaf_time"] = move_in_leaf_time;
        times["sort_time"] = sort_time;
        return times;
    }

    using dist_f = std::size_t (*)(const key_type &, const key_type &);

    static constexpr uint16_t SPLIT_INTERNAL_POS =
        node_t::internal_capacity / 2;
    static constexpr uint16_t SPLIT_LEAF_POS = (node_t::leaf_capacity + 1) / 2;
    static constexpr uint16_t IQR_SIZE_THRESH = SPLIT_LEAF_POS;
    static constexpr node_id_t INVALID_NODE_ID = -1;

    dist_f dist;

    BlockManager &manager;
    mutable std::vector<std::shared_mutex> mutexes;
    const node_id_t root_id;
    node_id_t head_id;
    node_id_t tail_id;

    mutable std::shared_mutex fp_mutex;
    // Captures below metadata
    node_id_t fp_id;
    key_type fp_min;
    key_type fp_max;
    uint16_t fp_size;
    // end of capture for fp_mutex

    mutable std::shared_mutex fp_meta_mutex;
    // Captures below metadata
    node_id_t fp_prev_id;
    key_type fp_prev_min;
    uint16_t fp_prev_size;
    // end of capture for fp_meta_mutex

    uint8_t height;

    reset_stats life;

    std::atomic<bool> fp_sorted{};

    std::atomic<uint32_t> ctr_fast{};
    std::atomic<uint32_t> ctr_fast_fail{};
    std::atomic<uint32_t> ctr_hard{};
    std::atomic<uint32_t> ctr_sort{};
    std::atomic<uint32_t> fp_slot{};
    mutable std::atomic<uint32_t> ctr_root_shared{};
    mutable uint32_t ctr_root_unique{};
    uint32_t ctr_root{};
    std::atomic<uint32_t> size{};
    std::atomic<uint32_t> leaves{};
    std::atomic<uint32_t> internal{};
    std::atomic<uint32_t> ctr_redistribute{};
    std::atomic<uint32_t> ctr_soft{};

    // timers for profiling
    long long find_leaf_slot_time = 0;
    long long move_in_leaf_time = 0;
    long long sort_time = 0;

    void create_new_root(const key_type &key, node_id_t right_node_id) {
        ++ctr_root;
        node_id_t left_node_id = manager.allocate();
        node_t root(manager.open_block(root_id));
        node_t left_node(manager.open_block(left_node_id));
        ++internal;
        std::memcpy(left_node.info, root.info, 4096);
        left_node.info->id = left_node_id;
        manager.mark_dirty(left_node_id);

        manager.mark_dirty(root_id);
        root.info->size = 1;
        root.keys[0] = key;
        root.children[0] = left_node_id;
        root.children[1] = right_node_id;
        ++height;
    }

    void find_leaf_shared(node_t &node, const key_type &key) const {
        node_id_t node_id = root_id;
        mutexes[node_id].lock_shared();
        ++ctr_root_shared;
        node.load(manager.open_block(node_id));
        do {
            const node_id_t parent_id = node_id;
            const uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];
            mutexes[node_id].lock_shared();
            mutexes[parent_id].unlock_shared();
            node.load(manager.open_block(node_id));
        } while (node.info->type == INTERNAL);
    }

    void find_leaf_exclusive(node_t &node, path_t &path, const key_type &key,
                             key_type &leaf_max) const {
        node_id_t node_id = root_id;

        mutexes[node_id].lock();
        ++ctr_root_unique;
        path.reserve(height);
        node.load(manager.open_block(node_id));

        do {
            if (node.info->size < node_t::internal_capacity) {
                for (const auto &parent_id : path) {
                    mutexes[parent_id].unlock();
                }
                path.clear();
            }
            path.push_back(node_id);
            uint16_t slot = node.child_slot(key);

            if (slot != node.info->size) {
                leaf_max = node.keys[slot];
            }

            node_id = node.children[slot];

            mutexes[node_id].lock();
            node.load(manager.open_block(node_id));
        } while (node.info->type == bp_node_type::INTERNAL);
        if (node.info->size < node_t::leaf_capacity) {
            for (const auto &parent_id : path) {
                mutexes[parent_id].unlock();
            }
            path.clear();
        }
    }

    void find_leaf_exclusive(node_t &node, const key_type &key,
                             key_type &leaf_max) const {
        node_id_t parent_id = root_id;

        mutexes[root_id].lock_shared();
        ++ctr_root_shared;
        uint8_t i = height;
        node.load(manager.open_block(parent_id));

        while (--i > 0) {
            const uint16_t slot = node.child_slot(key);

            if (slot != node.info->size) {
                leaf_max = node.keys[slot];
            }

            const node_id_t child_id = node.children[slot];

            mutexes[child_id].lock_shared();

            mutexes[parent_id].unlock_shared();
            node.load(manager.open_block(child_id));

            parent_id = child_id;
        }
        const uint16_t slot = node.child_slot(key);

        if (slot < node.info->size) {
            leaf_max = node.keys[slot];
        }

        const node_id_t leaf_id = node.children[slot];

        mutexes[leaf_id].lock();

        mutexes[parent_id].unlock_shared();
        node.load(manager.open_block(leaf_id));
    }

    void internal_insert(const path_t &path, key_type key, node_id_t child_id) {
        for (node_id_t node_id : std::ranges::reverse_view(path)) {
            node_t node(manager.open_block(node_id));

            uint16_t index = node.child_slot(key);

            manager.mark_dirty(node_id);
            if (node.info->size < node_t::internal_capacity) {
                std::memmove(node.keys + index + 1, node.keys + index,
                             (node.info->size - index) * sizeof(key_type));
                std::memmove(node.children + index + 2,
                             node.children + index + 1,
                             (node.info->size - index) * sizeof(uint32_t));
                node.keys[index] = key;
                node.children[index + 1] = child_id;
                ++node.info->size;

                mutexes[node_id].unlock();
                return;
            }

            node_id_t new_node_id = manager.allocate();
            node_t new_node(manager.open_block(new_node_id), INTERNAL);
            ++internal;
            manager.mark_dirty(new_node_id);

            node.info->size = SPLIT_INTERNAL_POS;
            new_node.info->id = new_node_id;
            new_node.info->size = node_t::internal_capacity - node.info->size;

            if (index < node.info->size) {
                std::memcpy(new_node.keys, node.keys + node.info->size,
                            new_node.info->size * sizeof(key_type));
                std::memmove(node.keys + index + 1, node.keys + index,
                             (node.info->size - index) * sizeof(key_type));
                node.keys[index] = key;
                std::memcpy(new_node.children, node.children + node.info->size,
                            (new_node.info->size + 1) * sizeof(uint32_t));
                std::memmove(node.children + index + 2,
                             node.children + index + 1,
                             (node.info->size - index + 1) * sizeof(uint32_t));
                node.children[index + 1] = child_id;

                key = node.keys[node.info->size];
            } else if (index == node.info->size) {
                std::memcpy(new_node.keys, node.keys + node.info->size,
                            new_node.info->size * sizeof(key_type));
                std::memcpy(new_node.children + 1,
                            node.children + 1 + node.info->size,
                            new_node.info->size * sizeof(uint32_t));
                new_node.children[0] = child_id;
            } else {
                std::memcpy(new_node.keys, node.keys + node.info->size + 1,
                            (index - node.info->size - 1) * sizeof(key_type));
                std::memcpy(
                    new_node.keys + index - node.info->size, node.keys + index,
                    (node_t::internal_capacity - index) * sizeof(key_type));
                new_node.keys[index - node.info->size - 1] = key;
                std::memcpy(new_node.children,
                            node.children + 1 + node.info->size,
                            (index - node.info->size) * sizeof(uint32_t));
                std::memcpy(new_node.children + 1 + index - node.info->size,
                            node.children + 1 + index,
                            new_node.info->size * sizeof(uint32_t));
                new_node.children[index - node.info->size] = child_id;

                key = node.keys[node.info->size];
            }
            child_id = new_node_id;
            if (node_id != root_id) {
                mutexes[node_id].unlock();
            }
        }
        create_new_root(key, child_id);

        mutexes[root_id].unlock();
    }

    bool leaf_insert(node_t &leaf, uint16_t index, const key_type &key,
                     const value_type &value, bool fast) {
        if (index < leaf.info->size && leaf.keys[index] == key) {
            manager.mark_dirty(leaf.info->id);
            leaf.values[index] = value;

            mutexes[leaf.info->id].unlock();
            return true;
        }

        if (leaf.info->size >= node_t::leaf_capacity) {
            return false;
        }

        if (fast && fp_sorted) {
            if (leaf.keys[index - 1] > key) {
                fp_sorted = false;
            }
        }

        ++size;
        manager.mark_dirty(leaf.info->id);

        if (index < leaf.info->size) {
            std::chrono::high_resolution_clock::time_point start =
                std::chrono::high_resolution_clock::now();
            std::memmove(leaf.keys + index + 1, leaf.keys + index,
                         (leaf.info->size - index) * sizeof(key_type));
            std::memmove(leaf.values + index + 1, leaf.values + index,
                         (leaf.info->size - index) * sizeof(value_type));
            std::chrono::high_resolution_clock::time_point end =
                std::chrono::high_resolution_clock::now();
            move_in_leaf_time +=
                std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                     start)
                    .count();
        }
        leaf.keys[index] = key;
        leaf.values[index] = value;
        ++leaf.info->size;

        if (fast) {
            if (leaf.info->id == fp_id) {
                ++fp_size;
            } else if (leaf.info->next_id == fp_id) {
                // This block is executed when the current leaf's next_id
                // matches fp_id. It updates fp_prev_id, fp_prev_min, and
                // fp_prev_size to track the previous leaf's state.
                fp_prev_id = leaf.info->id;
                fp_prev_min = leaf.keys[0];
                fp_prev_size = leaf.info->size;
            }
        }

        mutexes[leaf.info->id].unlock();
        return true;
    }

    void std_sort_leaf(node_t &leaf) {
        std::array<std::pair<key_type, value_type>, node_t::leaf_capacity> kvs;
        for (uint16_t i = 0; i < leaf.info->size; i++) {
            kvs[i] = {leaf.keys[i], leaf.values[i]};
        }
        std::sort(
            kvs.begin(), kvs.begin() + leaf.info->size,
            [](const auto &a, const auto &b) { return a.first < b.first; });
        for (uint16_t i = 0; i < leaf.info->size; i++) {
            leaf.keys[i] = kvs[i].first;
            leaf.values[i] = kvs[i].second;
        }
    }

    void sort_leaf(node_t &leaf) {
        auto start = std::chrono::high_resolution_clock::now();

        int depth_limit = 2 * std::log2(leaf.info->size);
        utils::sort::introsort(leaf.keys, leaf.values, 0, leaf.info->size - 1,
                               depth_limit);

        auto end = std::chrono::high_resolution_clock::now();
        sort_time +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
    }

    /*
        Function to split the leaf node and insert the new key-value pair
        Requires:
            (1) leaf to be locked
            (2) fp_mutex to be locked
            (3) fp_meta_mutex to be locked
    */
    void update_fp_metadata_split(node_t &leaf, node_t &new_leaf,
                                  bool fp_move) {
        if (leaf.info->id == fp_id) {
            if (fp_move) {
                fp_prev_min = fp_min;
                fp_prev_size = leaf.info->size;
                fp_prev_id = fp_id;
                fp_id = new_leaf.info->id;
                fp_min = new_leaf.keys[0];
                fp_size = new_leaf.info->size;
            } else {
                fp_max = new_leaf.keys[0];
                fp_size = leaf.info->size;
            }
        } else if (new_leaf.info->next_id == fp_id) {
            fp_prev_id = new_leaf.info->id;
            fp_prev_min = new_leaf.keys[0];
            fp_prev_size = new_leaf.info->size;
        }
    }

    /*
        Function to determine the split position of the leaf node
        Requires (from caller):
            (1) leaf to be locked
            (2) fp_mutex to be locked
            (3) fp_meta_mutex to be locked
    */
    uint16_t determine_split_pos(node_t &leaf, uint16_t index, bool &fp_move) {
        uint16_t split_leaf_pos = SPLIT_LEAF_POS;
        // requires leaf, fp_mutex and fp_meta_mutex to be locked by caller
        if (leaf.info->id == fp_id) {
            // determine split position based on fast path metadata
            if (fp_prev_id == INVALID_NODE_ID ||
                fp_prev_size < IQR_SIZE_THRESH) {
                // move the fast-path to new leaf
                fp_move = true;
            } else {
                size_t max_distance = IKR::upper_bound(
                    dist(fp_min, fp_prev_min), fp_prev_size, fp_size);
                uint16_t outlier_pos = leaf.value_slot2(fp_min + max_distance);
                if (outlier_pos <= SPLIT_LEAF_POS) {
                    // retain fast-path as is
                    split_leaf_pos = outlier_pos;
                } else {
                    split_leaf_pos = outlier_pos - 10 < SPLIT_LEAF_POS
                                         ? SPLIT_LEAF_POS
                                         : outlier_pos - 10;
                    // move fast-path to new leaf
                    fp_move = true;
                }
                if (index < outlier_pos) {
                    split_leaf_pos++;
                }
            }
        }
        return split_leaf_pos;
    }

    void split_insert(node_t &leaf, uint16_t index, const path_t &path,
                      const key_type &key, const value_type &value, bool fast) {
        ++size;
        uint16_t split_leaf_pos = SPLIT_LEAF_POS;

        bool fp_move = false;
        if (fast) {
            // requires fp_mutex and fp_meta_mutex to be locked by caller
            if (leaf.info->id == fp_id) {
                if (fp_prev_id == INVALID_NODE_ID ||
                    fp_prev_size < IQR_SIZE_THRESH) {
                    fp_move = true;
                } else {
                    size_t max_distance = IKR::upper_bound(
                        dist(fp_min, fp_prev_min), fp_prev_size, fp_size);
                    uint16_t outlier_pos =
                        leaf.value_slot2(fp_min + max_distance);
                    if (outlier_pos <= SPLIT_LEAF_POS) {
                        split_leaf_pos = outlier_pos;
                    } else {
                        split_leaf_pos = outlier_pos - 10 < SPLIT_LEAF_POS
                                             ? SPLIT_LEAF_POS
                                             : outlier_pos - 10;
                        fp_move = true;
                    }
                    if (index < outlier_pos) {
                        split_leaf_pos++;
                    }
                }
            }
        }

        node_id_t new_leaf_id = manager.allocate();
        node_t new_leaf(manager.open_block(new_leaf_id), LEAF);
        ++leaves;
        manager.mark_dirty(new_leaf_id);

        leaf.info->size = split_leaf_pos;
        new_leaf.info->id = new_leaf_id;
        new_leaf.info->next_id = leaf.info->next_id;
        new_leaf.info->size = node_t::leaf_capacity + 1 - leaf.info->size;
        leaf.info->next_id = new_leaf_id;

        if (index < leaf.info->size) {
            std::memcpy(new_leaf.keys, leaf.keys + leaf.info->size - 1,
                        new_leaf.info->size * sizeof(key_type));
            std::memmove(leaf.keys + index + 1, leaf.keys + index,
                         (leaf.info->size - index - 1) * sizeof(key_type));
            leaf.keys[index] = key;
            std::memcpy(new_leaf.values, leaf.values + leaf.info->size - 1,
                        new_leaf.info->size * sizeof(value_type));
            std::memmove(leaf.values + index + 1, leaf.values + index,
                         (leaf.info->size - index - 1) * sizeof(value_type));
            leaf.values[index] = value;
        } else {
            uint16_t new_index = index - leaf.info->size;
            std::memcpy(new_leaf.keys, leaf.keys + leaf.info->size,
                        new_index * sizeof(key_type));
            new_leaf.keys[new_index] = key;
            std::memcpy(new_leaf.keys + new_index + 1, leaf.keys + index,
                        (node_t::leaf_capacity - index) * sizeof(key_type));
            std::memcpy(new_leaf.values, leaf.values + leaf.info->size,
                        new_index * sizeof(value_type));
            new_leaf.values[new_index] = value;
            std::memcpy(new_leaf.values + new_index + 1, leaf.values + index,
                        (node_t::leaf_capacity - index) * sizeof(value_type));
        }
        if (leaf.info->id == tail_id) {
            tail_id = new_leaf_id;
        }

        if (fast) {
            // requires fp_mutex and fp_meta_mutex to be locked by caller
            if (leaf.info->id == fp_id) {
                if (fp_move) {
                    fp_prev_min = fp_min;
                    fp_prev_size = leaf.info->size;
                    fp_prev_id = fp_id;
                    fp_id = new_leaf_id;
                    fp_min = new_leaf.keys[0];
                    fp_size = new_leaf.info->size;
                } else {
                    fp_max = new_leaf.keys[0];
                    fp_size = leaf.info->size;
                }
            } else if (new_leaf.info->next_id == fp_id) {
                fp_prev_id = new_leaf_id;
                fp_prev_min = new_leaf.keys[0];
                fp_prev_size = new_leaf.info->size;
            }
        }

        mutexes[leaf.info->id].unlock();
        internal_insert(path, new_leaf.keys[0], new_leaf_id);
    }

    static std::size_t cmp(const key_type &max, const key_type &min) {
        return max - min;
    }

   public:
    explicit BTree(BlockManager &m)
        : manager(m),
          mutexes(m.get_capacity()),
          root_id(m.allocate()),
          height(1),
          life(sqrt(node_t::leaf_capacity)) {
        head_id = tail_id = m.allocate();

        fp_min = {};

        fp_id = tail_id;
        fp_max = {};
        dist = cmp;
        fp_prev_id = INVALID_NODE_ID;
        fp_prev_min = {};
        fp_prev_size = 0;
        fp_size = 0;

        node_t leaf(manager.open_block(head_id), LEAF);
        manager.mark_dirty(head_id);
        leaf.info->id = head_id;
        leaf.info->next_id = head_id;
        leaf.info->size = 0;

        node_t root(manager.open_block(root_id), INTERNAL);
        manager.mark_dirty(root_id);
        root.info->id = root_id;
        root.info->next_id = root_id;
        root.info->size = 0;
        root.children[0] = head_id;
        fp_sorted = true;

        if constexpr (LEAF_APPENDS_ENABLED) {
            std::cout << "leaf appends enabled" << std::endl;
        }
    }

    bool update(const key_type &key, const value_type &value) {
        node_t leaf;
        key_type max;
        find_leaf_exclusive(leaf, key, max);
        uint16_t index = leaf.value_slot(key);
        if (index >= leaf.info->size || leaf.keys[index] != key) {
            mutexes[leaf.info->id].unlock();
            return false;
        }
        manager.mark_dirty(leaf.info->id);
        leaf.values[index] = value;
        mutexes[leaf.info->id].unlock();
        return true;
    }

    bool reset_fast_path(node_t &leaf, key_type &leaf_max) {
        // if leaf appends are enabled, we need to sort the fast-path
        if constexpr (LEAF_APPENDS_ENABLED) {
            if (!fp_sorted) {
                mutexes[fp_id].lock();
                node_t fp_leaf(manager.open_block(fp_id), LEAF);
                sort_leaf(fp_leaf);
                fp_sorted = true;
                ++ctr_sort;
                manager.mark_dirty(fp_id);
                mutexes[fp_id].unlock();
            }
        }

        // update associated metadata
        if (fp_id != tail_id && leaf.keys[0] == fp_max) {
            // in this case, we end up inserting to fp-next
            fp_prev_id = fp_id;
            fp_prev_size = fp_size;
            fp_prev_min = fp_min;
        } else {
            fp_prev_id = INVALID_NODE_ID;
        }
        fp_id = leaf.info->id;
        fp_min = leaf.keys[0];
        fp_max = leaf_max;
        fp_size = leaf.info->size;
        life.reset();
        return true;
    }

    void insert(const key_type &key, const value_type &value) {
        path_t path;
        uint16_t index;
        node_t leaf;

        bool fast;
        key_type leaf_max{};

        std::unique_lock fp_lock(fp_mutex);

        if ((fp_id == head_id || fp_min <= key)

            && (fp_id == tail_id || key < fp_max)

        ) {
            fast = true;

            mutexes[fp_id].lock();
            leaf.load(manager.open_block(fp_id));

            life.success();

            if constexpr (LEAF_APPENDS_ENABLED) {
                if (leaf_insert(leaf, leaf.info->size, key, value, true)) {
                    ++ctr_fast;
                    return;
                }
            } else {
                std::chrono::high_resolution_clock::time_point start =
                    std::chrono::high_resolution_clock::now();
                index = leaf.value_slot(key);

                std::chrono::high_resolution_clock::time_point end =
                    std::chrono::high_resolution_clock::now();
                find_leaf_slot_time +=
                    std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                         start)
                        .count();
                if (leaf_insert(leaf, index, key, value, true)) {
                    ++ctr_fast;
                    return;
                }
            }

            if constexpr (LEAF_APPENDS_ENABLED) {
                if (!fp_sorted) {
                    sort_leaf(leaf);
                    fp_sorted = true;
                    ++ctr_sort;
                }
            }

            ++ctr_fast_fail;
            mutexes[fp_id].unlock();
            find_leaf_exclusive(leaf, path, key, leaf_max);
        } else {
            fast = false;
            bool reset = life.failure();
            if (!reset) {
                fp_lock.unlock();
            }

            find_leaf_exclusive(leaf, key, leaf_max);

            if (reset) {
                ++ctr_hard;
                if constexpr (LEAF_APPENDS_ENABLED) {
                    if (!fp_sorted) {
                        mutexes[fp_id].lock();
                        node_t fp_leaf(manager.open_block(fp_id), LEAF);
                        sort_leaf(fp_leaf);
                        fp_sorted = true;
                        ++ctr_sort;
                        manager.mark_dirty(fp_id);
                        mutexes[fp_id].unlock();
                    }
                }
                // now update associated metadata
                if (fp_id != tail_id && leaf.keys[0] == fp_max) {
                    fp_prev_id = fp_id;
                    fp_prev_size = fp_size;
                    fp_prev_min = fp_min;
                } else {
                    fp_prev_id = INVALID_NODE_ID;
                }
                fp_id = leaf.info->id;
                fp_min = leaf.keys[0];
                fp_max = leaf_max;
                fp_size = leaf.info->size;
                life.reset();
                fast = true;
            }

            index = leaf.value_slot(key);
            if (leaf_insert(leaf, index, key, value, fast)) {
                return;
            }

            mutexes[leaf.info->id].unlock();
            find_leaf_exclusive(leaf, path, key, leaf_max);
        }
        index = leaf.value_slot(key);
        if (leaf_insert(leaf, index, key, value, fast)) {
            for (const auto &parent_id : path) {
                mutexes[parent_id].unlock();
            }
            return;
        }
        split_insert(leaf, index, path, key, value, fast);
    }

    uint32_t select_k(size_t count, const key_type &min_key) const {
        node_t leaf;
        find_leaf_shared(leaf, min_key);
        uint16_t index = leaf.value_slot(min_key);
        uint32_t loads = 1;
        uint16_t curr_size = leaf.info->size - index;
        while (count > curr_size) {
            count -= curr_size;
            if (leaf.info->id == tail_id) {
                break;
            }
            node_id_t next_id = leaf.info->next_id;
            mutexes[next_id].lock_shared();
            mutexes[leaf.info->id].unlock_shared();
            leaf.load(manager.open_block(next_id));

            curr_size = leaf.info->size;
            ++loads;
        }
        mutexes[leaf.info->id].unlock_shared();
        return loads;
    }

    uint32_t range(const key_type &min_key, const key_type &max_key) const {
        uint32_t loads = 1;
        node_t leaf;
        find_leaf_shared(leaf, min_key);
        while (leaf.keys[leaf.info->size - 1] < max_key) {
            if (leaf.info->id == tail_id) {
                break;
            }
            node_id_t next_id = leaf.info->next_id;
            mutexes[next_id].lock_shared();
            mutexes[leaf.info->id].unlock_shared();
            leaf.load(manager.open_block(next_id));

            ++loads;
        }
        mutexes[leaf.info->id].unlock_shared();
        return loads;
    }

    std::optional<value_type> get(const key_type &key) const {
        node_t leaf;
        find_leaf_shared(leaf, key);
        std::shared_lock lock(mutexes[leaf.info->id], std::adopt_lock);
        uint16_t index = leaf.value_slot(key);
        return index < leaf.info->size &&
               (leaf.keys[index] == key ? std::make_optional(leaf.values[index])
                                        : std::nullopt);
    }

    bool contains(const key_type &key) const {
        node_t leaf;
        find_leaf_shared(leaf, key);
        std::shared_lock lock(mutexes[leaf.info->id], std::adopt_lock);
        if (leaf.info->id != fp_id) {
            uint16_t index = leaf.value_slot(key);
            return index < leaf.info->size && (leaf.keys[index] == key);
        } else {
            // do a linear scan of node
            for (uint16_t i = 0; i < leaf.info->size; i++) {
                if (leaf.keys[i] == key) {
                    return true;
                }
            }
            return false;
        }
    }
};
}  // namespace ConcurrentQuITBTree
