#pragma once

#include <cstring>
#include <limits>
#include <optional>
#include <ranges>
#include <vector>

#include "BTreeNode.hpp"
#include "MemoryBlockManager.hpp"

namespace LILBTree {
template <typename key_type, typename value_type>
class BTree {
   public:
    using node_id_t = uint32_t;
    using BlockManager = InMemoryBlockManager<node_id_t>;
    using node_t =
        BTreeNode<node_id_t, key_type, value_type, BlockManager::block_size>;
    using step = node_id_t;
    using path_t = std::vector<step>;

    static constexpr const char *name = "LILBTree";
    static constexpr const bool concurrent = false;
    static constexpr uint16_t SPLIT_INTERNAL_POS =
        node_t::internal_capacity / 2;
    static constexpr uint16_t SPLIT_LEAF_POS = (node_t::leaf_capacity + 1) / 2;
    static constexpr node_id_t INVALID_NODE_ID =
        std::numeric_limits<node_id_t>::max();

    explicit BTree(BlockManager &m)
        : manager(m),
          root_id(m.allocate()),
          head_id(m.allocate()),
          height(1),
          size(0) {
        lil_min = std::numeric_limits<key_type>::min();
        lil_max = std::numeric_limits<key_type>::max();
        lil_id = head_id;
        node_t leaf(manager.open_block(head_id), bp_node_type::LEAF);
        leaves = 1;
        manager.mark_dirty(head_id);
        leaf.info->id = head_id;
        leaf.info->next_id = INVALID_NODE_ID;
        leaf.info->size = 0;
        node_t root(manager.open_block(root_id), bp_node_type::INTERNAL);
        manager.mark_dirty(root_id);
        root.info->id = root_id;
        root.info->next_id = INVALID_NODE_ID;
        root.info->size = 0;
        root.children[0] = head_id;
    }

    friend std::ostream &operator<<(std::ostream &os, const BTree &tree) {
        os << tree.size << ", " << +tree.height << ", " << tree.internal << ", "
           << tree.leaves << ", " << tree.ctr_fast;
        // os << tree.ctr_fast;
        return os;
    }

    std::unordered_map<std::string, uint64_t> get_stats() const {
        return {{"size", size},
                {"height", height},
                {"internal", internal},
                {"leaves", leaves},
                {"fast_inserts", ctr_fast}};
    }

    bool update(const key_type &key, const value_type &value) {
        node_t leaf;
        find_leaf(leaf, key);
        uint16_t index = leaf.value_slot(key);
        if (index >= leaf.info->size || leaf.keys[index] != key) {
            return false;
        }
        manager.mark_dirty(leaf.info->id);
        leaf.values[index] = value;
        return true;
    }

    void insert(const key_type &key, const value_type &value) {
        node_t leaf;
        path_t path;
        uint16_t index;
        if (lil_min <= key && key < lil_max) {
            leaf.load(manager.open_block(lil_id));
            index = leaf.value_slot(key);
            if (leaf_insert(leaf, index, key, value)) {
                ++ctr_fast;
                return;
            }
            find_leaf(leaf, path, key, lil_max);
        } else {
            find_leaf(leaf, path, key, lil_max);
            index = leaf.value_slot(key);
            lil_id = leaf.info->id;
            lil_min = leaf.keys[0];
            if (leaf_insert(leaf, index, key, value)) {
                return;
            }
        }
        if (index < SPLIT_LEAF_POS) {
            node_id_t new_id;
            split_insert(leaf, index, key, value, lil_max, new_id);
            internal_insert(path, lil_max, new_id);
        } else {
            split_insert(leaf, index, key, value, lil_min, lil_id);
            internal_insert(path, lil_min, lil_id);
        }
    }

    uint32_t select_k(size_t count, const key_type &min_key) const {
        node_t leaf;
        find_leaf(leaf, min_key);
        uint16_t index = leaf.value_slot(min_key);
        uint32_t loads = 1;
        uint16_t curr_size = leaf.info->size - index;
        while (count > curr_size) {
            count -= curr_size;
            node_id_t next_id = leaf.info->next_id;
            if (next_id == INVALID_NODE_ID) {
                break;
            }
            leaf.load(manager.open_block(next_id));
            curr_size = leaf.info->size;
            ++loads;
        }
        return loads;
    }

    uint32_t range(const key_type &min_key, const key_type &max_key) const {
        uint32_t loads = 1;
        node_t leaf;
        find_leaf(leaf, min_key);
        while (leaf.keys[leaf.info->size - 1] < max_key) {
            node_id_t next_id = leaf.info->next_id;
            if (next_id == INVALID_NODE_ID) {
                break;
            }
            leaf.load(manager.open_block(next_id));
            ++loads;
        }
        return loads;
    }

    std::optional<value_type> get(const key_type &key) const {
        node_t leaf;
        find_leaf(leaf, key);
        uint16_t index = leaf.value_slot(key);
        return index < leaf.info->size &&
               (leaf.keys[index] == key ? std::make_optional(leaf.values[index])
                                        : std::nullopt);
    }

    bool contains(const key_type &key) const {
        node_t leaf;
        find_leaf(leaf, key);
        uint16_t index = leaf.value_slot(key);
        return index < leaf.info->size && (leaf.keys[index] == key);
    }

   private:
    void create_new_root(const key_type &key, node_id_t right_node_id) {
        node_id_t left_node_id = manager.allocate();
        node_t root(manager.open_block(root_id));
        node_t left_node(manager.open_block(left_node_id));
        ++internal;
        std::memcpy(left_node.info, root.info, BlockManager::block_size);
        left_node.info->id = left_node_id;
        manager.mark_dirty(left_node_id);
        manager.mark_dirty(root_id);
        root.info->size = 1;
        root.keys[0] = key;
        root.children[0] = left_node_id;
        root.children[1] = right_node_id;
        ++height;
    }

    void find_leaf(node_t &node, const key_type &key) const {
        node_id_t node_id = root_id;
        node.load(manager.open_block(node_id));
        do {
            uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];
            node.load(manager.open_block(node_id));
        } while (node.info->type == bp_node_type::INTERNAL);
    }

    void find_leaf(node_t &node, path_t &path, const key_type &key,
                   key_type &leaf_max) const {
        leaf_max = std::numeric_limits<key_type>::max();
        node_id_t node_id = root_id;
        path.reserve(height);
        node.load(manager.open_block(node_id));
        do {
            path.push_back(node_id);
            uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];
            if (slot != node.info->size) {
                leaf_max = node.keys[slot];
            }
            node.load(manager.open_block(node_id));
        } while (node.info->type == bp_node_type::INTERNAL);
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
                return;
            }
            node_id_t new_node_id = manager.allocate();
            node_t new_node(manager.open_block(new_node_id),
                            bp_node_type::INTERNAL);
            ++internal;
            manager.mark_dirty(new_node_id);
            node.info->size = SPLIT_INTERNAL_POS;
            new_node.info->id = new_node_id;
            new_node.info->next_id = node.info->next_id;
            new_node.info->size = node_t::internal_capacity - node.info->size;
            node.info->next_id = new_node_id;
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
        }
        create_new_root(key, child_id);
    }

    bool leaf_insert(node_t &leaf, uint16_t index, const key_type &key,
                     const value_type &value) {
        if (index < leaf.info->size && leaf.keys[index] == key) {
            manager.mark_dirty(leaf.info->id);
            leaf.values[index] = value;
            return true;
        }
        if (leaf.info->size >= node_t::leaf_capacity) {
            return false;
        }
        ++size;
        manager.mark_dirty(leaf.info->id);
        std::memmove(leaf.keys + index + 1, leaf.keys + index,
                     (leaf.info->size - index) * sizeof(key_type));
        std::memmove(leaf.values + index + 1, leaf.values + index,
                     (leaf.info->size - index) * sizeof(value_type));
        leaf.keys[index] = key;
        leaf.values[index] = value;
        ++leaf.info->size;
        return true;
    }

    void split_insert(node_t &leaf, uint16_t index, const key_type &key,
                      const value_type &value, key_type &new_key,
                      node_id_t &new_id) {
        ++size;
        uint16_t split_leaf_pos = SPLIT_LEAF_POS;
        node_id_t new_leaf_id = manager.allocate();
        node_t new_leaf(manager.open_block(new_leaf_id), bp_node_type::LEAF);
        ++leaves;
        manager.mark_dirty(new_leaf_id);
        new_leaf.info->id = new_leaf_id;
        new_leaf.info->next_id = leaf.info->next_id;
        new_leaf.info->size = node_t::leaf_capacity + 1 - split_leaf_pos;
        leaf.info->next_id = new_leaf_id;
        leaf.info->size = split_leaf_pos;
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
        new_key = new_leaf.keys[0];
        new_id = new_leaf_id;
    }

    BlockManager &manager;
    const node_id_t root_id;
    node_id_t head_id;
    node_id_t lil_id;
    key_type lil_min;
    key_type lil_max;

    // not necessary but good to have
    uint8_t height;
    uint32_t size;
    size_t ctr_fast{};
    uint32_t leaves;
    uint32_t internal;
};
}  // namespace LILBTree
