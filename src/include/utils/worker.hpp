#pragma once
#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "../config.hpp"
#include "trees.hpp"
#include "utils.hpp"

namespace utils::worker {
struct Ticket {
    std::atomic<size_t> _idx;
    const size_t _size;

    size_t get() {
        size_t idx = _idx++;
        return idx < _size ? idx : _size;
    }

    Ticket(size_t begin, size_t end) : _idx(begin), _size(end) {}
};

template <typename tree_t, typename key_type>
void insert_worker(tree_t &tree, const std::vector<key_type> &data,
                   Ticket &line, const key_type &offset) {
    auto idx = line.get();
    const auto &size = line._size;
    while (idx < size) {
        const key_type &key = data[idx] + offset;
        tree.insert(key, {});
        idx = line.get();
    }
}

template <typename tree_t, typename key_type>
void update_worker(tree_t &tree, const std::vector<key_type> &data,
                   Ticket &line, const key_type &offset) {
    auto idx = line.get();
    const auto &size = line._size;
    while (idx < size) {
        const key_type &key = data[idx] + offset;
        tree.update(key, {});
        idx = line.get();
    }
}

template <typename tree_t, typename key_type>
void query_worker(tree_t &tree, const std::vector<key_type> &data, Ticket &line,
                  const key_type &offset) {
    size_t idx = line.get();
    const auto &size = line._size;
    while (idx < size) {
        const key_type &key = data[idx];
        tree.contains(key + offset);
        idx = line.get();
    }
}

template <typename WorkerFunc, typename tree_t, typename key_type>
auto work(WorkerFunc worker_func, tree_t &tree,
          const std::vector<key_type> &data, size_t begin, size_t end,
          uint8_t num_threads, const key_type &offset) {
    Ticket line(begin, end);
    auto start = std::chrono::high_resolution_clock::now();
    {
        std::vector<std::jthread> threads;
        for (size_t i = 0; i < num_threads; ++i) {
            threads.emplace_back(worker_func, std::ref(tree), std::ref(data),
                                 std::ref(line), std::ref(offset));

            cpu_set_t cpuset;
            size_t cpu = (i >> 1) + (i & 1) * 48;
            CPU_ZERO(&cpuset);
            CPU_SET(cpu, &cpuset);
            pthread_setaffinity_np(threads[i].native_handle(),
                                   sizeof(cpu_set_t), &cpuset);
        }
    }
    return std::chrono::high_resolution_clock::now() - start;
}
}  // namespace utils::worker