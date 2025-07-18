#pragma once
#include <cassert>
#include <chrono>
#include <iostream>
#include <random>
#include <vector>

#include "../config.hpp"
#include "logging.hpp"
#include "metrics.hpp"
#include "trees.hpp"
#include "utils.hpp"
#include "worker.hpp"
namespace utils {
namespace executor {
enum RANGE_QUERY_TYPE { SHORT, MID, LONG };
template <typename tree_t, typename key_type>
size_t range_queries(tree_t &tree, const std::vector<key_type> &data,
                     size_t num_inserts, size_t range, const key_type &offset,
                     size_t size, std::mt19937 &generator) {
    size_t leaf_accesses = 0;
    size_t k = num_inserts / size;
    std::uniform_int_distribution<size_t> index(0, num_inserts - k - 1);
    for (size_t i = 0; i < range; i++) {
        const key_type min_key = data[index(generator)] + offset;
        leaf_accesses += tree.select_k(k, min_key);
    }
    return leaf_accesses;
}

template <typename tree_t, typename key_type>
class Workload {
    tree_t &tree;
    const Config &conf;
    const key_type offset;
    std::ofstream results;
    std::mt19937 generator;
    utils::logging::Logger &log;
    utils::executor::metrics::Latency timer;

   public:
    Workload(tree_t &tree, const Config &conf)
        : tree(tree),
          conf(conf),
          offset(0),
          results(conf.results_csv, std::ofstream::app),
          generator(conf.seed),
          log(utils::logging::Logger::get_instance()) {
        if (!results) {
            log.error("Error: could not open config file {}", conf.results_csv);
        }
    }

    void run_all(std::vector<std::vector<key_type> > &data) {
        for (size_t j = 0; j < conf.repeat; ++j) {
            for (size_t k = 0; k < data.size(); ++k) {
                run(conf.files[k], data[k]);
            }
        }
    }

    void run_preload(const std::vector<key_type> &data, size_t begin,
                     size_t num_load) {
        if (num_load > 0) {
            log.trace("Preload ({})", num_load);
            auto duration = utils::worker::work(
                utils::worker::insert_worker<tree_t, key_type>, tree, data,
                begin, num_load, conf.num_threads, offset);
            results << ", " << duration.count();
            timer.preload = duration.count();
        }
    }

    void run_writes(const std::vector<key_type> &data, size_t begin,
                    size_t raw_writes) {
        if (raw_writes > 0) {
            log.trace("Raw write ({})", raw_writes);
            auto duration = utils::worker::work(
                utils::worker::insert_worker<tree_t, key_type>, tree, data,
                begin, begin + raw_writes, conf.num_threads, offset);
            results << ", " << duration.count();
            timer.raw_writes = duration.count();
        }
    }

    void run_reads(const std::vector<key_type> &data, size_t num_inserts,
                   size_t raw_queries) {
        if (raw_queries > 0) {
            log.trace("Raw read ({})", raw_queries);
            std::vector<key_type> queries;
            std::uniform_int_distribution<size_t> index(0, num_inserts - 1);
            for (size_t i = 0; i < raw_queries; i++) {
                queries.emplace_back(data[index(generator)] + offset);
            }
            auto duration = utils::worker::work(
                utils::worker::query_worker<tree_t, key_type>, tree, data, 0,
                raw_queries, conf.num_threads, offset);
            results << ", " << duration.count();
            timer.raw_reads = duration.count();
        }
    }

    void run_mixed(const std::vector<key_type> &data, size_t begin,
                   size_t mixed_writes, size_t mixed_reads) {
        if (mixed_writes > 0 || mixed_reads > 0) {
            uint32_t ctr_empty = 0;
            std::uniform_int_distribution coin(0, 1);
            size_t mix_inserts = 0;
            size_t mix_queries = 0;
            utils::worker::Ticket line(begin, begin + mixed_writes);

            log.trace("Mixed load ({})", mixed_writes + mixed_reads);
            auto start = std::chrono::high_resolution_clock::now();
            while (mix_inserts < mixed_writes || mix_queries < mixed_reads) {
                auto idx = line.get();
                if (mix_queries >= mixed_reads ||
                    (mix_inserts < mixed_writes && coin(generator))) {
                    const key_type &key = data[idx] + offset;
                    tree.insert(key, idx);

                    mix_inserts++;
                } else {
                    key_type query_index = generator() % idx + offset;

                    const bool res = tree.contains(query_index);

                    ctr_empty += !res;
                    mix_queries++;
                }
            }
            auto duration = std::chrono::high_resolution_clock::now() - start;
            results << ", " << duration.count() << ", " << ctr_empty;
            timer.mixed = duration.count();
        }
    }

    void run_updates(const std::vector<key_type> &data, size_t num_inserts,
                     size_t num_updates) {
        if (num_updates > 0) {
            log.trace("Updates ({})", num_updates);
            std::vector<key_type> updates;
            std::uniform_int_distribution<size_t> index(0, num_inserts - 1);
            for (size_t i = 0; i < num_updates; i++) {
                updates.emplace_back(data[index(generator)] + offset);
            }
            auto duration = utils::worker::work(
                utils::worker::update_worker<tree_t, key_type>, tree, updates,
                0, num_updates, conf.num_threads, offset);
            results << ", " << duration.count();
            timer.updates = duration.count();
        }
    }

    void run_range(const std::vector<key_type> &data, size_t num_inserts,
                   size_t range, size_t size, RANGE_QUERY_TYPE type) {
        if (range > 0) {
            // std::cout << "Range (" << range << ")\n";
            log.trace("Range ({})", range);
            auto start = std::chrono::high_resolution_clock::now();
            size_t leaf_accesses = range_queries(tree, data, num_inserts, range,
                                                 offset, size, generator);
            auto duration = std::chrono::high_resolution_clock::now() - start;
            auto accesses = (leaf_accesses + range - 1) / range;  // ceil
            results << ", " << duration.count() << ", " << accesses;
            switch (type) {
                case SHORT:
                    timer.short_range = duration.count();
                    break;
                case MID:
                    timer.mid_range = duration.count();
                    break;
                case LONG:
                    timer.long_range = duration.count();
                    break;
            }
        }
    }

    void print_timers() {
        log.trace("******** Execution Latency ********");
        log.info("Preload: {}", timer.preload);
        log.info("Raw Writes: {}", timer.raw_writes);
        log.info("Raw Reads: {}", timer.raw_reads);
        log.info("Mixed: {}", timer.mixed);
        log.info("Updates: {}", timer.updates);
        log.info("Short Range: {}", timer.short_range);
        log.info("Mid Range: {}", timer.mid_range);
        log.info("Long Range: {}", timer.long_range);
    }

    void print_stats(std::string stats_type,
                     std::unordered_map<std::string, uint64_t> &stats) {
        log.trace("******** {} ********", stats_type);
        for (const auto &[key, value] : stats) {
            log.info("{}: {}", key, value);
        }
    }

    void run(const char *name, const std::vector<key_type> &data) {
        const size_t num_inserts = data.size();
        const size_t raw_writes = conf.raw_write_perc / 100.0 * num_inserts;
        const size_t mixed_writes =
            conf.mixed_writes_perc / 100.0 * num_inserts;
        assert(num_inserts >= raw_writes + mixed_writes);
        const size_t num_load = num_inserts - raw_writes - mixed_writes;
        const size_t raw_queries = conf.raw_read_perc / 100.0 * num_inserts;
        const size_t mixed_reads = conf.mixed_reads_perc / 100.0 * num_inserts;
        const size_t num_updates = conf.updates_perc / 100.0 * num_inserts;

        // tree.reset_ctr();

        std::filesystem::path file(name);
        results << tree_t::name << ", " << conf.num_threads << ", "
                << file.filename().c_str() << ", " << offset;

        run_preload(data, 0, num_load);
        run_writes(data, num_load, raw_writes);
        run_mixed(data, num_load + raw_writes, mixed_writes, mixed_reads);
        run_reads(data, num_inserts, raw_queries);
        run_updates(data, num_inserts, num_updates);
        run_range(data, num_inserts, conf.short_range, 1000,
                  RANGE_QUERY_TYPE::SHORT);
        run_range(data, num_inserts, conf.mid_range, 100,
                  RANGE_QUERY_TYPE::MID);
        run_range(data, num_inserts, conf.long_range, 10,
                  RANGE_QUERY_TYPE::LONG);

        if (conf.validate) {
            size_t count = 0;
            for (const auto &item : data) {
                if (!tree.contains(item)) {
                    count++;
#ifdef DEBUG
                    // std::cerr << item << " not found" << std::endl;
                    log.error("Key {} not found", item);
                    break;
#endif
                }
            }
            if (count) {
                // std::cerr << "Error: " << count << " keys not found\n";
                log.error("Error: {} keys not found", count);
            } else {
                // std::cerr << "All good\n";
                log.info("All good");
            }
        }

        results << ", ";
        results << tree << std::endl;
        print_timers();
        auto stats = tree.get_stats();
        print_stats("Tree Stats", stats);
    }
};

}  // namespace executor
}  // namespace utils