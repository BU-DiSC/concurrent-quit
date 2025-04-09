#include <cassert>
#include <iostream>
#include <vector>

#include "config.hpp"
#include "executor.hpp"
#include "trees.hpp"
#include "utils.hpp"

using key_type = uint32_t;
using value_type = uint32_t;

// Default to SimpleBTree if nothing else is defined
#if defined(FOR_TAILBTREE)
using namespace TailBTree;
#elif defined(FOR_LILBTREE)
using namespace LILBTree;
#elif defined(FOR_QUIT)
using namespace QuITBTree;
#elif defined(FOR_CONCURRENT_SIMPLE)
using namespace ConcurrentSimpleBTree;
#elif defined(FOR_CONCURRENT_TAIL)
using namespace ConcurrentTailBTree;
#elif defined(FOR_CONCURRENT_QUIT)
using namespace ConcurrentQuITBTree;
#elif defined(FOR_CONCURRENT_QUIT_APPENDS)
using namespace ConcurrentQuITBTree;
#else
using namespace SimpleBTree;  // FOR_SIMPLEBTREE or fallback
#endif

#if defined(FOR_CONCURRENT_QUIT_APPENDS)
using tree_t = BTree<key_type, value_type, true>;
#else
using tree_t = BTree<key_type, value_type>;
#endif

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cerr << "Usage: ./<tree_name> <input_file>..." << std::endl;
        return -1;
    }

    std::string config_file = "config.toml";
    Config conf;
    utils::infra::config::load_configurations(conf, config_file);
    utils::infra::config::load_configurations(conf, argc, argv);
    utils::infra::config::print_configurations(conf);

    tree_t::BlockManager manager(conf.blocks_in_memory);

    std::cout << "Writing CSV Results to: " << conf.results_csv << std::endl;

    std::vector<std::vector<key_type> > data;
    utils::infra::load::load_data(data, conf);

    std::cout << "Running " << tree_t::name << " with " << conf.num_threads
              << " threads\n";
    for (size_t i = 0; i < conf.runs; ++i) {
        manager.reset();
        tree_t tree(manager);
        utils::executor::Workload<tree_t, key_type> workload(tree, conf);
        workload.run_all(data);
    }
    return 0;
}
