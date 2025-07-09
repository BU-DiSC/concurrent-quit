#include <cassert>
#include <iostream>
#include <vector>

#include "config.hpp"
#include "trees.hpp"
#include "utils/executor.hpp"
#include "utils/logging.hpp"
#include "utils/utils.hpp"

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
using namespace ConcurrentQuITBTreeAppends;
#elif defined(FOR_CONCURRENT_QUIT_ATOMIC)
using namespace ConcurrentQuITBTreeAtomic;
#elif defined(FOR_CONCURRENT_QUIT_ATOMIC2)
using namespace ConcurrentQuITBTreeAtomic2;
#else
// using namespace SimpleBTree;  // FOR_SIMPLEBTREE or fallback
using namespace ConcurrentQuITBTreeAtomic2;
#endif

#if defined(FOR_CONCURRENT_QUIT_APPENDS) || \
    defined(FOR_CONCURRENT_QUIT_ATOMIC) ||  \
    defined(FOR_CONCURRENT_QUIT_ATOMIC2)
using tree_t = BTree<key_type, value_type, true>;
#else
using tree_t = BTree<key_type, value_type>;
#endif

int main(int argc, char **argv) {
    // initialize logger
    auto &log = utils::logging::Logger::get_instance();

    if (argc < 2) {
        log.error("Usage: ./<tree_name> <input_file>...");
        return -1;
    }

    std::string config_file = "config.toml";
    Config conf;
    utils::infra::config::load_configurations(conf, config_file);
    utils::infra::config::load_configurations(conf, argc, argv);
    utils::infra::config::print_configurations(conf);

    tree_t::BlockManager manager(conf.blocks_in_memory);

    log.info("Writing CSV Results to: {}", conf.results_csv);

    std::vector<std::vector<key_type> > data;
    utils::infra::load::load_data(data, conf);

    log.trace("Running {} with {} threads", tree_t::name, conf.num_threads);
    for (size_t i = 0; i < conf.runs; ++i) {
        manager.reset();
        tree_t tree(manager);
        utils::executor::Workload<tree_t, key_type> workload(tree, conf);
        workload.run_all(data);
    }
    return 0;
}
