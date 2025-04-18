

#include "config.hpp"

#include <getopt.h>

#include <algorithm>
#include <fstream>
#include <iostream>

#include "utils/logging.hpp"

static std::string str_val(const std::string &val) {
    return val.substr(1, val.size() - 2);
}

static bool str_bool(const std::string &val) { return val == "true"; }

void Config::parse(const char *file) {
    if (file == nullptr) return;

    std::ifstream infile(file);
    if (!infile) {
        return;
    }
    std::cerr << "Loading config file " << file << std::endl;
    std::string line;
    while (std::getline(infile, line)) {
        line.erase(std::remove_if(line.begin(), line.end(), isspace),
                   line.end());
        if (line.empty() || line[0] == '#') continue;
        std::string knob_name = line.substr(0, line.find('='));
        std::string knob_value =
            line.substr(knob_name.length() + 1, line.size());
        if (knob_name == "BLOCKS_IN_MEMORY") {
            blocks_in_memory = std::stoi(knob_value);
        } else if (knob_name == "RAW_READS_PERCENTAGE") {
            raw_read_perc = std::stoi(knob_value);
        } else if (knob_name == "RAW_WRITES_PERCENTAGE") {
            raw_write_perc = std::stoi(knob_value);
        } else if (knob_name == "MIXED_WRITES_PERCENTAGE") {
            mixed_writes_perc = std::stoi(knob_value);
        } else if (knob_name == "MIXED_READ_PERCENTAGE") {
            mixed_reads_perc = std::stoi(knob_value);
        } else if (knob_name == "UPDATES_PERCENTAGE") {
            updates_perc = std::stoi(knob_value);
        } else if (knob_name == "SHORT_RANGE_QUERIES") {
            short_range = std::stoi(knob_value);
        } else if (knob_name == "MID_RANGE_QUERIES") {
            mid_range = std::stoi(knob_value);
        } else if (knob_name == "LONG_RANGE_QUERIES") {
            long_range = std::stoi(knob_value);
        } else if (knob_name == "RUNS") {
            runs = std::stoi(knob_value);
        } else if (knob_name == "REPEAT") {
            repeat = std::stoi(knob_value);
        } else if (knob_name == "SEED") {
            seed = std::stoi(knob_value);
        } else if (knob_name == "NUM_THREADS") {
            num_threads = std::stoi(knob_value);
        } else if (knob_name == "RESULTS_FILE") {
            results_csv = str_val(knob_value);
        } else if (knob_name == "RESULTS_LOG") {
            results_log = str_val(knob_value);
        } else if (knob_name == "BINARY_INPUT") {
            binary_input = str_bool(knob_value);
        } else if (knob_name == "VALIDATE") {
            validate = str_bool(knob_value);
        } else if (knob_name == "VERBOSE") {
            verbose = str_bool(knob_value);
        } else {
            std::cerr << "Invalid knob name: " << knob_name << std::endl;
        }
    }
}

void Config::parse(int argc, char **argv) {
    int i = 1;
    static struct option long_options[] = {
        {"blocks_in_memory", required_argument, nullptr, i++},
        {"raw_read_perc", required_argument, nullptr, i++},
        {"raw_write_perc", required_argument, nullptr, i++},
        {"mixed_writes_perc", required_argument, nullptr, i++},
        {"mixed_reads_perc", required_argument, nullptr, i++},
        {"updates_perc", required_argument, nullptr, i++},
        {"short_range", required_argument, nullptr, i++},
        {"mid_range", required_argument, nullptr, i++},
        {"long_range", required_argument, nullptr, i++},
        {"runs", required_argument, nullptr, i++},
        {"repeat", required_argument, nullptr, i++},
        {"seed", required_argument, nullptr, i++},
        {"num_threads", required_argument, nullptr, i++},
        {"results_csv", required_argument, nullptr, i++},
        {"results_log", required_argument, nullptr, i++},
        {"txt_input", no_argument, nullptr, i++},
        {"validate", no_argument, nullptr, i++},
        {"verbose", no_argument, nullptr, i++},
        {nullptr, 0, nullptr, 0},
    };
    // static struct option long_options[] = {
    //     {"blocks_in_memory", required_argument, nullptr, 1},
    //     {"raw_read_perc", required_argument, nullptr, 2},
    //     {"raw_write_perc", required_argument, nullptr, 3},
    //     {"mixed_writes_perc", required_argument, nullptr, 4},
    //     {"mixed_reads_perc", required_argument, nullptr, 5},
    //     {"updates_perc", required_argument, nullptr, 6},
    //     {"short_range", required_argument, nullptr, 7},
    //     {"mid_range", required_argument, nullptr, 8},
    //     {"long_range", required_argument, nullptr, 9},
    //     {"runs", required_argument, nullptr, 10},
    //     {"repeat", required_argument, nullptr, 11},
    //     {"seed", required_argument, nullptr, 12},
    //     {"num_threads", required_argument, nullptr, 13},
    //     {"results_csv", required_argument, nullptr, 14},
    //     {"results_log", required_argument, nullptr, 15},
    //     {"txt_input", no_argument, nullptr, 16},
    //     {"validate", no_argument, nullptr, 17},
    //     {"verbose", no_argument, nullptr, 18},
    //     {nullptr, 0, nullptr, 0},
    // };

    while (true) {
        int c = getopt_long(argc, argv, "", long_options, nullptr);
        if (c == -1) break;

        switch (c) {
            case 1:
                blocks_in_memory = std::stoi(optarg);
                break;
            case 2:
                raw_read_perc = std::stoi(optarg);
                break;
            case 3:
                raw_write_perc = std::stoi(optarg);
                break;
            case 4:
                mixed_writes_perc = std::stoi(optarg);
                break;
            case 5:
                mixed_reads_perc = std::stoi(optarg);
                break;
            case 6:
                updates_perc = std::stoi(optarg);
                break;
            case 7:
                short_range = std::stoi(optarg);
                break;
            case 8:
                mid_range = std::stoi(optarg);
                break;
            case 9:
                long_range = std::stoi(optarg);
                break;
            case 10:
                runs = std::stoi(optarg);
                break;
            case 11:
                repeat = std::stoi(optarg);
                break;
            case 12:
                seed = std::stoi(optarg);
                break;
            case 13:
                num_threads = std::stoi(optarg);
                break;
            case 14:
                results_csv = optarg;
                break;
            case 15:  // results_log
                results_log = optarg;
                break;
            case 16:
                binary_input = false;
                break;
            case 17:
                validate = true;
                break;
            case 18:
                verbose = true;
                break;
            default:
                printf("?? getopt returned character code 0%o ??\n", c);
        }
    }

    files = std::span(argv + optind, argc - optind);
}

void Config::print() {
    std::cout << "blocks_in_memory: " << blocks_in_memory
              << "\nraw_read_perc: " << raw_read_perc
              << "\nraw_write_perc: " << raw_write_perc
              << "\nmixed_writes_perc: " << mixed_writes_perc
              << "\nmixed_reads_perc: " << mixed_reads_perc
              << "\nupdates_perc: " << updates_perc
              << "\nshort_range: " << short_range
              << "\nmid_range: " << mid_range << "\nlong_range: " << long_range
              << "\nruns: " << runs << "\nrepeat: " << repeat
              << "\nseed: " << seed << "\nnum_threads: " << num_threads
              << "\nresults_csv: " << results_csv
              << "\nresults_log: " << results_log
              << "\nbinary_input: " << binary_input
              << "\nvalidate: " << validate << "\nverbose: " << verbose
              << "\nfiles:\n";

    for (const auto &file : files) {
        std::cout << '\t' << file << '\n';
    }
}

void Config::print(utils::logging::Logger &log) {
    log.trace("********** Configurations *****");
    log.info("blocks_in_memory: {}", blocks_in_memory);
    log.info("raw_read_perc: {}", raw_read_perc);
    log.info("raw_write_perc: {}", raw_write_perc);
    log.info("mixed_writes_perc: {}", mixed_writes_perc);
    log.info("mixed_reads_perc: {}", mixed_reads_perc);
    log.info("updates_perc: {}", updates_perc);
    log.info("short_range: {}", short_range);
    log.info("mid_range: {}", mid_range);
    log.info("long_range: {}", long_range);
    log.info("runs: {}", runs);
    log.info("repeat: {}", repeat);
    log.info("seed: {}", seed);
    log.info("num_threads: {}", num_threads);
    log.info("results_csv: {}", results_csv);
    log.info("results_log: {}", results_log);
    log.info("binary_input: {}", binary_input);
    log.info("validate: {}", validate);
    log.info("verbose: {}", verbose);
    log.info("files:");
    for (const auto &file : files) {
        log.info("\t{}", file);
    }
}
