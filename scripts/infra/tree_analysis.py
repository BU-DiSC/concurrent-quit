from dataclasses import dataclass
import re
import os 
import logging 
import subprocess
import random 
from pip._vendor import tomli

@dataclass
class TreeAnalysisArgs: 
    blocks_in_memory: int = 2000000
    raw_read_perc: float = 0
    raw_write_perc: float = 0
    mixed_writes_perc: float = 0
    mixed_reads_perc: float = 0
    updates_perc: float = 0
    short_range: float = 0
    mid_range: float = 0
    long_range: float = 0
    runs: int = 1 
    repeat: int = 1
    seed: int = 1234
    num_threads: int = 1
    results_csv: str = "results.csv"
    results_log: str = "results.log"
    binary_input: bool = True
    validate: bool = False
    verbose: bool = False
    input_file: str = None

@dataclass 
class TreeAnalysisResults:
    # latency results
    preload_time: int = 0
    raw_writes_time: int = 0
    raw_reads_time: int = 0
    mixed_time: int= 0
    updates_time: int = 0
    short_range_time: int = 0
    mid_range_time: int = 0
    long_range_time: int = 0

    # index stats
    size: int = 0
    height: int = 0
    internal: int = 0
    leaves: int = 0
    fast_inserts: int = 0
    redistribute: int = 0
    soft_resets: int = 0
    hard_resets: int = 0
    fast_inserts_fail: int = 0
    sort: int = 0

    # workload info 
    N: int = 0
    K: int = 0
    L: int = 0
    threads: int = 1
    

@dataclass 
class TreeAnalysisRegex: 
    def __init__(self) -> None: 
        flags = re.MULTILINE
        self.preload_time_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] Preload: (\d+)", flags)
        self.raw_writes_time_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] Raw Writes: (\d+)", flags)
        self.raw_reads_time_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] Raw Reads: (\d+)", flags)
        self.mixed_time_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] Mixed: (\d+)", flags)
        self.updates_time_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] Updates: (\d+)", flags)
        self.short_range_time_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] Short Range: (\d+)", flags)
        self.mid_range_time_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] Mid Range: (\d+)", flags)
        self.long_range_time_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] Long Range: (\d+)", flags)

        # index stats regex
        self.size_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] size: (\d+)", flags)
        self.height_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] height: (\d+)", flags)
        self.internal_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] internal: (\d+)", flags)
        self.leaves_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] leaves: (\d+)", flags)
        self.fast_inserts_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] fast_inserts: (\d+)", flags)
        self.redistribute_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] redistribute: (\d+)", flags)
        self.soft_resets_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] soft_resets: (\d+)", flags)
        self.hard_resets_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] hard_resets: (\d+)", flags)
        self.fast_inserts_fail_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] fast_inserts_fail: (\d+)", flags)
        self.sort_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] sort: (\d+)", flags)

        # workload info regex (match last N_K_L pattern anywhere in the log)
        self.workload_regex = re.compile(r"(\d+)_(\d+)_(\d+)(?!.*\d+_\d+_\d+)")

        # args regex
        self.blocks_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] blocks_in_memory: (\d+)", flags)
        self.raw_read_perc_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] raw_read_perc: (\d+)", flags)
        self.raw_write_perc_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] raw_write_perc: (\d+)", flags)
        self.mixed_writes_perc_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] mixed_writes_perc: (\d+)", flags)
        self.mixed_reads_perc_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] mixed_reads_perc: (\d+)", flags)
        self.updates_perc_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] updates_perc: (\d+)", flags)
        self.short_range_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] short_range: (\d+)", flags)
        self.mid_range_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] mid_range: (\d+)", flags)
        self.long_range_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] long_range: (\d+)", flags)
        self.runs_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] runs: (\d+)", flags)
        self.repeat_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] repeat: (\d+)", flags)
        self.seed_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] seed: (\d+)", flags)
        self.num_threads_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] num_threads: (\d+)", flags)
        self.results_csv_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] results_csv: (.*)", flags)
        self.results_log_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] results_log: (.*)", flags)
        self.binary_input_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] binary_input: (true|false)", flags)
        self.validate_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] validate: (true|false)", flags)
        self.verbose_regex = re.compile(r"\[[0-9 :.-]+\] \[.*?\] \[info\] verbose: (true|false)", flags)


class PyTreeAnalysis: 
    def __init__(self) -> None: 
        self.tree_analysis_regex = TreeAnalysisRegex()

    def parse_results(self, process_results: str) -> TreeAnalysisResults:
        args = TreeAnalysisArgs()
        # parse the args
        blocks = self.tree_analysis_regex.blocks_regex.search(process_results)
        args.blocks_in_memory = int(blocks.group(1)) if blocks else 0
        raw_read_perc = self.tree_analysis_regex.raw_read_perc_regex.search(process_results)
        args.raw_read_perc = int(raw_read_perc.group(1)) if raw_read_perc else 0
        raw_write_perc = self.tree_analysis_regex.raw_write_perc_regex.search(process_results)
        args.raw_write_perc = int(raw_write_perc.group(1)) if raw_write_perc else 0
        mixed_writes_perc = self.tree_analysis_regex.mixed_writes_perc_regex.search(process_results)
        args.mixed_writes_perc = int(mixed_writes_perc.group(1)) if mixed_writes_perc else 0
        mixed_reads_perc = self.tree_analysis_regex.mixed_reads_perc_regex.search(process_results)
        args.mixed_reads_perc = int(mixed_reads_perc.group(1)) if mixed_reads_perc else 0
        updates_perc = self.tree_analysis_regex.updates_perc_regex.search(process_results)
        args.updates_perc = int(updates_perc.group(1)) if updates_perc else 0
        short_range = self.tree_analysis_regex.short_range_regex.search(process_results)
        args.short_range = int(short_range.group(1)) if short_range else 0
        mid_range = self.tree_analysis_regex.mid_range_regex.search(process_results)
        args.mid_range = int(mid_range.group(1)) if mid_range else 0
        long_range = self.tree_analysis_regex.long_range_regex.search(process_results)
        args.long_range = int(long_range.group(1)) if long_range else 0
        runs = self.tree_analysis_regex.runs_regex.search(process_results)
        args.runs = int(runs.group(1)) if runs else 0
        repeat = self.tree_analysis_regex.repeat_regex.search(process_results)
        args.repeat = int(repeat.group(1)) if repeat else 0
        seed = self.tree_analysis_regex.seed_regex.search(process_results)
        args.seed = int(seed.group(1)) if seed else 0
        num_threads = self.tree_analysis_regex.num_threads_regex.search(process_results)
        args.num_threads = int(num_threads.group(1)) if num_threads else 0
        results_csv = self.tree_analysis_regex.results_csv_regex.search(process_results)
        args.results_csv = results_csv.group(1) if results_csv else ""
        results_log = self.tree_analysis_regex.results_log_regex.search(process_results)
        args.results_log = results_log.group(1) if results_log else ""
        binary_input = self.tree_analysis_regex.binary_input_regex.search(process_results)
        args.binary_input = binary_input.group(1).lower() == "true" if binary_input else False
        validate = self.tree_analysis_regex.validate_regex.search(process_results)
        args.validate = validate.group(1).lower() == "true" if validate else False
        verbose = self.tree_analysis_regex.verbose_regex.search(process_results)
        args.verbose = verbose.group(1).lower() == "true" if verbose else False

        results = TreeAnalysisResults()

        preload_time = self.tree_analysis_regex.preload_time_regex.search(process_results)
        results.preload_time = int(preload_time.group(1)) if preload_time else 0

        raw_writes_time = self.tree_analysis_regex.raw_writes_time_regex.search(process_results)
        results.raw_writes_time = int(raw_writes_time.group(1)) if raw_writes_time else 0

        raw_reads_time = self.tree_analysis_regex.raw_reads_time_regex.search(process_results)
        results.raw_reads_time = int(raw_reads_time.group(1)) if raw_reads_time else 0

        mixed_time = self.tree_analysis_regex.mixed_time_regex.search(process_results)
        results.mixed_time = int(mixed_time.group(1)) if mixed_time else 0

        updates_time = self.tree_analysis_regex.updates_time_regex.search(process_results)
        results.updates_time = int(updates_time.group(1)) if updates_time else 0

        short_range_time = self.tree_analysis_regex.short_range_time_regex.search(process_results)
        results.short_range_time = int(short_range_time.group(1)) if short_range_time else 0

        mid_range_time = self.tree_analysis_regex.mid_range_time_regex.search(process_results)
        results.mid_range_time = int(mid_range_time.group(1)) if mid_range_time else 0

        long_range_time = self.tree_analysis_regex.long_range_time_regex.search(process_results)
        results.long_range_time = int(long_range_time.group(1)) if long_range_time else 0

        # index stats
        size = self.tree_analysis_regex.size_regex.search(process_results)
        results.size = int(size.group(1)) if size else 0
        height = self.tree_analysis_regex.height_regex.search(process_results)
        results.height = int(height.group(1)) if height else 0
        internal = self.tree_analysis_regex.internal_regex.search(process_results)
        results.internal = int(internal.group(1)) if internal else 0
        leaves = self.tree_analysis_regex.leaves_regex.search(process_results)
        results.leaves = int(leaves.group(1)) if leaves else 0
        fast_inserts = self.tree_analysis_regex.fast_inserts_regex.search(process_results)
        results.fast_inserts = int(fast_inserts.group(1)) if fast_inserts else 0
        redistribute = self.tree_analysis_regex.redistribute_regex.search(process_results)
        results.redistribute = int(redistribute.group(1)) if redistribute else 0
        soft_resets = self.tree_analysis_regex.soft_resets_regex.search(process_results)
        results.soft_resets = int(soft_resets.group(1)) if soft_resets else 0
        hard_resets = self.tree_analysis_regex.hard_resets_regex.search(process_results)
        results.hard_resets = int(hard_resets.group(1)) if hard_resets else 0
        fast_inserts_fail = self.tree_analysis_regex.fast_inserts_fail_regex.search(process_results)
        results.fast_inserts_fail = int(fast_inserts_fail.group(1)) if fast_inserts_fail else 0
        sort = self.tree_analysis_regex.sort_regex.search(process_results)
        results.sort = int(sort.group(1)) if sort else 0

        results.threads = args.num_threads

        # workload info
        workload = self.tree_analysis_regex.workload_regex.search(process_results)
        if workload: 
            results.N, results.K, results.L = map(int, workload.groups())
        else:
            print("Workload info not found in the results")
            logging.error("Workload info not found in the results")
            results.N, results.K, results.L = 0, 0, 0
        return args, results
    
    def run_single_tree_analysis(self, executable_path: str, 
                                 input_files: list, 
                                 config_file_path: str = None):
        
        config = TreeAnalysisArgs()

        # load config file if provided 
        if config_file_path is not None: 
            with open(config_file_path, 'rb') as f:
                config_data = tomli.load(f)
                # parse the config file
                config.blocks_in_memory = config_data["BLOCKS_IN_MEMORY"]
                config.raw_read_perc = config_data["RAW_READ_PERCENTAGE"]
                config.raw_write_perc = config_data["RAW_WRITE_PERCENTAGE"]
                config.mixed_writes_perc = config_data["MIXED_WRITES_PERCENTAGE"]
                config.mixed_reads_perc = config_data["MIXED_READS_PERCENTAGE"]
                config.updates_perc = config_data["UPDATES_PERCENTAGE"]
                config.short_range = config_data["SHORT_RANGE_QUERIES"]
                config.mid_range = config_data["MID_RANGE_QUERIES"]
                config.long_range = config_data["LONG_RANGE_QUERIES"]
                config.runs = config_data["RUNS"]
                config.repeat = config_data["REPEAT"]
                config.seed = config_data["SEED"]
                config.num_threads = config_data["NUM_THREADS"]
                config.results_csv = config_data["RESULTS_FILE"]
                config.results_log = config_data["RESULTS_LOG"]
                config.binary_input = config_data["BINARY_INPUT"]
                config.validate = config_data["VALIDATE"]
                config.verbose = config_data["VERBOSE"]

        # build the command 
        # for now, the config file is accepted in the cpp file itself 
        # TODO: modify the tree_analysis.cpp file to make config file optional
        cmd = [executable_path]
        for file in input_files:
            cmd.append(file)
     
        # print cmd as sample 
        logging.info(f"Running command: {cmd}")
        print(f"Running command: {cmd}")
        
        process = subprocess.Popen(
            " ".join(cmd),
            stdout=subprocess.PIPE, 
            universal_newlines=True,
            shell=True)
        assert process.stdout is not None, "Error: process stdout is None"
        process_results, _ = process.communicate()
        logging.debug(f"{process_results}")

        # parse the results
        args, results = self.parse_results(process_results)

        return args, results
    
    def log_stats(self, results: TreeAnalysisResults):
        logging.info(f"Stats:")
        logging.info(f"N: {results.N}")
        logging.info(f"K: {results.K}")
        logging.info(f"L: {results.L}")
        logging.info(f"Threads: {results.threads}")
        logging.info(f"Preload time: {results.preload_time}")
        logging.info(f"Raw writes time: {results.raw_writes_time}")
        logging.info(f"Raw reads time: {results.raw_reads_time}")
        logging.info(f"Mixed time: {results.mixed_time}")
        logging.info(f"Updates time: {results.updates_time}")
        logging.info(f"Short range time: {results.short_range_time}")
        logging.info(f"Mid range time: {results.mid_range_time}")
        logging.info(f"Long range time: {results.long_range_time}") 
        logging.info(f"Size: {results.size}")
        logging.info(f"Height: {results.height}")   
        logging.info(f"Internal: {results.internal}")
        logging.info(f"Leaves: {results.leaves}")
        logging.info(f"Fast inserts: {results.fast_inserts}")
        logging.info(f"Redistribute: {results.redistribute}")
        logging.info(f"Soft resets: {results.soft_resets}")
        logging.info(f"Hard resets: {results.hard_resets}")
        logging.info(f"Fast inserts fail: {results.fast_inserts_fail}")
        logging.info(f"Sort: {results.sort}")
       

        


# call the tree analysis
# tree_analysis = PyTreeAnalysis("./build/quit")
# tree_analysis.run_single_tree_analysis(["../../quick-insertion-tree/scripts/workloads/5_50_100"])