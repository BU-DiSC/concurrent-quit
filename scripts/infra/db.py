import sqlite3
from datetime import datetime, timezone

from .tree_analysis import TreeAnalysisArgs, TreeAnalysisResults

class IndexBenchDB: 
    def __init__(self, db_path: str) -> None:
        self.db_con = sqlite3.connect(db_path)
        cursor = self.db_con.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS index_bench (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TXT,
                index_type TXT NOT NULL,
                workload_file TXT,
                N INTEGER, 
                K INTEGER,
                L INTEGER,
                threads INTEGER,
                preload_time INTEGER,
                raw_writes_time INTEGER,
                raw_reads_time INTEGER,
                mixed_time INTEGER,
                updates_time INTEGER,
                short_range_time INTEGER,
                mid_range_time INTEGER,
                long_range_time INTEGER,
                size INTEGER,
                height INTEGER,
                internal INTEGER,
                leaves INTEGER,
                fast_inserts INTEGER,
                redistribute INTEGER,
                soft_resets INTEGER,
                hard_resets INTEGER,
                fast_inserts_fail INTEGER,
                sort INTEGER
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS execution_args (
                id INTEGER PRIMARY KEY,
                blocks_in_memory INTEGER,
                raw_read_perc REAL,
                raw_write_perc REAL,
                mixed_writes_perc REAL,
                mixed_reads_perc REAL,
                updates_perc REAL,
                short_range REAL,
                mid_range REAL,
                long_range REAL,
                runs INTEGER,
                repeat INTEGER,
                seed INTEGER,
                num_threads INTEGER,
                results_csv TXT,
                results_log TXT,
                binary_input BOOLEAN,
                validate BOOLEAN,
                verbose BOOLEAN,
                input_file TXT,
                FOREIGN KEY (id) REFERENCES index_bench (id)
            );
        """)
        self.db_con.commit()

    def insert_row(self, index_type: str, workload_file: str, 
                   tree_analysis_args: TreeAnalysisArgs, tree_analysis_results: TreeAnalysisResults):
        cursor = self.db_con.cursor()
        timestamp = datetime.now(timezone.utc)
        cursor.execute("""
            INSERT INTO index_bench (
                timestamp, index_type, workload_file, N, K, L, threads,
                preload_time, raw_writes_time, raw_reads_time,
                mixed_time, updates_time, short_range_time,
                mid_range_time, long_range_time,
                size, height, internal, leaves,
                fast_inserts, redistribute,
                soft_resets, hard_resets,
                fast_inserts_fail, sort
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            timestamp, index_type, workload_file,
            tree_analysis_results.N, tree_analysis_results.K, tree_analysis_results.L,
            tree_analysis_results.threads, tree_analysis_results.preload_time, tree_analysis_results.raw_writes_time,
            tree_analysis_results.raw_reads_time, tree_analysis_results.mixed_time,
            tree_analysis_results.updates_time, tree_analysis_results.short_range_time,
            tree_analysis_results.mid_range_time, tree_analysis_results.long_range_time,
            tree_analysis_results.size, tree_analysis_results.height,
            tree_analysis_results.internal, tree_analysis_results.leaves,
            tree_analysis_results.fast_inserts, tree_analysis_results.redistribute,
            tree_analysis_results.soft_resets, tree_analysis_results.hard_resets,
            tree_analysis_results.fast_inserts_fail, tree_analysis_results.sort
            )
        )
        # Get the last inserted row id
        last_id = cursor.lastrowid
        # Insert execution arguments
        cursor.execute("""
            INSERT INTO execution_args (
                id, blocks_in_memory, raw_read_perc, raw_write_perc,
                mixed_writes_perc, mixed_reads_perc, updates_perc,
                short_range, mid_range, long_range,
                runs, repeat, seed,
                num_threads, results_csv, results_log,
                binary_input, validate, verbose,
                input_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? , ?);
        """, (
            last_id, tree_analysis_args.blocks_in_memory,
            tree_analysis_args.raw_read_perc, tree_analysis_args.raw_write_perc,
            tree_analysis_args.mixed_writes_perc, tree_analysis_args.mixed_reads_perc,
            tree_analysis_args.updates_perc, tree_analysis_args.short_range,
            tree_analysis_args.mid_range, tree_analysis_args.long_range,
            tree_analysis_args.runs, tree_analysis_args.repeat,
            tree_analysis_args.seed, tree_analysis_args.num_threads,
            tree_analysis_args.results_csv, tree_analysis_args.results_log,
            int(tree_analysis_args.binary_input), int(tree_analysis_args.validate),
            int(tree_analysis_args.verbose), tree_analysis_args.input_file
            )
        )
        self.db_con.commit()

