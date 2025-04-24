import os
import argparse
import logging
from infra.db import IndexBenchDB
from infra.tree_analysis import PyTreeAnalysis

def main(args):
    # Set up logging
    logger = logging.getLogger(__name__)

    logger.info(f"Executables: {args.executable}")

    # Gather all files 
    files = os.listdir(args.input_dir)
    logger.info(f"#. input files: {len(files)}")

    experiments = [(executable, file) for executable in args.executable for file in files]
    logger.info(f"#. experiments: {len(experiments)}")

    # Create the database connection
    db = IndexBenchDB(args.db_path)

    tree_analysis = PyTreeAnalysis()

    for executable, file in experiments: 
        logger.info(f"Running {executable} on {file}")
        args, results = tree_analysis.run_single_tree_analysis(
            executable_path=executable,
            input_files=[os.path.join(args.input_dir, file)],
            config_file_path=None
        )
        tree_analysis.log_stats(results)
        executable_name = os.path.basename(executable)
        db.insert_row(
            index_type=executable_name,
            workload_file=file,
            tree_analysis_args=args,
            tree_analysis_results=results
        ) 
    
        


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_dir", 
        type=str, 
        required=True, 
        help="Directory containing input files"
    )
    parser.add_argument(
        "--db_path", 
        type=str, 
        required=True, 
        help="Path to the database file"
    )
    # parse executables as a list 
    parser.add_argument(
        "--executable", 
        type=str, 
        required=True,
        help="Path to the executable",
        nargs="+"
    )
    try:
        args = parser.parse_args()
    except SystemExit:
        print("Error parsing arguments")
        exit(1)

    logging.basicConfig(
        format="[%(levelname)s][%(asctime)-15s][%(filename)s] %(message)s",
        datefmt="%d-%m-%y:%H:%M:%S",
        level=logging.INFO
    )
    logging.info(f"Logger level: INFO")

    main(args)



    