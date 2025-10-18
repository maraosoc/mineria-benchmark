# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
from collections import defaultdict
from typing import TypedDict
import pandas as pd
from typing import Dict, List, Any
import multiprocessing
import pathlib
import re
import time



class Result(TypedDict):
    rate_2xx: float
    rate_4xx: float
    rate_5xx: float

def load_dataset_from_path(file_path: str, file_format: str = 'json') -> pd.DataFrame:
    if file_format == 'json':
        df = pd.read_json(file_path, lines = True)
    else:
        df = pd.read_parquet(file_path)
    return df


def map_function(line: str) -> str:
    """Maps an error code from a single line of text, 
    return a tuple with the first number of the error code"""

    error_code = re.search(r"\b(\d{3})\b", line)[0][0] # type: ignore
    return error_code


def group_and_reduce_function(filepath: str, column: str = 'message') -> Dict[str, int]:
    dataframe = load_dataset_from_path(filepath)
    dataframe[column] = dataframe[column].map(map_function) # type: ignore
    df = dataframe.groupby(column)[column].count().to_dict() # type: ignore

    return df

def merge_results(results: List[Dict[str, int]]) -> Dict[str, int]:

    results_dict = defaultdict(list)
    for result in results:
        for key, value in result.items():
            results_dict[key].append(value) 
    return {key: sum(value) for key, value in results_dict.items()}
    


def main(directory: str):
    start_time = time.perf_counter()
    file_path = pathlib.Path(directory)
    with multiprocessing.Pool() as pool:
        results = pool.map(group_and_reduce_function, list(file_path.glob('*.json'))) # type: ignore
    calculations = merge_results(results)
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    print(f"Execution time: {elapsed_time:.6f} seconds")
    return calculations


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="Path to s3 bucket with all data")
    args = parser.parse_args()

    print(main(args.input))