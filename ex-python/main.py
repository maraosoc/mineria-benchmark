# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

from typing import TypedDict, Dict, List, Tuple, Any
from collections import defaultdict
import json
import time
import multiprocessing
import os
import re
import pathlib

class Result(TypedDict):
    rate_2xx: float
    rate_4xx: float
    rate_5xx: float



def map_function(line: str) -> Tuple[str, int]:
    """Maps an error code from a single line of text, 
    return a tuple with the first number of the error code"""

    error_code = re.search(r"\b(\d{3})\b", line)[0][0] # type: ignore
    return (error_code, 1)
    
def group_by_function(mapped_items: List[Tuple[str, int]]) -> List[Tuple[str, List[int]]]:
    "Takes a list of tuples and reduces them by key, adding up all the logs retrieved"

    grouped_dict = defaultdict(list)

    for key, value  in mapped_items:
        grouped_dict[key].append(value)

    return list(grouped_dict.items())


def reducer_function(grouped_items: Tuple[str, List[int]]) -> Tuple[str, int]:
    key, values = grouped_items    
    return (key, sum(values))


def map_json(filepath: str) -> List[Tuple[str, int]]:
    """Takes a json filepath, maps and reduces the logs, returning a list containing the count 
    of logs by error code"""

    with open(filepath, 'r') as file:
        mapped_data = [map_function(log) for log in file]
    grouped_data = group_by_function(mapped_data)
    reduced_data = [reducer_function(item) for item in grouped_data]

    return reduced_data

def merge_results(results: List[List[Tuple[str, int]]]):
    merged_dict = defaultdict(list)
    for calculation in results:
        for key, value in calculation:
            merged_dict[key].append(value)
    reduced_result = [reducer_function(error_code) for error_code in merged_dict.items()]
    return reduced_result

        
def main(directory: str):
    json_path = pathlib.Path(directory)
    start_time = time.perf_counter()
    with multiprocessing.Pool() as pool:
        results = pool.map(map_json, list(json_path.glob("*.json"))) # type: ignore
    calculations = merge_results(results)
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Execution time: {elapsed_time:.6f} seconds")
    print(calculations)
    return Result(
        rate_2xx = calculations[0][1],
        rate_4xx = calculations[1][1],
        rate_5xx = calculations[2][1],
    )

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="Path to s3 bucket with all data")
    args = parser.parse_args()
    main(args.input)
