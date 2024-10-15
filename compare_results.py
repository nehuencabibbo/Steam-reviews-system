import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import *

AMOUNT_OF_QUERYS = 5
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"

def add_line_to_corresponding_expected_query_result(line, results: Set[str], query: str):
    # Every line output is similar to:
    # ... Qi result: ...
    query_index = line.find(query) # Q5 result: ...
    if query_index == -1: return

    line = line[query_index + len(query):]
    _, result = line.split(':', maxsplit=1)

    result = result.strip()
    result = result[1:len(result) - 1] # Removes [ and ] from start and end
    result = result.replace("'", "") # Remove '
    result = ','.join([value.strip() for value in result.split(',')])

    # TODO: Handelear esto mejor 
    if result in results: raise Exception(f"{result} was a duplicate for {query} Analisis")

    results.add(result)

def add_line_to_corresponding_gotten_query_result(line, results: Set[str], query: str):
    # TODO: Implement
    # Every line output is similar to:
    # ... Qi result: ...


    return set()

def get_all_expected_query_results(file_name: str, query: str) -> Dict[str, Set[str]]:
    expected_results = set()
    with open(file_name) as myfile:
        for line in myfile: 
            add_line_to_corresponding_expected_query_result(line, expected_results, query)

    return expected_results


def get_all_gotten_query_results(file_name: str, query: str) -> Dict[str, Set[str]]:
    gotten_results = set()
    with open(file_name) as myfile:
        for line in myfile: 
            add_line_to_corresponding_gotten_query_result(line, gotten_results, query)

    return gotten_results

def show_query_comparison_result(expected_but_missing: List[str], not_expected_but_gotten: List[str], query: str):
    separator = f"\n{'=' * 5} RESULT ANALISIS FOR {query} {'=' * 5}"
    print(separator)
    if len(expected_but_missing) == 0 and len(not_expected_but_gotten) == 0:
        print(f"{GREEN}All results match!{RESET}")
    if len(expected_but_missing) != 0:
        for value in expected_but_missing:
            print(f"{RED}Expected but missing: {value}{RESET}")
    if len(not_expected_but_gotten) != 0:
        for value in not_expected_but_gotten:
            print(f"{YELLOW}Not expected but gotten: {value}{RESET}")
    

def compare_query(expected: Set[str], gotten: Set[str]):
    expected_but_missing = []
    for result in expected:
        if not result in gotten:
            expected_but_missing.append(result)

    not_expected_but_gotten = []
    for result in gotten: 
        if not result in expected:
            not_expected_but_gotten.append(result)

    return expected_but_missing, not_expected_but_gotten

def compare_all_querys(expected: str, gotten: str, query: str): 
    expected_results = get_all_expected_query_results(expected, query) 
    results_gotten = get_all_gotten_query_results(gotten, query) 


    expected_but_missing, not_expected_but_gotten = compare_query(
        expected_results, 
        results_gotten
    )

    show_query_comparison_result(expected_but_missing, not_expected_but_gotten, query)
            

def main():
    args = sys.argv[1:]

    if len(args) != 3:
        print("Incorrect amount of arguments")
        return -1 
    
    expected, gotten, query = args

    compare_all_querys(expected, gotten, query)


main()