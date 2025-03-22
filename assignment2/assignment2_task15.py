#!/usr/bin/env python3
"""
Minutes processing utility module.
This module provides functions for sorting meeting minutes data by date
and writing the results to a CSV file. It handles date formatting and
includes comprehensive error handling during file operations.
"""
import csv
import traceback
from typing import List, Tuple
from assignment2_task14 import minutes_list

# Import minutes1 for the fields
from assignment2_task12 import minutes1


# Task 15: Write Out Sorted List
def write_sorted_list() -> List[Tuple[str, str]]:
    """
    Sort minutes_list by date and write to a CSV file.

    Returns:
        List of tuples with meeting names and formatted date strings
    """
    # Sort by date (second element in each tuple)
    sorted_list = sorted(minutes_list, key=lambda x: x[1])

    # Convert dates back to strings
    converted_list = list(map(
        lambda x: (x[0], x[1].strftime("%B %d, %Y")),
        sorted_list
    ))

    try:
        with open(
            "./minutes.csv",
            "w",
            newline='',
            encoding="utf-8"
        ) as csv_file:
            csv_writer = csv.writer(csv_file)
            # Write headers first
            csv_writer.writerow(minutes1["fields"])
            # Write all rows
            for row in converted_list:
                csv_writer.writerow(row)
    except Exception as e:  # pylint: disable=broad-except
        trace_back = traceback.extract_tb(e.__traceback__)
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append(
                f'File : {trace[0]} , Line : {trace[1]}, '
                f'Func.Name : {trace[2]}, Message : {trace[3]}'
            )
        print(f"Exception type: {type(e).__name__}")
        message = str(e)
        if message:
            print(f"Exception message: {message}")
        print(f"Stack trace: {stack_trace}")

    return converted_list


# Call write_sorted_list
final_sorted_list = write_sorted_list()
print("Final Sorted List:", final_sorted_list)
