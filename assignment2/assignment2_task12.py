#!/usr/bin/env python3
"""
Module for reading and processing CSV files containing meeting minutes.
This module provides functions to read CSV files into dictionary structures.
"""
import csv
import traceback


# Task 12: Read minutes1.csv and minutes2.csv
def read_csv_to_dict(filename):
    """
    Helper function to read a CSV file and return a dict with fields
    and rows as tuples.
    """
    result_dict = {}
    rows_list = []

    try:
        with open(filename, "r", encoding="utf-8") as csv_file:
            csv_reader = csv.reader(csv_file)

            # Get the first row (headers) and store in dict
            first_row = next(csv_reader)
            result_dict["fields"] = first_row

            # Store all other rows as tuples in the rows list
            for row in csv_reader:
                rows_list.append(tuple(row))

            # Add rows list to the result dictionary
            result_dict["rows"] = rows_list

    except Exception as e:
        trace_back = traceback.extract_tb(e.__traceback__)
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append(
                f'File: {trace[0]}, '
                f'Line: {trace[1]}, '
                f'Func.Name: {trace[2]}, '
                f'Message: {trace[3]}'
            )
        print(f"Exception type: {type(e).__name__}")
        message = str(e)
        if message:
            print(f"Exception message: {message}")
        print(f"Stack trace: {stack_trace}")

    return result_dict


def read_minutes():
    """
    Read minutes1.csv and minutes2.csv files.
    """
    mins1 = read_csv_to_dict("../csv/minutes1.csv")
    mins2 = read_csv_to_dict("../csv/minutes2.csv")
    return mins1, mins2


# Call read_minutes and store results in global variables
minutes1, minutes2 = read_minutes()
print("Minutes1:", minutes1)
print("Minutes2:", minutes2)
