#!/usr/bin/env python3
"""Module for sorting employee data by last name using a lambda function."""
from assignment2_task02 import employees
from assignment2_task03 import column_index


# Task 7: Sort the Rows by last_name Using a Lambda
def sort_by_last_name() -> list:
    """
    Sort employees by last name.
    """
    last_name_column = column_index("last_name")
    employees["rows"].sort(key=lambda row: row[last_name_column])
    return employees["rows"]
