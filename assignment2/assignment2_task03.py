#!/usr/bin/env python3

"""Module for returning column index"""

from assignment2_task02 import employees


# Task 3: Find the Column Index
def column_index(column_name: str) -> int:
    """
    Find the index of a column in the employees fields.
    """
    return employees["fields"].index(column_name)


# Call column_index and store result in global variable
employee_id_column = column_index("employee_id")
