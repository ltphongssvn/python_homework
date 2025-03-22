#!/usr/bin/env python3
""""Module to find first name"""
from assignment2_task02 import employees
from assignment2_task03 import column_index


# Task 4: Find the Employee First Name
def first_name(row_number: int) -> str:
    """
    Find the first name at the specified row.
    """
    row = employees["rows"][row_number]
    first_name_column = column_index("first_name")
    return row[first_name_column]
