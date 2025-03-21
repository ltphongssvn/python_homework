#!/usr/bin/env python3
"""Module for creating employee dictionaries from row data,
excluding the employee_id field."""
from assignment2_task02 import employees


# Task 8: Create a dict for an Employee
def employee_dict(row: list) -> dict:
    """
    Create a dictionary for an employee from a row.
    """
    result = {}
    fields = employees["fields"]

    for i, field in enumerate(fields):
        if field != "employee_id":  # Skip employee_id
            result[field] = row[i]

    return result


if __name__ == "__main__":
    # Test employee_dict with a sample row
    print("Employee Dict:", employee_dict(employees["rows"][0]))
