#!/usr/bin/env python3
"""
Module for creating a dictionary of dictionaries for all employees. This module
depends on assignment2_task02, assignment2_task03, and assignment2_task08.
"""
from assignment2_task02 import employees
from assignment2_task03 import employee_id_column
from assignment2_task08 import employee_dict


# Task 9: A dict of dicts, for All Employees
def all_employees_dict():
    """
    Create a dictionary of dictionaries for all employees.
    """
    result = {}

    for row in employees["rows"]:
        employee_id = row[employee_id_column]
        result[employee_id] = employee_dict(row)

    return result


if __name__ == "__main__":
    # Call all_employees_dict and print result
    all_employees = all_employees_dict()
    print("All Employees Dict:", all_employees)
