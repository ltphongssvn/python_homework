#!/usr/bin/env python3
"""Module to find an employee"""
from assignment2_task02 import employees
from assignment2_task03 import employee_id_column


# Task 5: Find the Employee: a Function in a Function
def employee_find(employee_id: int) -> list:
    """
    Find employees with the specified employee_id.
    """
    def employee_match(row: list) -> bool:
        return int(row[employee_id_column]) == employee_id

    matches = list(filter(employee_match, employees["rows"]))
    return matches
