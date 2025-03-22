#!/usr/bin/env python3
"""Find an employee with lambda"""
from assignment2_task02 import employees
from assignment2_task03 import employee_id_column


# Task 6: Find the Employee with a Lambda
def employee_find_2(employee_id: int) -> list:
    """
    Find employees with the specified employee_id using a lambda.
    """
    matches = list(filter(lambda row: int(row[employee_id_column]) ==
                          employee_id, employees["rows"]))
    return matches
