#!/usr/bin/env python3
"""
Consolidated assignment file containing all tasks from assignment 2. This
module provides functions for reading and processing employee and meeting data,
manipulating dictionaries and sets, working with environment variables,
and more.
"""
import csv
import os
import traceback
from typing import Dict, List, Any, Tuple
from datetime import datetime
import custom_module


# Task 2: Read employees from CSV file
def read_employees() -> Dict[str, Any]:
    """Read employees from csv file and return as dictionary."""
    employees_dict: Dict[str, Any] = {}
    row_list: List[List[Any]] = []
    try:
        with open('../csv/employees.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)
            employees_dict['fields'] = headers
            for row in reader:
                row_list.append(row)
            employees_dict['rows'] = row_list
    except Exception as e:      # pylint: disable=broad-exception-caught
        trace_back = traceback.extract_tb(e.__traceback__)
        stack_trace = []
        for trace in trace_back:
            stack_trace.append(f'File : {trace[0]} , Line : {trace[1]},'
                               f'Func.Name : {trace[2]}, Message : {trace[3]}')
        print(f"Exception type: {type(e).__name__}")
        message = str(e)
        if message:
            print(f"Exception message: {message}")
        print(f"Stack trace: {stack_trace}")
    return employees_dict


# Initialize employees global variable
employees = read_employees()


# Task 3: Find the Column Index
def column_index(column_name: str) -> int:
    """
    Find the index of a column in the employees fields.
    """
    return employees["fields"].index(column_name)


# Store employee_id column index in global variable
employee_id_column = column_index("employee_id")


# Task 4: Find the Employee First Name
def first_name(row_number: int) -> str:
    """
    Find the first name at the specified row.
    """
    row = employees["rows"][row_number]
    first_name_column = column_index("first_name")
    return row[first_name_column]


# Task 5: Find the Employee: a Function in a Function
def employee_find(employee_id: int) -> list:
    """
    Find employees with the specified employee_id.
    """
    def employee_match(row: list) -> bool:
        return int(row[employee_id_column]) == employee_id
    matches = list(filter(employee_match, employees["rows"]))
    return matches


# Task 6: Find the Employee with a Lambda
def employee_find_2(employee_id: int) -> list:
    """
    Find employees with the specified employee_id using a lambda.
    """
    matches = list(filter(lambda row: int(row[employee_id_column]) ==
                          employee_id, employees["rows"]))
    return matches


# Task 7: Sort the Rows by last_name Using a Lambda
def sort_by_last_name() -> list:
    """
    Sort employees by last name.
    """
    last_name_column = column_index("last_name")
    employees["rows"].sort(key=lambda row: row[last_name_column])
    return employees["rows"]


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


# Task 10: Working with environment variables using the os module
def get_this_value():
    """
    Get the value of the THISVALUE environment variable.
    """
    return os.getenv("THISVALUE")


# Task 11: Creating Your Own Module
def set_that_secret(new_secret):
    """
    Set the secret in the custom_module.
    """
    # Import is placed inside function to handle
    # potential import errors gracefully
    try:
        custom_module.set_secret(new_secret)
        return True
    except ImportError:
        print("Error: custom_module not found")
        return False


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
    except Exception as e:  # pylint: disable=broad-except
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


# Initialize minutes1 and minutes2 global variables
minutes1, minutes2 = read_minutes()


# Task 13: Create minutes_set
def create_minutes_set():
    """
    Create a set from the rows of minutes1 and minutes2.
    """
    set1 = set(minutes1["rows"])
    set2 = set(minutes2["rows"])
    # Combine both sets (union)
    return set1.union(set2)


# Initialize minutes_set global variable
minutes_set = create_minutes_set()


# Task 14: Convert to datetime
def create_minutes_list():
    """
    Create a list from minutes_set with datetime objects.
    """
    # Convert set to list
    result_list = list(minutes_set)
    # Use map to convert dates to datetime objects
    result_list = list(map(
        lambda x: (x[0], datetime.strptime(x[1], "%B %d, %Y")),
        result_list
    ))
    return result_list


# Initialize minutes_list global variable
minutes_list = create_minutes_list()


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


# Main execution block
if __name__ == "__main__":
    # Print employees data
    print("Employees:", employees)

    # Test column_index
    print("Employee ID column index:", employee_id_column)

    # Test first_name function (if there's data)
    if employees["rows"]:
        print("First employee's first name:", first_name(0))

    # Test employee_find (with a sample ID, assuming 1 exists)
    print("Finding employee with ID 1:", employee_find(1))

    # Test employee_find_2 (with same sample ID)
    print("Finding employee with ID 1 using lambda:", employee_find_2(1))

    # Test sort_by_last_name
    sorted_employees = sort_by_last_name()
    print("Employees sorted by last name:", sorted_employees)

    # Test employee_dict with first employee row (if exists)
    if employees["rows"]:
        print("First employee as dict:", employee_dict(employees["rows"][0]))

    # Test all_employees_dict
    all_employees = all_employees_dict()
    print("All employees dict:", all_employees)

    # Test get_this_value
    this_value = get_this_value()
    print("THISVALUE environment variable:", this_value)

    # Test set_that_secret (commented out to avoid ImportError if
    # custom_module doesn't exist)
    # set_that_secret("new_secret_value")

    # Print minutes data
    print("Minutes1:", minutes1)
    print("Minutes2:", minutes2)

    # Print minutes_set
    print("Minutes Set:", minutes_set)

    # Print minutes_list
    print("Minutes List:", minutes_list)

    # Test write_sorted_list
    final_sorted_list = write_sorted_list()
    print("Final Sorted List:", final_sorted_list)
