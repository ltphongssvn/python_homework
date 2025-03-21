#!/usr/bin/env python3
import csv
import traceback
import os
import custom_module
from datetime import datetime


# Task 2: Read a CSV File
def read_employees():
    """
    Read the employees CSV file and return a dictionary with fields and rows.
    """
    result_dict = {}
    rows_list = []

    try:
        with open("../csv/employees.csv", "r") as csv_file:
            csv_reader = csv.reader(csv_file)

            # Get the first row (headers) and store in dict
            first_row = next(csv_reader)
            result_dict["fields"] = first_row

            # Store all other rows in the rows list
            for row in csv_reader:
                rows_list.append(row)

            # Add rows list to the result dictionary
            result_dict["rows"] = rows_list

    except Exception as e:
        trace_back = traceback.extract_tb(e.__traceback__)
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append(f'File : {trace[0]} , Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}')
        print(f"Exception type: {type(e).__name__}")
        message = str(e)
        if message:
            print(f"Exception message: {message}")
        print(f"Stack trace: {stack_trace}")

    return result_dict


# Call read_employees and store result in global variable
employees = read_employees()
print("Employees:", employees)


# Task 3: Find the Column Index
def column_index(column_name):
    """
    Find the index of a column in the employees fields.
    """
    return employees["fields"].index(column_name)


# Call column_index and store result in global variable
employee_id_column = column_index("employee_id")


# Task 4: Find the Employee First Name
def first_name(row_number):
    """
    Find the first name at the specified row.
    """
    first_name_column = column_index("first_name")
    return employees["rows"][row_number][first_name_column]


# Task 5: Find the Employee: a Function in a Function
def employee_find(employee_id):
    """
    Find employees with the specified employee_id.
    """
    def employee_match(row):
        return int(row[employee_id_column]) == employee_id

    matches = list(filter(employee_match, employees["rows"]))
    return matches


# Task 6: Find the Employee with a Lambda
def employee_find_2(employee_id):
    """
    Find employees with the specified employee_id using a lambda.
    """
    matches = list(filter(lambda row: int(row[employee_id_column]) == employee_id, employees["rows"]))
    return matches


# Task 7: Sort the Rows by last_name Using a Lambda
def sort_by_last_name():
    """
    Sort employees by last name.
    """
    last_name_column = column_index("last_name")
    employees["rows"].sort(key=lambda row: row[last_name_column])
    return employees["rows"]


# Call sort_by_last_name to sort the employees
sorted_employees = sort_by_last_name()
print("Sorted Employees:", employees)


# Task 8: Create a dict for an Employee
def employee_dict(row):
    """
    Create a dictionary for an employee from a row.
    """
    result = {}
    fields = employees["fields"]

    for i, field in enumerate(fields):
        if field != "employee_id":  # Skip employee_id
            result[field] = row[i]

    return result


# Test employee_dict with a sample row
print("Employee Dict:", employee_dict(employees["rows"][0]))


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


# Call all_employees_dict and print result
all_employees = all_employees_dict()
print("All Employees Dict:", all_employees)


# Task 10: Use the os Module
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
    custom_module.set_secret(new_secret)


# Call set_that_secret with a new secret
set_that_secret("my_new_secret")
print("Custom Module Secret:", custom_module.secret)


# Task 12: Read minutes1.csv and minutes2.csv
def read_csv_to_dict(filename):
    """
    Helper function to read a CSV file and return a dict with fields and rows as tuples.
    """
    result_dict = {}
    rows_list = []

    try:
        with open(filename, "r") as csv_file:
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
            stack_trace.append(f'File : {trace[0]} , Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}')
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
    minutes1 = read_csv_to_dict("../csv/minutes1.csv")
    minutes2 = read_csv_to_dict("../csv/minutes2.csv")

    return minutes1, minutes2


# Call read_minutes and store results in global variables
minutes1, minutes2 = read_minutes()
print("Minutes1:", minutes1)
print("Minutes2:", minutes2)


# Task 13: Create minutes_set
def create_minutes_set():
    """
    Create a set from the rows of minutes1 and minutes2.
    """
    set1 = set(minutes1["rows"])
    set2 = set(minutes2["rows"])

    # Combine both sets (union)
    return set1.union(set2)


# Call create_minutes_set and store in global variable
minutes_set = create_minutes_set()
print("Minutes Set:", minutes_set)


# Task 14: Convert to datetime
def create_minutes_list():
    """
    Create a list from minutes_set with datetime objects.
    """
    # Convert set to list
    minutes_list = list(minutes_set)

    # Use map to convert dates to datetime objects
    minutes_list = list(map(lambda x: (x[0], datetime.strptime(x[1], "%B %d, %Y")), minutes_list))

    return minutes_list


# Call create_minutes_list and store in global variable
minutes_list = create_minutes_list()
print("Minutes List:", minutes_list)


# Task 15: Write Out Sorted List
def write_sorted_list():
    """
    Sort minutes_list by date and write to a CSV file.
    """
    # Sort by date (second element in each tuple)
    sorted_list = sorted(minutes_list, key=lambda x: x[1])

    # Convert dates back to strings
    converted_list = list(map(lambda x: (x[0], x[1].strftime("%B %d, %Y")), sorted_list))

    try:
        with open("./minutes.csv", "w", newline='') as csv_file:
            csv_writer = csv.writer(csv_file)

            # Write headers first
            csv_writer.writerow(minutes1["fields"])

            # Write all rows
            for row in converted_list:
                csv_writer.writerow(row)

    except Exception as e:
        trace_back = traceback.extract_tb(e.__traceback__)
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append(f'File : {trace[0]} , Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}')
        print(f"Exception type: {type(e).__name__}")
        message = str(e)
        if message:
            print(f"Exception message: {message}")
        print(f"Stack trace: {stack_trace}")

    return converted_list


# Call write_sorted_list
final_sorted_list = write_sorted_list()
print("Final Sorted List:", final_sorted_list)