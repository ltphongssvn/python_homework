#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 3 - Data Analysis and Manipulation with Pandas.

This module performs basic operations with pandas DataFrames including
creation, manipulation, inspection, and cleaning of data.
"""
import json
import pandas as pd
import numpy as np

# Task 1: Introduction to Pandas - Creating and Manipulating DataFrames
# Create a DataFrame from a dictionary
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 35],
    'City': ['New York', 'Los Angeles', 'Chicago']
}
# Convert dictionary to DataFrame
task1_data_frame = pd.DataFrame(data)
print("Original DataFrame:")
print(task1_data_frame)

# Add a new column (Salary) to the DataFrame
task1_with_salary = task1_data_frame.copy()
task1_with_salary['Salary'] = [70000, 80000, 90000]
print("\nDataFrame with Salary column:")
print(task1_with_salary)

# Modify an existing column (increment Age by 1)
task1_older = task1_with_salary.copy()
task1_older['Age'] = task1_older['Age'] + 1
print("\nDataFrame with incremented Age:")
print(task1_older)

# Save DataFrame to CSV file
task1_older.to_csv('employees.csv', index=False)
print("\nDataFrame saved to 'employees.csv'")

# Task 2: Loading Data from CSV and JSON
# Read data from CSV file
task2_employees = pd.read_csv('employees.csv')
print("\nDataFrame loaded from CSV:")
print(task2_employees)

# Create a JSON file with additional employees
additional_employees = {
    'Name': ['Eve', 'Frank'],
    'Age': [28, 40],
    'City': ['Miami', 'Seattle'],
    'Salary': [60000, 95000]
}

# Write to JSON file
with open('additional_employees.json', 'w', encoding='utf-8') as f:
    json.dump(additional_employees, f)

# Read data from JSON file
json_employees = pd.read_json('additional_employees.json')
print("\nDataFrame loaded from JSON:")
print(json_employees)

# Combine DataFrames
more_employees = pd.concat(
    [task2_employees, json_employees],
    ignore_index=True
)
print("\nCombined DataFrame:")
print(more_employees)

# Task 3: Data Inspection - Using Head, Tail, and Info Methods
# Use head() method to get first three rows
first_three = more_employees.head(3)
print("\nFirst three rows:")
print(first_three)

# Use tail() method to get last two rows
last_two = more_employees.tail(2)
print("\nLast two rows:")
print(last_two)

# Get the shape of DataFrame
employee_shape = more_employees.shape
print("\nShape of the DataFrame:")
print(employee_shape)

# Use info() method for summary
print("\nDataFrame info:")
more_employees.info()

# Task 4: Data Cleaning
# Create a DataFrame from dirty_data.csv
dirty_data = pd.read_csv('dirty_data.csv')
print("\nDirty data loaded:")
print(dirty_data)

# Create a copy of dirty data
clean_data = dirty_data.copy()

# Remove duplicate rows
clean_data.drop_duplicates(inplace=True)
print("\nData after removing duplicates:")
print(clean_data)

# Convert Age to numeric and handle missing values
clean_data['Age'] = pd.to_numeric(clean_data['Age'], errors='coerce')
print("\nData after converting Age to numeric:")
print(clean_data)

# Convert Salary to numeric and replace placeholders with NaN
clean_data['Salary'] = clean_data['Salary'].replace(['unknown', 'n/a'], np.nan)
clean_data['Salary'] = pd.to_numeric(clean_data['Salary'], errors='coerce')
print("\nData after converting Salary to numeric:")
print(clean_data)

# Fill missing numeric values (Age with mean, Salary with median)
# Fixed the warnings by avoiding inplace=True with Series methods
clean_data['Age'] = clean_data['Age'].fillna(clean_data['Age'].mean())
clean_data['Salary'] = clean_data['Salary'].fillna(
    clean_data['Salary'].median()
)
print("\nData after filling missing values:")
print(clean_data)

# Convert Hire Date to datetime
clean_data['Hire Date'] = pd.to_datetime(
    clean_data['Hire Date'],
    errors='coerce'
)
print("\nData after converting Hire Date to datetime:")
print(clean_data)

# Strip whitespace and standardize Name and Department as uppercase
clean_data['Name'] = clean_data['Name'].str.strip().str.upper()
clean_data['Department'] = clean_data['Department'].str.strip().str.upper()
print("\nFinal clean data:")
print(clean_data)
