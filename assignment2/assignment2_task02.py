"""Module for reading employees from csv file and
returning the data as dictionary."""
import csv
import traceback
from typing import Dict, List, Any


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


employees = read_employees()

if __name__ == "__main__":
    print(employees)
