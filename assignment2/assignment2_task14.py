#!/usr/bin/env python3
"""
This module converts meeting minutes data to datetime objects.
It transforms the string dates from minutes_set into Python datetime objects
and creates a list of tuples with meeting information.
"""
from datetime import datetime
from assignment2_task13 import minutes_set


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


# Call create_minutes_list and store in global variable
minutes_list = create_minutes_list()
print("Minutes List:", minutes_list)
