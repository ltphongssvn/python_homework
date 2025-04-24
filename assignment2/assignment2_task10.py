"""
Assignment 2: Task 10 - Working with environment variables using the os module.
This module demonstrates how to access environment variables in Python.
"""
import os


def get_this_value():
    """
    Get the value of the THISVALUE environment variable.
    """
    return os.getenv("THISVALUE")
