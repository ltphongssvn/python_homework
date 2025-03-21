"""
Test module for the sorting and file output functionality
in assignment2 package.

This module tests the write_sorted_list function which should create
a sorted CSV file and return the sorted content as a list of tuples.
"""
import os
import assignment2_task15 as a2


def test_write_sorted_list():
    """
    Test the write_sorted_list function's output and file creation
    capabilities.

    This test verifies:
    1. The first element in the sorted list contains the expected name and date
    2. The function creates a 'minutes.csv' file in the current directory
    """
    sorted_list = a2.write_sorted_list()
    assert sorted_list[0] == ("Jason Tucker", "September 20, 1980")
    assert os.access("./minutes.csv", os.F_OK) is True
