#!/usr/bin/env python3
"""
This module contains functions for processing minutes data from task12
and creating a set from the combined rows of minutes1 and minutes2.
"""
from assignment2_task12 import minutes1, minutes2


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

if __name__ == "__main__":
    print("Minutes Set:", minutes_set)
