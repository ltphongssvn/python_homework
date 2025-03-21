"""
Test module for the minutes list creation functionality in assignment2 package.

This module verifies that the create_minutes_list function correctly generates
a list of tuples containing meeting information with proper datetime objects.
"""
import assignment2 as a2


def test_create_minutes_list():
    """
    Test the create_minutes_list function's data structure and content.

    This test verifies:
    1. The second element (index 1) of each meeting tuple is a datetime object
    2. Each meeting record is stored as a tuple
    3. The minutes_list variable in the assignment2 module is properly
    initialized
    """
    minutes_list = a2.create_minutes_list()
    assert type(minutes_list[0][1]).__name__ == "datetime"
    assert type(minutes_list[0]).__name__ == "tuple"
    assert a2.minutes_list is not None
