"""
Test module for employee record functionality in assignment2 package.

This module contains tests that verify the employee_find function
correctly returns employee records when searching by ID.
"""
import assignment2_task05 as a2


def test_employee_find():
    """
    Test that employee_find correctly returns matching employee records.

    This test verifies:
    1. The first element in the returned record has ID "3" when searching
    for ID 3
    2. Each employee record contains exactly 4 fields (ID, first name,
    last name, position)
    """
    match = a2.employee_find(3)
    assert match[0][0] == "3"
    assert len(match[0]) == 4
