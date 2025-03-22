"""
Test module for employee dictionary functionality in assignment2 package.

This module tests the all_employees_dict function which should return a
dictionary containing employee records with employee IDs as keys and
attribute dictionaries as values.
"""
import assignment2_task09 as a2


def test_all_employees_dict():
    """
    Test the all_employees_dict function's structure and content accuracy.

    This test verifies:
    1. The returned dictionary contains exactly 20 employee records
    2. The dictionary contains the expected data, such as employee #9
       having "Phillip" as their first name

    The function tests both the size of the collection and the accuracy
    of individual record retrieval using string-based dictionary keys.
    """
    dict_result = a2.all_employees_dict()
    assert len(dict_result.keys()) == 20
    assert dict_result["9"]["first_name"] == "Phillip"
