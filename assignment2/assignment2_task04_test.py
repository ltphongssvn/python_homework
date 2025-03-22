"""
Test module for assignment2 package's name handling functionality.

This module contains tests to verify the first_name function works correctly
by checking if it returns expected values for specific indices.
"""
import assignment2_task04 as a2


def test_first_name():
    """Test that the first_name function returns correct values.

    Verifies that the function returns either 'David' or 'Lauren' when
    provided with index 2, which should hold true both before and after
    sorting operations.
    """
    assert a2.first_name(2) in ("David", "Lauren")
    # values before and after sort
