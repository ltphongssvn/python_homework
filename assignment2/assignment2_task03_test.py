"""
Test module for the assignment2 package's column indexing functionality.

This module contains tests to verify that the column_index function and
employee_id_column variable work as expected.
"""
import assignment2_task03 as a2


def test_column_name():
    """Test the column_index function returns correct indices and verify
    employee_id_column value.

    This test verifies:
    1. The column_index function correctly returns 2 for the "last_name" column
    2. The employee_id_column variable is properly initialized (not None)
    """
    assert a2.column_index("last_name") == 2
    assert a2.employee_id_column is not None
