"""Test module for verifying the employee data reading functionality.

This module contains tests that validate the employee data structure
returned by the read_employees function from the assignment2 module.
It verifies the structure of the data and confirms key fields exist.
"""

import assignment2_task02 as a2


def test_read_employees():
    """Verify the read_employees function returns properly structured data.

    This test ensures that:
    1. The function returns a non-None value
    2. The module-level employees variable is populated
    3. The data contains exactly 4 field columns
    4. The second field is named "first_name"
    5. The data contains exactly 20 rows of employee records
    """
    employees = a2.read_employees()
    assert employees is not None
    assert a2.employees is not None
    assert len(a2.employees["fields"]) == 4
    assert a2.employees["fields"][1] == "first_name"
    assert len(a2.employees["rows"]) == 20
