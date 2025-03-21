"""Test module for the employee_find_2 function in assignment2_task06."""
import assignment2_task06 as a2


def test_employee_find_2():
    """Test that employee_find_2 correctly finds an employee by ID."""
    match = a2.employee_find_2(4)
    assert match[0][0] == "4"
    assert len(match[0]) == 4
