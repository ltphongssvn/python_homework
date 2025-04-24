"""Test module for the employee_dict function from assignment2_task08."""
import assignment2_task08 as a2


def test_employee_dict():
    """
    Test that employee_dict correctly creates a dictionary from employee data
    and properly excludes the employee_id field.
    """
    dict_result = a2.employee_dict(a2.employees["rows"][0])
    assert dict_result["last_name"] == "Wade"
    assert "employee_id" not in dict_result
