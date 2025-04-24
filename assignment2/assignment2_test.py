"""
Test module for assignment2.py.

This module contains unit tests for all functions in the assignment2 module,
verifying their functionality against expected outputs.
"""
import os
import custom_module
import assignment2 as a2


def test_read_employees():
    """Test the read_employees function and verify employee data structure."""
    employees = a2.read_employees()
    assert employees is not None
    assert a2.employees is not None
    assert len(a2.employees["fields"]) == 4
    assert a2.employees["fields"][1] == "first_name"
    assert len(a2.employees["rows"]) == 20


def test_column_name():
    """Test the column_index function and employee_id_column
    global variable."""
    assert a2.column_index("last_name") == 2
    assert a2.employee_id_column is not None


def test_first_name():
    """Test the first_name function retrieves correct employee first names."""
    assert a2.first_name(2) in ("David", "Lauren")


def test_employee_find():
    """Test the employee_find function locates employees by ID."""
    match = a2.employee_find(3)
    assert match[0][0] == "3"
    assert len(match[0]) == 4


def test_employee_find_2():
    """Test the employee_find_2 function (lambda version) locates
      employees by ID."""
    match = a2.employee_find_2(4)
    assert match[0][0] == "4"
    assert len(match[0]) == 4


def test_sort_by_last_name():
    """Test the sort_by_last_name function properly sorts employee rows."""
    rows = a2.sort_by_last_name()
    assert len(rows) == 20
    assert rows[0][2] == "Bowman"


def test_employee_dict():
    """Test the employee_dict function creates dictionaries
      without employee_id."""
    dict_result = a2.employee_dict(a2.employees["rows"][0])
    assert dict_result["last_name"] == "Bowman"
    assert "employee_id" not in dict_result


def test_all_employees_dict():
    """Test the all_employees_dict function creates a dictionary
      of all employees."""
    dict_result = a2.all_employees_dict()
    assert len(dict_result.keys()) == 20
    assert dict_result["9"]["first_name"] == "Phillip"


def test_get_this_value():
    """Test the get_this_value function retrieves environment variable."""
    assert a2.get_this_value() == "ABC"


def test_set_that_secret():
    """Test the set_that_secret function updates custom_module's secret."""
    a2.set_that_secret("swordfish")
    assert custom_module.secret == "swordfish"


def test_read_minutes():
    """Test the read_minutes function loads meeting minutes correctly."""
    d1, d2 = a2.read_minutes()
    assert d1["rows"][1] == ("Tony Henderson", "November 15, 1991")
    assert d2["rows"][2] == ("Sarah Murray", "November 19, 1988")
    assert a2.minutes1 is not None


def test_create_minutes_set():
    """Test the create_minutes_set function creates a set of
      unique meetings."""
    minutes_set = a2.create_minutes_set()
    assert type(minutes_set).__name__ == "set"
    assert len(minutes_set) == 46
    assert a2.minutes_set is not None


def test_create_minutes_list():
    """Test the create_minutes_list function converts dates to
      datetime objects."""
    minutes_list = a2.create_minutes_list()
    assert type(minutes_list[0][1]).__name__ == "datetime"
    assert type(minutes_list[0]).__name__ == "tuple"
    assert a2.minutes_list is not None


def test_write_sorted_list():
    """Test the write_sorted_list function writes sorted meetings to a file."""
    sorted_list = a2.write_sorted_list()
    assert sorted_list[0] == ("Jason Tucker", "September 20, 1980")
    assert os.access("./minutes.csv", os.F_OK) is True
