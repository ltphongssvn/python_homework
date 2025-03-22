"""
Test module for minute reading functionality in assignment2 package.

This module tests the read_minutes function which should retrieve meeting
records from two different sources and return them as structured dictionaries
containing attendee information and dates.
"""
import assignment2_task12 as a2


def test_read_minutes():
    """
    Test the read_minutes function's data retrieval and structure.

    This test verifies:
    1. The first dictionary contains the expected data for Tony Henderson
       at index 1
    2. The second dictionary contains the expected data for Sarah Murray
       at index 2
    3. The minutes1 variable in the assignment2 module is properly initialized

    Each dictionary should contain a 'rows' key with tuples of person names
    and meeting dates as values.
    """
    d1, d2 = a2.read_minutes()
    assert d1["rows"][1] == ("Tony Henderson", "November 15, 1991")
    assert d2["rows"][2] == ("Sarah Murray", "November 19, 1988")
    assert a2.minutes1 is not None
