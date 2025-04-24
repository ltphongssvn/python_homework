"""Test module for verifying the functionality of sort_by_last_name
in assignment2_task07."""
import assignment2_task07 as a2


def test_sort_by_last_name():
    """Test that sort_by_last_name properly sorts employees and returns
    the expected data."""
    rows = a2.sort_by_last_name()
    assert len(rows) == 20
    assert rows[0][2] == "Bowman"
