"""
Test module for the get_this_value function.

Tests the environment variable retrieval functionality.
"""
import assignment2_task10 as a2


def test_get_this_value():
    """
    Test that the get_this_value function correctly retrieves the THISVALUE
    environment variable.

    This test verifies that the function returns "ABC" when the THISVALUE
    environment variable is set to "ABC".
    """
    assert a2.get_this_value() == "ABC"
