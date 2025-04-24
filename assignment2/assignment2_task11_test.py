"""
Test module for secret setting functionality in assignment2 package.

This module tests the set_that_secret function which should update a secret
value in an external custom module, demonstrating inter-module variable
modification.
"""
import custom_module
import assignment2_task11 as a2


def test_set_that_secret():
    """
    Test the set_that_secret function's ability to modify external
    module variables.

    This test verifies that the set_that_secret function correctly updates the
    'secret' variable in the custom_module when provided with a new value.
    The function should enable cross-module variable updates, which
    demonstrates Python's module import system and variable scope mechanisms.

    The specific test value "swordfish" is used to confirm exact
    string matching and proper variable assignment.
    """
    a2.set_that_secret("swordfish")
    assert custom_module.secret == "swordfish"
