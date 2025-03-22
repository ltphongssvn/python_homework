#!/usr/bin/env python3
"""
Module for interacting with custom_module to set and retrieve secrets.
This module demonstrates how to work with custom Python modules.
"""
import custom_module


# Task 11: Creating Your Own Module
def set_that_secret(new_secret):
    """
    Set the secret in the custom_module.
    """
    custom_module.set_secret(new_secret)


if __name__ == "__main__":
    # Call set_that_secret with a new secret
    set_that_secret("my_new_secret")
    print("Custom Module Secret:", custom_module.secret)
