#!/usr/bin/env python3
# examine_csv.py
"""
Examine CSV File Structure
==========================

A diagnostic script to analyze the structure, encoding, and delimiter of CSV
files to help troubleshoot data import issues.
"""

import os

import pandas as pd


def examine_csv(filepath):
    """
    Examine a single CSV file's structure, encoding, and content in detail.

    Args:
        filepath (str): The full path to the CSV file to examine.
    """
    print(f"\nExamining: {filepath}")
    print("=" * 60)

    # Try reading with different parameters to diagnose issues
    try:
        # Standard read
        df = pd.read_csv(filepath, nrows=10)
        print("Standard read successful")
        print(f"Columns: {list(df.columns)}")
        if not df.empty:
            # Reformat long line for readability
            first_row_dict = df.iloc[0].to_dict()
            print(f"First row data: {first_row_dict}")
        else:
            print("First row data: No data")
    except (pd.errors.ParserError, UnicodeDecodeError, IOError) as e:
        print(f"Standard read failed: {e}")

    try:
        # Try with a different encoding as a fallback
        pd.read_csv(filepath, encoding="latin-1", nrows=10)
        print("\nLatin-1 encoding successful")
    except (pd.errors.ParserError, UnicodeDecodeError, IOError):
        # This is expected to fail sometimes, so we can pass silently
        pass

    try:
        # Check if the file might be tab-delimited instead of comma-delimited
        df = pd.read_csv(filepath, sep="\t", nrows=10)
        if len(df.columns) > 1:
            print("\nFile might be tab-delimited")
            print(f"Tab-delimited columns: {list(df.columns)}")
    except (pd.errors.ParserError, UnicodeDecodeError, IOError):
        # This is also expected to fail if it's not tab-delimited
        pass

    # Show raw content for manual inspection
    print("\nFirst 5 lines of raw file content:")
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            for i in range(5):
                line = f.readline()
                # Use repr() to show hidden characters like '\t' or '\r'
                print(f"Line {i+1}: {repr(line)}")
    except (IOError, UnicodeDecodeError) as e:
        print(f"Could not read raw file content: {e}")


def main():
    """
    Main function to find and examine all CSV files in the data directory.
    """
    # Use uppercase for constants as per PEP 8
    DATA_DIR = "baseball_data"

    if not os.path.isdir(DATA_DIR):
        print(f"Error: Data directory '{DATA_DIR}' not found.")
        print(
            "Please ensure you are running this from the 'assignment14' directory."
        )
        return

    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".csv"):
            examine_csv(os.path.join(DATA_DIR, filename))


if __name__ == "__main__":
    main()
