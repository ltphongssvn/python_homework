#!/usr/bin/env python3
# explore_data.py
import sqlite3

import pandas as pd


def explore_database():
    """Explore the baseball database structure and content"""
    conn = sqlite3.connect("../baseball_history.db")

    # Get all table names
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table';", conn
    )
    print("Available tables:", tables["name"].tolist())

    # Explore each table structure
    for table in tables["name"]:
        print(f"\n--- {table.upper()} TABLE ---")
        schema = pd.read_sql_query(f"PRAGMA table_info({table});", conn)
        print("Columns:", schema[["name", "type"]].to_string(index=False))

        # Sample data
        sample = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 5;", conn)
        print("Sample data:")
        print(sample.to_string(index=False))

    conn.close()


if __name__ == "__main__":
    explore_database()
