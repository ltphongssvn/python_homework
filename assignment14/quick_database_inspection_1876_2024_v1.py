#!/usr/bin/env python3
# quick_database_inspection_1876_2024_v1.py
"""
Provides utilities for inspecting the baseball_history SQLite database,
including listing tables, schemas, and sample records.
"""
import sqlite3

import pandas as pd

# edit path if necessary:
conn = sqlite3.connect("baseball_history.db")

tables = pd.read_sql_query(
    'SELECT name FROM sqlite_master WHERE type="table";', conn
)
print("Available tables:", tables["name"].tolist())

for table in tables["name"]:
    print(f"\n--- {table} ---")
    schema = pd.read_sql_query(f"PRAGMA table_info({table});", conn)
    print("Columns:", schema["name"].tolist())
    sample = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 3;", conn)
    print("Sample data shape:", sample.shape)

conn.close()
