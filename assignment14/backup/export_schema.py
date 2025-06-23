#!/usr/bin/env python3
# export_schema.py
"""
Database Schema Exporter
========================

This script connects to a SQLite database, extracts the CREATE TABLE
statements for all tables, and saves them to a formatted SQL file.
"""

import os
import sqlite3

# --- Centralized Configuration ---
# This configuration correctly finds the database whether the script is run
# from 'python_homework/' or 'python_homework/assignment14/'.
DB_NAME = "baseball_stats.db"
DB_FILE_IN_PARENT = os.path.join("assignment14", DB_NAME)
DB_FILE = DB_FILE_IN_PARENT if os.path.exists(DB_FILE_IN_PARENT) else DB_NAME

OUTPUT_SQL_FILE = "baseball_schema.sql"
# --- End of Configuration ---


def export_schema():
    """
    Connects to the database, exports the schema, and saves it to a file.
    """
    conn = None  # Initialize conn to None
    try:
        # Establish database connection with proper error handling
        if not os.path.exists(DB_FILE):
            print(f"Error: Database '{DB_FILE}' not found.")
            print("Please run the database_import.py script first.")
            return

        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        print(f"Successfully connected to database: {DB_FILE}")

        # Query to get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # Open output file with explicit UTF-8 encoding
        with open(OUTPUT_SQL_FILE, "w", encoding="utf-8") as f:
            for table_tuple in tables:
                table_name = table_tuple[0]

                # Get the CREATE statement for each table
                # The query is safe as table_name is from a trusted source (the DB itself)
                query = (
                    "SELECT sql FROM sqlite_master WHERE "
                    f"type='table' AND name='{table_name}';"
                )
                cursor.execute(query)
                create_statement = cursor.fetchone()[0]

                # Write to file with nice formatting
                f.write(f"-- Table: {table_name}\n")
                f.write(f"{create_statement};\n\n")

        print(f"Schema exported to {OUTPUT_SQL_FILE}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    export_schema()
