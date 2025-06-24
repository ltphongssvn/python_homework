#!/usr/bin/env python3
# check_db.py
"""
Simple diagnostic script to check baseball database.

This script connects to the baseball_stats.db file, inspects its structure,
and provides a summary of its contents to help diagnose data loading issues.
"""

import os
import sqlite3
import traceback  # Import at top level for better practice


def check_database():
    """Run basic checks on the baseball database."""
    # Try multiple possible paths to find the database file
    possible_paths = [
        "baseball_stats.db",
        "assignment14/baseball_stats.db",
        os.path.join("assignment14", "baseball_stats.db"),
        "../baseball_stats.db",
    ]

    db_path = None
    for path in possible_paths:
        if os.path.exists(path):
            db_path = path
            print(f"Found database at: {path}")
            break

    if not db_path:
        print("ERROR: Could not find baseball_stats.db")
        print("Searched in these locations:")
        for path in possible_paths:
            print(f"  - {path}")
        return

    conn = None  # Initialize conn to ensure it's available in finally
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        print(f"\nSuccessfully connected to: {db_path}")

        # 1. Check SQLite version
        cursor = conn.execute("SELECT sqlite_version()")
        version = cursor.fetchone()[0]
        print(f"SQLite version: {version}")

        # 2. List all tables
        print("\n" + "=" * 50)
        print("TABLES IN DATABASE:")
        print("=" * 50)

        cursor = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' ORDER BY name"
        )
        tables = cursor.fetchall()
        if not tables:
            print("WARNING: No tables found!")
            return

        table_names = []
        for (table_name,) in tables:
            table_names.append(table_name)
            print(f"\n{table_name}:")

            # Count rows in the table
            try:
                count_cursor = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                )
                count = count_cursor.fetchone()[0]
                print(f"  Rows: {count}")
            except sqlite3.Error as e:
                print(f"  Error counting rows: {e}")

            # Get column information for the table
            try:
                info_cursor = conn.execute(f"PRAGMA table_info({table_name})")
                columns = info_cursor.fetchall()
                print("  Columns:")
                for col in columns:
                    # col[1] is column name, col[2] is data type
                    print(f"    - {col[1]} ({col[2]})")
            except sqlite3.Error as e:
                print(f"  Error getting columns: {e}")

        # 3. Check for specific baseball tables
        print("\n" + "=" * 50)
        print("CHECKING FOR EXPECTED BASEBALL TABLES:")
        print("=" * 50)
        expected_tables = [
            "al_player_review",
            "nl_player_review",
            "al_pitcher_review",
            "nl_pitcher_review",
            "al_team_standings",
            "nl_team_standings",
        ]

        # Check existence of each expected table
        for table in expected_tables:
            status = "EXISTS" if table in table_names else "MISSING"
            print(f"{table}: {status}")

        # 4. Sample data from first player table found
        print("\n" + "=" * 50)
        print("SAMPLE DATA:")
        print("=" * 50)

        # Try to get sample data from player tables
        for table in ["al_player_review", "nl_player_review"]:
            try:
                cursor = conn.execute(f"SELECT * FROM {table} LIMIT 3")
                rows = cursor.fetchall()
                if rows:
                    print(f"\nFirst 3 rows from {table}:")
                    col_names = [desc[0] for desc in cursor.description]
                    print(f"Columns: {', '.join(col_names)}")

                    # Display each row's data
                    for i, row in enumerate(rows, 1):
                        print(f"\nRow {i}:")
                        for j, value in enumerate(row):
                            if value is not None:
                                print(f"  {col_names[j]}: {value}")
                    break  # Only show data from first table that has it
            except sqlite3.Error as e:
                print(f"Could not read from {table}: {e}")

    except sqlite3.Error as e:
        # Handle database-specific errors
        print(f"Database error: {e}")
        traceback.print_exc()
    except OSError as e:
        # Handle file system errors separately
        print(f"File system error: {e}")
        traceback.print_exc()
    except KeyboardInterrupt:
        # Handle user interruption gracefully
        print("\nOperation cancelled by user.")
    finally:
        # Always close the connection if it was opened
        if conn:
            conn.close()
            print("\n" + "=" * 50)
            print("Diagnostic complete!")


if __name__ == "__main__":
    print("Baseball Database Diagnostic Tool")
    print("=" * 50)
    check_database()
