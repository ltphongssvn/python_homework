"""
This script connects to a SQLite database (baseball_stats.db) and runs a
series of predefined SQL queries to display schema information, table contents,
and data summaries in a formatted, human-readable way in the terminal.
"""

import os
import sqlite3


def print_results(cursor, title):
    """
    Helper function to execute a query and print the results in a formatted table.
    This function handles fetching column names and dynamically adjusting column
    widths for clean output.
    """
    print(f"\n{'=' * 5} {title} {'=' * 5}")

    try:
        rows = cursor.fetchall()

        # Get column names from the cursor description, if available.
        if cursor.description:
            column_names = [desc[0] for desc in cursor.description]
        else:
            column_names = []

        if not rows:
            print("No results found for this query.")
            return

        # Calculate column widths, starting with header lengths.
        column_widths = [len(name) for name in column_names]

        # Check each cell to find the max width required for each column.
        for row in rows:
            for i, cell in enumerate(row):
                # Ensure we handle None values gracefully
                cell_str = str(cell) if cell is not None else "NULL"
                cell_width = len(cell_str)
                if cell_width > column_widths[i]:
                    column_widths[i] = cell_width

        # Print table header.
        header_parts = [
            name.ljust(column_widths[i]) for i, name in enumerate(column_names)
        ]
        header_line = " | ".join(header_parts)
        separator_line = "-+-".join(["-" * width for width in column_widths])
        print(header_line)
        print(separator_line)

        # Print each row of data.
        for row in rows:
            row_parts = []
            for i, cell in enumerate(row):
                # Ensure None is printed as 'NULL' and aligned correctly.
                formatted_cell = str(cell) if cell is not None else "NULL"
                row_parts.append(formatted_cell.ljust(column_widths[i]))
            row_line = " | ".join(row_parts)
            print(row_line)

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")


def main():
    """
    Main function to connect to the database, run queries, and display results.
    """
    db_file = "baseball_stats.db"

    if not os.path.exists(db_file):
        print(f"Error: Database file '{db_file}' not found.")
        print(
            "Please make sure the script is in the same directory as the "
            "database file."
        )
        return

    connection = None
    try:
        connection = sqlite3.connect(db_file)
        cursor = connection.cursor()
        print(f"Successfully connected to database: {db_file}")

        # A list of queries to execute, each with a descriptive title.
        queries_to_run = [
            # --- Overall Database Structure ---
            {
                "title": "All user tables and their creation statements",
                "sql": """
                    SELECT name, sql
                    FROM sqlite_master
                    WHERE type='table' AND name NOT LIKE 'sqlite_%'
                    ORDER BY name;
                """,
            },
            {
                "title": "Internal SQLite tables and their creation statements",
                "sql": """
                    SELECT name, sql
                    FROM sqlite_master
                    WHERE type='table' AND name LIKE 'sqlite_%'
                    ORDER BY name;
                """,
            },
            {
                "title": "All indexes in the database",
                "sql": """
                    SELECT name, tbl_name, sql
                    FROM sqlite_master
                    WHERE type='index'
                    ORDER BY tbl_name, name;
                """,
            },
            # --- Table Record Counts ---
            {
                "title": "Count records in each table",
                "sql": """
                    SELECT 'baseball_stats' as table_name, COUNT(*) as row_count
                    FROM baseball_stats
                    UNION ALL
                    SELECT 'team_standings', COUNT(*) FROM team_standings
                    UNION ALL
                    SELECT 'import_history', COUNT(*) FROM import_history;
                """,
            },
            # --- Detailed Schema for Each Table ---
            {
                "title": "Schema for 'baseball_stats'",
                "sql": "PRAGMA table_info(baseball_stats);",
            },
            {
                "title": "Schema for 'team_standings'",
                "sql": "PRAGMA table_info(team_standings);",
            },
            {
                "title": "Schema for 'import_history'",
                "sql": "PRAGMA table_info(import_history);",
            },
            {
                "title": "Schema for 'sqlite_sequence'",
                "sql": "PRAGMA table_info(sqlite_sequence);",
            },
            # --- Data Exploration Queries ---
            {
                "title": "Distinct years in 'baseball_stats'",
                "sql": "SELECT DISTINCT year FROM baseball_stats ORDER BY year DESC;",
            },
            {
                "title": "Sample of statistic types in 'baseball_stats'",
                "sql": "SELECT DISTINCT statistic FROM baseball_stats LIMIT 10;",
            },
            {
                "title": "Most Recent Team Standings (Top 10)",
                "sql": (
                    "SELECT * FROM team_standings "
                    "ORDER BY year DESC, created_at DESC LIMIT 10;"
                ),
            },
            {
                "title": "Most Recent Import History (Top 10)",
                "sql": (
                    "SELECT * FROM import_history "
                    "ORDER BY import_date DESC LIMIT 10;"
                ),
            },
            {
                "title": "AUTOINCREMENT Sequence Values",
                "sql": "SELECT * FROM sqlite_sequence;",
            },
        ]

        # Loop, execute each query, and print its results.
        for query_info in queries_to_run:
            cursor.execute(query_info["sql"])
            print_results(cursor, query_info["title"])

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if connection:
            connection.close()
            print("\nDatabase connection closed.")


# Ensures the main() function is called when the script is executed.
if __name__ == "__main__":
    main()
