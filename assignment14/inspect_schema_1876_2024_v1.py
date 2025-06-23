#!/usr/bin/env python3
# _02_inspect_schema_1876_2024_v1.py
"""
Refactored Database Schema Inspector
====================================

This program connects to the generated SQLite database and provides a clean,
well-formatted report on the schema, row counts, and sample data for each table.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class SchemaInspector:
    """Inspects and reports on the schema of a SQLite database."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            logger.error(
                f"Database file not found at: {self.db_path.resolve()}"
            )
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        self.conn = sqlite3.connect(self.db_path)
        logger.info(
            f"Successfully connected to database: {self.db_path.resolve()}"
        )

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.")

    def get_table_names(self) -> List[str]:
        """Retrieves a list of user-created table names from the database."""
        # --- FIX: Use standard sqlite_master query to exclude system tables ---
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
        cursor = self.conn.cursor()
        cursor.execute(query)
        # The result is a list of tuples, so we extract the first element of each
        tables = [table[0] for table in cursor.fetchall()]
        return sorted(tables)

    def get_table_info(self, table_name: str) -> List[Tuple]:
        """Retrieves the schema for a specific table."""
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info(`{table_name}`);")
        return cursor.fetchall()

    def get_table_row_count(self, table_name: str) -> int:
        """Gets the total number of rows in a table."""
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`;")
        return cursor.fetchone()[0]

    def get_sample_data(
        self, table_name: str, limit: int = 5
    ) -> Tuple[List[str], List[Tuple]]:
        """Retrieves sample rows from a table."""
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM `{table_name}` LIMIT {limit};")
        rows = cursor.fetchall()
        # Get column names from the cursor description
        column_names = [description[0] for description in cursor.description]
        return column_names, rows

    def generate_report(self):
        """Generates and prints a full schema and data report for all tables."""
        table_names = self.get_table_names()
        if not table_names:
            logger.warning("No user tables found in the database.")
            return

        logger.info(
            f"Found {len(table_names)} tables: {', '.join(table_names)}"
        )
        print("\n" + "=" * 80)

        for table_name in table_names:
            self._print_table_report(table_name)

    def _print_table_report(self, table_name: str):
        """Formats and prints the full report for a single table."""
        print(f"TABLE: {table_name.upper()}")
        print("-" * 80)

        # --- Schema ---
        schema = self.get_table_info(table_name)
        row_count = self.get_table_row_count(table_name)
        print(f"Schema and Information (Total Rows: {row_count:,}):")

        # --- FIX: Formatted table output for schema ---
        headers = [
            "Column ID",
            "Name",
            "Type",
            "Not Null",
            "Default",
            "Primary Key",
        ]
        col_widths = [len(h) for h in headers]

        schema_rows = []
        for col in schema:
            row = [str(item) for item in col]
            schema_rows.append(row)
            for i, item in enumerate(row):
                col_widths[i] = max(col_widths[i], len(item))

        header_str = " | ".join(
            f"{h:<{col_widths[i]}}" for i, h in enumerate(headers)
        )
        print(header_str)
        print("-" * len(header_str))

        for row in schema_rows:
            print(
                " | ".join(
                    f"{item:<{col_widths[i]}}" for i, item in enumerate(row)
                )
            )

        # --- Sample Data ---
        print("\nSample Data (First 5 Rows):")
        try:
            columns, sample_data = self.get_sample_data(table_name)
            if not sample_data:
                print("  No data in table.")
                print("=" * 80 + "\n")
                return

            # --- FIX: Dynamic column formatting for sample data ---
            data_widths = [len(str(col)) for col in columns]
            for row in sample_data:
                for i, cell in enumerate(row):
                    data_widths[i] = max(data_widths[i], len(str(cell)))

            sample_header = " | ".join(
                f"{h:<{data_widths[i]}}" for i, h in enumerate(columns)
            )
            print(sample_header)
            print("-" * len(sample_header))

            for row in sample_data:
                print(
                    " | ".join(
                        f"{str(cell):<{data_widths[i]}}"
                        for i, cell in enumerate(row)
                    )
                )

        except sqlite3.Error as e:
            print(f"  Could not retrieve sample data: {e}")

        print("=" * 80 + "\n")


def main():
    """Main function to run the schema inspector."""
    db_file = "baseball_history.db"
    logger.info(f"Attempting to inspect database: {db_file}")

    try:
        inspector = SchemaInspector(db_path=db_file)
        inspector.generate_report()
    except FileNotFoundError:
        logger.error(
            "Please ensure the database file exists and the script is in the correct directory."
        )
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if "inspector" in locals() and inspector.conn:
            inspector.close()


if __name__ == "__main__":
    main()
