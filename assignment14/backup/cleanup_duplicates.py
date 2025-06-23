#!/usr/bin/env python3
# cleanup_duplicates.py
"""
Database Cleanup Script for Baseball Statistics
===============================================
This script identifies and removes duplicate rows from the database,
ensuring data integrity before analysis.
"""

import os
import shutil
import sqlite3
import traceback
from datetime import datetime

# --- Centralized Configuration ---
# This configuration correctly finds the database whether the script is run
# from 'python_homework/' or 'python_homework/assignment14/'.
DB_NAME = "baseball_stats.db"
DB_FILE_IN_PARENT = os.path.join("assignment14", DB_NAME)
DB_FILE = DB_FILE_IN_PARENT if os.path.exists(DB_FILE_IN_PARENT) else DB_NAME
# --- End of Configuration ---


class DatabaseCleaner:
    """
    Handles the connection, analysis, and cleaning of the baseball database.
    """

    def __init__(self, db_path):
        """Initialize the cleaner with the database path."""
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            print(f"Connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            return False

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            print("\nDatabase connection closed.")

    def _get_columns_for_partition(self, table_name):
        """
        Get all column names for a table, excluding the primary key and timestamp.
        This is used to identify true content duplicates.
        """
        cursor = self.conn.execute(f"PRAGMA table_info({table_name})")
        return [
            row["name"]
            for row in cursor.fetchall()
            if row["name"] not in ("id", "created_at")
        ]

    def remove_duplicates(self, dry_run=True):
        """
        Remove rows that are perfect duplicates across all meaningful columns.
        """
        mode = "(DRY RUN)" if dry_run else "(LIVE RUN)"
        print("\n" + "=" * 60)
        print(f"ANALYZING AND REMOVING DUPLICATE ROWS {mode}")
        print("=" * 60)

        tables = [
            "al_player_review",
            "nl_player_review",
            "al_pitcher_review",
            "nl_pitcher_review",
            "al_team_standings",
            "nl_team_standings",
        ]
        total_removed = 0

        for table in tables:
            print(f"\nProcessing table: {table}")
            try:
                partition_cols = self._get_columns_for_partition(table)
                if not partition_cols:
                    print(f"  Could not get columns for {table}. Skipping.")
                    continue

                partition_clause = ", ".join(
                    f'"{col}"' for col in partition_cols
                )

                find_dupes_query = f"""
                SELECT id
                FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY {partition_clause}
                               ORDER BY id
                           ) as rn
                    FROM {table}
                ) t
                WHERE t.rn > 1;
                """

                cursor = self.conn.execute(find_dupes_query)
                ids_to_delete = [row["id"] for row in cursor.fetchall()]

                if not ids_to_delete:
                    print("  No duplicate rows found.")
                    continue

                if dry_run:
                    print(
                        f"  Would delete {len(ids_to_delete)} duplicate rows."
                    )
                    total_removed += len(ids_to_delete)
                else:
                    placeholders = ",".join("?" for _ in ids_to_delete)
                    delete_query = (
                        f"DELETE FROM {table} WHERE id IN ({placeholders})"
                    )

                    delete_cursor = self.conn.execute(
                        delete_query, ids_to_delete
                    )
                    rows_deleted = delete_cursor.rowcount
                    print(f"  Deleted {rows_deleted} duplicate rows.")
                    total_removed += rows_deleted

            except sqlite3.Error as e:
                print(
                    f"  An error occurred while processing table {table}: {e}"
                )

        if not dry_run:
            self.conn.commit()
            print(f"\nTotal rows removed: {total_removed}")
            print("Database changes committed.")
        else:
            print(f"\nTotal rows that would be removed: {total_removed}")
            print("No changes were made to the database.")

        return total_removed

    def backup_database(self):
        """Create a backup of the database before making changes."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{self.db_path}.backup_{timestamp}"
        print(f"\nCreating backup: {backup_path}")

        # Close current connection to ensure file is not locked
        self.close()

        try:
            shutil.copy2(self.db_path, backup_path)
            print("Backup created successfully!")
            # Reconnect after backup
            return self.connect()
        except OSError as e:
            print(f"Error creating backup: {e}")
            return False


def main():
    """Main function to run the cleanup process."""
    print("Baseball Statistics Database Cleanup")
    print("=" * 60)

    cleaner = DatabaseCleaner(DB_FILE)

    if not cleaner.connect():
        return

    try:
        # Step 1: Preview what would be removed
        duplicates_to_remove = cleaner.remove_duplicates(dry_run=True)

        if duplicates_to_remove > 0:
            print(f"\nTotal duplicates found: {duplicates_to_remove}")

            backup_prompt = "\nCreate a backup before proceeding? (yes/no): "
            backup_response = input(backup_prompt)
            if backup_response.lower() == "yes":
                if not cleaner.backup_database():
                    print("Failed to create backup. Aborting cleanup.")
                    return

            cleanup_prompt = (
                "\nDo you want to proceed with the cleanup? (yes/no): "
            )
            response = input(cleanup_prompt)
            if response.lower() == "yes":
                print("\nPerforming actual cleanup...")
                cleaner.remove_duplicates(dry_run=False)
                print("\nCleanup completed successfully!")
            else:
                print("\nCleanup cancelled. No changes were made.")
        else:
            print("\nNo duplicates found. Database is clean!")

    except (sqlite3.Error, OSError) as e:
        print(f"\nAn error occurred: {e}")
        traceback.print_exc()
    finally:
        cleaner.close()


if __name__ == "__main__":
    main()
