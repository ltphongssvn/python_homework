#!/usr/bin/env python3
# database_import.py
"""
Database Importer for Baseball Statistics
=========================================

This script handles the import of CSV files containing baseball statistics
into a structured SQLite database. It includes robust data cleaning,
transformation, and validation to ensure data integrity.
"""

import logging
import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

# --- Centralized Configuration ---
# The database and log files will be created in the current working directory.
# The data will be read from a 'baseball_data' subdirectory.
DATA_FOLDER_NAME = "baseball_data"
DB_NAME = "baseball_stats.db"

# Ensure the data directory exists in the current working directory
os.makedirs(DATA_FOLDER_NAME, exist_ok=True)
# --- End of Configuration ---

# Set up logging for debugging and error tracking
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            "database_import.log",
            mode="w",
            encoding="utf-8",
        ),
        logging.StreamHandler(sys.stdout),
    ],
)


class BaseballDatabaseImporter:
    """
    A class to handle importing baseball statistics CSV files into a SQLite database.
    This follows best practices for data validation, error handling, and logging.
    """

    def __init__(
        self, db_path: str = DB_NAME, data_folder: str = DATA_FOLDER_NAME
    ):
        """
        Initialize the importer with database and data folder paths.
        """
        self.db_path = db_path
        self.data_folder = Path(data_folder)
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None

        logging.info("Current working directory: %s", os.getcwd())
        logging.info("Data folder path: %s", self.data_folder.absolute())

        # Define the expected CSV files and their corresponding table names
        self.csv_mappings = {
            "Yearly_American_League_Player_Review.csv": "al_player_review",
            "Yearly_American_League_Pitcher_Review.csv": "al_pitcher_review",
            "Yearly_American_League_Team_Standings.csv": "al_team_standings",
            "Yearly_National_League_Player_Review.csv": "nl_player_review",
            "Yearly_National_League_Pitcher_Review.csv": "nl_pitcher_review",
            "Yearly_National_League_Team_Standings.csv": "nl_team_standings",
        }

        # A single, flexible schema for player/pitcher data
        self.review_schema = {
            "year": "INTEGER",
            "league": "TEXT",
            "player": "TEXT",
            "team": "TEXT",
            "pos": "TEXT",
            "g": "INTEGER",
            "ab": "INTEGER",
            "r": "INTEGER",
            "h": "INTEGER",
            "2b": "INTEGER",
            "3b": "INTEGER",
            "hr": "INTEGER",
            "rbi": "INTEGER",
            "bb": "INTEGER",
            "so": "INTEGER",
            "sb": "INTEGER",
            "cs": "INTEGER",
            "avg": "REAL",
            "obp": "REAL",
            "slg": "REAL",
            "w": "INTEGER",
            "l": "INTEGER",
            "era": "REAL",
            "sv": "INTEGER",
            "sho": "INTEGER",
            "ip": "REAL",
            "er": "INTEGER",
            "k": "INTEGER",
            "player_link": "TEXT",
        }

        # A specific schema for the transformed standings data
        self.standings_schema = {
            "year": "INTEGER",
            "league": "TEXT",
            "division": "TEXT",
            "team": "TEXT",
            "w": "INTEGER",
            "l": "INTEGER",
            "wp": "REAL",
            "gb": "TEXT",
        }

    def find_csv_files(self) -> Dict[str, Path]:
        """Locate CSV files, checking multiple possible locations."""
        found_files = {}
        # Prioritize the defined data_folder
        search_paths = [self.data_folder, Path.cwd()]
        logging.info(
            "Searching for CSV files in: %s", [str(p) for p in search_paths]
        )

        for csv_file in self.csv_mappings:
            for search_path in search_paths:
                file_path = search_path / csv_file
                if file_path.exists():
                    found_files[csv_file] = file_path
                    logging.info(
                        "[FOUND] %s at: %s", csv_file, file_path.absolute()
                    )
                    break
            else:
                logging.warning("[NOT FOUND] Could not find %s", csv_file)
        return found_files

    def connect_database(self) -> bool:
        """Establish connection to the SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            self.cursor.execute("PRAGMA foreign_keys = ON")
            self.cursor.execute("PRAGMA synchronous = NORMAL")
            self.cursor.execute("PRAGMA journal_mode = WAL")
            logging.info(
                "Successfully connected to database: %s", self.db_path
            )
            return True
        except sqlite3.Error as e:
            logging.error("Error connecting to database: %s", e)
            return False

    def close_database(self):
        """Close the database connection safely."""
        if self.conn:
            try:
                self.conn.commit()
                self.conn.close()
                logging.info("Database connection closed")
            except sqlite3.Error as e:
                logging.error("Error closing database: %s", e)

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean dataframe by handling duplicates, NaNs, and column names."""
        logging.info("Initial dataframe shape: %s", df.shape)
        df = df.dropna(how="all").drop_duplicates()
        df.columns = pd.Index([col.lower().strip() for col in df.columns])
        df = df.loc[:, ~df.columns.str.contains("unnamed", case=False)]
        logging.info("Cleaned dataframe shape: %s", df.shape)
        return df

    def transform_standings_data(
        self, df: pd.DataFrame, filename: str
    ) -> pd.DataFrame:
        """Transform team standings data to handle the division structure."""
        transformed_data = []
        current_division = None
        year_match = re.search(r"(\d{4})", filename)
        year = int(year_match.group(1)) if year_match else None
        league = (
            "American League" if "American" in filename else "National League"
        )

        for _, row in df.iterrows():
            team_value = str(row.get("team", "")).strip()
            if not team_value:
                continue

            has_stats = any(
                stat_col in row and pd.notna(row[stat_col])
                for stat_col in ["w", "W"]
            )
            if (
                any(div in team_value for div in ["East", "Central", "West"])
                and not has_stats
            ):
                current_division = team_value
                continue

            wins = next(
                (row[c] for c in ["w", "W"] if c in row and pd.notna(row[c])),
                None,
            )
            losses = next(
                (row[c] for c in ["l", "L"] if c in row and pd.notna(row[c])),
                None,
            )

            if wins is not None and losses is not None:
                win_pct = next(
                    (
                        row[c]
                        for c in ["wp", "WP", "pct"]
                        if c in row and pd.notna(row[c])
                    ),
                    None,
                )
                gb = next(
                    (
                        str(row[c]).replace("-", "0")
                        for c in ["gb", "GB"]
                        if c in row and pd.notna(row[c])
                    ),
                    "0",
                )
                transformed_data.append(
                    {
                        "year": year,
                        "league": league,
                        "division": current_division or "Unknown",
                        "team": team_value,
                        "w": wins,
                        "l": losses,
                        "wp": win_pct,
                        "gb": gb,
                    }
                )

        return pd.DataFrame(transformed_data)

    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """Create a table in the database with the provided schema."""
        if not self.cursor or not self.conn:
            return False
        try:
            columns_def = [
                f'"{col}" {sql_type}' for col, sql_type in schema.items()
            ]
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            create_query = (
                f"CREATE TABLE {table_name} ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                f"{', '.join(columns_def)}, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            self.cursor.execute(create_query)
            if "year" in schema:
                self.cursor.execute(
                    f"CREATE INDEX idx_{table_name}_year ON {table_name}(year)"
                )
            if "team" in schema:
                self.cursor.execute(
                    f"CREATE INDEX idx_{table_name}_team ON {table_name}(team)"
                )
            self.conn.commit()
            logging.info("Created table: %s", table_name)
            return True
        except sqlite3.Error as e:
            logging.error("Error creating table %s: %s", table_name, e)
            return False

    def import_csv_to_table(
        self, csv_file: str, file_path: Path, table_name: str
    ) -> bool:
        """Import a single CSV file to its corresponding table."""
        try:
            logging.info(
                "Starting import of %s to table %s", csv_file, table_name
            )
            df = pd.read_csv(
                file_path, encoding="utf-8-sig", on_bad_lines="warn"
            )
            df_cleaned = self.clean_dataframe(df)

            is_standings = "standings" in table_name.lower()
            schema = (
                self.standings_schema if is_standings else self.review_schema
            )

            df_final = (
                self.transform_standings_data(df_cleaned, csv_file)
                if is_standings
                else df_cleaned
            ).copy()

            for col in schema:
                if col not in df_final.columns:
                    df_final[col] = None
            df_final = df_final[list(schema.keys())].copy()

            if not self.create_table(table_name, schema):
                return False

            for col, dtype in schema.items():
                if col in df_final:
                    if dtype == "INTEGER":
                        df_final.loc[:, col] = (
                            pd.to_numeric(df_final[col], errors="coerce")
                            .fillna(0)
                            .astype("Int64")
                        )
                    elif dtype == "REAL":
                        df_final.loc[:, col] = pd.to_numeric(
                            df_final[col], errors="coerce"
                        )
                    elif dtype == "TEXT":
                        df_final.loc[:, col] = (
                            df_final[col].astype(str).replace("nan", "")
                        )

            if self.conn:
                df_final.to_sql(
                    table_name,
                    self.conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                self.conn.commit()

            if self.cursor:
                count = self.cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                logging.info(
                    "Successfully imported %d records to %s", count, table_name
                )
            return True
        except (IOError, pd.errors.ParserError, sqlite3.Error) as e:
            logging.error(
                "Error importing %s: %s: %s", csv_file, type(e).__name__, e
            )
            if self.conn:
                self.conn.rollback()
            return False

    def run_import(self) -> bool:
        """Main method to run the complete import process."""
        logging.info(
            "=" * 60
            + "\nStarting baseball database import at %s\n"
            + "=" * 60,
            datetime.now(),
        )
        if not self.connect_database():
            return False

        found_files = self.find_csv_files()
        if not found_files:
            logging.error(
                "No CSV files found! Please check the '%s' directory.",
                DATA_FOLDER_NAME,
            )
            return False

        success_count = 0
        for csv_file, table_name in self.csv_mappings.items():
            if csv_file in found_files:
                logging.info("\nProcessing file: %s", csv_file)
                if self.import_csv_to_table(
                    csv_file, found_files[csv_file], table_name
                ):
                    success_count += 1
            else:
                logging.warning("Skipping %s - file not found", csv_file)

        self.generate_summary_report()
        self.close_database()

        logging.info("=" * 60)
        logging.info(
            "Import complete: %d/%d files imported successfully",
            success_count,
            len(found_files),
        )
        logging.info("=" * 60)
        return success_count > 0

    def generate_summary_report(self):
        """Generate a summary report of the imported data."""
        if not self.cursor:
            return
        logging.info("\n%s\nDATABASE IMPORT SUMMARY\n%s", "=" * 60, "=" * 60)
        try:
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            tables = self.cursor.execute(query).fetchall()
            total_records = 0
            for (table_name,) in tables:
                count = self.cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                total_records += count
                columns = self.cursor.execute(
                    f"PRAGMA table_info({table_name})"
                ).fetchall()
                col_names = [
                    c[1] for c in columns if c[1] not in ["id", "created_at"]
                ]

                logging.info("\nTable: %s", table_name)
                logging.info("  Rows: %s", f"{count:,}")
                col_str = ", ".join(col_names[:10])
                if len(col_names) > 10:
                    col_str += f" ... and {len(col_names) - 10} more"
                logging.info("  Columns: %s", col_str)
            logging.info(
                "\nTotal records in database: %s", f"{total_records:,}"
            )
        except sqlite3.Error as e:
            logging.error("Error generating summary: %s", e)


def main():
    """Main function to run the import process."""
    importer = BaseballDatabaseImporter()
    success = importer.run_import()
    if success:
        db_path = os.path.abspath(importer.db_path)
        print(f"\nDatabase import successful. File saved to: {db_path}")
    else:
        print(
            "\nDatabase import failed. Please check database_import.log for details."
        )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
