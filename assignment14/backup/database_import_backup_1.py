#!/usr/bin/env python3
# database_import.py
"""
Database Importer for Baseball Statistics - Enhanced Version
===========================================================

This enhanced version includes better debugging, column mapping,
and data validation to ensure successful imports.
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
DATA_FOLDER_NAME = "baseball_data"
DB_NAME = "baseball_stats.db"

# Ensure the data directory exists
os.makedirs(DATA_FOLDER_NAME, exist_ok=True)
# --- End of Configuration ---

# Enhanced logging with more detail
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detail
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            "database_import_debug.log",
            mode="w",
            encoding="utf-8",
        ),
        logging.StreamHandler(sys.stdout),
    ],
)


class BaseballDatabaseImporter:
    """
    Enhanced importer with better debugging and column mapping capabilities.
    """

    def __init__(
        self, db_path: str = DB_NAME, data_folder: str = DATA_FOLDER_NAME
    ):
        """Initialize the importer with enhanced debugging."""
        self.db_path = db_path
        self.data_folder = Path(data_folder)
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None

        logging.info("Current working directory: %s", os.getcwd())
        logging.info("Data folder path: %s", self.data_folder.absolute())

        # CSV mappings remain the same
        self.csv_mappings = {
            "Yearly_American_League_Player_Review.csv": "al_player_review",
            "Yearly_American_League_Pitcher_Review.csv": "al_pitcher_review",
            "Yearly_American_League_Team_Standings.csv": "al_team_standings",
            "Yearly_National_League_Player_Review.csv": "nl_player_review",
            "Yearly_National_League_Pitcher_Review.csv": "nl_pitcher_review",
            "Yearly_National_League_Team_Standings.csv": "nl_team_standings",
        }

        # Enhanced column mappings to handle various naming conventions
        # This maps possible CSV column names to our database column names
        self.column_mappings = {
            # Player name variations
            "player": [
                "player",
                "name",
                "player name",
                "player_name",
                "playername",
            ],
            "team": ["team", "tm", "team_name", "club"],
            "pos": ["pos", "position", "pos."],
            # Batting statistics variations
            "g": ["g", "games", "gp", "games played"],
            "ab": ["ab", "at bats", "at_bats", "atbats"],
            "r": ["r", "runs", "runs scored"],
            "h": ["h", "hits"],
            "2b": ["2b", "2B", "doubles", "2base"],
            "3b": ["3b", "3B", "triples", "3base"],
            "hr": ["hr", "HR", "home runs", "homers"],
            "rbi": ["rbi", "RBI", "rbis", "runs batted in"],
            "bb": ["bb", "BB", "walks", "base on balls"],
            "so": ["so", "SO", "k", "K", "strikeouts"],
            "sb": ["sb", "SB", "stolen bases", "steals"],
            "cs": ["cs", "CS", "caught stealing"],
            "avg": ["avg", "AVG", "ba", "BA", "batting average", "average"],
            "obp": ["obp", "OBP", "on base percentage"],
            "slg": ["slg", "SLG", "slugging", "slugging percentage"],
            # Pitching statistics variations
            "w": ["w", "W", "wins"],
            "l": ["l", "L", "losses"],
            "era": ["era", "ERA", "earned run average"],
            "sv": ["sv", "SV", "saves"],
            "sho": ["sho", "SHO", "shutouts"],
            "ip": ["ip", "IP", "innings pitched", "innings"],
            "er": ["er", "ER", "earned runs"],
            "k": ["k", "K", "so", "SO", "strikeouts"],
            # Other fields
            "year": ["year", "season", "yr"],
            "league": ["league", "lg"],
            "division": ["division", "div"],
            "wp": ["wp", "WP", "pct", "win%", "win percentage"],
            "gb": ["gb", "GB", "games back", "games_back"],
        }

        # Schema definitions remain the same
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

    def map_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map various possible column names to our standardized names.
        This handles cases where CSVs use different naming conventions.
        """
        current_columns = df.columns.str.lower().str.strip()
        new_column_names = {}

        logging.debug(f"Original columns: {list(df.columns)}")

        for col_idx, csv_col in enumerate(current_columns):
            mapped = False
            for db_col, variations in self.column_mappings.items():
                if csv_col in [v.lower() for v in variations]:
                    new_column_names[df.columns[col_idx]] = db_col
                    mapped = True
                    break

            if not mapped:
                # Keep original name if no mapping found
                new_column_names[df.columns[col_idx]] = csv_col

        df_mapped = df.rename(columns=new_column_names)
        logging.debug(f"Mapped columns: {list(df_mapped.columns)}")

        return df_mapped

    def diagnose_csv_structure(self, file_path: Path) -> Dict[str, any]:
        """
        Analyze CSV structure to understand why imports might fail.
        This helps identify issues before attempting import.
        """
        logging.info(f"Diagnosing CSV structure for: {file_path.name}")

        try:
            # Read first few rows to understand structure
            df_sample = pd.read_csv(file_path, nrows=10, encoding="utf-8-sig")

            diagnosis = {
                "total_columns": len(df_sample.columns),
                "column_names": list(df_sample.columns),
                "has_unnamed_columns": any(
                    "Unnamed" in str(col) for col in df_sample.columns
                ),
                "first_row_sample": (
                    df_sample.iloc[0].to_dict() if len(df_sample) > 0 else {}
                ),
                "appears_valid": True,
            }

            # Check if it looks like player data
            if (
                "player" in str(file_path).lower()
                or "pitcher" in str(file_path).lower()
            ):
                # Look for player-related columns
                player_cols = ["player", "name", "ab", "hits", "average"]
                found_player_cols = sum(
                    1
                    for col in df_sample.columns
                    if any(pc in col.lower() for pc in player_cols)
                )
                diagnosis["found_player_columns"] = found_player_cols
                diagnosis["appears_valid"] = found_player_cols > 0

            logging.info(f"Diagnosis: {diagnosis}")
            return diagnosis

        except Exception as e:
            logging.error(f"Error diagnosing CSV: {e}")
            return {"appears_valid": False, "error": str(e)}

    def extract_year_from_data(
        self, df: pd.DataFrame, filename: str
    ) -> Optional[int]:
        """
        Try to extract year from the data itself or filename.
        Sometimes the year is in the data rather than the filename.
        """
        # First try filename
        year_match = re.search(r"(\d{4})", filename)
        if year_match:
            return int(year_match.group(1))

        # Then check if there's a year column in the data
        if "year" in df.columns and len(df) > 0:
            year_val = df["year"].iloc[0]
            if pd.notna(year_val):
                return int(year_val)

        # Check if filename contains year info differently
        for year in range(2000, 2025):
            if str(year) in filename:
                return year

        logging.warning(f"Could not extract year from {filename}")
        return None

    def clean_and_validate_dataframe(
        self, df: pd.DataFrame, table_type: str
    ) -> pd.DataFrame:
        """
        Enhanced cleaning with validation specific to baseball data.
        """
        logging.info(f"Cleaning dataframe for {table_type}")
        logging.info(f"Initial shape: {df.shape}")

        # Remove completely empty rows
        df = df.dropna(how="all")

        # Map column names to standard names
        df = self.map_column_names(df)

        # Remove unnamed columns
        df = df.loc[:, ~df.columns.str.contains("unnamed", case=False)]

        # For player/pitcher data, validate we have actual player names
        if "player" in df.columns:
            # Log sample of player column to debug
            logging.debug(
                f"Sample player values: {df['player'].head(10).tolist()}"
            )

            # Remove rows where player name is missing or invalid
            df = df[df["player"].notna()]
            df = df[df["player"] != ""]
            df = df[df["player"] != "None"]
            df = df[~df["player"].str.contains("Total", case=False, na=False)]
            df = df[
                ~df["player"].str.contains(
                    "League Average", case=False, na=False
                )
            ]

            logging.info(f"Rows with valid player names: {len(df)}")

        # Remove duplicate rows
        df = df.drop_duplicates()

        logging.info(f"Cleaned shape: {df.shape}")
        return df

    def find_csv_files(self) -> Dict[str, Path]:
        """Locate CSV files with enhanced logging."""
        found_files = {}
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

                    # Diagnose the file structure
                    self.diagnose_csv_structure(file_path)
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

    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """Create a table with the provided schema."""
        if not self.cursor or not self.conn:
            return False

        try:
            columns_def = [
                f'"{col}" {sql_type}' for col, sql_type in schema.items()
            ]

            # Drop existing table
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

            # Create new table
            create_query = (
                f"CREATE TABLE {table_name} ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                f"{', '.join(columns_def)}, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            self.cursor.execute(create_query)

            # Create indices
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

    def transform_standings_data(
        self, df: pd.DataFrame, filename: str
    ) -> pd.DataFrame:
        """
        Transform standings data with enhanced year extraction.
        """
        transformed_data = []
        current_division = None

        # Extract year and league
        year = self.extract_year_from_data(df, filename)
        league = (
            "American League" if "American" in filename else "National League"
        )

        logging.info(f"Processing standings for {league} year {year}")

        for idx, row in df.iterrows():
            team_value = str(row.get("team", "")).strip()
            if not team_value:
                continue

            # Check if this row is a division header
            if any(div in team_value for div in ["East", "Central", "West"]):
                # Check if it has stats - if not, it's a header
                has_stats = any(
                    stat_col in row and pd.notna(row[stat_col])
                    for stat_col in ["w", "W", "wins"]
                )
                if not has_stats:
                    current_division = team_value
                    logging.debug(f"Found division header: {current_division}")
                    continue

            # Extract wins and losses
            wins = None
            losses = None
            for w_col in ["w", "W", "wins"]:
                if w_col in row and pd.notna(row[w_col]):
                    wins = row[w_col]
                    break

            for l_col in ["l", "L", "losses"]:
                if l_col in row and pd.notna(row[l_col]):
                    losses = row[l_col]
                    break

            if wins is not None and losses is not None:
                # Get other stats
                win_pct = None
                for wp_col in ["wp", "WP", "pct", "win%"]:
                    if wp_col in row and pd.notna(row[wp_col]):
                        win_pct = row[wp_col]
                        break

                gb = "0"
                for gb_col in ["gb", "GB", "games back"]:
                    if gb_col in row and pd.notna(row[gb_col]):
                        gb = str(row[gb_col]).replace("-", "0")
                        break

                transformed_data.append(
                    {
                        "year": year,
                        "league": league,
                        "division": current_division or "Unknown",
                        "team": team_value,
                        "w": int(wins),
                        "l": int(losses),
                        "wp": float(win_pct) if win_pct else None,
                        "gb": gb,
                    }
                )

        result_df = pd.DataFrame(transformed_data)
        logging.info(f"Transformed {len(result_df)} standings records")
        return result_df

    def import_csv_to_table(
        self, csv_file: str, file_path: Path, table_name: str
    ) -> bool:
        """
        Import CSV with enhanced debugging and validation.
        """
        try:
            logging.info(
                f"Starting import of {csv_file} to table {table_name}"
            )

            # Read the CSV
            df = pd.read_csv(
                file_path, encoding="utf-8-sig", on_bad_lines="warn"
            )
            logging.info(f"Read {len(df)} rows from CSV")

            # Determine if this is standings data
            is_standings = "standings" in table_name.lower()

            # Clean and validate the data
            if is_standings:
                df_final = self.transform_standings_data(df, csv_file)
            else:
                df_cleaned = self.clean_and_validate_dataframe(df, table_name)

                # For player/pitcher data, add year and league info
                if "year" not in df_cleaned.columns:
                    year = self.extract_year_from_data(df_cleaned, csv_file)
                    if year:
                        df_cleaned["year"] = year

                if "league" not in df_cleaned.columns:
                    league = (
                        "American" if "American" in csv_file else "National"
                    )
                    df_cleaned["league"] = league

                df_final = df_cleaned

            # Log data sample for debugging
            if len(df_final) > 0:
                logging.debug(
                    f"Sample of final data:\n{df_final.head(3).to_string()}"
                )
            else:
                logging.warning(f"No valid data found in {csv_file}")
                return False

            # Select appropriate schema
            schema = (
                self.standings_schema if is_standings else self.review_schema
            )

            # Ensure all schema columns exist
            for col in schema:
                if col not in df_final.columns:
                    df_final[col] = None

            # Select only schema columns
            df_final = df_final[list(schema.keys())].copy()

            # Create the table
            if not self.create_table(table_name, schema):
                return False

            # Type conversion with better error handling
            for col, dtype in schema.items():
                if col in df_final:
                    if dtype == "INTEGER":
                        df_final[col] = (
                            pd.to_numeric(df_final[col], errors="coerce")
                            .fillna(0)
                            .astype("Int64")
                        )
                    elif dtype == "REAL":
                        df_final[col] = pd.to_numeric(
                            df_final[col], errors="coerce"
                        )
                    elif dtype == "TEXT":
                        df_final[col] = (
                            df_final[col].astype(str).replace("nan", "")
                        )

            # Import to database
            if self.conn:
                df_final.to_sql(
                    table_name,
                    self.conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                self.conn.commit()

                # Verify import
                count = self.cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                valid_count = 0
                if "player" in schema:
                    valid_count = self.cursor.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE player IS NOT NULL AND player != ''"
                    ).fetchone()[0]
                    logging.info(
                        f"Imported {count} total records, {valid_count} with valid player names"
                    )
                else:
                    logging.info(
                        f"Successfully imported {count} records to {table_name}"
                    )

                return count > 0

        except Exception as e:
            logging.error(
                f"Error importing {csv_file}: {type(e).__name__}: {e}"
            )
            if self.conn:
                self.conn.rollback()
            return False

    def run_import(self) -> bool:
        """Main import process with enhanced reporting."""
        logging.info("=" * 60)
        logging.info("Starting baseball database import at %s", datetime.now())
        logging.info("=" * 60)

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
                logging.info("\n" + "-" * 40)
                logging.info("Processing file: %s", csv_file)
                logging.info("-" * 40)

                if self.import_csv_to_table(
                    csv_file, found_files[csv_file], table_name
                ):
                    success_count += 1
                else:
                    logging.error(f"Failed to import {csv_file}")
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
        """Generate detailed summary of imported data."""
        if not self.cursor:
            return

        logging.info("\n" + "=" * 60)
        logging.info("DATABASE IMPORT SUMMARY")
        logging.info("=" * 60)

        try:
            # Get all tables
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            tables = self.cursor.execute(query).fetchall()

            total_records = 0
            total_valid_players = 0

            for (table_name,) in tables:
                count = self.cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                total_records += count

                logging.info(f"\nTable: {table_name}")
                logging.info(f"  Total rows: {count:,}")

                # For player/pitcher tables, show valid player count
                if "player" in table_name or "pitcher" in table_name:
                    valid_players = self.cursor.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE player IS NOT NULL AND player != ''"
                    ).fetchone()[0]
                    total_valid_players += valid_players
                    logging.info(f"  Valid player records: {valid_players:,}")

                    # Show sample of actual players
                    sample_players = self.cursor.execute(
                        f"SELECT DISTINCT player FROM {table_name} WHERE player IS NOT NULL AND player != '' LIMIT 5"
                    ).fetchall()
                    if sample_players:
                        logging.info(
                            f"  Sample players: {', '.join(p[0] for p in sample_players)}"
                        )

            logging.info(f"\nTotal records in database: {total_records:,}")
            if total_valid_players > 0:
                logging.info(
                    f"Total valid player records: {total_valid_players:,}"
                )
            else:
                logging.warning("WARNING: No valid player records found!")

        except sqlite3.Error as e:
            logging.error("Error generating summary: %s", e)


def main():
    """Run the import process."""
    importer = BaseballDatabaseImporter()
    success = importer.run_import()

    if success:
        db_path = os.path.abspath(importer.db_path)
        print(f"\nDatabase import completed. File saved to: {db_path}")
        print("Check database_import_debug.log for detailed information.")
    else:
        print(
            "\nDatabase import failed. Please check database_import_debug.log for details."
        )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
