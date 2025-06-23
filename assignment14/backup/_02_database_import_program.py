#!/usr/bin/env python3
# _03_database_import_program.py
"""
Baseball Statistics Database Import Module
=========================================

This script handles the database operations separately from web scraping,
demonstrating clean separation of concerns and database best practices.

Key features:
- Robust CSV parsing with pandas
- Comprehensive data cleaning and validation
- Type conversion based on statistic types
- Detailed before/after reporting
- Efficient batch database operations
"""

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Configure logging with best practices
# Handle encoding issues on Windows by specifying UTF-8
try:
    # Try to set UTF-8 encoding for the console on Windows
    import sys

    if sys.platform == "win32":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
except Exception:
    # If that fails, we'll just use the logging configuration below
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("database_import.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


@dataclass
class ImportStats:
    """Track import statistics for reporting"""

    files_processed: int = 0
    records_imported: int = 0
    records_skipped: int = 0
    errors_encountered: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


@dataclass
class CleaningStats:
    """Track data cleaning statistics"""

    original_rows: int = 0
    duplicates_removed: int = 0
    missing_values_filled: int = 0
    malformed_rows_fixed: int = 0
    rows_after_cleaning: int = 0
    type_conversions: Dict[str, int] = field(default_factory=dict)


class BaseballDatabaseImporter:
    """Database importer for baseball statistics

    This class demonstrates professional database management with pandas:
    - Comprehensive data validation and cleaning
    - Type conversion based on statistic types
    - Detailed logging of all transformations
    - Efficient batch operations
    """

    # Define statistic types for proper type conversion
    NUMERIC_STATS = {
        "Batting Average",
        "On-base Percentage",
        "Slugging Percentage",
        "OPS",
        "ERA",
        "WHIP",
        "Winning Percentage",
        "Games",
        "At Bats",
        "Runs",
        "Hits",
        "Doubles",
        "Triples",
        "Home Runs",
        "Runs Batted In",
        "Stolen Bases",
        "Caught Stealing",
        "Base on Balls",
        "Strikeouts",
        "Wins",
        "Losses",
        "Saves",
        "Complete Games",
        "Shutouts",
        "Innings Pitched",
        "Hits Allowed",
        "Earned Runs",
        "Walks",
        "Hit By Pitch",
        "Wild Pitches",
        "Balks",
    }

    def __init__(
        self, db_path: str = "baseball_stats.db", csv_dir: str = "scraped_data"
    ):
        """Initialize the database importer with paths and setup"""
        self.db_path = db_path
        self.csv_dir = Path(csv_dir)
        self.stats = ImportStats()
        self.cleaning_stats: Dict[str, CleaningStats] = {}

        if not self.csv_dir.exists():
            raise ValueError(f"CSV directory not found: {self.csv_dir}")

        self._init_database()

    @staticmethod
    def _convert_to_native_types(obj: Any) -> Any:
        """Convert numpy/pandas types to native Python types for JSON serialization

        This helper function handles the common issue of numpy/pandas types
        not being JSON serializable. It recursively converts data structures.
        """
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {
                key: BaseballDatabaseImporter._convert_to_native_types(value)
                for key, value in obj.items()
            }
        elif isinstance(obj, list):
            return [
                BaseballDatabaseImporter._convert_to_native_types(item)
                for item in obj
            ]
        else:
            return obj

    def _init_database(self):
        """Initialize SQLite database with optimized schema"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")

            # Create main statistics table
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS baseball_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    url TEXT,
                    team TEXT NOT NULL,
                    team_url TEXT,
                    year INTEGER NOT NULL CHECK (year >= 1900 AND year <= 2100),
                    league TEXT NOT NULL CHECK (league IN ('American League', 'National League')),
                    statistic TEXT NOT NULL,
                    value TEXT NOT NULL,
                    stat_type TEXT NOT NULL CHECK (stat_type IN ('player', 'pitcher')),
                    rank INTEGER DEFAULT 1 CHECK (rank >= 1),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(name, team, year, league, statistic, stat_type)
                )
            """
            )

            # Create team standings table
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS team_standings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER NOT NULL CHECK (year >= 1900 AND year <= 2100),
                    league TEXT NOT NULL CHECK (league IN ('American League', 'National League')),
                    division TEXT NOT NULL CHECK (division IN ('East', 'Central', 'West')),
                    team TEXT NOT NULL,
                    wins INTEGER NOT NULL CHECK (wins >= 0),
                    losses INTEGER NOT NULL CHECK (losses >= 0),
                    winning_percentage REAL CHECK (winning_percentage >= 0 AND winning_percentage <= 1),
                    games_back TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(year, league, division, team)
                )
            """
            )

            # Create import history table
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS import_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    records_imported INTEGER,
                    records_skipped INTEGER,
                    errors INTEGER,
                    status TEXT CHECK (status IN ('success', 'partial', 'failed')),
                    cleaning_details TEXT
                )
            """
            )

            # Create indexes for performance
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_stats_year_league ON baseball_stats(year, league)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_stats_name_year ON baseball_stats(name, year)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_standings_year_league ON team_standings(year, league)"
            )

            conn.commit()
            logger.info("Database initialized at %s", self.db_path)

    def _clean_player_data(
        self, df: pd.DataFrame, stat_type: str
    ) -> Tuple[pd.DataFrame, CleaningStats]:
        """Clean and validate player/pitcher data using pandas"""
        stats = CleaningStats()
        stats.original_rows = len(df)

        # Remove duplicates
        df = df.drop_duplicates()
        stats.duplicates_removed = stats.original_rows - len(df)

        # Handle required fields
        name_col = "player_name" if stat_type == "player" else "pitcher_name"
        required_cols = [name_col, "team", "year", "league"]

        missing_before = df[required_cols].isnull().sum().sum()
        df = df.dropna(subset=required_cols)
        stats.missing_values_filled = missing_before

        # Type conversions and validation
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df = df.dropna(subset=["year"])
        df = df[(df["year"] >= 1900) & (df["year"] <= 2100)]
        df["year"] = df["year"].astype(int)

        # Normalize text fields
        df[name_col] = df[name_col].str.strip()
        df["team"] = df["team"].str.strip()
        df["league"] = df["league"].str.strip()

        # Validate league values
        valid_leagues = ["American League", "National League"]
        df = df[df["league"].isin(valid_leagues)]

        # Reshape to long format
        metadata_cols = [name_col, "team", "year", "league"]
        stat_cols = [col for col in df.columns if col not in metadata_cols]

        df_long = df.melt(
            id_vars=metadata_cols,
            value_vars=stat_cols,
            var_name="statistic",
            value_name="value",
        )

        df_long = df_long.rename(columns={name_col: "name"})

        # Clean values
        df_long["value"] = df_long["value"].astype(str).str.strip()
        df_long = df_long[~df_long["value"].isin(["", "nan"])]

        # Track rows removed during value cleaning
        # rows_before_value_clean = len(df_long)
        df_long = df_long[df_long["value"] != ""]
        df_long = df_long[df_long["value"] != "nan"]
        # rows_removed_values = rows_before_value_clean - len(df_long)

        # Add metadata
        df_long["stat_type"] = stat_type
        df_long["url"] = ""
        df_long["team_url"] = ""
        df_long["rank"] = 1

        # Final deduplication
        rows_before_final_dedup = len(df_long)
        df_long = df_long.drop_duplicates(
            subset=["name", "team", "year", "league", "statistic", "stat_type"]
        )
        final_dups_removed = rows_before_final_dedup - len(df_long)

        stats.rows_after_cleaning = len(df_long)
        # Adjust duplicate count to include final deduplication
        stats.duplicates_removed += final_dups_removed

        # For reporting, we need to track the transformation properly
        # In wide-to-long format, rows can increase, so we track differently
        logger.info(
            "Cleaned %s data: %s original rows -> %s output rows",
            stat_type,
            stats.original_rows,
            stats.rows_after_cleaning,
        )

        return df_long, stats

    def _clean_standings_data(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, CleaningStats]:
        """Clean and validate team standings data"""
        stats = CleaningStats()
        stats.original_rows = len(df)

        # Remove duplicates
        df = df.drop_duplicates()
        stats.duplicates_removed = stats.original_rows - len(df)

        # Handle required fields
        required_cols = [
            "year",
            "league",
            "division",
            "team",
            "wins",
            "losses",
        ]
        missing_before = df[required_cols].isnull().sum().sum()
        df = df.dropna(subset=required_cols)
        stats.missing_values_filled = missing_before

        # Type conversions
        numeric_cols = ["year", "wins", "losses"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=[col])

        df["year"] = df["year"].astype(int)
        df["wins"] = df["wins"].astype(int)
        df["losses"] = df["losses"].astype(int)

        # Calculate winning percentage
        total_games = df["wins"] + df["losses"]
        df["winning_percentage"] = df["wins"] / total_games

        if "games_back" not in df.columns:
            df["games_back"] = "0"
        else:
            df["games_back"] = df["games_back"].fillna("0").astype(str)

        # Validate constraints
        df = df[(df["year"] >= 1900) & (df["year"] <= 2100)]
        df = df[df["league"].isin(["American League", "National League"])]
        df = df[df["division"].isin(["East", "Central", "West"])]

        stats.rows_after_cleaning = len(df)

        logger.info(
            "Cleaned standings data: %s -> %s rows",
            stats.original_rows,
            stats.rows_after_cleaning,
        )

        return df, stats

    def _import_csv_file(
        self, filepath: Path, data_type: str
    ) -> Dict[str, int]:
        """Generic CSV import method"""
        logger.info("Importing %s data from %s", data_type, filepath)

        try:
            df = pd.read_csv(filepath, encoding="utf-8")

            if data_type in ["player", "pitcher"]:
                df_clean, cleaning_stats = self._clean_player_data(
                    df, data_type
                )
                columns = [
                    "name",
                    "url",
                    "team",
                    "team_url",
                    "year",
                    "league",
                    "statistic",
                    "value",
                    "stat_type",
                    "rank",
                ]
                batch_method = self._execute_batch_insert
            else:  # standings
                df_clean, cleaning_stats = self._clean_standings_data(df)
                columns = [
                    "year",
                    "league",
                    "division",
                    "team",
                    "wins",
                    "losses",
                    "winning_percentage",
                    "games_back",
                ]
                batch_method = self._execute_standings_batch_insert

            self.cleaning_stats[data_type] = cleaning_stats

            records = df_clean[columns].values.tolist()
            imported = 0

            if records:
                batch_size = 1000 if data_type != "standings" else 500
                for i in range(0, len(records), batch_size):
                    batch = records[i : i + batch_size]
                    batch_method(batch)
                imported = len(records)

            # For wide-to-long transformations, the output can have more rows than input
            # This happens when each player has multiple statistics
            # So we track "invalid/removed" rows rather than simple subtraction
            invalid_rows = (
                cleaning_stats.duplicates_removed
                + cleaning_stats.missing_values_filled
                + cleaning_stats.malformed_rows_fixed
            )
            skipped = max(0, invalid_rows)  # Ensure non-negative

            logger.info(
                "%s import complete: %s imported, %s invalid rows removed",
                data_type.capitalize(),
                imported,
                skipped,
            )

            return {"imported": imported, "skipped": skipped, "errors": 0}

        except FileNotFoundError:
            logger.error("File not found: %s", filepath)
            return {"imported": 0, "skipped": 0, "errors": 1}
        except pd.errors.EmptyDataError:
            logger.error("Empty CSV file: %s", filepath)
            return {"imported": 0, "skipped": 0, "errors": 1}
        except (sqlite3.Error, ValueError) as e:
            logger.error("Import error for %s: %s", data_type, e)
            return {"imported": 0, "skipped": 0, "errors": 1}

    def _execute_batch_insert(self, data: List[Any]) -> None:
        """Execute batch insert for baseball statistics"""
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO baseball_stats
                   (name, url, team, team_url, year, league,
                    statistic, value, stat_type, rank)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                data,
            )
            conn.commit()

    def _execute_standings_batch_insert(self, data: List[Any]) -> None:
        """Execute batch insert for team standings"""
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO team_standings
                   (year, league, division, team, wins, losses,
                    winning_percentage, games_back)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                data,
            )
            conn.commit()

    def import_all(self) -> None:
        """Import all CSV files from the directory"""
        self.stats.start_time = datetime.now()
        logger.info("Starting import from %s", self.csv_dir)

        import_tasks = [
            ("player_statistics.csv", "player"),
            ("pitcher_statistics.csv", "pitcher"),
            ("team_standings.csv", "standings"),
        ]

        for filename, data_type in import_tasks:
            filepath = self.csv_dir / filename

            if filepath.exists():
                self.stats.files_processed += 1

                try:
                    results = self._import_csv_file(filepath, data_type)
                    self.stats.records_imported += results["imported"]
                    self.stats.records_skipped += results["skipped"]
                    self.stats.errors_encountered += results["errors"]

                    self._record_import_history(
                        filename, results, self.cleaning_stats.get(data_type)
                    )

                except (OSError, IOError) as e:
                    logger.error("IO error importing %s: %s", filename, e)
                    self._record_import_history(
                        filename,
                        {"imported": 0, "skipped": 0, "errors": 1},
                        None,
                    )
            else:
                logger.warning("File not found: %s", filepath)

        self.stats.end_time = datetime.now()
        self.generate_import_report()
        self.verify_database_integrity()

    def _record_import_history(
        self,
        filename: str,
        results: Dict[str, int],
        cleaning_stats: Optional[CleaningStats],
    ) -> None:
        """Record import history for auditing"""
        status = (
            "success"
            if results["errors"] == 0 and results["imported"] > 0
            else "partial" if results["imported"] > 0 else "failed"
        )

        cleaning_details = ""
        if cleaning_stats:
            # Convert all values to native Python types to avoid JSON serialization issues
            details = {
                "original": int(cleaning_stats.original_rows),
                "duplicates_removed": int(cleaning_stats.duplicates_removed),
                "missing_handled": int(cleaning_stats.missing_values_filled),
                "malformed_fixed": int(cleaning_stats.malformed_rows_fixed),
                "final": int(cleaning_stats.rows_after_cleaning),
            }
            cleaning_details = json.dumps(
                self._convert_to_native_types(details)
            )

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO import_history
                   (filename, records_imported, records_skipped,
                    errors, status, cleaning_details)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    filename,
                    results["imported"],
                    results["skipped"],
                    results["errors"],
                    status,
                    cleaning_details,
                ),
            )
            conn.commit()

    def verify_database_integrity(self) -> None:
        """Verify database integrity and generate statistics"""
        logger.info("Verifying database integrity...")

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Check for duplicates
            cursor.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT name, team, year, league, statistic, stat_type, COUNT(*) as cnt
                    FROM baseball_stats
                    GROUP BY name, team, year, league, statistic, stat_type
                    HAVING cnt > 1
                )
            """
            )
            duplicate_count = cursor.fetchone()[0]

            if duplicate_count > 0:
                logger.warning("Found %s duplicate entries", duplicate_count)

            # Generate statistics
            stats_queries = {
                "unique_players": "SELECT COUNT(DISTINCT name) FROM baseball_stats WHERE stat_type = 'player'",
                "unique_pitchers": "SELECT COUNT(DISTINCT name) FROM baseball_stats WHERE stat_type = 'pitcher'",
                "total_stats": "SELECT COUNT(*) FROM baseball_stats",
                "total_standings": "SELECT COUNT(*) FROM team_standings",
            }

            results = {}
            for key, query in stats_queries.items():
                cursor.execute(query)
                results[key] = cursor.fetchone()[0]

            cursor.execute("SELECT MIN(year), MAX(year) FROM baseball_stats")
            year_range = cursor.fetchone()

            logger.info("Database statistics:")
            for key, value in results.items():
                logger.info("  - %s: %s", key.replace("_", " ").title(), value)

            if year_range[0] and year_range[1]:
                logger.info(
                    "  - Year range: %s - %s", year_range[0], year_range[1]
                )

    def generate_import_report(self) -> None:
        """Generate comprehensive import report"""
        duration = (
            (self.stats.end_time - self.stats.start_time).total_seconds()
            if self.stats.start_time and self.stats.end_time
            else 0.0
        )

        total_processed = (
            self.stats.records_imported + self.stats.records_skipped
        )
        success_rate = (
            (self.stats.records_imported / total_processed * 100)
            if total_processed > 0
            else 0.0
        )

        report_lines = [
            "=" * 60,
            "DATABASE IMPORT REPORT",
            "=" * 60,
            (
                f"Import Date: {self.stats.start_time.strftime('%Y-%m-%d %H:%M:%S')}"
                if self.stats.start_time
                else ""
            ),
            f"Duration: {duration:.2f} seconds",
            f"Database: {self.db_path}",
            "",
            f"Files Processed: {self.stats.files_processed}",
            f"Records Imported: {self.stats.records_imported}",
            f"Records Skipped: {self.stats.records_skipped}",
            f"Errors Encountered: {self.stats.errors_encountered}",
            f"Success Rate: {success_rate:.1f}%",
            "",
            "CLEANING SUMMARY:",
            "=" * 60,
        ]

        for file_type, stats in self.cleaning_stats.items():
            reduction = (
                ((1 - stats.rows_after_cleaning / stats.original_rows) * 100)
                if stats.original_rows > 0
                else 0.0
            )

            report_lines.extend(
                [
                    f"\n{file_type.upper()} Data Cleaning:",
                    f"  - Original rows: {stats.original_rows}",
                    f"  - Rows after cleaning: {stats.rows_after_cleaning}",
                    f"  - Reduction: {reduction:.1f}%",
                ]
            )

        report_lines.append("=" * 60)

        report = "\n".join(report_lines)
        print(report)

        with open(
            self.csv_dir / "import_report.txt", "w", encoding="utf-8"
        ) as f:
            f.write(report)

    def export_database_summary(self) -> None:
        """Export database summary to CSV for analysis"""
        logger.info("Exporting database summary...")

        with sqlite3.connect(self.db_path) as conn:
            # Export summaries
            summaries = {
                "player_summary.csv": """
                    SELECT year, league, COUNT(DISTINCT name) as unique_players, COUNT(*) as total_records
                    FROM baseball_stats WHERE stat_type = 'player'
                    GROUP BY year, league ORDER BY year DESC, league
                """,
                "pitcher_summary.csv": """
                    SELECT year, league, COUNT(DISTINCT name) as unique_pitchers, COUNT(*) as total_records
                    FROM baseball_stats WHERE stat_type = 'pitcher'
                    GROUP BY year, league ORDER BY year DESC, league
                """,
                "standings_summary.csv": """
                    SELECT year, league, division, COUNT(*) as teams,
                           AVG(wins) as avg_wins, AVG(losses) as avg_losses
                    FROM team_standings
                    GROUP BY year, league, division ORDER BY year DESC, league, division
                """,
            }

            for filename, query in summaries.items():
                df = pd.read_sql_query(query, conn)
                df.to_csv(self.csv_dir / filename, index=False)

            logger.info("Database summaries exported")


def main():
    """Main execution function"""
    try:
        importer = BaseballDatabaseImporter(
            db_path="baseball_stats.db", csv_dir="scraped_data"
        )
        importer.import_all()
        importer.export_database_summary()
        logger.info("Database import complete!")

    except ValueError as e:
        logger.error("Configuration error: %s", e)
        raise
    except (OSError, IOError) as e:
        logger.error("File system error: %s", e)
        raise
    except sqlite3.Error as e:
        logger.error("Database error: %s", e)
        raise


if __name__ == "__main__":
    main()
