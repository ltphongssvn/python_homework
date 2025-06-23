#!/usr/bin/env python3
# _03_database_import_program.py
"""
Baseball Statistics Database Import Module
=========================================

This script handles the database operations separately from web scraping,
demonstrating clean separation of concerns and database best practices.

Key features:
- Robust CSV parsing with error handling
- Efficient batch database operations
- Data validation and integrity checks
- Comprehensive reporting and statistics
"""

import csv
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Configure logging with best practices
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("database_import.log"),
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


class BaseballDatabaseImporter:
    """Database importer for baseball statistics

    This class demonstrates professional database management:
    - Connection pooling and context managers
    - Transaction management
    - Prepared statements for efficiency
    - Data validation and error recovery
    """

    def __init__(
        self, db_path: str = "baseball_stats.db", csv_dir: str = "scraped_data"
    ):
        """Initialize the database importer with paths and setup

        Args:
            db_path: Path to the SQLite database file
            csv_dir: Directory containing CSV files to import
        """
        self.db_path = db_path
        self.csv_dir = Path(csv_dir)
        self.stats = ImportStats()

        # Validate CSV directory exists
        if not self.csv_dir.exists():
            raise ValueError(f"CSV directory not found: {self.csv_dir}")

        # Initialize database
        self._init_database()

    def _init_database(self):
        """Initialize SQLite database with optimized schema

        This method demonstrates:
        - Proper schema design with constraints
        - Index creation for query performance
        - Transaction management
        """
        with sqlite3.connect(self.db_path) as conn:
            # Enable foreign keys for data integrity
            conn.execute("PRAGMA foreign_keys = ON")

            # Create tables with proper constraints
            conn.executescript(
                """
                -- Main statistics table (normalized design)
                CREATE TABLE IF NOT EXISTS baseball_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    url TEXT,
                    team TEXT NOT NULL,
                    team_url TEXT,
                    year INTEGER NOT NULL
                        CHECK (year >= 1900 AND year <= 2100),
                    league TEXT NOT NULL
                        CHECK (league IN ('American League',
                                        'National League')),
                    statistic TEXT NOT NULL,
                    value TEXT NOT NULL,
                    stat_type TEXT NOT NULL
                        CHECK (stat_type IN ('player', 'pitcher')),
                    rank INTEGER DEFAULT 1 CHECK (rank >= 1),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(name, team, year, league, statistic, stat_type)
                );

                -- Team standings table
                CREATE TABLE IF NOT EXISTS team_standings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER NOT NULL
                        CHECK (year >= 1900 AND year <= 2100),
                    league TEXT NOT NULL
                        CHECK (league IN ('American League',
                                        'National League')),
                    division TEXT NOT NULL
                        CHECK (division IN ('East', 'Central', 'West')),
                    team TEXT NOT NULL,
                    wins INTEGER NOT NULL CHECK (wins >= 0),
                    losses INTEGER NOT NULL CHECK (losses >= 0),
                    winning_percentage REAL
                        CHECK (winning_percentage >= 0
                               AND winning_percentage <= 1),
                    games_back TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(year, league, division, team)
                );

                -- Import history for auditing
                CREATE TABLE IF NOT EXISTS import_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    records_imported INTEGER,
                    records_skipped INTEGER,
                    errors INTEGER,
                    status TEXT
                        CHECK (status IN ('success', 'partial', 'failed'))
                );

                -- Create indexes for common queries
                CREATE INDEX IF NOT EXISTS idx_stats_year_league
                    ON baseball_stats(year, league);
                CREATE INDEX IF NOT EXISTS idx_stats_name_year
                    ON baseball_stats(name, year);
                CREATE INDEX IF NOT EXISTS idx_stats_team_year
                    ON baseball_stats(team, year);
                CREATE INDEX IF NOT EXISTS idx_stats_type_statistic
                    ON baseball_stats(stat_type, statistic);
                CREATE INDEX IF NOT EXISTS idx_standings_year_league
                    ON team_standings(year, league);
                CREATE INDEX IF NOT EXISTS idx_standings_team_year
                    ON team_standings(team, year);
            """
            )

            conn.commit()
            logger.info("Database initialized at %s", self.db_path)

    def validate_player_row(
        self, row: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """Validate player statistics row

        Returns: (is_valid, error_message)
        """
        required_fields = ["player_name", "team", "year", "league"]

        # Check required fields
        for field in required_fields:
            if not row.get(field):
                return False, f"Missing required field: {field}"

        # Validate year
        try:
            year = int(row["year"])
            if year < 1900 or year > 2100:
                return False, f"Invalid year: {year}"
        except ValueError:
            return False, f"Invalid year format: {row['year']}"

        # Validate league
        valid_leagues = ["American League", "National League"]
        if row["league"] not in valid_leagues:
            return False, f"Invalid league: {row['league']}"

        return True, None

    def validate_pitcher_row(
        self, row: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """Validate pitcher statistics row

        Returns: (is_valid, error_message)
        """
        required_fields = ["pitcher_name", "team", "year", "league"]

        # Check required fields
        for field in required_fields:
            if not row.get(field):
                return False, f"Missing required field: {field}"

        # Validate year
        try:
            year = int(row["year"])
            if year < 1900 or year > 2100:
                return False, f"Invalid year: {year}"
        except ValueError:
            return False, f"Invalid year format: {row['year']}"

        # Validate league
        valid_leagues = ["American League", "National League"]
        if row["league"] not in valid_leagues:
            return False, f"Invalid league: {row['league']}"

        return True, None

    def validate_standings_row(
        self, row: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """Validate team standings row

        Returns: (is_valid, error_message)
        """
        required_fields = [
            "year",
            "league",
            "division",
            "team",
            "wins",
            "losses",
        ]

        # Check required fields
        for field in required_fields:
            if not row.get(field):
                return False, f"Missing required field: {field}"

        # Validate numeric fields
        try:
            year = int(row["year"])
            wins = int(row["wins"])
            losses = int(row["losses"])

            if year < 1900 or year > 2100:
                return False, f"Invalid year: {year}"
            if wins < 0 or wins > 200:
                return False, f"Invalid wins: {wins}"
            if losses < 0 or losses > 200:
                return False, f"Invalid losses: {losses}"

        except ValueError as e:
            return False, f"Invalid numeric value: {e}"

        # Validate league and division
        valid_leagues = ["American League", "National League"]
        valid_divisions = ["East", "Central", "West"]

        if row["league"] not in valid_leagues:
            return False, f"Invalid league: {row['league']}"
        if row["division"] not in valid_divisions:
            return False, f"Invalid division: {row['division']}"

        return True, None

    def import_player_statistics(self, filepath: Path) -> Dict[str, int]:
        """Import player statistics from CSV file

        This method demonstrates:
        - Batch inserts for performance
        - Data validation
        - Error handling and recovery
        """
        logger.info("Importing player statistics from %s", filepath)

        imported = 0
        skipped = 0
        errors = 0

        try:
            with open(filepath, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)

                # Prepare batch insert
                batch_data = []

                for row_num, row in enumerate(reader, 1):
                    try:
                        # Validate row
                        is_valid, error_msg = self.validate_player_row(row)
                        if not is_valid:
                            logger.warning("Row %s: %s", row_num, error_msg)
                            skipped += 1
                            continue

                        # Extract player name and basic info
                        player_name = row["player_name"]
                        team = row["team"]
                        year = int(row["year"])
                        league = row["league"]

                        # Process each statistic column
                        stat_columns = [
                            col
                            for col in row.keys()
                            if col
                            not in ["player_name", "team", "year", "league"]
                        ]

                        for stat in stat_columns:
                            value = row.get(stat, "").strip()
                            if value:  # Only import non-empty values
                                batch_data.append(
                                    (
                                        player_name,
                                        "",  # URL not in pivoted format
                                        team,
                                        "",  # Team URL not in pivoted format
                                        year,
                                        league,
                                        stat,
                                        value,
                                        "player",
                                        1,  # rank
                                    )
                                )

                        # Execute batch insert when buffer is full
                        if len(batch_data) >= 1000:
                            self._execute_batch_insert(batch_data)
                            imported += len(batch_data)
                            batch_data = []

                    except ValueError as e:
                        logger.error("Error processing row %s: %s", row_num, e)
                        errors += 1
                    except Exception as e:
                        logger.error(
                            "Unexpected error in row %s: %s", row_num, e
                        )
                        errors += 1

                # Insert remaining data
                if batch_data:
                    self._execute_batch_insert(batch_data)
                    imported += len(batch_data)

        except FileNotFoundError:
            logger.error("File not found: %s", filepath)
            raise
        except Exception as e:
            logger.error("Failed to import player statistics: %s", e)
            raise

        logger.info(
            "Player import complete: %s imported, %s skipped, %s errors",
            imported,
            skipped,
            errors,
        )
        return {"imported": imported, "skipped": skipped, "errors": errors}

    def import_pitcher_statistics(self, filepath: Path) -> Dict[str, int]:
        """Import pitcher statistics from CSV file"""
        logger.info("Importing pitcher statistics from %s", filepath)

        imported = 0
        skipped = 0
        errors = 0

        try:
            with open(filepath, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)

                batch_data = []

                for row_num, row in enumerate(reader, 1):
                    try:
                        # Validate row
                        is_valid, error_msg = self.validate_pitcher_row(row)
                        if not is_valid:
                            logger.warning("Row %s: %s", row_num, error_msg)
                            skipped += 1
                            continue

                        # Extract pitcher info
                        pitcher_name = row["pitcher_name"]
                        team = row["team"]
                        year = int(row["year"])
                        league = row["league"]

                        # Process each statistic column
                        stat_columns = [
                            col
                            for col in row.keys()
                            if col
                            not in ["pitcher_name", "team", "year", "league"]
                        ]

                        for stat in stat_columns:
                            value = row.get(stat, "").strip()
                            if value:
                                batch_data.append(
                                    (
                                        pitcher_name,
                                        "",
                                        team,
                                        "",
                                        year,
                                        league,
                                        stat,
                                        value,
                                        "pitcher",
                                        1,
                                    )
                                )

                        # Execute batch insert
                        if len(batch_data) >= 1000:
                            self._execute_batch_insert(batch_data)
                            imported += len(batch_data)
                            batch_data = []

                    except ValueError as e:
                        logger.error("Error processing row %s: %s", row_num, e)
                        errors += 1
                    except Exception as e:
                        logger.error(
                            "Unexpected error in row %s: %s", row_num, e
                        )
                        errors += 1

                # Insert remaining data
                if batch_data:
                    self._execute_batch_insert(batch_data)
                    imported += len(batch_data)

        except FileNotFoundError:
            logger.error("File not found: %s", filepath)
            raise
        except Exception as e:
            logger.error("Failed to import pitcher statistics: %s", e)
            raise

        logger.info(
            "Pitcher import complete: %s imported, %s skipped, %s errors",
            imported,
            skipped,
            errors,
        )
        return {"imported": imported, "skipped": skipped, "errors": errors}

    def import_team_standings(self, filepath: Path) -> Dict[str, int]:
        """Import team standings from CSV file"""
        logger.info("Importing team standings from %s", filepath)

        imported = 0
        skipped = 0
        errors = 0

        try:
            with open(filepath, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)

                batch_data = []

                for row_num, row in enumerate(reader, 1):
                    try:
                        # Validate row
                        is_valid, error_msg = self.validate_standings_row(row)
                        if not is_valid:
                            logger.warning("Row %s: %s", row_num, error_msg)
                            skipped += 1
                            continue

                        # Parse values
                        year = int(row["year"])
                        wins = int(row["wins"])
                        losses = int(row["losses"])

                        # Handle winning percentage
                        wp = row.get("winning_percentage", "")
                        if wp:
                            winning_percentage = float(wp)
                        else:
                            # Calculate if not provided
                            total_games = wins + losses
                            if total_games > 0:
                                winning_percentage = wins / total_games
                            else:
                                winning_percentage = 0.0

                        batch_data.append(
                            (
                                year,
                                row["league"],
                                row["division"],
                                row["team"],
                                wins,
                                losses,
                                winning_percentage,
                                row.get("games_back", "0"),
                            )
                        )

                        # Execute batch insert
                        if len(batch_data) >= 500:
                            self._execute_standings_batch_insert(batch_data)
                            imported += len(batch_data)
                            batch_data = []

                    except ValueError as e:
                        logger.error("Error processing row %s: %s", row_num, e)
                        errors += 1
                    except Exception as e:
                        logger.error(
                            "Unexpected error in row %s: %s", row_num, e
                        )
                        errors += 1

                # Insert remaining data
                if batch_data:
                    self._execute_standings_batch_insert(batch_data)
                    imported += len(batch_data)

        except FileNotFoundError:
            logger.error("File not found: %s", filepath)
            raise
        except Exception as e:
            logger.error("Failed to import team standings: %s", e)
            raise

        logger.info(
            "Standings import complete: %s imported, %s skipped, %s errors",
            imported,
            skipped,
            errors,
        )
        return {"imported": imported, "skipped": skipped, "errors": errors}

    def _execute_batch_insert(self, data: List[Tuple]):
        """Execute batch insert for baseball statistics

        Uses executemany for performance and proper transaction handling.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO baseball_stats
                (name, url, team, team_url, year, league,
                 statistic, value, stat_type, rank)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                data,
            )
            conn.commit()

    def _execute_standings_batch_insert(self, data: List[Tuple]):
        """Execute batch insert for team standings"""
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO team_standings
                (year, league, division, team, wins, losses,
                 winning_percentage, games_back)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                data,
            )
            conn.commit()

    def import_all(self):
        """Import all CSV files from the directory

        This method orchestrates the entire import process.
        """
        self.stats.start_time = datetime.now()
        logger.info("Starting import from %s", self.csv_dir)

        # Define expected files and their import methods
        import_tasks = [
            ("player_statistics.csv", self.import_player_statistics),
            ("pitcher_statistics.csv", self.import_pitcher_statistics),
            ("team_standings.csv", self.import_team_standings),
        ]

        for filename, import_method in import_tasks:
            filepath = self.csv_dir / filename

            if filepath.exists():
                logger.info("Processing %s...", filename)
                self.stats.files_processed += 1

                try:
                    results = import_method(filepath)
                    self.stats.records_imported += results["imported"]
                    self.stats.records_skipped += results["skipped"]
                    self.stats.errors_encountered += results["errors"]

                    # Record import history
                    self._record_import_history(
                        filename,
                        results["imported"],
                        results["skipped"],
                        results["errors"],
                    )

                except Exception as e:
                    logger.error("Failed to import %s: %s", filename, e)
                    self._record_import_history(
                        filename, 0, 0, 1, status="failed"
                    )
            else:
                logger.warning("File not found: %s", filepath)

        self.stats.end_time = datetime.now()

        # Generate import report
        self.generate_import_report()

        # Verify database integrity
        self.verify_database_integrity()

    def _record_import_history(
        self,
        filename: str,
        imported: int,
        skipped: int,
        errors: int,
        status: Optional[str] = None,
    ):
        """Record import history for auditing"""
        if status is None:
            if errors == 0 and imported > 0:
                status = "success"
            elif imported > 0:
                status = "partial"
            else:
                status = "failed"

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO import_history
                (filename, records_imported, records_skipped,
                 errors, status)
                VALUES (?, ?, ?, ?, ?)
            """,
                (filename, imported, skipped, errors, status),
            )
            conn.commit()

    def verify_database_integrity(self):
        """Verify database integrity and generate statistics"""
        logger.info("Verifying database integrity...")

        with sqlite3.connect(self.db_path) as conn:
            # Check for data consistency
            cursor = conn.cursor()

            # Check for duplicate entries
            cursor.execute(
                """
                SELECT name, team, year, league, statistic,
                       stat_type, COUNT(*) as count
                FROM baseball_stats
                GROUP BY name, team, year, league, statistic, stat_type
                HAVING count > 1
            """
            )
            duplicates = cursor.fetchall()

            if duplicates:
                logger.warning("Found %s duplicate entries", len(duplicates))

            # Generate database statistics
            cursor.execute(
                """
                SELECT COUNT(DISTINCT name)
                FROM baseball_stats
                WHERE stat_type = 'player'
            """
            )
            unique_players = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT COUNT(DISTINCT name)
                FROM baseball_stats
                WHERE stat_type = 'pitcher'
            """
            )
            unique_pitchers = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM baseball_stats")
            total_stats = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM team_standings")
            total_standings = cursor.fetchone()[0]

            cursor.execute("SELECT MIN(year), MAX(year) FROM baseball_stats")
            year_range = cursor.fetchone()

            logger.info("Database statistics:")
            logger.info("  - Unique players: %s", unique_players)
            logger.info("  - Unique pitchers: %s", unique_pitchers)
            logger.info("  - Total statistics: %s", total_stats)
            logger.info("  - Total standings: %s", total_standings)
            logger.info(
                "  - Year range: %s - %s", year_range[0], year_range[1]
            )

    def generate_import_report(self):
        """Generate comprehensive import report"""
        if self.stats.start_time and self.stats.end_time:
            duration = (
                self.stats.end_time - self.stats.start_time
            ).total_seconds()
        else:
            duration = 0.0

        # Calculate success rate safely
        total_processed = (
            self.stats.records_imported + self.stats.records_skipped
        )
        if total_processed > 0:
            success_rate = self.stats.records_imported / total_processed * 100
        else:
            success_rate = 0.0

        report_lines = [
            "=" * 60,
            "DATABASE IMPORT REPORT",
            "=" * 60,
        ]

        if self.stats.start_time:
            report_lines.append(
                f"Import Date: "
                f"{self.stats.start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

        report_lines.extend(
            [
                f"Duration: {duration:.2f} seconds",
                f"Database: {self.db_path}",
                "",
                f"Files Processed: {self.stats.files_processed}",
                f"Records Imported: {self.stats.records_imported}",
                f"Records Skipped: {self.stats.records_skipped}",
                f"Errors Encountered: {self.stats.errors_encountered}",
                "",
                f"Success Rate: {success_rate:.1f}%",
                "=" * 60,
            ]
        )

        report = "\n".join(report_lines)
        print(report)

        # Save report to file
        report_file = self.csv_dir / "import_report.txt"
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(report)

    def export_database_summary(self):
        """Export database summary to CSV for analysis"""
        logger.info("Exporting database summary...")

        with sqlite3.connect(self.db_path) as conn:
            # Export player summary
            player_summary = pd.read_sql_query(
                """
                SELECT
                    year,
                    league,
                    COUNT(DISTINCT name) as unique_players,
                    COUNT(*) as total_records
                FROM baseball_stats
                WHERE stat_type = 'player'
                GROUP BY year, league
                ORDER BY year DESC, league
            """,
                conn,
            )

            player_summary.to_csv(
                self.csv_dir / "player_summary.csv", index=False
            )

            # Export pitcher summary
            pitcher_summary = pd.read_sql_query(
                """
                SELECT
                    year,
                    league,
                    COUNT(DISTINCT name) as unique_pitchers,
                    COUNT(*) as total_records
                FROM baseball_stats
                WHERE stat_type = 'pitcher'
                GROUP BY year, league
                ORDER BY year DESC, league
            """,
                conn,
            )

            pitcher_summary.to_csv(
                self.csv_dir / "pitcher_summary.csv", index=False
            )

            # Export standings summary
            standings_summary = pd.read_sql_query(
                """
                SELECT
                    year,
                    league,
                    division,
                    COUNT(*) as teams,
                    AVG(wins) as avg_wins,
                    AVG(losses) as avg_losses
                FROM team_standings
                GROUP BY year, league, division
                ORDER BY year DESC, league, division
            """,
                conn,
            )

            standings_summary.to_csv(
                self.csv_dir / "standings_summary.csv", index=False
            )

            logger.info("Database summaries exported")


def main():
    """Main execution function

    Demonstrates clean error handling and user feedback.
    """
    try:
        # Create importer instance
        importer = BaseballDatabaseImporter(
            db_path="baseball_stats.db", csv_dir="scraped_data"
        )

        # Import all CSV files
        importer.import_all()

        # Export database summaries
        importer.export_database_summary()

        logger.info("Database import complete!")

    except ValueError as e:
        logger.error("Configuration error: %s", e)
        raise
    except Exception as e:
        logger.error("Import failed: %s", e)
        raise


if __name__ == "__main__":
    main()
