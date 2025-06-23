#!/usr/bin/env python3
# _02_database_import_program_1876_2024_v1.py
"""
Database Importer - Final Refactored Version with Comparison Report
===================================================================

This program intelligently discovers and imports all year-specific CSV files,
and concludes by generating a validation report that compares the imported
data totals against the source scraper's summary log.
"""

import csv
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s",
)
logger = logging.getLogger(__name__)


class DatabaseImporter:
    """Imports all yearly baseball data CSVs and generates a comparison report."""

    def __init__(self, db_path: str, input_dir: str):
        self.db_path = Path(db_path)
        self.input_dir = Path(input_dir)
        self.stats: Dict[str, Any] = {
            "start_time": datetime.now(),
            "files_processed": 0,
            "files_failed": 0,
            "standings_imported": 0,
            "player_stats_imported": 0,
            "pitcher_stats_imported": 0,
        }
        self.conn = sqlite3.connect(self.db_path)
        logger.info(
            f"Database connection established to {self.db_path.resolve()}"
        )

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.")

    def create_and_import_all_data(self):
        """Discovers and imports all standings and stats files."""
        self._create_standings_table()
        standings_files = sorted(self.input_dir.glob("*_standings_*.csv"))
        logger.info(f"Found {len(standings_files)} standings files to import.")
        for file_path in standings_files:
            self._import_standings_file(file_path)

        stats_files = sorted(self.input_dir.glob("*_stats_*.csv"))
        logger.info(f"Found {len(stats_files)} statistics files to process.")
        self._consolidate_and_import_stats(stats_files)

    def _create_standings_table(self):
        logger.info("Ensuring 'team_standings' table exists...")
        sql = """
        CREATE TABLE IF NOT EXISTS team_standings (
            id INTEGER PRIMARY KEY, year INTEGER, league TEXT, division TEXT,
            team TEXT, wins INTEGER, losses INTEGER, winning_percentage REAL,
            games_back TEXT, UNIQUE(year, league, team)
        );"""
        with self.conn:
            self.conn.execute(sql)

    def _import_standings_file(self, file_path: Path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                data = list(reader)
            if not data:
                return

            newly_inserted_rows = 0
            with self.conn:
                cursor = self.conn.cursor()
                for row in data:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO team_standings (year, league, division, team, wins, losses, winning_percentage, games_back)
                        VALUES (:year, :league, :division, :team, :wins, :losses, :winning_percentage, :games_back)
                    """,
                        row,
                    )
                    newly_inserted_rows += cursor.rowcount

            self.stats["standings_imported"] += newly_inserted_rows
            self.stats["files_processed"] += 1
            logger.info(
                f"Processed {file_path.name}: {len(data)} records found, {newly_inserted_rows} new records inserted."
            )
        except Exception as e:
            logger.error(f"Failed to import {file_path.name}: {e}")
            self.stats["files_failed"] += 1

    def _consolidate_and_import_stats(self, stats_files: List[Path]):
        logger.info(f"Consolidating {len(stats_files)} stats files...")
        self._process_stats_category(stats_files, "player_stats", "player")
        self._process_stats_category(stats_files, "pitcher_stats", "pitcher")

    def _process_stats_category(
        self, files: List[Path], table_name: str, stat_type: str
    ):
        logger.info(f"Processing data for '{table_name}' table...")
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY, year INTEGER, league TEXT, stat_type TEXT,
            statistic TEXT, name TEXT, team TEXT, value TEXT,
            UNIQUE(year, league, statistic, name, team)
        );"""
        with self.conn:
            self.conn.execute(create_sql)

        total_new_records = 0
        for file_path in files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows_to_insert = [
                        row
                        for row in reader
                        if row.get("stat_type") == stat_type
                    ]
                if not rows_to_insert:
                    continue

                with self.conn:
                    cursor = self.conn.cursor()
                    cursor.executemany(
                        f"""
                        INSERT OR IGNORE INTO {table_name} (year, league, stat_type, statistic, name, team, value)
                        VALUES (:year, :league, :stat_type, :statistic, :name, :team, :value)
                    """,
                        rows_to_insert,
                    )
                    total_new_records += cursor.rowcount
                self.stats["files_processed"] += 1
            except Exception as e:
                logger.error(
                    f"Failed during import of {file_path.name} into {table_name}: {e}"
                )
                self.stats["files_failed"] += 1

        if stat_type == "player":
            self.stats["player_stats_imported"] = total_new_records
        else:
            self.stats["pitcher_stats_imported"] = total_new_records
        logger.info(
            f"Completed processing for '{table_name}'. Total new records inserted: {total_new_records}"
        )

    def generate_import_summary(self):
        """Generates a summary of only the database import actions."""
        # This function is kept to provide the original summary as well
        logger.info("--- Database Import Summary ---")
        # (Content is now merged into the main comparison for final output)

    # --- NEW METHOD: Loads and parses the scraper's summary file ---
    def load_scraper_summary(
        self, summary_path: Path
    ) -> Optional[Dict[str, int]]:
        logger.info(f"Loading scraper summary from: {summary_path}")
        if not summary_path.exists():
            logger.error(f"Scraper summary file not found at {summary_path}")
            return None
        try:
            with open(summary_path, "r", encoding="utf-8") as f:
                content = f.read()

            def parse_value(pattern):
                match = re.search(pattern, content)
                return int(match.group(1).replace(",", "")) if match else 0

            return {
                "player_stats": parse_value(r"Player Statistics:\s*([\d,]+)"),
                "pitcher_stats": parse_value(
                    r"Pitcher Statistics:\s*([\d,]+)"
                ),
                "standings": parse_value(r"Team Standings:\s*([\d,]+)"),
            }
        except Exception as e:
            logger.error(f"Failed to parse scraper summary file: {e}")
            return None

    # --- NEW METHOD: Generates the final comparison report ---
    def generate_comparison_report(self, scraper_stats: Dict[str, int]):
        importer_stats = self.stats

        # Prepare data for the comparison table
        data_map = {
            "Player Statistics": ("player_stats", "player_stats_imported"),
            "Pitcher Statistics": ("pitcher_stats", "pitcher_stats_imported"),
            "Team Standings": ("standings", "standings_imported"),
        }

        table_rows = []
        for label, (scraped_key, imported_key) in data_map.items():
            scraped = scraper_stats.get(scraped_key, 0)
            imported = importer_stats.get(imported_key, 0)
            rate = (imported / scraped * 100) if scraped > 0 else 100.0
            table_rows.append((label, scraped, imported, f"{rate:.1f}%"))

        total_scraped = sum(scraper_stats.values())
        total_imported = sum(
            importer_stats[k]
            for k in [
                "player_stats_imported",
                "pitcher_stats_imported",
                "standings_imported",
            ]
        )
        total_rate = (
            (total_imported / total_scraped * 100)
            if total_scraped > 0
            else 100.0
        )

        # Format the table for printing
        header = f"| {'Data Type':<25} | {'Records Scraped':>18} | {'Records Imported':>18} | {'Success Rate':>15} |"
        separator = (
            "+"
            + "-" * 27
            + "+"
            + "-" * 20
            + "+"
            + "-" * 20
            + "+"
            + "-" * 17
            + "+"
        )

        report_lines = [
            f"\n\n{'=' * 88}",
            "DATA INTEGRITY VALIDATION: SCRAPER vs. DATABASE",
            f"{'=' * 88}",
            separator,
            header,
            separator,
        ]

        for label, scraped, imported, rate_str in table_rows:
            report_lines.append(
                f"| {label:<25} | {scraped:>18,} | {imported:>18,} | {rate_str:>15} |"
            )

        report_lines.append(separator)
        report_lines.append(
            f"| {'TOTAL RECORDS':<25} | {total_scraped:>18,} | {total_imported:>18,} | {f'{total_rate:.2f}%':>15} |"
        )
        report_lines.append(separator)

        final_report = "\n".join(report_lines)
        logger.info(final_report)

        summary_file = (
            self.db_path.parent
            / f"db_import_comparison_summary_{datetime.now():%Y%m%d_%H%M%S}.txt"
        )
        try:
            with open(summary_file, "w", encoding="utf-8") as f:
                f.write(
                    final_report.replace(" |", "|").replace("| ", "|")
                )  # Clean up for file
            logger.info(f"Comparison report saved to {summary_file}")
        except IOError as e:
            logger.error(f"Failed to save comparison report: {e}")


def main():
    """Main function to orchestrate the database import and final comparison."""
    logger.info("Starting database import process for all yearly files.")

    # Define paths
    base_dir = Path(__file__).parent
    scraper_output_dir = base_dir / "baseball_data_refactored"
    db_importer = DatabaseImporter(
        db_path=str(base_dir / "baseball_history.db"),
        input_dir=str(scraper_output_dir),
    )

    try:
        # Run the full import process
        db_importer.create_and_import_all_data()
        logger.info("Database import process completed.")

        # --- NEW: Load scraper summary and generate comparison report ---
        scraper_summary_path = Path(
            "C:/Users/LENOVO/python_homework/assignment14/baseball_data_refactored/scraping_summary_20250621_214150.txt"
        )
        scraper_stats = db_importer.load_scraper_summary(scraper_summary_path)

        if scraper_stats:
            db_importer.generate_comparison_report(scraper_stats)
        else:
            logger.error(
                "Could not generate comparison report because scraper summary was not found or could not be parsed."
            )

    except Exception as e:
        logger.error(f"The database import process failed: {e}", exc_info=True)
    finally:
        db_importer.close()


if __name__ == "__main__":
    main()
