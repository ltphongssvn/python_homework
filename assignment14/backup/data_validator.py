#!/usr/bin/env python3
# data_validator.py
"""
Baseball Database Import Verifier
=================================

A comprehensive validation tool for the baseball statistics database.
This class performs deep data quality checks on structure, content, and
integrity, generating a detailed validation report.
"""

import os
import re
import sqlite3
import sys
import traceback
import unicodedata
from collections import defaultdict
from datetime import datetime

import pandas as pd

# --- Centralized Configuration ---
# The script expects to be run from the 'assignment14' directory.
DB_NAME = "baseball_stats.db"
OUTPUT_DIR = "."  # Reports will be saved in the current directory.
# --- End of Configuration ---


class BaseballDataValidator:
    """
    Performs data quality checks and generates validation reports.
    """

    def __init__(self, db_path=DB_NAME):
        """Initialize the validator and connect to the database."""
        self.db_path = db_path
        self.conn = None
        self.validation_results = []
        self.detailed_findings = []
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        self.stat_ranges = {
            "avg": (0.100, 0.450),
            "obp": (0.100, 0.600),
            "slg": (0.100, 1.000),
            "era": (0.50, 10.0),
            "wp": (0.0, 1.0),
            "hr": (0, 80),
            "rbi": (0, 200),
            "sb": (0, 150),
            "w": (0, 35),
            "sv": (0, 70),
            "k": (0, 400),
            "so": (0, 250),
        }
        self.team_name_variations = {
            "Yankees": ["New York Yankees", "NY Yankees", "NYY"],
            "Red Sox": ["Boston Red Sox", "BOS", "Boston"],
            "Dodgers": ["Los Angeles Dodgers", "LA Dodgers", "LAD"],
        }

    def connect(self):
        """Establish connection to the SQLite database."""
        if not os.path.exists(self.db_path):
            print(f"Error: Database '{self.db_path}' not found.")
            return False
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.text_factory = str
            return True
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            return False

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def log_result(self, category, test_name, status, details=""):
        """Log a validation result to memory and print to console."""
        self.validation_results.append(
            {
                "category": category,
                "test": test_name,
                "status": status,
                "details": details,
                "timestamp": datetime.now(),
            }
        )
        symbol = "✓" if status == "PASS" else "✗" if status == "FAIL" else "⚠"
        print(f"{symbol} {category} - {test_name}: {status}")
        if details and status != "PASS":
            print(f"  > Details: {details}")

    def log_detailed_finding(
        self,
        category,
        issue_type,
        table,
        record_id,
        field,
        value,
        expected,
        severity,
    ):
        """Log detailed findings for the comparison report."""
        self.detailed_findings.append(
            {
                "category": category,
                "issue_type": issue_type,
                "table": table,
                "record_id": record_id,
                "field": field,
                "current_value": value,
                "expected_value": expected,
                "severity": severity,
                "timestamp": datetime.now(),
            }
        )

    def _get_table_columns(self, table_name):
        """Get a list of actual column names for a table."""
        try:
            cursor = self.conn.execute(f"PRAGMA table_info({table_name})")
            return [row[1].lower() for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    def validate_character_encoding(self):
        """Validate character encoding for names with special characters."""
        print("\n" + "=" * 60 + "\nVALIDATING CHARACTER ENCODING\n" + "=" * 60)
        tables = {
            "al_player_review": ["player"],
            "nl_player_review": ["player"],
            "al_pitcher_review": ["player"],
            "nl_pitcher_review": ["player"],
            "al_team_standings": ["team"],
            "nl_team_standings": ["team"],
        }
        pattern = re.compile(r"[^\x00-\x7F]+")  # Non-ASCII characters

        for table, name_cols in tables.items():
            try:
                actual_cols = self._get_table_columns(table)
                issues = 0
                for col in name_cols:
                    if col not in actual_cols:
                        continue
                    query = f"SELECT rowid, {col} FROM {table} WHERE {col} IS NOT NULL"
                    for row_id, value in self.conn.execute(query):
                        if value and pattern.search(value):
                            try:
                                normalized = unicodedata.normalize(
                                    "NFC", value
                                )
                                if normalized != value:
                                    issues += 1
                                    self.log_detailed_finding(
                                        "Encoding",
                                        "Normalization",
                                        table,
                                        row_id,
                                        col,
                                        value,
                                        normalized,
                                        "WARNING",
                                    )
                            except UnicodeError:
                                issues += 1
                                self.log_detailed_finding(
                                    "Encoding",
                                    "Decode Error",
                                    table,
                                    row_id,
                                    col,
                                    value,
                                    "Valid Unicode",
                                    "ERROR",
                                )
                if issues == 0:
                    self.log_result("Encoding", f"{table} names", "PASS")
                else:
                    details = f"{issues} potential encoding issues found"
                    self.log_result(
                        "Encoding", f"{table} names", "WARNING", details
                    )
            except sqlite3.Error as e:
                self.log_result(
                    "Encoding", f"{table} check", "FAIL", f"Query error: {e}"
                )

    def validate_data_types(self):
        """Validate that columns have appropriate data types."""
        print("\n" + "=" * 60 + "\nVALIDATING DATA TYPES\n" + "=" * 60)
        numeric_cols = [
            "avg",
            "obp",
            "slg",
            "era",
            "wp",
            "hr",
            "rbi",
            "sb",
            "w",
            "sv",
            "k",
            "g",
            "ab",
            "h",
            "r",
        ]
        tables = [
            "al_player_review",
            "nl_player_review",
            "al_pitcher_review",
            "nl_pitcher_review",
            "al_team_standings",
            "nl_team_standings",
        ]
        for table in tables:
            try:
                query = f"SELECT COUNT(*) FROM {table} WHERE typeof(year) != 'integer'"
                count = self.conn.execute(query).fetchone()[0]
                if count == 0:
                    self.log_result("Data Types", f"{table} Year", "PASS")
                else:
                    details = f"{count} rows with non-integer year."
                    self.log_result(
                        "Data Types", f"{table} Year", "FAIL", details
                    )

                for col in self._get_table_columns(table):
                    if col in numeric_cols:
                        query = f"""
                        SELECT rowid, {col} FROM {table}
                        WHERE {col} IS NOT NULL AND typeof({col}) = 'text'
                        AND {col} != '' LIMIT 10
                        """
                        text_vals = self.conn.execute(query).fetchall()
                        if text_vals:
                            for r_id, val in text_vals:
                                self.log_detailed_finding(
                                    "Data Types",
                                    "Text in numeric field",
                                    table,
                                    r_id,
                                    col,
                                    f"'{val}' (text)",
                                    "numeric",
                                    "ERROR",
                                )
                            self.log_result(
                                "Data Types",
                                f"{table}.{col}",
                                "FAIL",
                                "Found text values in numeric column",
                            )
            except sqlite3.OperationalError as e:
                self.log_result(
                    "Data Types",
                    f"Table check for {table}",
                    "FAIL",
                    f"Query failed: {e}",
                )

    def validate_missing_data(self):
        """Check for NULL values where data should exist."""
        print("\n" + "=" * 60 + "\nVALIDATING MISSING DATA\n" + "=" * 60)
        required_fields = {
            "al_player_review": ["player", "year", "team"],
            "nl_player_review": ["player", "year", "team"],
            "al_pitcher_review": ["player", "year", "team"],
            "nl_pitcher_review": ["player", "year", "team"],
            "al_team_standings": ["team", "year", "w", "l"],
            "nl_team_standings": ["team", "year", "w", "l"],
        }
        for table, fields in required_fields.items():
            try:
                cols = self._get_table_columns(table)
                for field in fields:
                    if field not in cols:
                        continue
                    query = f"SELECT COUNT(*) FROM {table} WHERE {field} IS NULL OR {field} = ''"
                    null_count = self.conn.execute(query).fetchone()[0]
                    if null_count == 0:
                        self.log_result(
                            "Missing Data", f"{table}.{field}", "PASS"
                        )
                    else:
                        details = (
                            f"{null_count} NULL/empty values in required field"
                        )
                        self.log_result(
                            "Missing Data", f"{table}.{field}", "FAIL", details
                        )
            except sqlite3.Error as e:
                self.log_result(
                    "Missing Data",
                    f"{table} check",
                    "FAIL",
                    f"Query error: {e}",
                )

    def validate_duplicates(self):
        """Check for duplicate entries."""
        print("\n" + "=" * 60 + "\nVALIDATING DUPLICATE ENTRIES\n" + "=" * 60)
        unique_constraints = {
            "al_player_review": ["player", "year", "team"],
            "nl_player_review": ["player", "year", "team"],
            "al_pitcher_review": ["player", "year", "team"],
            "nl_pitcher_review": ["player", "year", "team"],
            "al_team_standings": ["team", "year"],
            "nl_team_standings": ["team", "year"],
        }
        for table, key_fields in unique_constraints.items():
            try:
                cols = self._get_table_columns(table)
                if not all(field in cols for field in key_fields):
                    continue
                group_fields = ", ".join(key_fields)
                query = f"""
                SELECT {group_fields}, COUNT(*) as cnt, GROUP_CONCAT(rowid)
                FROM {table}
                WHERE {' AND '.join(f"{f} IS NOT NULL" for f in key_fields)}
                GROUP BY {group_fields} HAVING COUNT(*) > 1
                """
                duplicates = self.conn.execute(query).fetchall()
                if not duplicates:
                    self.log_result("Duplicates", f"{table}", "PASS")
                else:
                    for dup in duplicates[:5]:
                        self.log_detailed_finding(
                            "Duplicates",
                            "Duplicate record",
                            table,
                            dup[-1],
                            ", ".join(key_fields),
                            str(dup[:-2]),
                            "Unique",
                            "ERROR",
                        )
                    self.log_result(
                        "Duplicates",
                        f"{table}",
                        "FAIL",
                        f"{len(duplicates)} duplicate groups found",
                    )
            except sqlite3.Error as e:
                self.log_result(
                    "Duplicates", f"{table} check", "FAIL", f"Query error: {e}"
                )

    def validate_naming_consistency(self):
        """Check for inconsistent naming conventions."""
        print("\n" + "=" * 60 + "\nVALIDATING NAMING CONSISTENCY\n" + "=" * 60)
        team_tables = [
            "al_player_review",
            "nl_player_review",
            "al_pitcher_review",
            "nl_pitcher_review",
            "al_team_standings",
            "nl_team_standings",
        ]
        all_team_names = set()
        team_name_mapping = defaultdict(list)
        for table in team_tables:
            try:
                if "team" in self._get_table_columns(table):
                    query = f"SELECT DISTINCT team FROM {table} WHERE team IS NOT NULL"
                    for (team,) in self.conn.execute(query):
                        all_team_names.add(team)
                        team_name_mapping[team.lower().strip()].append(
                            (team, table)
                        )
            except sqlite3.Error:
                pass

        inconsistencies = 0
        for _, occurrences in team_name_mapping.items():
            unique_names = {name for name, _ in occurrences}
            if len(unique_names) > 1:
                inconsistencies += 1
                self.log_detailed_finding(
                    "Naming Consistency",
                    "Inconsistent team names",
                    "Multiple tables",
                    "N/A",
                    "team",
                    str(list(unique_names)),
                    "Single consistent name",
                    "WARNING",
                )
        if inconsistencies == 0:
            self.log_result("Naming Consistency", "Team names", "PASS")
        else:
            self.log_result(
                "Naming Consistency",
                "Team names",
                "WARNING",
                f"{inconsistencies} potential naming inconsistencies found",
            )

    def generate_report(self):
        """Generate a comprehensive validation report."""
        print("\n" + "=" * 80 + "\nVALIDATION SUMMARY REPORT\n" + "=" * 80)
        if not self.validation_results:
            print("No validation results to report.")
            return

        results_df = pd.DataFrame(self.validation_results)
        summary_df = (
            results_df.groupby(["category", "status"])
            .size()
            .unstack(fill_value=0)
        )

        print("\nValidation Results Summary:")
        print(summary_df.to_string())

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = os.path.join(
            OUTPUT_DIR, f"validation_report_{timestamp}.csv"
        )
        xlsx_file = csv_file.replace(".csv", ".xlsx")

        sheets = {
            "Summary": summary_df.reset_index(),
            "All Results": results_df,
        }
        if self.detailed_findings:
            sheets["Detailed Findings"] = pd.DataFrame(self.detailed_findings)

        results_df.to_csv(csv_file, index=False)
        print(f"\nDetailed CSV report saved to: {csv_file}")

        try:
            with pd.ExcelWriter(xlsx_file, engine="openpyxl") as writer:
                for sheet_name, df_sheet in sheets.items():
                    df_sheet.to_excel(
                        writer, sheet_name=sheet_name, index=False
                    )
            print(f"Excel report with summary saved to: {xlsx_file}")
        except ImportError:
            print(
                "\nCould not create Excel report. Please install 'openpyxl'."
            )
            print("Run: pip install openpyxl")

        total_tests = len(results_df)
        passed_tests = len(results_df[results_df["status"] == "PASS"])
        success_rate = (
            (passed_tests / total_tests) * 100 if total_tests > 0 else 100
        )
        print(f"\nOverall Success Rate: {success_rate:.2f}%")

    def run_all_validations(self):
        """Execute all validation steps in sequence."""
        if not self.connect():
            print("Failed to connect to database. Exiting.")
            return

        print(f"\nRunning validation suite on '{self.db_path}'...")
        try:
            self.validate_data_types()
            # self.validate_character_encoding() # Disabled for now
            self.validate_missing_data()
            self.validate_duplicates()
            self.validate_naming_consistency()
            self.generate_report()
        except (sqlite3.Error, pd.errors.DatabaseError) as e:
            print(f"A database error occurred during validation: {e}")
        except (IOError, ValueError, TypeError, KeyError):
            print("An unexpected processing error occurred:")
            traceback.print_exc()
        finally:
            self.close()


def main():
    """Main function to run the validation process."""
    db_path = sys.argv[1] if len(sys.argv) > 1 else DB_NAME
    print("Baseball Statistics Database Validator")
    print("=" * 50)
    print(f"Database path: {db_path}")

    validator = BaseballDataValidator(db_path)
    validator.run_all_validations()


if __name__ == "__main__":
    main()
