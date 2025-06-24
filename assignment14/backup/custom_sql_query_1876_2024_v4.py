#!/usr/bin/env python3
# custom_sql_query_1876_2024_v4.py
"""
Custom SQL Query Explorer for Baseball Statistics Database
=========================================================

A comprehensive collection of SQL queries for exploring the baseball_history.db database.
This tool demonstrates various SQL techniques from basic exploration to advanced analytics.

The queries are organized into categories, each building upon previous concepts
to create a learning journey through SQL and baseball statistics.
"""

import argparse
import logging
import math
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class StddevAggregate:
    """
    A custom aggregate class for calculating population standard deviation in SQLite.
    This is used as a fallback for older SQLite versions.
    """

    def __init__(self):
        self.data = []

    def step(self, value):
        """Called for each row. Adds the value to our list."""
        if value is not None:
            self.data.append(float(value))

    def finalize(self):
        """Called at the end. Calculates and returns the standard deviation."""
        if len(self.data) < 2:
            return None

        n = len(self.data)
        mean = sum(self.data) / n
        variance = sum((x - mean) ** 2 for x in self.data) / n
        return math.sqrt(variance)


class SQLQueryExplorer:
    """Manages and executes categorized SQL queries for baseball database exploration."""

    def __init__(self, db_path: str = "baseball_history.db"):
        """Initialize the query explorer with database connection."""
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")

        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

        try:
            self.conn.execute("SELECT SQRT(4)")
            self.has_math_functions = True
        except sqlite3.OperationalError:
            self.has_math_functions = False
            self.conn.create_function("SQRT", 1, math.sqrt)
            self.conn.create_function("POWER", 2, math.pow)
            self.conn.create_aggregate("STDDEV", 1, StddevAggregate)

        print(f"Connected to database: {self.db_path.name}")
        if not self.has_math_functions:
            print("Note: Using custom math functions for compatibility")
        self._initialize_queries()

    def _initialize_queries(self):
        """Initialize all SQL queries organized by category."""
        self.query_categories = {
            "1": {
                "name": "Database Structure Overview",
                "description": "Understand the scope and structure of your data",
                "queries": {
                    "a": {
                        "name": "Data Range Overview",
                        "description": "See the temporal and record scope of each table",
                        "sql": """
                            SELECT 'player_stats' as table_name,
                                   MIN(year) as earliest_year,
                                   MAX(year) as latest_year,
                                   COUNT(DISTINCT year) as total_years,
                                   COUNT(DISTINCT name) as unique_players,
                                   COUNT(*) as total_records
                            FROM player_stats
                            UNION ALL
                            SELECT 'pitcher_stats',
                                   MIN(year), MAX(year), COUNT(DISTINCT year),
                                   COUNT(DISTINCT name), COUNT(*)
                            FROM pitcher_stats
                            UNION ALL
                            SELECT 'team_standings',
                                   MIN(year), MAX(year), COUNT(DISTINCT year),
                                   COUNT(DISTINCT team), COUNT(*)
                            FROM team_standings
                        """,
                    },
                    "b": {
                        "name": "Available Player Statistics",
                        "description": "List all batting statistics tracked in the database",
                        "sql": """
                            SELECT statistic, COUNT(*) as occurrences,
                                   COUNT(DISTINCT year) as years_tracked,
                                   MIN(year) as first_year, MAX(year) as last_year
                            FROM player_stats
                            GROUP BY statistic ORDER BY occurrences DESC
                        """,
                    },
                    "c": {
                        "name": "Available Pitcher Statistics",
                        "description": "List all pitching statistics tracked in the database",
                        "sql": """
                            SELECT statistic,
                                   COUNT(*) as occurrences,
                                   COUNT(DISTINCT year) as years_tracked,
                                   MIN(year) as first_year,
                                   MAX(year) as last_year
                            FROM pitcher_stats
                            GROUP BY statistic
                            ORDER BY occurrences DESC
                        """,
                    },
                },
            },
            "2": {
                "name": "Record-Breaking Performances",
                "description": "Find the most exceptional single-season achievements",
                "queries": {
                    "a": {
                        "name": "Top Home Run Seasons",
                        "description": "The highest single-season home run totals in history",
                        "sql": """
                            SELECT year, name, team, value as home_runs, league
                            FROM player_stats
                            WHERE statistic = 'Home Runs'
                            ORDER BY CAST(value AS INTEGER) DESC
                            LIMIT 25
                        """,
                    },
                    "b": {
                        "name": "Lowest ERAs",
                        "description": "Best earned run averages in baseball history",
                        "sql": """
                            SELECT year, name, team, value as ERA, league
                            FROM pitcher_stats
                            WHERE statistic = 'ERA'
                              AND CAST(value AS REAL) > 0
                            ORDER BY CAST(value AS REAL) ASC
                            LIMIT 25
                        """,
                    },
                    "c": {
                        "name": "Highest Batting Averages",
                        "description": "Best single-season batting averages",
                        "sql": """
                            SELECT year, name, team, value as batting_avg, league
                            FROM player_stats
                            WHERE statistic = 'Batting Average'
                              AND CAST(value AS REAL) <= 1.0
                            ORDER BY CAST(value AS REAL) DESC
                            LIMIT 25
                        """,
                    },
                },
            },
            "3": {
                "name": "Historical Trends Analysis",
                "description": "See how baseball has evolved over the decades",
                "queries": {
                    "a": {
                        "name": "Home Run Evolution by Decade",
                        "description": "Track how home run hitting has changed over time",
                        "sql": """
                            SELECT
                                (year / 10) * 10 || 's' as decade,
                                SUM(CAST(value AS INTEGER)) as total_home_runs,
                                ROUND(AVG(CAST(value AS REAL)), 1) as avg_hr_leader,
                                MAX(CAST(value AS INTEGER)) as max_in_season
                            FROM player_stats
                            WHERE statistic = 'Home Runs'
                            GROUP BY (year / 10) * 10
                            ORDER BY decade
                        """,
                    },
                    "b": {
                        "name": "Pitching Evolution - ERA by Decade",
                        "description": "How pitching effectiveness has changed over time",
                        "sql": """
                            SELECT
                                (year / 10) * 10 || 's' as decade,
                                ROUND(AVG(CAST(value AS REAL)), 2) as avg_era_leader,
                                ROUND(MIN(CAST(value AS REAL)), 2) as best_era_in_decade
                            FROM pitcher_stats
                            WHERE statistic = 'ERA'
                              AND CAST(value AS REAL) > 0
                            GROUP BY (year / 10) * 10
                            ORDER BY decade
                        """,
                    },
                    "c": {
                        "name": "Offensive Production Trends",
                        "description": "Multiple offensive statistics by era",
                        "sql": """
                            SELECT
                                (year / 10) * 10 || 's' as decade,
                                statistic,
                                SUM(CAST(value AS INTEGER)) as total,
                                ROUND(AVG(CAST(value AS REAL)), 2) as average
                            FROM player_stats
                            WHERE statistic IN ('Home Runs', 'RBI', 'Hits', 'Runs')
                              AND year >= 1920
                            GROUP BY decade, statistic
                            ORDER BY decade, statistic
                        """,
                    },
                },
            },
            "4": {
                "name": "League Comparisons",
                "description": "Compare American League vs National League",
                "queries": {
                    "a": {
                        "name": "Offensive Statistics by League (Since 2000)",
                        "description": "Compare hitting between AL and NL in recent years",
                        "sql": """
                            SELECT
                                year, league, statistic,
                                SUM(CAST(value AS INTEGER)) as total,
                                ROUND(AVG(CAST(value AS REAL)), 2) as average
                            FROM player_stats
                            WHERE statistic IN ('Home Runs', 'RBI', 'Hits') AND year >= 2000
                            GROUP BY year, league, statistic
                            ORDER BY year DESC, statistic, league
                        """,
                    },
                    "b": {
                        "name": "League Competitive Balance (Std Dev)",
                        "description": "Which league has been more balanced based on win % std. dev.",
                        "sql": """
                            -- REFACTORED: This query now uses the STDDEV aggregate function
                            -- for simplicity and correctness, replacing a complex manual calculation.
                            SELECT
                                year, league,
                                COUNT(team) as teams,
                                ROUND(AVG(winning_percentage), 3) as avg_win_pct,
                                ROUND(STDDEV(winning_percentage), 3) as win_pct_stddev
                            FROM team_standings
                            WHERE year >= 2000
                            GROUP BY year, league
                            ORDER BY year DESC, league
                        """,
                    },
                },
            },
            "5": {
                "name": "Player Career Analysis",
                "description": "Analyze careers spanning multiple seasons",
                "queries": {
                    "a": {
                        "name": "Longest Careers",
                        "description": "Players with the most seasons in the database",
                        "sql": """
                            SELECT
                                name,
                                COUNT(DISTINCT year) as seasons_played,
                                MIN(year) as first_season,
                                MAX(year) as last_season
                            FROM player_stats
                            GROUP BY name
                            HAVING COUNT(DISTINCT year) >= 15
                            ORDER BY seasons_played DESC, first_season
                            LIMIT 50
                        """,
                    },
                    "b": {
                        "name": "Career Home Run Leaders",
                        "description": "Players with most career home runs in the database",
                        "sql": """
                            WITH career_totals AS (
                                SELECT
                                    name,
                                    SUM(CASE WHEN statistic = 'Home Runs' THEN CAST(value AS INTEGER) ELSE 0 END) as career_hr,
                                    SUM(CASE WHEN statistic = 'RBI' THEN CAST(value AS INTEGER) ELSE 0 END) as career_rbi,
                                    SUM(CASE WHEN statistic = 'Hits' THEN CAST(value AS INTEGER) ELSE 0 END) as career_hits,
                                    COUNT(DISTINCT year) as seasons
                                FROM player_stats
                                WHERE statistic IN ('Home Runs', 'RBI', 'Hits')
                                GROUP BY name
                            )
                            SELECT * FROM career_totals
                            WHERE career_hr > 300
                            ORDER BY career_hr DESC
                            LIMIT 50
                        """,
                    },
                    "c": {
                        "name": "Most Consistent Hitters (.300+ career avg, 10+ seasons)",
                        "description": "Players with best average performance over long careers",
                        "sql": """
                            SELECT
                                name,
                                COUNT(DISTINCT year) as seasons,
                                ROUND(AVG(CAST(value AS REAL)), 3) as career_avg,
                                ROUND(MAX(CAST(value AS REAL)) - MIN(CAST(value AS REAL)), 3) as consistency_range
                            FROM player_stats
                            WHERE statistic = 'Batting Average'
                              AND CAST(value AS REAL) > 0
                            GROUP BY name
                            HAVING COUNT(DISTINCT year) >= 10
                               AND AVG(CAST(value AS REAL)) >= 0.300
                            ORDER BY career_avg DESC
                            LIMIT 25
                        """,
                    },
                },
            },
            "6": {
                "name": "Team Dynasty Analysis",
                "description": "Identify periods of team dominance",
                "queries": {
                    "a": {
                        "name": "Consecutive Winning Seasons",
                        "description": "Find the longest streaks of seasons with a > .500 record",
                        "sql": """
                            WITH team_streaks AS (
                                SELECT
                                    team, year,
                                    CASE WHEN winning_percentage > 0.500 THEN 1 ELSE 0 END as winning_season,
                                    year - ROW_NUMBER() OVER (PARTITION BY team,
                                        CASE WHEN winning_percentage > 0.500 THEN 1 ELSE 0 END
                                        ORDER BY year) as streak_group
                                FROM team_standings
                            ),
                            winning_streaks AS (
                                SELECT
                                    team,
                                    MIN(year) as streak_start,
                                    MAX(year) as streak_end,
                                    COUNT(*) as consecutive_winning_seasons
                                FROM team_streaks
                                WHERE winning_season = 1
                                GROUP BY team, streak_group
                            )
                            SELECT * FROM winning_streaks
                            WHERE consecutive_winning_seasons >= 10
                            ORDER BY consecutive_winning_seasons DESC, streak_start DESC
                            LIMIT 25
                        """,
                    },
                    "b": {
                        "name": "Best Single-Season Teams",
                        "description": "Teams with the highest winning percentages",
                        "sql": """
                            SELECT
                                year, team, league, wins, losses, winning_percentage
                            FROM team_standings
                            WHERE winning_percentage >= 0.650
                            ORDER BY winning_percentage DESC, year DESC
                            LIMIT 50
                        """,
                    },
                },
            },
            "7": {
                "name": "Cross-Table Analysis",
                "description": "Combine player and team data for deeper insights",
                "queries": {
                    "a": {
                        "name": "Star Players on Great Teams (40+ HR, .600+ Team Win %)",
                        "description": "Great individual power seasons on great teams",
                        "sql": """
                            SELECT
                                ps.year, ps.name, ps.team,
                                ps.value as home_runs,
                                ts.winning_percentage
                            FROM player_stats ps
                            JOIN team_standings ts ON ps.year = ts.year AND ps.team = ts.team
                            WHERE ps.statistic = 'Home Runs'
                              AND ts.winning_percentage >= 0.600
                              AND CAST(ps.value AS INTEGER) >= 40
                            ORDER BY ps.year DESC, CAST(ps.value AS INTEGER) DESC
                        """,
                    },
                    "b": {
                        "name": "Pitcher ERA on Winning vs. Losing Teams",
                        "description": "Do pitchers perform better on winning teams?",
                        "sql": """
                            SELECT
                                CASE
                                    WHEN ts.winning_percentage >= 0.500 THEN 'Winning Team (>=.500)'
                                    ELSE 'Losing Team (<.500)'
                                END as team_type,
                                COUNT(DISTINCT ps.name) as pitcher_count,
                                ROUND(AVG(CAST(ps.value AS REAL)), 2) as avg_era
                            FROM pitcher_stats ps
                            JOIN team_standings ts ON ps.year = ts.year AND ps.team = ts.team
                            WHERE ps.statistic = 'ERA' AND CAST(ps.value AS REAL) > 0
                            GROUP BY team_type
                            ORDER BY team_type
                        """,
                    },
                },
            },
            "8": {
                "name": "Statistical Anomalies",
                "description": "Find outliers and unusual performances",
                "queries": {
                    "a": {
                        "name": "Extreme Batting Avg Outliers (Z-Score)",
                        "description": "Find seasons > 3 standard deviations from the yearly mean.",
                        "sql": """
                            -- REFACTORED: This query now uses multiple CTEs to correctly
                            -- calculate a Z-score, avoiding the illegal nesting of window functions.
                            WITH yearly_stats AS (
                                SELECT
                                    year, name, CAST(value AS REAL) as batting_avg
                                FROM player_stats
                                WHERE statistic = 'Batting Average' AND CAST(value AS REAL) > 0
                            ),
                            yearly_calcs AS (
                                SELECT
                                    year, name, batting_avg,
                                    AVG(batting_avg) OVER (PARTITION BY year) as year_avg
                                FROM yearly_stats
                            ),
                            yearly_z_score AS (
                                SELECT
                                    year, name, batting_avg, year_avg,
                                    -- FIX: Removed extra parenthesis before 'as'
                                    SQRT(AVG(POWER(batting_avg - year_avg, 2)) OVER (PARTITION BY year)) as year_stddev
                                FROM yearly_calcs
                            )
                            SELECT
                                year, name,
                                ROUND(batting_avg, 3) as batting_avg,
                                ROUND(year_avg, 3) as year_avg,
                                ROUND((batting_avg - year_avg) / NULLIF(year_stddev, 0), 2) as z_score
                            FROM yearly_z_score
                            WHERE ABS((batting_avg - year_avg) / NULLIF(year_stddev, 0)) > 3.0
                            ORDER BY z_score DESC;
                        """,
                    },
                    "b": {
                        "name": "Biggest Year-to-Year HR Improvements",
                        "description": "Players who dramatically improved their Home Run total",
                        "sql": """
                            WITH yoy_comparison AS (
                                SELECT
                                    p1.name, p1.year as year1, p2.year as year2,
                                    CAST(p1.value AS INTEGER) as hr_year1,
                                    CAST(p2.value AS INTEGER) as hr_year2
                                FROM player_stats p1
                                JOIN player_stats p2 ON p1.name = p2.name
                                    AND p1.statistic = p2.statistic AND p2.year = p1.year + 1
                                WHERE p1.statistic = 'Home Runs'
                            )
                            SELECT
                                name, year1, hr_year1, year2, hr_year2,
                                (hr_year2 - hr_year1) as improvement
                            FROM yoy_comparison
                            WHERE (hr_year2 - hr_year1) >= 20
                            ORDER BY improvement DESC
                            LIMIT 25;
                        """,
                    },
                },
            },
            "9": {
                "name": "Data Quality Checks",
                "description": "Verify data integrity and find potential issues",
                "queries": {
                    "a": {
                        "name": "Data Validation Report",
                        "description": "Check for suspicious or invalid data",
                        "sql": """
                            SELECT 'Invalid Batting Averages (> 1.0)' as issue, COUNT(*) as count
                            FROM player_stats
                            WHERE statistic = 'Batting Average' AND CAST(value AS REAL) > 1.0
                            UNION ALL
                            SELECT 'Duplicate Player Stat Records', COUNT(*)
                            FROM (
                                SELECT year, name, team, statistic, COUNT(*)
                                FROM player_stats
                                GROUP BY year, name, team, statistic HAVING COUNT(*) > 1
                            );
                        """,
                    },
                },
            },
        }

    def _display_results(self, cursor: sqlite3.Cursor, query_name: str):
        """Format and display query results in a readable table format."""
        try:
            rows = cursor.fetchall()
            if not rows:
                print(f"\n✗ No results found for: {query_name}")
                return

            headers = [description[0] for description in cursor.description]
            data = []
            col_widths = [len(h) for h in headers]

            for row in rows:
                row_data = []
                for i, value in enumerate(row):
                    if value is None:
                        formatted = "NULL"
                    elif isinstance(value, float):
                        if (
                            "avg" in headers[i].lower()
                            or "pct" in headers[i].lower()
                            or "stddev" in headers[i].lower()
                        ):
                            formatted = f"{value:.3f}"
                        elif "era" in headers[i].lower():
                            formatted = f"{value:.2f}"
                        else:
                            formatted = f"{value:,.2f}"
                    elif isinstance(value, int):
                        formatted = f"{value:,}"
                    else:
                        formatted = str(value)

                    row_data.append(formatted)
                    col_widths[i] = max(col_widths[i], len(formatted))
                data.append(row_data)

            print(
                f"\n✓ Results for: {query_name} ({len(rows)} rows returned)\n"
            )
            header_line = " | ".join(
                f"{h:<{col_widths[i]}}" for i, h in enumerate(headers)
            )
            print(header_line)
            print("-" * len(header_line))

            display_limit = 50
            for i, row in enumerate(data[:display_limit]):
                print(
                    " | ".join(
                        f"{cell:<{col_widths[j]}}"
                        for j, cell in enumerate(row)
                    )
                )

            if len(data) > display_limit:
                print(
                    f"\n... ({len(data) - display_limit} more rows not displayed)"
                )

        except Exception as e:
            print(f"\n✗ Error displaying results: {e}")
            logging.error(f"Display error: {e}", exc_info=True)

    def execute_query(self, sql: str, query_name: str = "Custom Query"):
        """Execute a single SQL query and display results."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            self._display_results(cursor, query_name)
        except sqlite3.Error as e:
            print(f"\n✗ SQL Error in '{query_name}': {e}")
            logging.error(
                f"SQL error for query '{query_name}': {e}\nQuery:\n{sql}"
            )

    def run_interactive(self):
        """Run the interactive query explorer interface."""
        print("\n" + "=" * 80)
        print("Baseball Statistics Database - SQL Query Explorer")
        print("=" * 80)
        print(
            "\nThis tool provides a curated collection of SQL queries that demonstrate"
        )
        print("various techniques for exploring baseball statistics data.")
        print(
            "\nEach category builds upon previous concepts, creating a learning journey"
        )
        print("through both SQL and baseball analytics.")

        while True:
            print("\n" + "-" * 60)
            print("Query Categories:")
            print("-" * 60)

            for key, category in sorted(self.query_categories.items()):
                print(f"{key}. {category['name']}")
                print(f"   {category['description']}")

            print("\nOptions:")
            print("E. Execute custom SQL query")
            print("A. Run all queries in a category")
            print("Q. Quit")

            choice = input("\nSelect category (or E/A/Q): ").strip().upper()

            if choice == "Q":
                break
            elif choice == "E":
                self._custom_query_mode()
            elif choice == "A":
                self._run_all_in_category()
            elif choice in self.query_categories:
                self._explore_category(choice)
            else:
                print("Invalid choice. Please try again.")

    def _explore_category(self, category_key: str):
        """Explore queries within a selected category."""
        category = self.query_categories[category_key]

        while True:
            print(f"\n{'=' * 60}\nCategory: {category['name']}\n{'=' * 60}")
            for key, query in sorted(category["queries"].items()):
                print(f"\n{key}. {query['name']}\n   {query['description']}")
            print(
                "\nOptions:\nB. Back to main menu\n*. Run all queries in this category"
            )

            choice = input("\nSelect query (or B/*): ").strip().lower()
            if choice == "b":
                break
            elif choice == "*":
                for key, query in sorted(category["queries"].items()):
                    print(f"\n{'─' * 60}")
                    self.execute_query(
                        query["sql"], f"{category['name']} - {query['name']}"
                    )
                    input("\nPress Enter to continue...")
            elif choice in category["queries"]:
                query = category["queries"][choice]
                self.execute_query(query["sql"], query["name"])
                print(
                    f"\nSQL Query:\n{'-' * 40}\n{query['sql'].strip()}\n{'-' * 40}"
                )
                input("\nPress Enter to continue...")
            else:
                print("Invalid choice. Please try again.")

    def _custom_query_mode(self):
        """Allow users to enter custom SQL queries."""
        print("\n" + "=" * 60 + "\nCustom SQL Query Mode\n" + "=" * 60)
        print(
            "Enter your SQL query (use \\q to quit, \\h for help, end with ';')"
        )

        while True:
            print("\n" + "-" * 40)
            query_lines = []
            prompt = "SQL> "
            while True:
                line = input(prompt)
                if line.strip().lower() == "\\q":
                    return
                if line.strip().lower() == "\\h":
                    self._show_query_help()
                    print(prompt, end="")
                    continue
                query_lines.append(line)
                if line.strip().endswith(";"):
                    break
                prompt = "...> "

            full_query = "\n".join(query_lines)

            normalized_query = full_query.strip()
            while normalized_query.startswith("--"):
                line_end = normalized_query.find("\n")
                if line_end == -1:
                    normalized_query = ""
                    break
                normalized_query = normalized_query[line_end:].strip()

            if not (
                normalized_query.upper().startswith("SELECT")
                or normalized_query.upper().startswith("WITH")
            ):
                print("✗ Error: Only SELECT or WITH queries are allowed.")
                continue

            forbidden_keywords = [
                "INSERT",
                "UPDATE",
                "DELETE",
                "DROP",
                "CREATE",
                "ALTER",
            ]
            if any(
                keyword in normalized_query.upper()
                for keyword in forbidden_keywords
            ):
                print("✗ Error: Data modification statements are not allowed.")
                continue

            self.execute_query(full_query, "Custom Query")

    def _show_query_help(self):
        """Display help for writing custom queries."""
        print("\n" + "=" * 60 + "\nCustom Query Help\n" + "=" * 60)
        print("\nTable Structure:")
        print("  - player_stats (year, name, team, statistic, value, league)")
        print("  - pitcher_stats (year, name, team, statistic, value, league)")
        print(
            "  - team_standings (year, team, wins, losses, winning_percentage, league, division)"
        )
        print("\nExample: Find players named 'Babe Ruth'")
        print("  SELECT * FROM player_stats WHERE name LIKE '%Ruth%';")

    def _run_all_in_category(self):
        """Run all queries in a selected category."""
        print("\nSelect category to run all queries:")
        for key, category in sorted(self.query_categories.items()):
            print(f"{key}. {category['name']}")
        choice = input("\nCategory: ").strip()

        if choice in self.query_categories:
            category = self.query_categories[choice]
            print(
                f"\n{'=' * 60}\nRunning all queries in: {category['name']}\n{'=' * 60}"
            )
            for key, query in sorted(category["queries"].items()):
                print(f"\n{'─' * 60}")
                self.execute_query(
                    query["sql"], f"{category['name']} - {query['name']}"
                )
                input("\nPress Enter for next query...")
        else:
            print("Invalid category.")

    def export_results_to_csv(self, sql: str, filename: str):
        """Export query results to CSV file."""
        import csv

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            if not rows:
                print("No results to export.")
                return
            headers = [description[0] for description in cursor.description]
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)
            print(f"✓ Exported {len(rows)} rows to {filename}")
        except Exception as e:
            print(f"✗ Export error: {e}")
            logging.error(f"Export error: {e}", exc_info=True)

    def run_batch_mode(
        self, category: Optional[str] = None, query: Optional[str] = None
    ):
        """Run specific queries in batch mode (non-interactive)."""
        if category and category in self.query_categories:
            cat = self.query_categories[category]
            if query and query in cat["queries"]:
                q = cat["queries"][query]
                print(
                    f"\nExecuting: {q['name']}\nDescription: {q['description']}"
                )
                self.execute_query(q["sql"], q["name"])
            else:
                print(f"\nRunning all queries in category: {cat['name']}")
                for key, q in sorted(cat["queries"].items()):
                    print(f"\n{'=' * 60}")
                    self.execute_query(q["sql"], q["name"])
        else:
            print("\n" + "=" * 60 + "\nRunning sample queries...\n" + "=" * 60)
            sample_queries = [
                ("1", "a"),
                ("2", "a"),
                ("3", "a"),
                ("5", "b"),
                ("6", "a"),
                ("7", "a"),
            ]
            for cat_key, query_key in sample_queries:
                cat = self.query_categories[cat_key]
                query = cat["queries"][query_key]
                print(f"\n{'─' * 60}\nCategory: {cat['name']}")
                self.execute_query(query["sql"], query["name"])

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            print("\nDatabase connection closed.")


def main():
    """Main entry point for the SQL Query Explorer."""
    parser = argparse.ArgumentParser(
        description="SQL Query Explorer for Baseball Statistics Database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                        # Run interactive mode
  %(prog)s --batch                # Run sample queries
  %(prog)s --batch -c 2           # Run all queries in category 2
  %(prog)s --batch -c 2 -q a      # Run specific query 2a
  %(prog)s --export query.csv -c 5 -q b  # Export query results to CSV
        """,
    )
    parser.add_argument(
        "--batch",
        "-b",
        action="store_true",
        help="Run in batch mode (non-interactive)",
    )
    parser.add_argument(
        "--category", "-c", help="Category number to run (use with --batch)"
    )
    parser.add_argument(
        "--query",
        "-q",
        help="Query letter to run within category (use with --category)",
    )
    parser.add_argument(
        "--export",
        "-e",
        metavar="FILE",
        help="Export query results to CSV file",
    )
    parser.add_argument(
        "--database",
        "-d",
        default="baseball_history.db",
        help="Path to database file",
    )
    args = parser.parse_args()

    explorer = None
    try:
        explorer = SQLQueryExplorer(args.database)

        if args.export:
            if not (args.category and args.query):
                print(
                    "Error: --export requires --category (-c) and --query (-q).",
                    file=sys.stderr,
                )
                sys.exit(1)

            if (
                args.category in explorer.query_categories
                and args.query
                in explorer.query_categories[args.category]["queries"]
            ):
                query = explorer.query_categories[args.category]["queries"][
                    args.query
                ]
                explorer.export_results_to_csv(query["sql"], args.export)
            else:
                print(
                    f"Query '{args.category}{args.query}' not found.",
                    file=sys.stderr,
                )
                sys.exit(1)
        elif args.batch:
            explorer.run_batch_mode(args.category, args.query)
        else:
            explorer.run_interactive()

    except FileNotFoundError as e:
        print(f"\n✗ FATAL ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting.")
    except Exception as e:
        logging.error("A fatal, unhandled error occurred", exc_info=True)
        print(f"\n✗ A fatal error occurred: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if explorer:
            explorer.close()


if __name__ == "__main__":
    main()
