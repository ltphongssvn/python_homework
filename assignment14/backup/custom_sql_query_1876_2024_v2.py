#!/usr/bin/env python3
# custom_sql_query_1876_2024_v1.py
"""
Custom SQL Query Explorer for Baseball Statistics Database
=========================================================

A comprehensive collection of SQL queries for exploring the baseball_history.db database.
This tool demonstrates various SQL techniques from basic exploration to advanced analytics.

The queries are organized into categories, each building upon previous concepts
to create a learning journey through SQL and baseball statistics.
"""

import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class SQLQueryExplorer:
    """Manages and executes categorized SQL queries for baseball database exploration."""

    def __init__(self, db_path: str = "baseball_history.db"):
        """Initialize the query explorer with database connection."""
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")

        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

        # Enable math functions for statistical queries
        self.conn.create_function("SQUARE", 1, lambda x: x * x if x else None)

        print(f"Connected to database: {self.db_path.name}")
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
                                   MIN(year),
                                   MAX(year),
                                   COUNT(DISTINCT year),
                                   COUNT(DISTINCT name),
                                   COUNT(*)
                            FROM pitcher_stats
                            UNION ALL
                            SELECT 'team_standings',
                                   MIN(year),
                                   MAX(year),
                                   COUNT(DISTINCT year),
                                   COUNT(DISTINCT team),
                                   COUNT(*)
                            FROM team_standings
                        """,
                    },
                    "b": {
                        "name": "Available Player Statistics",
                        "description": "List all batting statistics tracked in the database",
                        "sql": """
                            SELECT statistic,
                                   COUNT(*) as occurrences,
                                   COUNT(DISTINCT year) as years_tracked,
                                   MIN(year) as first_year,
                                   MAX(year) as last_year
                            FROM player_stats
                            GROUP BY statistic
                            ORDER BY occurrences DESC
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
                              AND CAST(value AS REAL) > 0  -- Exclude invalid values
                              AND CAST(value AS REAL) < 10  -- Reasonable ERA range
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
                              AND value NOT LIKE '.%'  -- Ensure proper format
                              AND CAST(value AS REAL) <= 1.0  -- Valid batting average
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
                                (year / 10) * 10 as decade,
                                COUNT(DISTINCT name) as players_with_hrs,
                                SUM(CAST(value AS INTEGER)) as total_home_runs,
                                ROUND(AVG(CAST(value AS REAL)), 2) as avg_home_runs_per_player,
                                MAX(CAST(value AS INTEGER)) as max_home_runs_in_season
                            FROM player_stats
                            WHERE statistic = 'Home Runs'
                            GROUP BY decade
                            ORDER BY decade
                        """,
                    },
                    "b": {
                        "name": "Pitching Evolution - ERA by Decade",
                        "description": "How pitching effectiveness has changed over time",
                        "sql": """
                            SELECT
                                (year / 10) * 10 as decade,
                                COUNT(DISTINCT name) as pitchers,
                                ROUND(AVG(CAST(value AS REAL)), 3) as avg_era,
                                ROUND(MIN(CAST(value AS REAL)), 3) as best_era,
                                ROUND(MAX(CAST(value AS REAL)), 3) as worst_era
                            FROM pitcher_stats
                            WHERE statistic = 'ERA'
                              AND CAST(value AS REAL) > 0
                              AND CAST(value AS REAL) < 10
                            GROUP BY decade
                            ORDER BY decade
                        """,
                    },
                    "c": {
                        "name": "Offensive Production Trends",
                        "description": "Multiple offensive statistics by era",
                        "sql": """
                            SELECT
                                (year / 10) * 10 as decade,
                                statistic,
                                COUNT(DISTINCT name) as players,
                                SUM(CAST(value AS INTEGER)) as total,
                                ROUND(AVG(CAST(value AS REAL)), 2) as average
                            FROM player_stats
                            WHERE statistic IN ('Home Runs', 'RBI', 'Hits', 'Runs')
                              AND year >= 1920  -- Live ball era
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
                        "name": "Offensive Statistics by League",
                        "description": "Compare hitting between AL and NL in recent years",
                        "sql": """
                            SELECT
                                year,
                                league,
                                statistic,
                                COUNT(*) as player_count,
                                SUM(CAST(value AS INTEGER)) as total,
                                ROUND(AVG(CAST(value AS REAL)), 2) as average
                            FROM player_stats
                            WHERE statistic IN ('Home Runs', 'RBI', 'Hits')
                              AND year >= 2010  -- Recent comparison
                            GROUP BY year, league, statistic
                            ORDER BY year DESC, statistic, league
                        """,
                    },
                    "b": {
                        "name": "League Competitive Balance",
                        "description": "Which league has been more balanced?",
                        "sql": """
                            WITH league_stats AS (
                                SELECT
                                    year,
                                    league,
                                    COUNT(DISTINCT team) as teams,
                                    AVG(winning_percentage) as mean_win_pct,
                                    MIN(winning_percentage) as min_win_pct,
                                    MAX(winning_percentage) as max_win_pct
                                FROM team_standings
                                WHERE year >= 2000
                                GROUP BY year, league
                            )
                            SELECT
                                year,
                                league,
                                teams,
                                ROUND(mean_win_pct, 3) as avg_win_pct,
                                ROUND(min_win_pct, 3) as worst_team,
                                ROUND(max_win_pct, 3) as best_team,
                                ROUND(max_win_pct - min_win_pct, 3) as win_pct_spread,
                                -- Calculate standard deviation manually
                                ROUND(SQRT(
                                    (SELECT AVG((ts.winning_percentage - ls.mean_win_pct) *
                                               (ts.winning_percentage - ls.mean_win_pct))
                                     FROM team_standings ts
                                     WHERE ts.year = ls.year AND ts.league = ls.league)
                                ), 3) as win_pct_stddev
                            FROM league_stats ls
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
                                MAX(year) as last_season,
                                MAX(year) - MIN(year) + 1 as career_span,
                                COUNT(DISTINCT team) as teams_played_for
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
                                    COUNT(DISTINCT year) as seasons,
                                    MIN(year) as first_year,
                                    MAX(year) as last_year
                                FROM player_stats
                                WHERE statistic IN ('Home Runs', 'RBI', 'Hits')
                                GROUP BY name
                            )
                            SELECT * FROM career_totals
                            WHERE career_hr > 200  -- 200+ home run club
                            ORDER BY career_hr DESC
                            LIMIT 50
                        """,
                    },
                    "c": {
                        "name": "Most Consistent Hitters",
                        "description": "Players with best average performance over long careers",
                        "sql": """
                            SELECT
                                name,
                                COUNT(DISTINCT year) as seasons,
                                ROUND(AVG(CAST(value AS REAL)), 3) as career_avg,
                                MIN(CAST(value AS REAL)) as worst_season,
                                MAX(CAST(value AS REAL)) as best_season,
                                ROUND(MAX(CAST(value AS REAL)) - MIN(CAST(value AS REAL)), 3) as consistency_range
                            FROM player_stats
                            WHERE statistic = 'Batting Average'
                              AND CAST(value AS REAL) > 0
                              AND CAST(value AS REAL) <= 1.0
                            GROUP BY name
                            HAVING COUNT(DISTINCT year) >= 10  -- At least 10 seasons
                               AND AVG(CAST(value AS REAL)) >= 0.300  -- .300+ career average
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
                        "description": "Find the longest streaks of winning seasons",
                        "sql": """
                            WITH team_streaks AS (
                                SELECT
                                    team,
                                    year,
                                    wins,
                                    losses,
                                    winning_percentage,
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
                                    COUNT(*) as consecutive_winning_seasons,
                                    ROUND(AVG(winning_percentage), 3) as avg_win_pct,
                                    SUM(wins) as total_wins,
                                    SUM(losses) as total_losses
                                FROM team_streaks
                                WHERE winning_season = 1
                                GROUP BY team, streak_group
                            )
                            SELECT * FROM winning_streaks
                            WHERE consecutive_winning_seasons >= 5
                            ORDER BY consecutive_winning_seasons DESC, avg_win_pct DESC
                            LIMIT 25
                        """,
                    },
                    "b": {
                        "name": "Best Single-Season Teams",
                        "description": "Teams with the highest winning percentages",
                        "sql": """
                            SELECT
                                year,
                                team,
                                league,
                                division,
                                wins,
                                losses,
                                winning_percentage,
                                games_back
                            FROM team_standings
                            WHERE winning_percentage >= 0.650  -- .650+ winning percentage
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
                        "name": "Star Players on Championship Teams",
                        "description": "Great individual seasons on great teams",
                        "sql": """
                            SELECT
                                ps.year,
                                ps.name,
                                ps.team,
                                ps.value as home_runs,
                                ts.wins,
                                ts.winning_percentage,
                                ts.league,
                                ts.division
                            FROM player_stats ps
                            JOIN team_standings ts ON ps.year = ts.year AND ps.team = ts.team
                            WHERE ps.statistic = 'Home Runs'
                              AND ts.winning_percentage >= 0.600  -- Successful teams
                              AND CAST(ps.value AS INTEGER) >= 40  -- 40+ HR seasons
                            ORDER BY ps.year DESC, CAST(ps.value AS INTEGER) DESC
                        """,
                    },
                    "b": {
                        "name": "Pitcher Performance by Team Success",
                        "description": "Do pitchers perform better on winning teams?",
                        "sql": """
                            SELECT
                                CASE
                                    WHEN ts.winning_percentage >= 0.600 THEN 'Excellent Teams (.600+)'
                                    WHEN ts.winning_percentage >= 0.500 THEN 'Winning Teams (.500-.599)'
                                    WHEN ts.winning_percentage >= 0.400 THEN 'Losing Teams (.400-.499)'
                                    ELSE 'Poor Teams (below .400)'
                                END as team_category,
                                COUNT(DISTINCT ps.name) as pitcher_count,
                                COUNT(*) as pitcher_seasons,
                                ROUND(AVG(CAST(ps.value AS REAL)), 3) as avg_era,
                                ROUND(MIN(CAST(ps.value AS REAL)), 3) as best_era
                            FROM pitcher_stats ps
                            JOIN team_standings ts ON ps.year = ts.year AND ps.team = ts.team
                            WHERE ps.statistic = 'ERA'
                              AND CAST(ps.value AS REAL) BETWEEN 0.5 AND 10
                            GROUP BY team_category
                            ORDER BY avg_era
                        """,
                    },
                },
            },
            "8": {
                "name": "Statistical Anomalies",
                "description": "Find outliers and unusual performances",
                "queries": {
                    "a": {
                        "name": "Extreme Statistical Outliers",
                        "description": "Performances that deviate significantly from the norm",
                        "sql": """
                            WITH batting_stats AS (
                                SELECT
                                    year,
                                    name,
                                    team,
                                    CAST(value AS REAL) as batting_avg
                                FROM player_stats
                                WHERE statistic = 'Batting Average'
                                  AND value NOT LIKE '.%'
                                  AND CAST(value AS REAL) > 0
                                  AND CAST(value AS REAL) <= 1.0
                            ),
                            stats_summary AS (
                                SELECT
                                    AVG(batting_avg) as mean_avg,
                                    -- Calculate standard deviation manually
                                    SQRT(AVG(SQUARE(batting_avg - (SELECT AVG(batting_avg) FROM batting_stats)))) as std_dev
                                FROM batting_stats
                            )
                            SELECT
                                bs.year,
                                bs.name,
                                bs.team,
                                bs.batting_avg,
                                ROUND((bs.batting_avg - ss.mean_avg) / ss.std_dev, 2) as z_score
                            FROM batting_stats bs
                            CROSS JOIN stats_summary ss
                            WHERE ABS((bs.batting_avg - ss.mean_avg) / ss.std_dev) > 2.5
                            ORDER BY z_score DESC
                        """,
                    },
                    "b": {
                        "name": "Biggest Year-to-Year Improvements",
                        "description": "Players who dramatically improved between seasons",
                        "sql": """
                            WITH yoy_comparison AS (
                                SELECT
                                    p1.name,
                                    p1.year as year1,
                                    p2.year as year2,
                                    p1.value as hr_year1,
                                    p2.value as hr_year2,
                                    CAST(p2.value AS INTEGER) - CAST(p1.value AS INTEGER) as improvement
                                FROM player_stats p1
                                JOIN player_stats p2 ON p1.name = p2.name
                                                    AND p1.statistic = p2.statistic
                                                    AND p2.year = p1.year + 1
                                WHERE p1.statistic = 'Home Runs'
                                  AND CAST(p1.value AS INTEGER) >= 10  -- Had at least 10 HR in first year
                            )
                            SELECT * FROM yoy_comparison
                            WHERE improvement >= 20  -- 20+ HR improvement
                            ORDER BY improvement DESC
                            LIMIT 25
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
                            SELECT 'Invalid Batting Averages' as issue_type, COUNT(*) as count
                            FROM player_stats
                            WHERE statistic = 'Batting Average'
                              AND (CAST(value AS REAL) > 1.0 OR CAST(value AS REAL) <= 0)

                            UNION ALL

                            SELECT 'Suspicious ERAs (< 0.5 or > 15)', COUNT(*)
                            FROM pitcher_stats
                            WHERE statistic = 'ERA'
                              AND (CAST(value AS REAL) < 0.5 OR CAST(value AS REAL) > 15)

                            UNION ALL

                            SELECT 'Duplicate Records', COUNT(*)
                            FROM (
                                SELECT year, name, team, statistic, COUNT(*) as dupe_count
                                FROM player_stats
                                GROUP BY year, name, team, statistic
                                HAVING COUNT(*) > 1
                            )

                            UNION ALL

                            SELECT 'Teams with Invalid Win%', COUNT(*)
                            FROM team_standings
                            WHERE winning_percentage > 1.0 OR winning_percentage < 0
                        """,
                    },
                    "b": {
                        "name": "Missing Data Analysis",
                        "description": "Identify gaps in the historical record",
                        "sql": """
                            WITH year_range AS (
                                SELECT MIN(year) as min_year, MAX(year) as max_year
                                FROM (
                                    SELECT year FROM player_stats
                                    UNION SELECT year FROM pitcher_stats
                                    UNION SELECT year FROM team_standings
                                )
                            ),
                            all_years AS (
                                SELECT min_year + (n - 1) as year
                                FROM year_range
                                CROSS JOIN (
                                    SELECT ROW_NUMBER() OVER () as n
                                    FROM player_stats
                                    LIMIT (SELECT max_year - min_year + 1 FROM year_range)
                                )
                                WHERE min_year + (n - 1) <= (SELECT max_year FROM year_range)
                            )
                            SELECT
                                'player_stats' as table_name,
                                GROUP_CONCAT(ay.year) as missing_years
                            FROM all_years ay
                            LEFT JOIN (SELECT DISTINCT year FROM player_stats) ps ON ay.year = ps.year
                            WHERE ps.year IS NULL

                            UNION ALL

                            SELECT
                                'pitcher_stats',
                                GROUP_CONCAT(ay.year)
                            FROM all_years ay
                            LEFT JOIN (SELECT DISTINCT year FROM pitcher_stats) ps ON ay.year = ps.year
                            WHERE ps.year IS NULL
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

            # Get column headers
            headers = [description[0] for description in cursor.description]

            # Convert rows to list of lists and track column widths
            data = []
            col_widths = [len(h) for h in headers]

            for row in rows:
                row_data = []
                for i, value in enumerate(row):
                    # Format different data types appropriately
                    if value is None:
                        formatted = "NULL"
                    elif isinstance(value, float):
                        # Special formatting for percentages and averages
                        if headers[i].lower() in [
                            "batting_avg",
                            "career_avg",
                            "avg",
                            "winning_percentage",
                            "avg_win_pct",
                        ]:
                            formatted = f"{value:.3f}"
                        elif headers[i].lower() in [
                            "era",
                            "avg_era",
                            "best_era",
                            "worst_era",
                        ]:
                            formatted = f"{value:.2f}"
                        else:
                            formatted = (
                                f"{value:.2f}"
                                if value < 100
                                else f"{int(value):,}"
                            )
                    elif isinstance(value, int):
                        formatted = f"{value:,}"
                    else:
                        formatted = str(value)

                    row_data.append(formatted)
                    col_widths[i] = max(col_widths[i], len(formatted))

                data.append(row_data)

            # Display results with formatting
            print(f"\n✓ Results for: {query_name}")
            print(f"  ({len(rows)} rows returned)")
            print()

            # Header row
            header_line = " | ".join(
                f"{h:<{col_widths[i]}}" for i, h in enumerate(headers)
            )
            print(header_line)
            print("-" * len(header_line))

            # Data rows (limit display to prevent overwhelming output)
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
            logging.error(f"SQL error: {e}")

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

            # Display main categories
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
            print(f"\n{'=' * 60}")
            print(f"Category: {category['name']}")
            print(f"{'=' * 60}")

            # Display queries in this category
            for key, query in sorted(category["queries"].items()):
                print(f"\n{key}. {query['name']}")
                print(f"   {query['description']}")

            print("\nOptions:")
            print("B. Back to main menu")
            print("*. Run all queries in this category")

            choice = input("\nSelect query (or B/*): ").strip().lower()

            if choice == "b":
                break
            elif choice == "*":
                # Run all queries in category
                for key, query in sorted(category["queries"].items()):
                    print(f"\n{'─' * 60}")
                    self.execute_query(
                        query["sql"], f"{category['name']} - {query['name']}"
                    )
                    input("\nPress Enter to continue...")
            elif choice in category["queries"]:
                query = category["queries"][choice]
                self.execute_query(query["sql"], query["name"])

                # Show the SQL for learning purposes
                print(f"\nSQL Query:")
                print("-" * 40)
                print(query["sql"].strip())
                print("-" * 40)

                input("\nPress Enter to continue...")
            else:
                print("Invalid choice. Please try again.")

    def _custom_query_mode(self):
        """Allow users to enter custom SQL queries."""
        print("\n" + "=" * 60)
        print("Custom SQL Query Mode")
        print("=" * 60)
        print("Enter your SQL query (use \\q to quit, \\h for help)")
        print("Note: Only SELECT statements are allowed for safety.")

        while True:
            print("\n" + "-" * 40)
            query = []
            print("SQL> ", end="")

            # Multi-line query input
            while True:
                line = input()
                if line.strip().lower() == "\\q":
                    return
                elif line.strip().lower() == "\\h":
                    self._show_query_help()
                    print("SQL> ", end="")
                    continue

                query.append(line)

                # Check if query is complete (ends with semicolon)
                if line.strip().endswith(";"):
                    break
                print("...> ", end="")  # Continuation prompt

            full_query = "\n".join(query)

            # Enhanced safety check - allow WITH clauses and SELECT
            normalized_query = full_query.strip().upper()
            if not (
                normalized_query.startswith("SELECT")
                or normalized_query.startswith("WITH")
            ):
                print(
                    "✗ Error: Only SELECT queries (including WITH clauses) are allowed."
                )
                continue

            # Additional check to prevent data modification even in CTEs
            forbidden_keywords = [
                "INSERT",
                "UPDATE",
                "DELETE",
                "DROP",
                "CREATE",
                "ALTER",
            ]
            if any(
                keyword in normalized_query for keyword in forbidden_keywords
            ):
                print("✗ Error: Data modification statements are not allowed.")
                continue

            self.execute_query(full_query, "Custom Query")

    def _show_query_help(self):
        """Display help for writing custom queries."""
        print("\n" + "=" * 60)
        print("Custom Query Help")
        print("=" * 60)
        print("\nTable Structure:")
        print(
            "  - player_stats: batting statistics (year, name, team, statistic, value)"
        )
        print(
            "  - pitcher_stats: pitching statistics (year, name, team, statistic, value)"
        )
        print(
            "  - team_standings: team records (year, team, wins, losses, winning_percentage)"
        )
        print("\nCommon Patterns:")
        print(
            "  - Cast value column: CAST(value AS INTEGER) or CAST(value AS REAL)"
        )
        print("  - Filter by statistic: WHERE statistic = 'Home Runs'")
        print(
            "  - Join tables: JOIN team_standings ts ON ps.year = ts.year AND ps.team = ts.team"
        )
        print("\nExample Queries:")
        print("  SELECT * FROM player_stats WHERE name LIKE '%Ruth%';")
        print(
            "  SELECT year, name, value FROM pitcher_stats WHERE statistic = 'Wins' ORDER BY CAST(value AS INTEGER) DESC LIMIT 10;"
        )

    def _run_all_in_category(self):
        """Run all queries in a selected category."""
        print("\nSelect category to run all queries:")
        for key, category in sorted(self.query_categories.items()):
            print(f"{key}. {category['name']}")

        choice = input("\nCategory: ").strip()

        if choice in self.query_categories:
            category = self.query_categories[choice]
            print(f"\n{'=' * 60}")
            print(f"Running all queries in: {category['name']}")
            print(f"{'=' * 60}")

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
                print(f"No results to export.")
                return

            headers = [description[0] for description in cursor.description]

            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)

                for row in rows:
                    writer.writerow(row)

            print(f"✓ Exported {len(rows)} rows to {filename}")

        except Exception as e:
            print(f"✗ Export error: {e}")
            logging.error(f"Export error: {e}", exc_info=True)

    def run_batch_mode(
        self, category: Optional[str] = None, query: Optional[str] = None
    ):
        """Run specific queries in batch mode (non-interactive)."""
        if category and category in self.query_categories:
            if query and query in self.query_categories[category]["queries"]:
                # Run specific query
                q = self.query_categories[category]["queries"][query]
                print(f"\nExecuting: {q['name']}")
                print(f"Description: {q['description']}")
                self.execute_query(q["sql"], q["name"])
            else:
                # Run all queries in category
                cat = self.query_categories[category]
                print(f"\nRunning all queries in category: {cat['name']}")
                for key, q in sorted(cat["queries"].items()):
                    print(f"\n{'=' * 60}")
                    self.execute_query(q["sql"], q["name"])
        else:
            # Run a selection of interesting queries
            print("\n" + "=" * 60)
            print("Running sample queries from each category...")
            print("=" * 60)

            sample_queries = [
                ("1", "a"),  # Database overview
                ("2", "a"),  # Top home run seasons
                ("3", "a"),  # Home run evolution
                ("5", "b"),  # Career home run leaders
                ("6", "a"),  # Team dynasties
                ("7", "a"),  # Stars on great teams
            ]

            for cat_key, query_key in sample_queries:
                cat = self.query_categories[cat_key]
                query = cat["queries"][query_key]
                print(f"\n{'─' * 60}")
                print(f"Category: {cat['name']}")
                self.execute_query(query["sql"], query["name"])

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            print("\nDatabase connection closed.")


def main():
    """Main entry point for the SQL Query Explorer."""
    import argparse

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
        help="Path to database file (default: baseball_history.db)",
    )

    args = parser.parse_args()

    try:
        # Initialize the query explorer
        explorer = SQLQueryExplorer(args.database)

        if args.export and args.category and args.query:
            # Export mode
            if args.category in explorer.query_categories:
                if (
                    args.query
                    in explorer.query_categories[args.category]["queries"]
                ):
                    query = explorer.query_categories[args.category][
                        "queries"
                    ][args.query]
                    explorer.export_results_to_csv(query["sql"], args.export)
                else:
                    print(
                        f"Query '{args.query}' not found in category '{args.category}'"
                    )
            else:
                print(f"Category '{args.category}' not found")
        elif args.batch:
            # Batch mode
            explorer.run_batch_mode(args.category, args.query)
        else:
            # Interactive mode
            explorer.run_interactive()

    except FileNotFoundError as e:
        print(f"\n✗ Error: {e}")
        print(f"Please ensure the database file exists at the specified path.")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        print(f"\n✗ Fatal error: {e}")
        sys.exit(1)
    finally:
        if "explorer" in locals():
            explorer.close()


if __name__ == "__main__":
    main()
