#!/usr/bin/env python3
# custom_sql_query_1876_2024_v6.py
"""
Enhanced SQL Query Explorer for Baseball Statistics Database
===========================================================

A comprehensive collection of SQL queries for exploring the baseball_history.db database.
This enhanced version incorporates all queries from the SQL file while maintaining
a clean, organized structure.

The queries are organized into categories and subcategories, each building upon
previous concepts to create a learning journey through SQL and baseball statistics.
"""

import argparse
import json
import logging
import math
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class QueryComplexity(Enum):
    """Enum to categorize query complexity levels"""

    BASIC = "basic"
    INTERMEDIATE = "intermediate"
    ADVANCED = "advanced"
    EXPERT = "expert"


@dataclass
class Query:
    """Data class to represent a single SQL query"""

    name: str
    description: str
    sql: str
    complexity: QueryComplexity = QueryComplexity.BASIC
    tags: List[str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []


class QueryLibrary:
    """
    Manages the complete library of SQL queries.
    This separates query management from execution logic.
    """

    def __init__(self):
        self.queries = self._load_all_queries()
        self.categories = self._organize_categories()

    def _load_all_queries(self) -> Dict[str, Query]:
        """
        Load all queries into a flat dictionary for easy access.
        In a production system, this might load from a JSON file or database.
        """
        queries = {}

        # Database Overview Queries
        queries["db_overview_temporal_scope"] = Query(
            name="Database Temporal Scope",
            description="Understanding the time range covered by each table",
            sql="""
                SELECT 'player_stats' as table_name,
                       MIN(year) as earliest_year,
                       MAX(year) as latest_year,
                       COUNT(DISTINCT year) as total_years,
                       COUNT(DISTINCT name) as unique_records,
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
            complexity=QueryComplexity.BASIC,
            tags=["overview", "metadata", "union"],
        )

        queries["db_overview_missing_years"] = Query(
            name="Missing Years Detection",
            description="Find gaps in the historical data",
            sql="""
                WITH all_years AS (
                    SELECT DISTINCT year FROM pitcher_stats
                    UNION
                    SELECT DISTINCT year FROM player_stats
                    UNION
                    SELECT DISTINCT year FROM team_standings
                ),
                year_range AS (
                    SELECT MIN(year) as min_year, MAX(year) as max_year
                    FROM all_years
                ),
                expected_years(year) AS (
                    SELECT min_year FROM year_range
                    UNION ALL
                    SELECT year + 1 FROM expected_years
                    WHERE year < (SELECT max_year FROM year_range)
                )
                SELECT e.year as missing_year
                FROM expected_years e
                LEFT JOIN all_years a ON e.year = a.year
                WHERE a.year IS NULL
                ORDER BY e.year
            """,
            complexity=QueryComplexity.ADVANCED,
            tags=["cte", "recursive", "data-quality"],
        )

        queries["stat_types_overview"] = Query(
            name="Available Statistics Overview",
            description="List all types of statistics tracked in the database",
            sql="""
                SELECT 'Batting' as category, statistic, COUNT(*) as occurrences
                FROM player_stats
                GROUP BY statistic
                UNION ALL
                SELECT 'Pitching', statistic, COUNT(*)
                FROM pitcher_stats
                GROUP BY statistic
                ORDER BY category, occurrences DESC
            """,
            complexity=QueryComplexity.BASIC,
            tags=["overview", "statistics"],
        )

        # Basic Statistical Queries
        queries["basic_home_run_leaders"] = Query(
            name="All-Time Home Run Leaders (Single Season)",
            description="Players with the most home runs in a single season",
            sql="""
                SELECT year, name, team, league,
                       CAST(value AS INTEGER) as home_runs
                FROM player_stats
                WHERE statistic = 'Home Runs'
                ORDER BY home_runs DESC
                LIMIT 25
            """,
            complexity=QueryComplexity.BASIC,
            tags=["batting", "records", "home-runs"],
        )

        queries["basic_era_leaders"] = Query(
            name="Best Single-Season ERAs",
            description="Pitchers with the lowest earned run averages",
            sql="""
                SELECT year, name, team, league,
                       CAST(value AS REAL) as era
                FROM pitcher_stats
                WHERE statistic = 'ERA'
                  AND CAST(value AS REAL) > 0
                ORDER BY era ASC
                LIMIT 25
            """,
            complexity=QueryComplexity.BASIC,
            tags=["pitching", "records", "era"],
        )

        queries["basic_batting_avg_leaders"] = Query(
            name="Highest Batting Averages",
            description="Best single-season batting averages in history",
            sql="""
                SELECT year, name, team, league,
                       CAST(value AS REAL) as batting_avg
                FROM player_stats
                WHERE statistic = 'Batting Average'
                  AND CAST(value AS REAL) <= 1.0
                  AND CAST(value AS REAL) > 0
                ORDER BY batting_avg DESC
                LIMIT 25
            """,
            complexity=QueryComplexity.BASIC,
            tags=["batting", "records", "average"],
        )

        # Trend Analysis Queries
        queries["trend_home_runs_by_decade"] = Query(
            name="Home Run Evolution by Decade",
            description="How home run hitting has changed over time",
            sql="""
                SELECT
                    (year / 10) * 10 || 's' as decade,
                    COUNT(DISTINCT name) as players_with_hrs,
                    SUM(CAST(value AS INTEGER)) as total_home_runs,
                    ROUND(AVG(CAST(value AS REAL)), 2) as avg_home_runs,
                    MAX(CAST(value AS INTEGER)) as max_home_runs_in_season
                FROM player_stats
                WHERE statistic = 'Home Runs'
                GROUP BY (year / 10) * 10
                ORDER BY decade
            """,
            complexity=QueryComplexity.INTERMEDIATE,
            tags=["trends", "home-runs", "historical"],
        )

        queries["trend_pitching_era_by_decade"] = Query(
            name="ERA Evolution by Decade",
            description="How pitching effectiveness has changed over time",
            sql="""
                SELECT
                    (year / 10) * 10 || 's' as decade,
                    COUNT(DISTINCT name) as pitchers,
                    ROUND(AVG(CAST(value AS REAL)), 3) as avg_era,
                    ROUND(MIN(CAST(value AS REAL)), 3) as best_era_in_decade
                FROM pitcher_stats
                WHERE statistic = 'ERA'
                  AND CAST(value AS REAL) > 0
                GROUP BY (year / 10) * 10
                ORDER BY decade
            """,
            complexity=QueryComplexity.INTERMEDIATE,
            tags=["trends", "pitching", "historical"],
        )

        # Career Analysis Queries
        queries["career_longest_careers"] = Query(
            name="Players with Longest Careers",
            description="Players who appeared in the most seasons",
            sql="""
                SELECT
                    name,
                    COUNT(DISTINCT year) as seasons_played,
                    MIN(year) as first_season,
                    MAX(year) as last_season,
                    COUNT(DISTINCT team) as teams_played_for
                FROM player_stats
                GROUP BY name
                HAVING COUNT(DISTINCT year) >= 15
                ORDER BY seasons_played DESC, first_season
                LIMIT 50
            """,
            complexity=QueryComplexity.INTERMEDIATE,
            tags=["career", "longevity"],
        )

        queries["career_home_run_totals"] = Query(
            name="Career Home Run Leaders",
            description="Players with most career home runs (300+ HR club)",
            sql="""
                WITH career_totals AS (
                    SELECT
                        name,
                        SUM(CASE WHEN statistic = 'Home Runs'
                            THEN CAST(value AS INTEGER) ELSE 0 END) as career_hr,
                        SUM(CASE WHEN statistic = 'RBI'
                            THEN CAST(value AS INTEGER) ELSE 0 END) as career_rbi,
                        SUM(CASE WHEN statistic = 'Hits'
                            THEN CAST(value AS INTEGER) ELSE 0 END) as career_hits,
                        COUNT(DISTINCT year) as seasons
                    FROM player_stats
                    WHERE statistic IN ('Home Runs', 'RBI', 'Hits')
                    GROUP BY name
                )
                SELECT * FROM career_totals
                WHERE career_hr >= 300
                ORDER BY career_hr DESC
                LIMIT 50
            """,
            complexity=QueryComplexity.ADVANCED,
            tags=["career", "home-runs", "cte"],
        )

        queries["career_pitcher_wins"] = Query(
            name="Career Wins Leaders (Pitchers)",
            description="Pitchers with the most career wins",
            sql="""
                SELECT
                    name,
                    SUM(CAST(value AS INTEGER)) as career_wins,
                    COUNT(DISTINCT year) as seasons,
                    MIN(year) as first_year,
                    MAX(year) as last_year
                FROM pitcher_stats
                WHERE statistic = 'Wins'
                GROUP BY name
                ORDER BY career_wins DESC
                LIMIT 20
            """,
            complexity=QueryComplexity.INTERMEDIATE,
            tags=["career", "pitching", "wins"],
        )

        # Team Analysis Queries
        queries["team_dynasties"] = Query(
            name="Team Dynasties (Consecutive Winning Seasons)",
            description="Teams with longest streaks of winning seasons",
            sql="""
                WITH team_streaks AS (
                    SELECT
                        team, year,
                        CASE WHEN winning_percentage > 0.500 THEN 1 ELSE 0 END as winning_season,
                        year - ROW_NUMBER() OVER (
                            PARTITION BY team,
                            CASE WHEN winning_percentage > 0.500 THEN 1 ELSE 0 END
                            ORDER BY year
                        ) as streak_group
                    FROM team_standings
                ),
                winning_streaks AS (
                    SELECT
                        team,
                        MIN(year) as streak_start,
                        MAX(year) as streak_end,
                        COUNT(*) as consecutive_winning_seasons,
                        ROUND(AVG(winning_percentage), 3) as avg_win_pct
                    FROM team_streaks
                    WHERE winning_season = 1
                    GROUP BY team, streak_group
                )
                SELECT * FROM winning_streaks
                WHERE consecutive_winning_seasons >= 5
                ORDER BY consecutive_winning_seasons DESC, avg_win_pct DESC
                LIMIT 25
            """,
            complexity=QueryComplexity.EXPERT,
            tags=["teams", "dynasties", "window-functions"],
        )

        queries["team_best_seasons"] = Query(
            name="Best Single-Season Teams",
            description="Teams with the highest winning percentages",
            sql="""
                SELECT
                    year, team, league, division,
                    wins, losses, winning_percentage
                FROM team_standings
                WHERE winning_percentage >= 0.650
                ORDER BY winning_percentage DESC, year DESC
                LIMIT 50
            """,
            complexity=QueryComplexity.BASIC,
            tags=["teams", "records"],
        )

        # League Comparison Queries
        queries["league_offensive_comparison"] = Query(
            name="AL vs NL Offensive Production",
            description="Compare hitting statistics between leagues",
            sql="""
                SELECT
                    year, league, statistic,
                    COUNT(*) as player_count,
                    SUM(CAST(value AS INTEGER)) as total,
                    ROUND(AVG(CAST(value AS REAL)), 2) as average
                FROM player_stats
                WHERE statistic IN ('Home Runs', 'RBI', 'Hits')
                  AND year >= 2000
                GROUP BY year, league, statistic
                ORDER BY year DESC, statistic, league
            """,
            complexity=QueryComplexity.INTERMEDIATE,
            tags=["league-comparison", "offense"],
        )

        queries["league_competitive_balance"] = Query(
            name="League Competitive Balance Analysis",
            description="Which league has been more balanced (using standard deviation)",
            sql="""
                SELECT
                    year, league,
                    COUNT(team) as teams,
                    ROUND(AVG(winning_percentage), 3) as avg_win_pct,
                    ROUND(STDDEV(winning_percentage), 3) as win_pct_stddev,
                    MAX(wins) - MIN(wins) as win_spread
                FROM team_standings
                WHERE year >= 2000
                GROUP BY year, league
                ORDER BY year DESC, league
            """,
            complexity=QueryComplexity.ADVANCED,
            tags=["league-comparison", "statistics", "balance"],
        )

        # Cross-Table Analysis
        queries["cross_stars_on_winners"] = Query(
            name="Star Players on Winning Teams",
            description="Great individual seasons on successful teams",
            sql="""
                SELECT
                    ps.year, ps.name, ps.team,
                    ps.value as home_runs,
                    ts.wins, ts.winning_percentage
                FROM player_stats ps
                JOIN team_standings ts
                    ON ps.year = ts.year AND ps.team = ts.team
                WHERE ps.statistic = 'Home Runs'
                  AND ts.winning_percentage >= 0.600
                  AND CAST(ps.value AS INTEGER) >= 40
                ORDER BY ps.year DESC, CAST(ps.value AS INTEGER) DESC
            """,
            complexity=QueryComplexity.INTERMEDIATE,
            tags=["cross-table", "players", "teams"],
        )

        queries["cross_pitcher_team_correlation"] = Query(
            name="Pitcher Performance vs Team Success",
            description="Do pitchers perform better on winning teams?",
            sql="""
                SELECT
                    CASE
                        WHEN ts.winning_percentage >= 0.500 THEN 'Winning Team (>=.500)'
                        ELSE 'Losing Team (<.500)'
                    END as team_type,
                    COUNT(DISTINCT ps.name) as pitcher_count,
                    ROUND(AVG(CAST(ps.value AS REAL)), 3) as avg_era
                FROM pitcher_stats ps
                JOIN team_standings ts
                    ON ps.year = ts.year AND ps.team = ts.team
                WHERE ps.statistic = 'ERA'
                  AND CAST(ps.value AS REAL) BETWEEN 0.5 AND 10
                GROUP BY team_type
                ORDER BY team_type
            """,
            complexity=QueryComplexity.INTERMEDIATE,
            tags=["cross-table", "pitching", "correlation"],
        )

        # Statistical Anomaly Detection
        queries["anomaly_batting_avg_outliers"] = Query(
            name="Extreme Batting Average Outliers",
            description="Find batting averages that are statistical outliers",
            sql="""
                WITH batting_stats AS (
                    SELECT
                        year, name, team,
                        CAST(value AS REAL) as batting_avg
                    FROM player_stats
                    WHERE statistic = 'Batting Average'
                      AND CAST(value AS REAL) BETWEEN 0.001 AND 1.0
                ),
                percentiles AS (
                    SELECT
                        year, name, team, batting_avg,
                        PERCENT_RANK() OVER (ORDER BY batting_avg) as pct_rank
                    FROM batting_stats
                )
                SELECT
                    year, name, team,
                    ROUND(batting_avg, 3) as batting_avg,
                    CASE
                        WHEN pct_rank >= 0.99 THEN 'Top 1%'
                        WHEN pct_rank >= 0.95 THEN 'Top 5%'
                        WHEN pct_rank <= 0.01 THEN 'Bottom 1%'
                        WHEN pct_rank <= 0.05 THEN 'Bottom 5%'
                    END as percentile_group
                FROM percentiles
                WHERE pct_rank >= 0.95 OR pct_rank <= 0.05
                ORDER BY batting_avg DESC
            """,
            complexity=QueryComplexity.EXPERT,
            tags=["anomaly", "statistics", "outliers"],
        )

        queries["anomaly_yoy_improvements"] = Query(
            name="Biggest Year-to-Year Improvements",
            description="Players with dramatic statistical improvements",
            sql="""
                WITH yoy_comparison AS (
                    SELECT
                        p1.name,
                        p1.year as year1,
                        p2.year as year2,
                        CAST(p1.value AS INTEGER) as hr_year1,
                        CAST(p2.value AS INTEGER) as hr_year2,
                        CAST(p2.value AS INTEGER) - CAST(p1.value AS INTEGER) as improvement
                    FROM player_stats p1
                    JOIN player_stats p2
                        ON p1.name = p2.name
                        AND p1.statistic = p2.statistic
                        AND p2.year = p1.year + 1
                    WHERE p1.statistic = 'Home Runs'
                )
                SELECT * FROM yoy_comparison
                WHERE improvement >= 20
                ORDER BY improvement DESC
                LIMIT 25
            """,
            complexity=QueryComplexity.ADVANCED,
            tags=["anomaly", "improvement", "year-over-year"],
        )

        # Data Quality Queries
        queries["quality_validation_report"] = Query(
            name="Data Quality Validation Report",
            description="Check for various data quality issues",
            sql="""
                SELECT 'Invalid Batting Averages (> 1.0)' as issue_type,
                       COUNT(*) as count
                FROM player_stats
                WHERE statistic = 'Batting Average'
                  AND CAST(value AS REAL) > 1.0

                UNION ALL

                SELECT 'Suspicious Home Run Totals (> 100)', COUNT(*)
                FROM player_stats
                WHERE statistic = 'Home Runs'
                  AND CAST(value AS INTEGER) > 100

                UNION ALL

                SELECT 'Zero or Negative ERAs', COUNT(*)
                FROM pitcher_stats
                WHERE statistic = 'ERA'
                  AND CAST(value AS REAL) <= 0

                UNION ALL

                SELECT 'Non-numeric Statistical Values', COUNT(*)
                FROM player_stats
                WHERE value LIKE '%[a-zA-Z]%'
                   OR value LIKE '%-%'
                   OR value = ''
            """,
            complexity=QueryComplexity.INTERMEDIATE,
            tags=["data-quality", "validation"],
        )

        queries["quality_duplicate_check"] = Query(
            name="Duplicate Record Detection",
            description="Find potential duplicate entries in the database",
            sql="""
                SELECT
                    'player_stats' as table_name,
                    year, name, team, statistic,
                    COUNT(*) as duplicate_count
                FROM player_stats
                GROUP BY year, name, team, statistic
                HAVING COUNT(*) > 1

                UNION ALL

                SELECT
                    'pitcher_stats',
                    year, name, team, statistic,
                    COUNT(*)
                FROM pitcher_stats
                GROUP BY year, name, team, statistic
                HAVING COUNT(*) > 1

                ORDER BY duplicate_count DESC
            """,
            complexity=QueryComplexity.INTERMEDIATE,
            tags=["data-quality", "duplicates"],
        )

        # Advanced Historical Analysis
        queries["advanced_era_normalized"] = Query(
            name="ERA+ Leaders (Normalized by League Average)",
            description="Find dominant pitchers relative to their era",
            sql="""
                WITH era_stats AS (
                    SELECT
                        year, league, name, team,
                        CAST(value AS REAL) as era_value
                    FROM pitcher_stats
                    WHERE statistic = 'ERA'
                      AND CAST(value AS REAL) > 0
                ),
                era_rankings AS (
                    SELECT
                        year, league, name, team, era_value,
                        RANK() OVER (PARTITION BY year, league ORDER BY era_value) as era_rank,
                        AVG(era_value) OVER (PARTITION BY year, league) as league_avg_era
                    FROM era_stats
                )
                SELECT
                    year, league, name, team,
                    ROUND(era_value, 2) as era,
                    ROUND(league_avg_era, 2) as league_avg,
                    ROUND((league_avg_era / era_value) * 100, 0) as era_plus
                FROM era_rankings
                WHERE era_rank <= 5
                ORDER BY year DESC, league, era_rank
            """,
            complexity=QueryComplexity.EXPERT,
            tags=["advanced", "era", "normalized", "window-functions"],
        )

        queries["advanced_triple_crown_watch"] = Query(
            name="Triple Crown Watch",
            description="Players leading in batting average, home runs, and RBIs",
            sql="""
                WITH stat_leaders AS (
                    SELECT
                        year, league, statistic, name,
                        RANK() OVER (
                            PARTITION BY year, league, statistic
                            ORDER BY CAST(value AS REAL) DESC
                        ) as stat_rank
                    FROM player_stats
                    WHERE statistic IN ('Batting Average', 'Home Runs', 'RBI')
                      AND CAST(value AS REAL) > 0
                ),
                triple_crown_candidates AS (
                    SELECT
                        year, league, name,
                        SUM(CASE WHEN stat_rank = 1 THEN 1 ELSE 0 END) as categories_led
                    FROM stat_leaders
                    GROUP BY year, league, name
                    HAVING categories_led >= 2
                )
                SELECT * FROM triple_crown_candidates
                ORDER BY year DESC, categories_led DESC
            """,
            complexity=QueryComplexity.EXPERT,
            tags=["advanced", "triple-crown", "leaders"],
        )

        # More queries would be added here following the same pattern...
        # This is a subset to demonstrate the structure

        return queries

    def _organize_categories(self) -> Dict[str, Dict[str, Any]]:
        """
        Organize queries into a hierarchical category structure.
        This makes the queries easier to navigate and discover.
        """
        return {
            "1": {
                "name": "Database Overview & Structure",
                "description": "Understand the scope and structure of your data",
                "subcategories": {
                    "a": {
                        "name": "Temporal Analysis",
                        "queries": [
                            "db_overview_temporal_scope",
                            "db_overview_missing_years",
                        ],
                    },
                    "b": {
                        "name": "Statistical Coverage",
                        "queries": ["stat_types_overview"],
                    },
                },
            },
            "2": {
                "name": "Record-Breaking Performances",
                "description": "Find the most exceptional single-season achievements",
                "subcategories": {
                    "a": {
                        "name": "Batting Records",
                        "queries": [
                            "basic_home_run_leaders",
                            "basic_batting_avg_leaders",
                        ],
                    },
                    "b": {
                        "name": "Pitching Records",
                        "queries": ["basic_era_leaders"],
                    },
                },
            },
            "3": {
                "name": "Historical Trends & Evolution",
                "description": "See how baseball has evolved over the decades",
                "subcategories": {
                    "a": {
                        "name": "Offensive Evolution",
                        "queries": ["trend_home_runs_by_decade"],
                    },
                    "b": {
                        "name": "Pitching Evolution",
                        "queries": ["trend_pitching_era_by_decade"],
                    },
                },
            },
            "4": {
                "name": "Career Analysis",
                "description": "Analyze careers spanning multiple seasons",
                "subcategories": {
                    "a": {
                        "name": "Career Longevity",
                        "queries": ["career_longest_careers"],
                    },
                    "b": {
                        "name": "Career Achievements",
                        "queries": [
                            "career_home_run_totals",
                            "career_pitcher_wins",
                        ],
                    },
                },
            },
            "5": {
                "name": "Team Analysis",
                "description": "Explore team performance and dynasties",
                "subcategories": {
                    "a": {
                        "name": "Team Dynasties",
                        "queries": ["team_dynasties", "team_best_seasons"],
                    }
                },
            },
            "6": {
                "name": "League Comparisons",
                "description": "Compare American League vs National League",
                "subcategories": {
                    "a": {
                        "name": "Performance Comparisons",
                        "queries": [
                            "league_offensive_comparison",
                            "league_competitive_balance",
                        ],
                    }
                },
            },
            "7": {
                "name": "Cross-Table Analysis",
                "description": "Combine player and team data for deeper insights",
                "subcategories": {
                    "a": {
                        "name": "Player-Team Correlations",
                        "queries": [
                            "cross_stars_on_winners",
                            "cross_pitcher_team_correlation",
                        ],
                    }
                },
            },
            "8": {
                "name": "Statistical Anomalies",
                "description": "Find outliers and unusual performances",
                "subcategories": {
                    "a": {
                        "name": "Performance Outliers",
                        "queries": [
                            "anomaly_batting_avg_outliers",
                            "anomaly_yoy_improvements",
                        ],
                    }
                },
            },
            "9": {
                "name": "Data Quality & Validation",
                "description": "Verify data integrity and find potential issues",
                "subcategories": {
                    "a": {
                        "name": "Quality Checks",
                        "queries": [
                            "quality_validation_report",
                            "quality_duplicate_check",
                        ],
                    }
                },
            },
            "10": {
                "name": "Advanced Analytics",
                "description": "Complex statistical analysis and normalized metrics",
                "subcategories": {
                    "a": {
                        "name": "Era-Adjusted Statistics",
                        "queries": [
                            "advanced_era_normalized",
                            "advanced_triple_crown_watch",
                        ],
                    }
                },
            },
        }

    def get_query(self, query_id: str) -> Optional[Query]:
        """Retrieve a query by its ID"""
        return self.queries.get(query_id)

    def search_queries(self, search_term: str) -> List[str]:
        """Search for queries by name, description, or tags"""
        search_term = search_term.lower()
        matching_queries = []

        for query_id, query in self.queries.items():
            if (
                search_term in query.name.lower()
                or search_term in query.description.lower()
                or any(search_term in tag for tag in query.tags)
            ):
                matching_queries.append(query_id)

        return matching_queries

    def get_queries_by_complexity(
        self, complexity: QueryComplexity
    ) -> List[str]:
        """Get all queries of a specific complexity level"""
        return [
            qid
            for qid, q in self.queries.items()
            if q.complexity == complexity
        ]

    def get_queries_by_tag(self, tag: str) -> List[str]:
        """Get all queries with a specific tag"""
        return [qid for qid, q in self.queries.items() if tag in q.tags]


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
    """
    Enhanced SQL Query Explorer that uses the QueryLibrary for better organization.
    This class focuses on execution and user interaction.
    """

    def __init__(self, db_path: str = "baseball_history.db"):
        """Initialize the query explorer with database connection and query library."""
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")

        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

        # Initialize the query library
        self.library = QueryLibrary()

        # Check for and setup math functions if needed
        try:
            self.conn.execute("SELECT SQRT(4)")
            self.has_math_functions = True
        except sqlite3.OperationalError:
            self.has_math_functions = False
            self.conn.create_function("SQRT", 1, math.sqrt)
            self.conn.create_function("POWER", 2, math.pow)
            self.conn.create_aggregate("STDDEV", 1, StddevAggregate)

        print(f"Connected to database: {self.db_path.name}")
        print(
            f"Loaded {len(self.library.queries)} queries in {len(self.library.categories)} categories"
        )
        if not self.has_math_functions:
            print("Note: Using custom math functions for compatibility")

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
        """Run the enhanced interactive query explorer interface."""
        print("\n" + "=" * 80)
        print("Enhanced Baseball Statistics Database - SQL Query Explorer")
        print("=" * 80)
        print(
            "\nThis tool provides a comprehensive collection of SQL queries that demonstrate"
        )
        print("various techniques for exploring baseball statistics data.")
        print(f"\nTotal queries available: {len(self.library.queries)}")

        while True:
            print("\n" + "-" * 60)
            print("Main Menu:")
            print("-" * 60)

            print("\n1. Browse by Category")
            print("2. Search Queries")
            print("3. Filter by Complexity")
            print("4. Filter by Tag")
            print("5. Execute Custom SQL")
            print("6. Run Random Sample")
            print("Q. Quit")

            choice = input("\nSelect option: ").strip().upper()

            if choice == "Q":
                break
            elif choice == "1":
                self._browse_categories()
            elif choice == "2":
                self._search_interface()
            elif choice == "3":
                self._filter_by_complexity()
            elif choice == "4":
                self._filter_by_tag()
            elif choice == "5":
                self._custom_query_mode()
            elif choice == "6":
                self._run_random_sample()
            else:
                print("Invalid choice. Please try again.")

    def _browse_categories(self):
        """Browse queries organized by category."""
        while True:
            print("\n" + "=" * 60)
            print("Query Categories:")
            print("=" * 60)

            for key, category in sorted(self.library.categories.items()):
                print(f"\n{key}. {category['name']}")
                print(f"   {category['description']}")

            print("\nB. Back to main menu")

            choice = input("\nSelect category: ").strip().upper()

            if choice == "B":
                break
            elif choice in self.library.categories:
                self._explore_category(choice)
            else:
                print("Invalid choice. Please try again.")

    def _explore_category(self, category_key: str):
        """Explore queries within a selected category."""
        category = self.library.categories[category_key]

        while True:
            print(f"\n{'=' * 60}")
            print(f"Category: {category['name']}")
            print(f"{'=' * 60}")

            for sub_key, subcategory in sorted(
                category["subcategories"].items()
            ):
                print(f"\n{sub_key}. {subcategory['name']}")
                print(f"   Queries: {len(subcategory['queries'])}")

            print("\nB. Back to categories")
            print("*. Run all queries in this category")

            choice = input("\nSelect subcategory: ").strip().lower()

            if choice == "b":
                break
            elif choice == "*":
                self._run_all_in_category(category)
            elif choice in category["subcategories"]:
                self._explore_subcategory(category, choice)
            else:
                print("Invalid choice. Please try again.")

    def _explore_subcategory(self, category: Dict, sub_key: str):
        """Explore queries within a subcategory."""
        subcategory = category["subcategories"][sub_key]

        while True:
            print(f"\n{'=' * 60}")
            print(f"Subcategory: {subcategory['name']}")
            print(f"{'=' * 60}")

            for i, query_id in enumerate(subcategory["queries"], 1):
                query = self.library.get_query(query_id)
                if query:
                    print(f"\n{i}. {query.name}")
                    print(f"   {query.description}")
                    print(f"   Complexity: {query.complexity.value}")
                    print(f"   Tags: {', '.join(query.tags)}")

            print("\nB. Back")
            print("*. Run all queries")

            choice = input("\nSelect query number: ").strip().lower()

            if choice == "b":
                break
            elif choice == "*":
                for query_id in subcategory["queries"]:
                    query = self.library.get_query(query_id)
                    if query:
                        self.execute_query(query.sql, query.name)
                        input("\nPress Enter to continue...")
            else:
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(subcategory["queries"]):
                        query_id = subcategory["queries"][idx]
                        query = self.library.get_query(query_id)
                        if query:
                            self.execute_query(query.sql, query.name)
                            print(
                                f"\nSQL Query:\n{'-' * 40}\n{query.sql.strip()}\n{'-' * 40}"
                            )
                            input("\nPress Enter to continue...")
                except ValueError:
                    print("Invalid choice. Please enter a number.")

    def _search_interface(self):
        """Search for queries by keyword."""
        search_term = input("\nEnter search term: ").strip()

        if not search_term:
            return

        matching_queries = self.library.search_queries(search_term)

        if not matching_queries:
            print(f"\nNo queries found matching '{search_term}'")
            return

        print(f"\nFound {len(matching_queries)} matching queries:")

        for i, query_id in enumerate(matching_queries, 1):
            query = self.library.get_query(query_id)
            if query:
                print(f"\n{i}. {query.name}")
                print(f"   {query.description}")

        choice = input("\nSelect query number (or Enter to cancel): ").strip()

        if choice:
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(matching_queries):
                    query_id = matching_queries[idx]
                    query = self.library.get_query(query_id)
                    if query:
                        self.execute_query(query.sql, query.name)
                        input("\nPress Enter to continue...")
            except ValueError:
                print("Invalid choice.")

    def _filter_by_complexity(self):
        """Filter queries by complexity level."""
        print("\nComplexity Levels:")
        for i, complexity in enumerate(QueryComplexity, 1):
            print(f"{i}. {complexity.value.capitalize()}")

        choice = input("\nSelect complexity level: ").strip()

        try:
            idx = int(choice) - 1
            complexities = list(QueryComplexity)
            if 0 <= idx < len(complexities):
                selected_complexity = complexities[idx]
                query_ids = self.library.get_queries_by_complexity(
                    selected_complexity
                )

                print(
                    f"\nFound {len(query_ids)} {selected_complexity.value} queries:"
                )

                for i, query_id in enumerate(
                    query_ids[:10], 1
                ):  # Show first 10
                    query = self.library.get_query(query_id)
                    if query:
                        print(f"\n{i}. {query.name}")
                        print(f"   {query.description}")

                if len(query_ids) > 10:
                    print(f"\n... and {len(query_ids) - 10} more")
        except (ValueError, IndexError):
            print("Invalid choice.")

    def _filter_by_tag(self):
        """Filter queries by tag."""
        # Collect all unique tags
        all_tags = set()
        for query in self.library.queries.values():
            all_tags.update(query.tags)

        sorted_tags = sorted(all_tags)

        print("\nAvailable Tags:")
        for i, tag in enumerate(sorted_tags, 1):
            print(f"{i}. {tag}")

        choice = input("\nSelect tag number: ").strip()

        try:
            idx = int(choice) - 1
            if 0 <= idx < len(sorted_tags):
                selected_tag = sorted_tags[idx]
                query_ids = self.library.get_queries_by_tag(selected_tag)

                print(
                    f"\nFound {len(query_ids)} queries with tag '{selected_tag}':"
                )

                for i, query_id in enumerate(
                    query_ids[:10], 1
                ):  # Show first 10
                    query = self.library.get_query(query_id)
                    if query:
                        print(f"\n{i}. {query.name}")
                        print(f"   {query.description}")

                if len(query_ids) > 10:
                    print(f"\n... and {len(query_ids) - 10} more")
        except (ValueError, IndexError):
            print("Invalid choice.")

    def _run_random_sample(self):
        """Run a random sample of queries."""
        import random

        sample_size = 5
        query_ids = random.sample(
            list(self.library.queries.keys()),
            min(sample_size, len(self.library.queries)),
        )

        print(f"\nRunning {len(query_ids)} random queries:")

        for query_id in query_ids:
            query = self.library.get_query(query_id)
            if query:
                print(f"\n{'=' * 60}")
                print(f"Query: {query.name}")
                print(f"Complexity: {query.complexity.value}")
                print(f"Tags: {', '.join(query.tags)}")
                print("=" * 60)
                self.execute_query(query.sql, query.name)
                input("\nPress Enter for next query...")

    def _run_all_in_category(self, category: Dict):
        """Run all queries in a category."""
        total_queries = sum(
            len(sub["queries"]) for sub in category["subcategories"].values()
        )

        print(
            f"\nRunning all {total_queries} queries in category: {category['name']}"
        )

        for sub_key, subcategory in sorted(category["subcategories"].items()):
            print(f"\n{'=' * 60}")
            print(f"Subcategory: {subcategory['name']}")
            print("=" * 60)

            for query_id in subcategory["queries"]:
                query = self.library.get_query(query_id)
                if query:
                    self.execute_query(query.sql, query.name)
                    input("\nPress Enter for next query...")

    def _custom_query_mode(self):
        """Allow users to enter custom SQL queries."""
        print("\n" + "=" * 60)
        print("Custom SQL Query Mode")
        print("=" * 60)
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
                    continue

                query_lines.append(line)
                if line.strip().endswith(";"):
                    break
                prompt = "...> "

            full_query = "\n".join(query_lines)

            # Validate query
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
                or normalized_query.upper().startswith("EXPLAIN")
                or normalized_query.upper().startswith("PRAGMA")
            ):
                print(
                    "✗ Error: Only SELECT, WITH, EXPLAIN, or PRAGMA queries are allowed."
                )
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
        print("\n" + "=" * 60)
        print("Custom SQL Query Help")
        print("=" * 60)
        print("\nTable Structure:")
        print("  - player_stats (year, name, team, statistic, value, league)")
        print("  - pitcher_stats (year, name, team, statistic, value, league)")
        print(
            "  - team_standings (year, team, wins, losses, winning_percentage, league, division)"
        )
        print("\nExample Queries:")
        print("  -- Find players named 'Babe Ruth'")
        print("  SELECT * FROM player_stats WHERE name LIKE '%Ruth%';")
        print("\n  -- Top home run hitters in 2000")
        print("  SELECT name, value FROM player_stats")
        print("  WHERE statistic = 'Home Runs' AND year = 2000")
        print("  ORDER BY CAST(value AS INTEGER) DESC LIMIT 10;")
        print("\n  -- Team with most wins in history")
        print("  SELECT year, team, wins FROM team_standings")
        print("  ORDER BY wins DESC LIMIT 1;")

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

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            print("\nDatabase connection closed.")


def main():
    """Main entry point for the enhanced SQL Query Explorer."""
    parser = argparse.ArgumentParser(
        description="Enhanced SQL Query Explorer for Baseball Statistics Database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                        # Run interactive mode
  %(prog)s --search "home runs"  # Search for queries about home runs
  %(prog)s --complexity basic     # Show all basic queries
  %(prog)s --tag pitching        # Show all pitching-related queries
  %(prog)s --export results.csv --query "SELECT * FROM player_stats LIMIT 10"
        """,
    )

    parser.add_argument(
        "--database",
        "-d",
        default="baseball_history.db",
        help="Path to database file",
    )
    parser.add_argument(
        "--search",
        "-s",
        help="Search for queries containing this term",
    )
    parser.add_argument(
        "--complexity",
        "-c",
        choices=["basic", "intermediate", "advanced", "expert"],
        help="Filter queries by complexity level",
    )
    parser.add_argument(
        "--tag",
        "-t",
        help="Filter queries by tag",
    )
    parser.add_argument(
        "--export",
        "-e",
        metavar="FILE",
        help="Export query results to CSV file",
    )
    parser.add_argument(
        "--query",
        "-q",
        help="SQL query to execute (use with --export)",
    )

    args = parser.parse_args()

    explorer = None
    try:
        explorer = SQLQueryExplorer(args.database)

        if args.export and args.query:
            explorer.export_results_to_csv(args.query, args.export)
        elif args.search:
            # Run search in non-interactive mode
            matching = explorer.library.search_queries(args.search)
            print(f"Found {len(matching)} queries matching '{args.search}':")
            for qid in matching[:10]:
                query = explorer.library.get_query(qid)
                if query:
                    print(f"\n- {query.name}")
                    print(f"  {query.description}")
        elif args.complexity:
            # Show queries of specific complexity
            complexity = QueryComplexity(args.complexity)
            query_ids = explorer.library.get_queries_by_complexity(complexity)
            print(f"Found {len(query_ids)} {args.complexity} queries")
        elif args.tag:
            # Show queries with specific tag
            query_ids = explorer.library.get_queries_by_tag(args.tag)
            print(f"Found {len(query_ids)} queries with tag '{args.tag}'")
        else:
            # Run interactive mode
            explorer.run_interactive()

    except FileNotFoundError as e:
        print(f"\n✗ FATAL ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting.")
    except Exception as e:
        logging.error("A fatal error occurred", exc_info=True)
        print(f"\n✗ A fatal error occurred: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if explorer:
            explorer.close()


if __name__ == "__main__":
    main()
