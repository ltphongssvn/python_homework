#!/usr/bin/env python3
# database_query_program.py
"""
Baseball Statistics Database Query Program
==========================================
A command-line interface for querying baseball statistics with support for
complex queries, joins, and flexible filtering.
"""

import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

# Note: To use this script, you must install the 'tabulate' library:
# pip install tabulate types-tabulate
from tabulate import tabulate


class BaseballDatabaseQuery:
    """Main class for handling database queries and user interaction"""

    def __init__(self, db_path: str):
        """
        Initialize the query program with database connection.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None

        self.tables = {
            "al_player_review": "American League Player Statistics",
            "al_pitcher_review": "American League Pitcher Statistics",
            "al_team_standings": "American League Team Standings",
            "nl_player_review": "National League Player Statistics",
            "nl_pitcher_review": "National League Pitcher Statistics",
            "nl_team_standings": "National League Team Standings",
        }

        self.query_templates = {
            "1": "Player stats by year",
            "2": "Team standings with top player",
            "3": "Top performers by statistic",
            "4": "Player career summary",
            "5": "Custom SQL query",
            "6": "Database diagnostics",
        }

    def connect(self) -> bool:
        """Establish database connection."""
        try:
            if not os.path.exists(self.db_path):
                print(f"Error: Database file '{self.db_path}' not found.")
                return False

            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            print(f"Successfully connected to database: {self.db_path}")
            return True

        except sqlite3.Error as e:
            print(f"Database connection error: {e}")
            return False

    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    def execute_query(
        self, query: str, params: Optional[Tuple] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """Execute a SQL query with error handling."""
        if not self.cursor:
            print("Error: No database connection.")
            return None
        try:
            self.cursor.execute(query, params or ())
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows] if rows else []
        except sqlite3.Error as e:
            print(f"Query execution error: {e}\nQuery: {query}")
            return None

    def display_results(self, results: List[Dict[str, Any]], limit: int = 50):
        """Display query results in a formatted table."""
        if not results:
            print("No results found.")
            return

        display_results = results[:limit]
        print(f"\nShowing {len(display_results)} of {len(results)} results:")
        print(
            tabulate(
                display_results,
                headers="keys",
                tablefmt="grid",
                floatfmt=".3f",
            )
        )
        if len(results) > limit:
            print(f"\n... and {len(results) - limit} more rows")

    def get_player_stats_by_year(self):
        """Query template: Get player statistics for a specific year."""
        print("\n=== Player Statistics by Year ===")
        league = input("Enter league (AL/NL/both): ").upper()
        year_str = input("Enter year (e.g., 2024): ")
        stat_type = input("Enter stat type (batting/pitching/both): ").lower()

        try:
            year = int(year_str)
        except ValueError:
            print("Invalid year. Please enter a numeric year.")
            return

        queries, params = [], []
        player_q = """
            SELECT '{league_code}' as league, year, player, team, pos,
                   g, ab, r, h, hr, rbi, avg, obp, slg
            FROM {league_code_lower}_player_review
            WHERE year = ? AND pos != 'P' AND player IS NOT NULL AND ab > 0
            ORDER BY avg DESC
        """
        pitcher_q = """
            SELECT '{league_code}' as league, year, player, team,
                   w, l, era, g, ip, k, bb
            FROM {league_code_lower}_pitcher_review
            WHERE year = ? AND player IS NOT NULL AND (w + l > 0 OR sv > 0)
            ORDER BY era ASC
        """
        if league in ["AL", "BOTH"]:
            if stat_type in ["batting", "both"]:
                queries.append(
                    player_q.format(league_code="AL", league_code_lower="al")
                )
                params.append((year,))
            if stat_type in ["pitching", "both"]:
                queries.append(
                    pitcher_q.format(league_code="AL", league_code_lower="al")
                )
                params.append((year,))
        if league in ["NL", "BOTH"]:
            if stat_type in ["batting", "both"]:
                queries.append(
                    player_q.format(league_code="NL", league_code_lower="nl")
                )
                params.append((year,))
            if stat_type in ["pitching", "both"]:
                queries.append(
                    pitcher_q.format(league_code="NL", league_code_lower="nl")
                )
                params.append((year,))

        all_results = []
        for i, query in enumerate(queries):
            results = self.execute_query(query, params[i])
            if results:
                all_results.extend(results)
        self.display_results(all_results)

    def get_team_standings_with_players(self):
        """Query template: Join team standings with player performance."""
        print("\n=== Team Standings with Top Players ===")
        league = input("Enter league (AL/NL): ").upper()
        year_str = input("Enter year: ")
        try:
            year = int(year_str)
        except ValueError:
            print("Invalid year.")
            return
        if league not in ["AL", "NL"]:
            print("Invalid league. Please enter AL or NL.")
            return

        query = f"""
            WITH top_players AS (
                SELECT team, player, avg, hr, rbi,
                       ROW_NUMBER() OVER (
                           PARTITION BY team ORDER BY avg DESC
                       ) as rk
                FROM {league.lower()}_player_review
                WHERE year = ? AND ab > 100
            )
            SELECT s.team, s.w, s.l, s.wp, s.gb, tp.player as top_player,
                   tp.avg as player_avg, tp.hr as player_hr,
                   tp.rbi as player_rbi
            FROM {league.lower()}_team_standings s
            LEFT JOIN top_players tp ON s.team = tp.team AND tp.rk = 1
            WHERE s.year = ? ORDER BY s.w DESC
        """
        results = self.execute_query(query, (year, year))
        self.display_results(results)

    def get_top_performers(self):
        """Query template: Find top performers by various statistics."""
        print("\n=== Top Performers ===")
        league = input("Enter league (AL/NL/both): ").upper()
        stat = input("Enter statistic (avg/hr/rbi/era/k/w): ").lower()
        year = input("Enter year (or 'all' for all years): ")
        limit_str = input("Top how many? (default 10): ") or "10"

        try:
            limit = int(limit_str)
            if year != "all":
                year_val = int(year)
        except ValueError:
            print("Invalid year or limit. Please enter numbers.")
            return

        batting_stats = ["avg", "hr", "rbi", "r", "h", "sb"]
        pitching_stats = ["era", "k", "w", "sv", "ip"]

        if stat in batting_stats:
            table_suffix, order, min_filter = (
                "_player_review",
                "DESC",
                "ab > 100",
            )
        elif stat in pitching_stats:
            table_suffix, order, min_filter = (
                "_pitcher_review",
                "DESC",
                "ip > 50",
            )
            if stat == "era":
                order = "ASC"
        else:
            print(f"Unknown statistic: {stat}")
            return

        tables = []
        if league in ["AL", "BOTH"]:
            tables.append(f"al{table_suffix}")
        if league in ["NL", "BOTH"]:
            tables.append(f"nl{table_suffix}")

        union_parts = []
        for table in tables:
            query_part = f"""
                SELECT '{table[:2].upper()}' as league, year, player, team, {stat}
                FROM {table} WHERE {min_filter}
            """
            if year != "all":
                query_part += f" AND year = {year_val}"
            union_parts.append(query_part)

        full_query = f"""
            SELECT * FROM ({' UNION ALL '.join(union_parts)})
            ORDER BY {stat} {order} LIMIT {limit}
        """
        results = self.execute_query(full_query)
        self.display_results(results)

    def get_player_career_summary(self):
        """Query template: Get career summary for a specific player."""
        print("\n=== Player Career Summary ===")
        player_name = input("Enter player name (partial match supported): ")
        search_pattern = f"%{player_name}%"

        query = """
            SELECT 'Career Batting' as stat_type, COUNT(DISTINCT year) as seasons,
                   SUM(g) as total_games, SUM(ab) as total_ab,
                   SUM(h) as total_hits, SUM(hr) as total_hr,
                   SUM(rbi) as total_rbi,
                   ROUND(CAST(SUM(h) AS FLOAT) / NULLIF(SUM(ab), 0), 3) as career_avg
            FROM (SELECT * FROM al_player_review WHERE player LIKE ?
                  UNION ALL
                  SELECT * FROM nl_player_review WHERE player LIKE ?)
            UNION ALL
            SELECT 'Career Pitching' as stat_type, COUNT(DISTINCT year) as seasons,
                   SUM(g) as total_games, SUM(w) as total_wins,
                   SUM(l) as total_losses, SUM(sv) as total_saves,
                   SUM(k) as total_strikeouts, ROUND(AVG(era), 3) as career_era
            FROM (SELECT * FROM al_pitcher_review WHERE player LIKE ?
                  UNION ALL
                  SELECT * FROM nl_pitcher_review WHERE player LIKE ?)
        """
        params = (
            search_pattern,
            search_pattern,
            search_pattern,
            search_pattern,
        )
        results = self.execute_query(query, params)
        if results:
            print(f"\nCareer Summary for players matching '{player_name}':")
            self.display_results(results)

    def database_diagnostics(self):
        """Check database for data quality issues."""
        print("\n=== Database Diagnostics ===")
        for table in self.tables:
            print(f"\nAnalyzing table: {table}")
            query = f"SELECT COUNT(*) as total FROM {table}"
            result = self.execute_query(query)
            total_rows = result[0]["total"] if result else 0
            print(f"  - Total rows: {total_rows}")

    def custom_query(self):
        """Allow users to enter custom SQL queries."""
        print("\n=== Custom SQL Query ===")
        print("Available tables:")
        for table, description in self.tables.items():
            print(f"  - {table}: {description}")
        print(
            "\nEnter your SQL query (type 'done' on a new line when finished):"
        )
        lines = []
        while True:
            line = input()
            if line.strip().lower() == "done":
                break
            lines.append(line)
        query = "\n".join(lines)

        forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE"]
        if any(keyword in query.upper() for keyword in forbidden):
            print("Error: Destructive operations are not allowed.")
            return

        results = self.execute_query(query)
        if results is not None:
            self.display_results(results)

    def show_menu(self):
        """Display main menu."""
        print("\n" + "=" * 50)
        print("Baseball Statistics Database Query Program")
        print("=" * 50)
        print("\nSelect a query template:")
        for key, description in self.query_templates.items():
            print(f"  {key}. {description}")
        print("  Q. Quit")
        print("=" * 50)

    def run(self):
        """Main program loop."""
        if not self.connect():
            return

        print("\nWelcome to the Baseball Statistics Database Query Program!")

        action_map = {
            "1": self.get_player_stats_by_year,
            "2": self.get_team_standings_with_players,
            "3": self.get_top_performers,
            "4": self.get_player_career_summary,
            "5": self.custom_query,
            "6": self.database_diagnostics,
        }

        try:
            while True:
                self.show_menu()
                choice = input("\nEnter your choice: ").strip().upper()
                if choice == "Q":
                    print("Thank you for using the program!")
                    break

                action = action_map.get(choice)
                if action:
                    action()
                else:
                    print("Invalid choice. Please try again.")
                input("\nPress Enter to continue...")
        except KeyboardInterrupt:
            print("\n\nProgram interrupted by user.")
        finally:
            self.disconnect()


def main():
    """Entry point for the program."""
    db_path = "baseball_stats.db"
    if os.path.exists(os.path.join("assignment14", db_path)):
        db_path = os.path.join("assignment14", db_path)

    query_program = BaseballDatabaseQuery(db_path)
    query_program.run()


if __name__ == "__main__":
    main()
