#!/usr/bin/env python3
# _03_database_query_program_1876_2024_v2.py
"""
Database Query Program - Enhanced for Final Schema
==================================================

An interactive command-line tool to query the baseball_history.db SQLite database.
This version implements all menu options and includes robust error handling.
"""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DatabaseQuery:
    """Handles all database query operations and user interaction."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(
                f"Database file not found: {self.db_path.resolve()}"
            )
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        # Enable foreign keys if they exist
        self.conn.execute("PRAGMA foreign_keys = ON")
        print(f"Successfully connected to database: {self.db_path.name}")

        # Cache available statistics for validation
        self._cache_available_stats()

    def _cache_available_stats(self):
        """Cache available statistics from both tables for validation."""
        cursor = self.conn.cursor()

        # Get player statistics
        cursor.execute(
            "SELECT DISTINCT statistic FROM player_stats ORDER BY statistic"
        )
        self.player_stats = [row[0] for row in cursor.fetchall()]

        # Get pitcher statistics
        cursor.execute(
            "SELECT DISTINCT statistic FROM pitcher_stats ORDER BY statistic"
        )
        self.pitcher_stats = [row[0] for row in cursor.fetchall()]

        # Get available years
        cursor.execute(
            """
            SELECT DISTINCT year FROM (
                SELECT year FROM player_stats
                UNION
                SELECT year FROM pitcher_stats
                UNION
                SELECT year FROM team_standings
            ) ORDER BY year
        """
        )
        self.available_years = [row[0] for row in cursor.fetchall()]

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print("\nDatabase connection closed.")

    def _display_results(self, cursor: sqlite3.Cursor, title: str):
        """Formats and prints query results in a clean, aligned table."""
        rows = cursor.fetchall()
        if not rows:
            print(f"\nNo results found for '{title}'.")
            return

        headers = [description[0] for description in cursor.description]
        col_widths = [len(h) for h in headers]

        # Convert rows to strings and calculate column widths
        str_rows = []
        for row in rows:
            str_row = []
            for item in row:
                if item is None:
                    str_row.append("N/A")
                elif isinstance(item, float):
                    # Format floats nicely (e.g., batting averages)
                    str_row.append(
                        f"{item:.3f}" if item < 1 else f"{item:.1f}"
                    )
                else:
                    str_row.append(str(item))
            str_rows.append(str_row)

        # Calculate maximum width for each column
        for row in str_rows:
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(cell))

        # Build header and separator
        header_str = " | ".join(
            f"{h.upper():<{col_widths[i]}}" for i, h in enumerate(headers)
        )
        separator = "-" * len(header_str)

        # Display results
        print(f"\n--- Query Results: {title} ---")
        print(header_str)
        print(separator)
        for row in str_rows:
            print(
                " | ".join(
                    f"{cell:<{col_widths[i]}}" for i, cell in enumerate(row)
                )
            )
        print(f"---\nReturned {len(rows)} row(s).")

    def _get_user_input(self, prompt: str, input_type: type = str) -> Any:
        """Generic helper to get and validate user input."""
        while True:
            try:
                user_input = input(prompt).strip()
                if input_type == str:
                    return user_input
                return input_type(user_input)
            except ValueError:
                print(
                    f"Invalid input. Please enter a value of type {input_type.__name__}."
                )
            except KeyboardInterrupt:
                print("\nOperation cancelled.")
                return None

    def _query_player_pitcher_stats(self):
        """Query 1: Player/Pitcher stats by year."""
        print(
            "\nAvailable year range:",
            f"{min(self.available_years)}-{max(self.available_years)}",
        )
        year = self._get_user_input("Enter year (e.g., 1927): ", int)
        if year is None:
            return

        stat_type = self._get_user_input(
            "Enter type ('player' or 'pitcher'): "
        ).lower()

        if stat_type not in ["player", "pitcher"]:
            print("Invalid type. Please enter 'player' or 'pitcher'.")
            return

        # Show available statistics for the chosen type
        available_stats = (
            self.player_stats if stat_type == "player" else self.pitcher_stats
        )
        print(f"\nAvailable {stat_type} statistics:")
        for i, stat in enumerate(available_stats, 1):
            print(f"  {i}. {stat}")

        stat_choice = self._get_user_input("Enter statistic name or number: ")

        # Handle numeric input
        if stat_choice.isdigit():
            idx = int(stat_choice) - 1
            if 0 <= idx < len(available_stats):
                statistic = available_stats[idx]
            else:
                print("Invalid number.")
                return
        else:
            statistic = stat_choice

        # Query the appropriate table
        table = "player_stats" if stat_type == "player" else "pitcher_stats"
        query = f"""
            SELECT name, team, value, league
            FROM {table}
            WHERE year = ? AND statistic LIKE ?
            ORDER BY
                CASE
                    WHEN statistic IN ('ERA', 'WHIP') THEN CAST(value AS REAL)
                    ELSE -CAST(value AS REAL)
                END
            LIMIT 20;
        """

        cursor = self.conn.cursor()
        cursor.execute(query, (year, f"%{statistic}%"))
        self._display_results(cursor, f"{statistic} Leaders for {year}")

    def _query_team_standings(self):
        """Query 2: Team standings with statistics."""
        year = self._get_user_input(
            "Enter year for standings (e.g., 1927): ", int
        )
        if year is None:
            return

        league = self._get_user_input(
            "Enter league ('American' or 'National'): "
        ).strip()

        # Handle various league input formats
        league_pattern = f"%{league}%"

        query = """
            SELECT
                division,
                team,
                wins,
                losses,
                winning_percentage,
                games_back
            FROM team_standings
            WHERE year = ? AND league LIKE ?
            ORDER BY division, wins DESC;
        """
        cursor = self.conn.cursor()
        cursor.execute(query, (year, league_pattern))
        self._display_results(
            cursor, f"Team Standings for {year} {league} League"
        )

    def _query_top_performers(self):
        """Query 3: Top performers by statistic."""
        # Show all available statistics
        all_stats = sorted(set(self.player_stats + self.pitcher_stats))
        print("\nAvailable statistics:")
        for i, stat in enumerate(all_stats, 1):
            stat_type = "P" if stat in self.pitcher_stats else "B"
            if stat in self.player_stats and stat in self.pitcher_stats:
                stat_type = "B/P"
            print(f"  {i:2d}. {stat} ({stat_type})")

        stat_input = self._get_user_input("\nEnter statistic name or number: ")

        # Handle numeric input
        if stat_input.isdigit():
            idx = int(stat_input) - 1
            if 0 <= idx < len(all_stats):
                stat = all_stats[idx]
            else:
                print("Invalid number.")
                return
        else:
            stat = stat_input

        limit = self._get_user_input(
            "Enter number of results to show (e.g., 10): ", int
        )
        if limit is None:
            return

        # Query both tables and combine results
        # Special handling for stats where lower is better
        lower_is_better = stat.upper() in ["ERA", "WHIP"]
        order = "ASC" if lower_is_better else "DESC"

        query = f"""
            SELECT year, name, team, value, league, stat_source
            FROM (
                SELECT year, name, team, value, league, 'player' as stat_source
                FROM player_stats WHERE statistic LIKE ?
                UNION ALL
                SELECT year, name, team, value, league, 'pitcher' as stat_source
                FROM pitcher_stats WHERE statistic LIKE ?
            )
            ORDER BY CAST(value AS REAL) {order}
            LIMIT ?;
        """
        cursor = self.conn.cursor()
        cursor.execute(query, (f"%{stat}%", f"%{stat}%", limit))
        self._display_results(cursor, f"Top {limit} Performers for '{stat}'")

    def _query_player_summary(self):
        """Query 4: Player career summary."""
        player_name = self._get_user_input("Enter player's name to search: ")
        if not player_name:
            return

        # First check if this is a hitter or pitcher
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM player_stats WHERE name LIKE ?",
            (f"%{player_name}%",),
        )
        is_hitter = cursor.fetchone()[0] > 0

        cursor.execute(
            "SELECT COUNT(*) FROM pitcher_stats WHERE name LIKE ?",
            (f"%{player_name}%",),
        )
        is_pitcher = cursor.fetchone()[0] > 0

        if is_hitter:
            # Hitting statistics summary
            query = """
                SELECT
                    year,
                    team,
                    MAX(CASE WHEN statistic = 'Batting Average' THEN value END) AS 'AVG',
                    MAX(CASE WHEN statistic = 'Home Runs' THEN value END) AS 'HR',
                    MAX(CASE WHEN statistic = 'RBI' THEN value END) AS 'RBI',
                    MAX(CASE WHEN statistic = 'Hits' THEN value END) AS 'H',
                    MAX(CASE WHEN statistic = 'Runs' THEN value END) AS 'R',
                    MAX(CASE WHEN statistic = 'Stolen Bases' THEN value END) AS 'SB'
                FROM
                    player_stats
                WHERE
                    name LIKE ?
                GROUP BY
                    year, team
                ORDER BY
                    year ASC;
            """
            cursor.execute(query, (f"%{player_name}%",))
            self._display_results(
                cursor, f"Career Hitting Summary for '{player_name}'"
            )

        if is_pitcher:
            # Pitching statistics summary
            query = """
                SELECT
                    year,
                    team,
                    MAX(CASE WHEN statistic = 'ERA' THEN value END) AS 'ERA',
                    MAX(CASE WHEN statistic = 'Wins' THEN value END) AS 'W',
                    MAX(CASE WHEN statistic = 'Losses' THEN value END) AS 'L',
                    MAX(CASE WHEN statistic = 'Strikeouts' THEN value END) AS 'SO',
                    MAX(CASE WHEN statistic = 'WHIP' THEN value END) AS 'WHIP',
                    MAX(CASE WHEN statistic = 'Saves' THEN value END) AS 'SV'
                FROM
                    pitcher_stats
                WHERE
                    name LIKE ?
                GROUP BY
                    year, team
                ORDER BY
                    year ASC;
            """
            cursor.execute(query, (f"%{player_name}%",))
            self._display_results(
                cursor, f"Career Pitching Summary for '{player_name}'"
            )

        if not is_hitter and not is_pitcher:
            print(f"No player found matching '{player_name}'")

    def _query_head_to_head(self):
        """Query 5: Compare players head-to-head."""
        player1 = self._get_user_input("Enter name of first player: ")
        player2 = self._get_user_input("Enter name of second player: ")

        if not player1 or not player2:
            return

        # Career totals comparison for hitters
        query = """
            WITH player_totals AS (
                SELECT
                    name,
                    COUNT(DISTINCT year) as seasons,
                    SUM(CASE WHEN statistic = 'Home Runs' THEN CAST(value AS INTEGER) ELSE 0 END) as Total_HR,
                    SUM(CASE WHEN statistic = 'Hits' THEN CAST(value AS INTEGER) ELSE 0 END) as Total_Hits,
                    SUM(CASE WHEN statistic = 'RBI' THEN CAST(value AS INTEGER) ELSE 0 END) as Total_RBI,
                    SUM(CASE WHEN statistic = 'Runs' THEN CAST(value AS INTEGER) ELSE 0 END) as Total_Runs,
                    ROUND(AVG(CASE WHEN statistic = 'Batting Average' THEN CAST(value AS REAL) END), 3) as Career_AVG
                FROM
                    player_stats
                WHERE
                    name LIKE ? OR name LIKE ?
                GROUP BY
                    name
            )
            SELECT * FROM player_totals
            ORDER BY Total_HR DESC;
        """
        cursor = self.conn.cursor()
        cursor.execute(query, (f"%{player1}%", f"%{player2}%"))
        self._display_results(
            cursor,
            f"Head-to-Head Career Comparison: '{player1}' vs '{player2}'",
        )

    def _query_team_performance(self):
        """Query 6: Team performance analysis."""
        team = self._get_user_input(
            "Enter team name (e.g., Yankees, Red Sox): "
        )
        if not team:
            return

        # Get year range for the team
        start_year = self._get_user_input(
            "Enter start year (or press Enter for all): "
        )
        end_year = self._get_user_input(
            "Enter end year (or press Enter for all): "
        )

        # Build year condition
        year_condition = ""
        params = [f"%{team}%"]

        if start_year and start_year.isdigit():
            year_condition += " AND year >= ?"
            params.append(int(start_year))
        if end_year and end_year.isdigit():
            year_condition += " AND year <= ?"
            params.append(int(end_year))

        query = f"""
            SELECT
                year,
                team,
                wins,
                losses,
                winning_percentage,
                games_back,
                league,
                division
            FROM team_standings
            WHERE team LIKE ? {year_condition}
            ORDER BY year DESC;
        """

        cursor = self.conn.cursor()
        cursor.execute(query, params)
        self._display_results(cursor, f"Performance History for '{team}'")

        # Additional summary statistics
        summary_query = f"""
            SELECT
                COUNT(*) as total_seasons,
                AVG(wins) as avg_wins,
                AVG(losses) as avg_losses,
                AVG(winning_percentage) as avg_win_pct,
                MAX(wins) as best_wins,
                MIN(wins) as worst_wins
            FROM team_standings
            WHERE team LIKE ? {year_condition};
        """

        cursor.execute(summary_query, params)
        summary = cursor.fetchone()

        if summary["total_seasons"] > 0:
            print(f"\nTeam Summary:")
            print(f"  Total Seasons: {summary['total_seasons']}")
            print(
                f"  Average Record: {summary['avg_wins']:.1f}-{summary['avg_losses']:.1f}"
            )
            print(f"  Average Win %: {summary['avg_win_pct']:.3f}")
            print(f"  Best Season: {summary['best_wins']} wins")
            print(f"  Worst Season: {summary['worst_wins']} wins")

    def _query_custom_sql(self):
        """Query 7: Execute custom SQL query."""
        print("\nCustom SQL Query Mode")
        print(
            "Enter your SQL query (type 'help' for examples, 'exit' to return to menu)"
        )
        print("WARNING: Only SELECT statements are allowed for safety.")

        while True:
            query = input("\nSQL> ").strip()

            if query.lower() == "exit":
                break
            elif query.lower() == "help":
                self._show_sql_help()
                continue
            elif not query:
                continue

            # Safety check - only allow SELECT statements
            if not query.upper().startswith("SELECT"):
                print("ERROR: Only SELECT statements are allowed.")
                continue

            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                self._display_results(cursor, "Custom Query Results")
            except sqlite3.Error as e:
                print(f"SQL Error: {e}")

    def _show_sql_help(self):
        """Show SQL query examples."""
        print("\nSQL Query Examples:")
        print("\n1. Find all players with 50+ home runs in a season:")
        print("   SELECT year, name, team, value FROM player_stats")
        print(
            "   WHERE statistic = 'Home Runs' AND CAST(value AS INTEGER) >= 50"
        )
        print("   ORDER BY CAST(value AS INTEGER) DESC;")

        print("\n2. Find best team winning percentages:")
        print("   SELECT year, team, wins, losses, winning_percentage")
        print("   FROM team_standings")
        print("   WHERE winning_percentage >= 0.650")
        print("   ORDER BY winning_percentage DESC;")

        print("\n3. Compare leagues by home runs:")
        print(
            "   SELECT year, league, SUM(CAST(value AS INTEGER)) as total_hr"
        )
        print("   FROM player_stats")
        print("   WHERE statistic = 'Home Runs'")
        print("   GROUP BY year, league")
        print("   ORDER BY year DESC, total_hr DESC;")

    def _database_diagnostics(self):
        """Query 8: Database diagnostics and statistics."""
        print("\nDatabase Diagnostics")
        print("=" * 60)

        cursor = self.conn.cursor()

        # Table information
        cursor.execute(
            """
            SELECT name, type
            FROM sqlite_master
            WHERE type IN ('table', 'view')
            ORDER BY type, name
        """
        )
        tables = cursor.fetchall()

        print("\nTables and Views:")
        for table in tables:
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table['name']}")
            count = cursor.fetchone()[0]
            print(f"  {table['name']} ({table['type']}): {count:,} rows")

        # Database statistics
        print("\nDatabase Statistics:")

        # Year range
        cursor.execute(
            """
            SELECT MIN(year), MAX(year) FROM (
                SELECT year FROM player_stats
                UNION SELECT year FROM pitcher_stats
                UNION SELECT year FROM team_standings
            )
        """
        )
        min_year, max_year = cursor.fetchone()
        print(f"  Year Range: {min_year} - {max_year}")

        # Unique players
        cursor.execute(
            """
            SELECT COUNT(DISTINCT name) FROM (
                SELECT name FROM player_stats
                UNION SELECT name FROM pitcher_stats
            )
        """
        )
        player_count = cursor.fetchone()[0]
        print(f"  Unique Players: {player_count:,}")

        # Unique teams
        cursor.execute("SELECT COUNT(DISTINCT team) FROM team_standings")
        team_count = cursor.fetchone()[0]
        print(f"  Unique Teams: {team_count:,}")

        # Statistics breakdown
        print("\nStatistics Available:")
        print("  Player Statistics:")
        for stat in self.player_stats:
            print(f"    - {stat}")

        print("\n  Pitcher Statistics:")
        for stat in self.pitcher_stats:
            print(f"    - {stat}")

        # Database file info
        db_size = self.db_path.stat().st_size / (1024 * 1024)  # Convert to MB
        print(f"\nDatabase File Size: {db_size:.2f} MB")
        print(f"Database Path: {self.db_path.resolve()}")

    def main_loop(self):
        """The main interactive loop for the program."""
        print("\n" + "=" * 60)
        print("Welcome to the Baseball Statistics Database Query Program!")
        print("=" * 60)
        print("\nThis database contains historical baseball statistics from")
        print(f"{min(self.available_years)} to {max(self.available_years)}")
        print("\nThe database uses a normalized structure where statistics")
        print("are stored as name-value pairs for maximum flexibility.")

        menu_options = {
            "1": (
                "Player/Pitcher stats by year",
                self._query_player_pitcher_stats,
            ),
            "2": (
                "Team standings with statistics",
                self._query_team_standings,
            ),
            "3": ("Top performers by statistic", self._query_top_performers),
            "4": ("Player career summary", self._query_player_summary),
            "5": ("Compare players head-to-head", self._query_head_to_head),
            "6": ("Team performance analysis", self._query_team_performance),
            "7": ("Custom SQL query", self._query_custom_sql),
            "8": ("Database diagnostics", self._database_diagnostics),
        }

        while True:
            print("\n" + "=" * 60)
            print("Baseball Statistics Database Query Program")
            print("=" * 60)
            print("\nSelect a query template:")
            for key, (text, _) in menu_options.items():
                print(f"  {key}. {text}")
            print("  Q. Quit")
            print("=" * 60)

            choice = input("\nEnter your choice: ").strip().lower()

            if choice == "q":
                break

            # Get the function from the dictionary and call it
            selected_option = menu_options.get(choice)
            if selected_option:
                try:
                    selected_option[1]()  # Call the function
                except Exception as e:
                    logging.error(f"Error executing query: {e}", exc_info=True)
                    print(f"\nAn error occurred: {e}")
            else:
                print("Invalid choice, please try again.")

            input("\nPress Enter to return to the menu...")


def main():
    """Main entry point for the program."""
    db_file = "baseball_history.db"
    try:
        query_tool = DatabaseQuery(db_path=db_file)
        query_tool.main_loop()
    except FileNotFoundError:
        print(f"\nERROR: Database file '{db_file}' not found.")
        print("Please ensure the database file is in the current directory.")
        print(f"Current directory: {Path.cwd()}")
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e, exc_info=True)
        print(f"\nFATAL ERROR: {e}")
    finally:
        if "query_tool" in locals() and hasattr(query_tool, "conn"):
            query_tool.close()


if __name__ == "__main__":
    main()
