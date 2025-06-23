#!/usr/bin/env python3
# database_query_program.py
"""
Baseball Statistics Database Query Program
==========================================
A command-line interface for querying baseball statistics with support for
complex queries, joins, and flexible filtering.

This version is designed to work with the normalized database schema where
all statistics are stored in a single baseball_stats table with statistic
name-value pairs.
"""

import os
import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

# Note: To use this script, you must install the 'tabulate' library:
# pip install tabulate
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

        # Common baseball statistics we'll be working with
        self.batting_stats = [
            "Batting Average",
            "Home Runs",
            "Runs Batted In",
            "Runs",
            "Hits",
            "Doubles",
            "Triples",
            "Stolen Bases",
            "At Bats",
            "Games",
            "On-base Percentage",
            "Slugging Percentage",
        ]

        self.pitching_stats = [
            "ERA",
            "Wins",
            "Losses",
            "Saves",
            "Strikeouts",
            "Games",
            "Complete Games",
            "Shutouts",
            "Innings Pitched",
            "Base on Balls",
            "Winning Percentage",
        ]

        self.query_templates = {
            "1": "Player/Pitcher stats by year",
            "2": "Team standings with statistics",
            "3": "Top performers by statistic",
            "4": "Player career summary",
            "5": "Compare players head-to-head",
            "6": "Team performance analysis",
            "7": "Custom SQL query",
            "8": "Database diagnostics",
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

    def get_available_statistics(self, stat_type: str = None) -> List[str]:
        """Get list of available statistics in the database."""
        query = "SELECT DISTINCT statistic FROM baseball_stats"
        params = ()
        if stat_type:
            query += " WHERE stat_type = ?"
            params = (stat_type,)
        query += " ORDER BY statistic"

        results = self.execute_query(query, params)
        return [r["statistic"] for r in results] if results else []

    def pivot_statistics(
        self, player_name: str, year: int, stat_type: str
    ) -> Dict[str, Any]:
        """
        Convert the normalized statistics into a single row per player.
        This reconstructs what would be columns in a traditional baseball stats table.
        """
        query = """
            SELECT statistic, value
            FROM baseball_stats
            WHERE name = ? AND year = ? AND stat_type = ?
        """
        results = self.execute_query(query, (player_name, year, stat_type))

        if not results:
            return {}

        # Convert list of statistic-value pairs into a dictionary
        stats_dict = {"name": player_name, "year": year}
        for row in results:
            stats_dict[row["statistic"]] = row["value"]

        return stats_dict

    def get_player_stats_by_year(self):
        """Query template: Get player/pitcher statistics for a specific year."""
        print("\n=== Player/Pitcher Statistics by Year ===")

        # Get user inputs
        league = input("Enter league (AL/NL/both): ").strip()
        year_str = input("Enter year (e.g., 2024): ").strip()
        stat_type = (
            input("Enter stat type (batting/pitching/both): ").lower().strip()
        )

        try:
            year = int(year_str)
        except ValueError:
            print("Invalid year. Please enter a numeric year.")
            return

        # Map user input to database values
        league_map = {
            "al": "American League",
            "nl": "National League",
            "both": "both",
        }

        stat_type_map = {
            "batting": "player",
            "pitching": "pitcher",
            "both": "both",
        }

        db_league = league_map.get(league.lower())
        db_stat_type = stat_type_map.get(stat_type)

        if not db_league or not db_stat_type:
            print("Invalid league or stat type.")
            return

        # Build the query to get unique players/pitchers
        base_query = """
            SELECT DISTINCT name, team, league, stat_type
            FROM baseball_stats
            WHERE year = ?
        """
        params = [year]

        if db_league != "both":
            base_query += " AND league = ?"
            params.append(db_league)

        if db_stat_type != "both":
            base_query += " AND stat_type = ?"
            params.append(db_stat_type)

        base_query += " ORDER BY league, stat_type, name"

        # Get the list of players/pitchers
        players = self.execute_query(base_query, tuple(params))

        if not players:
            print("No data found for the specified criteria.")
            return

        # For each player, pivot their statistics into a row
        results = []
        for player in players:
            # Get all statistics for this player
            stats_query = """
                SELECT statistic, value
                FROM baseball_stats
                WHERE name = ? AND year = ? AND team = ? AND stat_type = ?
            """
            stats = self.execute_query(
                stats_query,
                (player["name"], year, player["team"], player["stat_type"]),
            )

            if stats:
                # Create a pivoted row
                row = {
                    "League": player["league"],
                    "Type": player["stat_type"],
                    "Name": player["name"],
                    "Team": player["team"],
                }

                # Add each statistic as a column
                for stat in stats:
                    row[stat["statistic"]] = stat["value"]

                results.append(row)

        self.display_results(results)

    def get_team_standings_with_stats(self):
        """Query template: Join team standings with aggregated team statistics."""
        print("\n=== Team Standings with Statistics ===")

        league = input("Enter league (AL/NL): ").strip()
        year_str = input("Enter year: ").strip()

        try:
            year = int(year_str)
        except ValueError:
            print("Invalid year.")
            return

        league_map = {"al": "American League", "nl": "National League"}
        db_league = league_map.get(league.lower())

        if not db_league:
            print("Invalid league. Please enter AL or NL.")
            return

        # Query to get team standings with aggregated stats
        query = """
            WITH team_stats AS (
                SELECT
                    team,
                    COUNT(DISTINCT CASE WHEN stat_type = 'player' THEN name END) as num_batters,
                    COUNT(DISTINCT CASE WHEN stat_type = 'pitcher' THEN name END) as num_pitchers,
                    COUNT(DISTINCT name) as total_players
                FROM baseball_stats
                WHERE year = ? AND league = ?
                GROUP BY team
            )
            SELECT
                s.division,
                s.team,
                s.wins,
                s.losses,
                s.winning_percentage,
                s.games_back,
                COALESCE(ts.num_batters, 0) as batters,
                COALESCE(ts.num_pitchers, 0) as pitchers,
                COALESCE(ts.total_players, 0) as total_players
            FROM team_standings s
            LEFT JOIN team_stats ts ON s.team = ts.team
            WHERE s.year = ? AND s.league = ?
            ORDER BY s.division, s.wins DESC
        """

        results = self.execute_query(query, (year, db_league, year, db_league))
        self.display_results(results)

    def get_top_performers(self):
        """Query template: Find top performers by various statistics."""
        print("\n=== Top Performers ===")

        # Show available statistics
        print("\nAvailable statistics in database:")
        stats = self.get_available_statistics()
        for i, stat in enumerate(stats, 1):
            print(f"  {i}. {stat}")

        stat_choice = input("\nEnter statistic name or number: ").strip()

        # Handle numeric choice
        try:
            stat_index = int(stat_choice) - 1
            if 0 <= stat_index < len(stats):
                selected_stat = stats[stat_index]
            else:
                print("Invalid number.")
                return
        except ValueError:
            selected_stat = stat_choice

        league = input("Enter league (AL/NL/both): ").strip()
        year = input("Enter year (or 'all' for all years): ").strip()
        limit_str = input("Top how many? (default 10): ").strip() or "10"

        try:
            limit = int(limit_str)
            if year != "all":
                year_val = int(year)
        except ValueError:
            print("Invalid input.")
            return

        # Build the query
        query = """
            SELECT
                name,
                team,
                year,
                league,
                statistic,
                value,
                stat_type
            FROM baseball_stats
            WHERE statistic = ?
        """
        params = [selected_stat]

        # Add league filter
        league_map = {"al": "American League", "nl": "National League"}
        if league.lower() in league_map:
            query += " AND league = ?"
            params.append(league_map[league.lower()])

        # Add year filter
        if year != "all":
            query += " AND year = ?"
            params.append(year_val)

        # Determine sort order based on statistic type
        # Lower is better for ERA, higher is better for most other stats
        if selected_stat.upper() == "ERA":
            query += " ORDER BY CAST(value AS REAL) ASC"
        else:
            query += " ORDER BY CAST(value AS REAL) DESC"

        query += f" LIMIT {limit}"

        results = self.execute_query(query, tuple(params))
        self.display_results(results)

    def get_player_career_summary(self):
        """Query template: Get career summary for a specific player."""
        print("\n=== Player Career Summary ===")

        player_name = input(
            "Enter player name (partial match supported): "
        ).strip()
        search_pattern = f"%{player_name}%"

        # First, find all matching players and their types
        query = """
            SELECT DISTINCT name, stat_type
            FROM baseball_stats
            WHERE name LIKE ?
            ORDER BY name
        """

        matches = self.execute_query(query, (search_pattern,))

        if not matches:
            print(f"No players found matching '{player_name}'")
            return

        print(f"\nFound {len(matches)} player(s) matching '{player_name}':")

        for match in matches:
            print(f"\n--- {match['name']} ({match['stat_type']}) ---")

            # Get career span
            span_query = """
                SELECT
                    MIN(year) as first_year,
                    MAX(year) as last_year,
                    COUNT(DISTINCT year) as seasons,
                    COUNT(DISTINCT team) as teams
                FROM baseball_stats
                WHERE name = ? AND stat_type = ?
            """

            span = self.execute_query(
                span_query, (match["name"], match["stat_type"])
            )
            if span:
                s = span[0]
                print(
                    f"Career: {s['first_year']}-{s['last_year']} ({s['seasons']} seasons, {s['teams']} team(s))"
                )

            # Get statistics by year
            stats_query = """
                SELECT
                    year,
                    team,
                    league,
                    statistic,
                    value
                FROM baseball_stats
                WHERE name = ? AND stat_type = ?
                ORDER BY year DESC, statistic
            """

            stats = self.execute_query(
                stats_query, (match["name"], match["stat_type"])
            )

            if stats:
                # Group by year for better display
                by_year = defaultdict(
                    lambda: {"team": "", "league": "", "stats": {}}
                )

                for stat in stats:
                    year = stat["year"]
                    by_year[year]["team"] = stat["team"]
                    by_year[year]["league"] = stat["league"]
                    by_year[year]["stats"][stat["statistic"]] = stat["value"]

                # Display each year
                year_data = []
                for year in sorted(by_year.keys(), reverse=True):
                    row = {
                        "Year": year,
                        "Team": by_year[year]["team"],
                        "League": by_year[year]["league"],
                    }
                    # Add selected statistics
                    for stat_name, stat_value in by_year[year][
                        "stats"
                    ].items():
                        row[stat_name] = stat_value
                    year_data.append(row)

                self.display_results(year_data, limit=20)

    def compare_players(self):
        """Compare two players head-to-head."""
        print("\n=== Player Comparison ===")

        player1 = input("Enter first player name: ").strip()
        player2 = input("Enter second player name: ").strip()
        year = input(
            "Enter year to compare (or 'career' for all years): "
        ).strip()

        if year.lower() == "career":
            # Career comparison
            query = """
                SELECT
                    name,
                    stat_type,
                    statistic,
                    COUNT(*) as years,
                    GROUP_CONCAT(value, ', ') as values,
                    ROUND(AVG(CAST(value AS REAL)), 3) as avg_value
                FROM baseball_stats
                WHERE name IN (?, ?)
                GROUP BY name, stat_type, statistic
                ORDER BY name, statistic
            """
            params = (player1, player2)
        else:
            # Single year comparison
            try:
                year_val = int(year)
            except ValueError:
                print("Invalid year.")
                return

            query = """
                SELECT
                    name,
                    team,
                    league,
                    stat_type,
                    statistic,
                    value
                FROM baseball_stats
                WHERE name IN (?, ?) AND year = ?
                ORDER BY name, statistic
            """
            params = (player1, player2, year_val)

        results = self.execute_query(query, params)

        if not results:
            print("No data found for these players.")
            return

        # Organize results for side-by-side comparison
        comparison = defaultdict(lambda: {player1: "N/A", player2: "N/A"})

        for row in results:
            stat = row["statistic"]
            player = row["name"]
            if year.lower() == "career":
                value = f"{row['avg_value']} ({row['years']} years)"
            else:
                value = row["value"]
            comparison[stat][player] = value

        # Display comparison
        comp_data = []
        for stat, values in comparison.items():
            comp_data.append(
                {
                    "Statistic": stat,
                    player1: values.get(player1, "N/A"),
                    player2: values.get(player2, "N/A"),
                }
            )

        self.display_results(comp_data, limit=100)

    def team_performance_analysis(self):
        """Analyze team performance across years."""
        print("\n=== Team Performance Analysis ===")

        team = input("Enter team name: ").strip()

        # Get team's historical performance
        query = """
            SELECT
                year,
                division,
                wins,
                losses,
                winning_percentage,
                games_back,
                wins + losses as games_played
            FROM team_standings
            WHERE team = ?
            ORDER BY year DESC
        """

        results = self.execute_query(query, (team,))

        if not results:
            print(f"No data found for team '{team}'")

            # Show available teams
            teams_query = (
                "SELECT DISTINCT team FROM team_standings ORDER BY team"
            )
            teams = self.execute_query(teams_query)
            if teams:
                print("\nAvailable teams:")
                for t in teams:
                    print(f"  - {t['team']}")
            return

        print(f"\n{team} Historical Performance:")
        self.display_results(results)

        # Calculate summary statistics
        total_wins = sum(r["wins"] for r in results)
        total_losses = sum(r["losses"] for r in results)
        avg_win_pct = sum(r["winning_percentage"] for r in results) / len(
            results
        )

        print(f"\nSummary Statistics:")
        print(f"  Total Seasons: {len(results)}")
        print(f"  Total Wins: {total_wins}")
        print(f"  Total Losses: {total_losses}")
        print(f"  Average Win %: {avg_win_pct:.3f}")
        print(
            f"  Best Season: {max(results, key=lambda x: x['winning_percentage'])['year']} "
            f"({max(r['winning_percentage'] for r in results):.3f})"
        )
        print(
            f"  Worst Season: {min(results, key=lambda x: x['winning_percentage'])['year']} "
            f"({min(r['winning_percentage'] for r in results):.3f})"
        )

    def database_diagnostics(self):
        """Check database for data quality issues and show summary statistics."""
        print("\n=== Database Diagnostics ===")

        # Table summary
        print("\nTable Summary:")
        tables = ["baseball_stats", "team_standings", "import_history"]
        for table in tables:
            query = f"SELECT COUNT(*) as count FROM {table}"
            result = self.execute_query(query)
            if result:
                print(f"  {table}: {result[0]['count']} rows")

        # Data coverage
        print("\nData Coverage:")
        coverage_query = """
            SELECT
                MIN(year) as first_year,
                MAX(year) as last_year,
                COUNT(DISTINCT year) as years,
                COUNT(DISTINCT name) as players,
                COUNT(DISTINCT team) as teams
            FROM baseball_stats
        """
        coverage = self.execute_query(coverage_query)
        if coverage:
            c = coverage[0]
            print(
                f"  Years: {c['first_year']}-{c['last_year']} ({c['years']} years)"
            )
            print(f"  Players: {c['players']}")
            print(f"  Teams: {c['teams']}")

        # Statistics breakdown
        print("\nStatistics Breakdown:")
        stats_query = """
            SELECT
                stat_type,
                COUNT(DISTINCT name) as players,
                COUNT(DISTINCT statistic) as stat_types,
                COUNT(*) as total_records
            FROM baseball_stats
            GROUP BY stat_type
        """
        stats = self.execute_query(stats_query)
        for s in stats:
            print(
                f"  {s['stat_type']}: {s['players']} players, "
                f"{s['stat_types']} statistics, {s['total_records']} records"
            )

        # Recent imports
        print("\nRecent Imports:")
        import_query = """
            SELECT * FROM import_history
            ORDER BY import_date DESC
            LIMIT 5
        """
        imports = self.execute_query(import_query)
        if imports:
            self.display_results(imports)
        else:
            print("  No import history found")

    def custom_query(self):
        """Allow users to enter custom SQL queries."""
        print("\n=== Custom SQL Query ===")
        print("Available tables and key columns:")
        print(
            "  - baseball_stats: name, team, year, league, statistic, value, stat_type"
        )
        print(
            "  - team_standings: team, year, league, division, wins, losses, winning_percentage"
        )
        print(
            "  - import_history: filename, import_date, records_imported, status"
        )
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

        # Safety check - prevent destructive operations
        forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE"]
        if any(keyword in query.upper() for keyword in forbidden):
            print("Error: Destructive operations are not allowed.")
            return

        results = self.execute_query(query)
        if results is not None:
            self.display_results(results)

    def show_menu(self):
        """Display main menu."""
        print("\n" + "=" * 60)
        print("Baseball Statistics Database Query Program")
        print("=" * 60)
        print("\nSelect a query template:")
        for key, description in self.query_templates.items():
            print(f"  {key}. {description}")
        print("  Q. Quit")
        print("=" * 60)

    def run(self):
        """Main program loop."""
        if not self.connect():
            return

        print("\nWelcome to the Baseball Statistics Database Query Program!")
        print("This database uses a normalized structure where statistics")
        print("are stored as name-value pairs for maximum flexibility.")

        action_map = {
            "1": self.get_player_stats_by_year,
            "2": self.get_team_standings_with_stats,
            "3": self.get_top_performers,
            "4": self.get_player_career_summary,
            "5": self.compare_players,
            "6": self.team_performance_analysis,
            "7": self.custom_query,
            "8": self.database_diagnostics,
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
    # Look for database in current directory or assignment14 subdirectory
    db_path = "baseball_stats.db"
    if not os.path.exists(db_path) and os.path.exists(
        os.path.join("assignment14", db_path)
    ):
        db_path = os.path.join("assignment14", db_path)

    query_program = BaseballDatabaseQuery(db_path)
    query_program.run()


if __name__ == "__main__":
    main()
