#!/usr/bin/env python3
# check_data_quality.py
"""
Comprehensive data quality check for baseball database.
This script examines whether the database contains actual player data
or just empty/null records.
"""

import os
import sqlite3
import traceback
from datetime import datetime


def check_data_quality():
    """Perform detailed data quality analysis on baseball database."""

    # Find the database - try multiple possible locations
    possible_paths = [
        "baseball_stats.db",
        os.path.join("assignment14", "baseball_stats.db"),
        os.path.join("..", "baseball_stats.db"),
    ]

    db_path = None
    for path in possible_paths:
        if os.path.exists(path):
            db_path = path
            break

    if not db_path:
        print("Error: Could not find baseball_stats.db!")
        print("Searched in:", possible_paths)
        return

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        print("Baseball Database Data Quality Analysis")
        print("=" * 60)
        print(f"Database: {db_path}")
        print(f"Analysis time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        # Tables to analyze
        player_tables = [
            "al_player_review",
            "nl_player_review",
            "al_pitcher_review",
            "nl_pitcher_review",
        ]

        standings_tables = ["al_team_standings", "nl_team_standings"]

        # Analyze player/pitcher tables
        print("\n1. PLAYER/PITCHER DATA ANALYSIS")
        print("-" * 60)

        for table in player_tables:
            print(f"\n{table.upper()}:")

            # Get total row count
            cursor = conn.execute(f"SELECT COUNT(*) as total FROM {table}")
            total_rows = cursor.fetchone()["total"]
            print(f"  Total rows: {total_rows}")

            if total_rows == 0:
                print("  Table is empty!")
                continue

            # Count rows with actual player names
            cursor = conn.execute(
                f"""
                SELECT COUNT(*) as valid_players
                FROM {table}
                WHERE player IS NOT NULL
                  AND player != ''
                  AND player != 'None'
            """
            )
            valid_players = cursor.fetchone()["valid_players"]
            print(f"  Rows with valid player names: {valid_players}")

            # Count rows with meaningful statistics
            cursor = conn.execute(
                f"""
                SELECT COUNT(*) as rows_with_stats
                FROM {table}
                WHERE (ab > 0 OR ip > 0 OR g > 0)
                  AND player IS NOT NULL
            """
            )
            rows_with_stats = cursor.fetchone()["rows_with_stats"]
            print(f"  Rows with actual statistics: {rows_with_stats}")

            # Get year range of data
            cursor = conn.execute(
                f"""
                SELECT MIN(year) as min_year, MAX(year) as max_year
                FROM {table}
                WHERE year IS NOT NULL
            """
            )
            year_data = cursor.fetchone()
            print(
                f"  Year range: {year_data['min_year']} - {year_data['max_year']}"
            )

            # Sample some rows with actual data
            cursor = conn.execute(
                f"""
                SELECT year, player, team, g, ab, h, avg, w, l, era
                FROM {table}
                WHERE player IS NOT NULL
                  AND player != ''
                  AND player != 'None'
                  AND (ab > 0 OR w > 0 OR l > 0)
                ORDER BY year DESC,
                         CASE WHEN ab > 0 THEN ab ELSE 0 END DESC,
                         CASE WHEN w > 0 THEN w ELSE 0 END DESC
                LIMIT 5
            """
            )

            sample_data = cursor.fetchall()
            if sample_data:
                print("Sample of valid data (top 5 players):")
                for row in sample_data:
                    if "player" in table:  # Position players
                        if row["ab"] and row["ab"] > 0:
                            avg_str = (
                                f"AVG: {row['avg']:.3f}"
                                if row["avg"]
                                else "AVG: N/A"
                            )
                            print(
                                f"    {row['year']}: {row['player']} ({row['team']}) "
                                f"- {row['g']}G, {row['ab']}AB, {row['h']}H, {avg_str}"
                            )
                    else:  # Pitchers
                        if (row["w"] and row["w"] > 0) or (
                            row["l"] and row["l"] > 0
                        ):
                            era_str = (
                                f"ERA: {row['era']:.2f}"
                                if row["era"]
                                else "ERA: N/A"
                            )
                            print(
                                f"    {row['year']}: {row['player']} ({row['team']}) "
                                f"- {row['w']}W-{row['l']}L, {era_str}"
                            )
            else:
                print("  No valid player data found!")

            # Check for duplicate/empty rows
            cursor = conn.execute(
                f"""
                SELECT COUNT(*) as empty_rows
                FROM {table}
                WHERE (player IS NULL OR player = '' OR player = 'None')
                  AND (ab = 0 OR ab IS NULL)
                  AND (w = 0 OR w IS NULL)
                  AND (l = 0 OR l IS NULL)
            """
            )
            empty_rows = cursor.fetchone()["empty_rows"]
            print(f"  Empty/null rows: {empty_rows}")

            # Calculate data quality percentage
            if total_rows > 0:
                quality_pct = (valid_players / total_rows) * 100
                print(
                    f"  Data quality: {quality_pct:.1f}% of rows have valid player names"
                )

        # Analyze standings tables
        print("\n\n2. TEAM STANDINGS ANALYSIS")
        print("-" * 60)

        for table in standings_tables:
            print(f"\n{table.upper()}:")

            cursor = conn.execute(f"SELECT COUNT(*) as total FROM {table}")
            total_rows = cursor.fetchone()["total"]
            print(f"  Total rows: {total_rows}")

            # Count valid team entries
            cursor = conn.execute(
                f"""
                SELECT COUNT(*) as valid_teams
                FROM {table}
                WHERE team IS NOT NULL
                  AND team != ''
                  AND w >= 0
                  AND l >= 0
            """
            )
            valid_teams = cursor.fetchone()["valid_teams"]
            print(f"  Valid team records: {valid_teams}")

            # Show sample standings
            cursor = conn.execute(
                f"""
                SELECT year, division, team, w, l, wp, gb
                FROM {table}
                WHERE team IS NOT NULL
                ORDER BY year DESC, w DESC
                LIMIT 5
            """
            )

            standings = cursor.fetchall()
            if standings:
                print("  Sample standings (top 5):")
                for row in standings:
                    wp_str = f"{row['wp']:.3f}" if row["wp"] else "N/A"
                    gb_str = row["gb"] if row["gb"] else "-"
                    print(
                        f"    {row['year']} {row['division']}: {row['team']} "
                        f"({row['w']}-{row['l']}, {wp_str}, GB: {gb_str})"
                    )

        # Summary analysis
        print("\n\n3. SUMMARY ANALYSIS")
        print("-" * 60)

        # Check which years have the most complete data
        print("\nData completeness by year (AL players):")
        cursor = conn.execute(
            """
            SELECT year,
                   COUNT(*) as total_rows,
                   COUNT(CASE WHEN player IS NOT NULL
                              AND player != ''
                              AND player != 'None'
                         THEN 1 END) as valid_rows
            FROM al_player_review
            GROUP BY year
            ORDER BY year DESC
            LIMIT 10
        """
        )

        year_summary = cursor.fetchall()
        for row in year_summary:
            completeness = (
                (row["valid_rows"] / row["total_rows"] * 100)
                if row["total_rows"] > 0
                else 0
            )
            print(
                f"  {row['year']}: {row['valid_rows']}/{row['total_rows']} "
                f"valid rows ({completeness:.1f}%)"
            )

        # Final recommendation
        print("\n\n4. RECOMMENDATIONS")
        print("-" * 60)

        # Check if we have any good data at all
        cursor = conn.execute(
            """
            SELECT COUNT(*) as good_player_rows
            FROM (
                SELECT * FROM al_player_review WHERE player IS NOT NULL AND ab > 0
                UNION ALL
                SELECT * FROM nl_player_review WHERE player IS NOT NULL AND ab > 0
            )
        """
        )
        good_rows = cursor.fetchone()["good_player_rows"]

        if good_rows == 0:
            print(
                "⚠️  WARNING: The database appears to contain mostly empty data!"
            )
            print("   - Most rows have NULL player names and zero statistics")
            print("   - This suggests the data import process may have failed")
            print("   - You should re-run your data scraping/import script")
        elif good_rows < 100:
            print("⚠️  WARNING: Very limited valid data found!")
            print(
                f"   - Only {good_rows} rows contain actual player statistics"
            )
            print("   - The data import may have partially failed")
            print("   - Check your data source and import process")
        else:
            print("✓  Database contains valid player data")
            print(f"   - Found {good_rows} rows with player statistics")
            print(
                "   - However, there are many empty rows that should be cleaned"
            )
            print("   - Run the cleanup script to remove empty/duplicate rows")

        conn.close()

    except sqlite3.Error as e:
        # Handle database-specific errors
        print(f"Database error: {e}")
    except FileNotFoundError as e:
        # Handle case where database file doesn't exist
        print(f"Database file not found: {e}")
    except PermissionError as e:
        # Handle case where we don't have permission to read the database
        print(f"Permission denied accessing database: {e}")
    except KeyboardInterrupt:
        # Handle user pressing Ctrl+C
        print("\nAnalysis interrupted by user.")
    except MemoryError:
        # Handle running out of memory (possible with very large databases)
        print("Error: Insufficient memory to complete analysis.")
        print("Try analyzing smaller portions of the data.")
    except OSError as e:
        # Handle other operating system errors
        print(f"Operating system error: {e}")

        traceback.print_exc()


if __name__ == "__main__":
    check_data_quality()
