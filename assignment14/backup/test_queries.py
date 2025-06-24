#!/usr/bin/env python3
# test_queries.py
"""
Test Queries for Baseball Statistics Database
=============================================

This script executes a series of test queries against the baseball_stats.db
to validate its contents and demonstrate basic data retrieval patterns.
"""

import os
import sqlite3

import pandas as pd

# --- Centralized Configuration ---
# This configuration correctly finds the database whether the script is run
# from 'python_homework/' or 'python_homework/assignment14/'.
DB_NAME = "baseball_stats.db"
DB_FILE_IN_PARENT = os.path.join("assignment14", DB_NAME)
DB_FILE = DB_FILE_IN_PARENT if os.path.exists(DB_FILE_IN_PARENT) else DB_NAME
# --- End of Configuration ---


def run_test_queries():
    """
    Execute test queries to validate the baseball database.
    This function demonstrates proper error handling and result formatting.
    """
    conn = None  # Initialize conn to None
    try:
        # Establish database connection with proper error handling
        if not os.path.exists(DB_FILE):
            print(f"Error: Database '{DB_FILE}' not found.")
            print("Please run the database_import.py script first.")
            return

        conn = sqlite3.connect(DB_FILE)
        print(f"Successfully connected to database: {DB_FILE}")

        # Query 1: Count records by league for players
        print("\n=== Player Records by League ===")
        query1 = """
        SELECT league, COUNT(*) as record_count
        FROM (
            SELECT league FROM al_player_review
            UNION ALL
            SELECT league FROM nl_player_review
        )
        GROUP BY league;
        """
        result1 = pd.read_sql_query(query1, conn)
        print(result1.to_string(index=False))
        print()

        # Query 2: Aaron Judge's stats (FIXED to pivot long-format data)
        # This query now correctly gathers all stats for a single player
        # and displays them in a single row.
        print("=== Aaron Judge's Statistics (Pivoted) ===")
        query2 = """
        SELECT
            player,
            team,
            league,
            MAX(CASE WHEN statistic = 'avg' THEN value END) as "AVG",
            MAX(CASE WHEN statistic = 'hr' THEN value END) as "HR",
            MAX(CASE WHEN statistic = 'rbi' THEN value END) as "RBI",
            MAX(CASE WHEN statistic = 'obp' THEN value END) as "OBP",
            MAX(CASE WHEN statistic = 'slg' THEN value END) as "SLG"
        FROM (
            SELECT player, team, league, statistic, "#" as value
            FROM al_player_review
            UNION ALL
            SELECT player, team, league, statistic, "#" as value
            FROM nl_player_review
        )
        WHERE player = 'Aaron Judge'
        GROUP BY player, team, league;
        """
        result2 = pd.read_sql_query(query2, conn)
        if not result2.empty:
            print(result2.to_string(index=False))
        else:
            print("No aggregate records found for Aaron Judge.")
        print()

        # Query 3: AL East standings (FIXED to use correct division name)
        # Note: This will be empty if the importer script failed to load standings.
        print("=== American League East Standings ===")
        query3 = """
        SELECT team, w, l, wp, gb
        FROM al_team_standings
        WHERE division = 'East'
        ORDER BY w DESC;
        """
        result3 = pd.read_sql_query(query3, conn)
        print(result3.to_string(index=False))
        print()

        # Query 4: NL West standings (FIXED to use correct division name)
        # Note: This will be empty if the importer script failed to load standings.
        print("=== National League West Standings ===")
        query4 = """
        SELECT team, w, l, wp, gb
        FROM nl_team_standings
        WHERE division = 'West'
        ORDER BY w DESC;
        """
        result4 = pd.read_sql_query(query4, conn)
        print(result4.to_string(index=False))

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except pd.errors.DatabaseError as e:
        print(f"Pandas database error: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    run_test_queries()
