#!/usr/bin/env python3
# advanced_analytics.py
"""
Advanced Baseball Analytics
===========================

This script demonstrates sophisticated SQL patterns and data analysis techniques
for deriving advanced baseball metrics and insights from the database.
"""

import os
import sqlite3

import numpy as np
import pandas as pd

# --- Centralized Configuration ---
OUTPUT_DIR = "assignment14"
DB_NAME = "baseball_stats.db"
# Corrected the path to look for the database in the current directory
# instead of a subdirectory.
DB_FILE = DB_NAME
# --- End of Configuration ---


class AdvancedBaseballAnalytics:
    """
    Performs advanced analytics using complex SQL queries.
    """

    def __init__(self, db_path=DB_FILE):
        """Initialize with database connection and custom SQL functions."""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        # Enable advanced SQLite features and custom functions
        self.conn.create_function("SQRT", 1, np.sqrt)
        self.conn.create_function("POWER", 2, pow)

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def _get_combined_query_base(self, table_prefix):
        """Helper to create a UNION ALL query base for AL and NL tables."""
        return f"""
        SELECT * FROM al_{table_prefix}
        UNION ALL
        SELECT * FROM nl_{table_prefix}
        """

    def sabermetric_calculations(self, top_n=20):
        """
        Calculate advanced sabermetric statistics using SQL.
        """
        print(
            "\n"
            + "=" * 60
            + "\nADVANCED SABERMETRIC CALCULATIONS\n"
            + "=" * 60
        )
        query = """
        WITH PlayerStats AS (
            SELECT
                player, team, league, avg, hr, rbi, obp, slg, bb, sb
            FROM ({})
        ),
        CalculatedStats AS (
            SELECT
                *,
                (COALESCE(obp, 0) + COALESCE(slg, 0)) as OPS,
                -- Simplified WAR estimate for demonstration
                ROUND((COALESCE(hr, 0) * 1.4) + (COALESCE(rbi, 0) * 0.3) +
                      (COALESCE(sb, 0) * 0.2) + (COALESCE(bb, 0) * 0.1) +
                      (COALESCE(avg, 0) * 50), 2) as WAR_estimate
            FROM PlayerStats
        )
        SELECT
            player, team, league,
            ROUND(OPS, 3) as OPS, WAR_estimate,
            RANK() OVER (PARTITION BY league ORDER BY OPS DESC) as OPS_Rank
        FROM CalculatedStats
        WHERE obp IS NOT NULL AND slg IS NOT NULL
        ORDER BY OPS DESC
        LIMIT ?
        """.format(
            self._get_combined_query_base("player_review")
        )

        results = pd.read_sql_query(query, self.conn, params=(top_n,))
        print("\nTop 20 Players by OPS with estimated WAR:")
        print(results.to_string(index=False))
        return results

    def pythagorean_expectation(self):
        """
        Calculate Pythagorean expectation to predict team win-loss records.
        """
        print(
            "\n" + "=" * 60 + "\nPYTHAGOREAN EXPECTATION ANALYSIS\n" + "=" * 60
        )
        query = """
        WITH TeamStats AS (
            SELECT
                ts.team, ts.league, ts.w as actual_wins, ts.l as actual_losses,
                (ts.w + ts.l) as games,
                -- Estimate runs scored and allowed
                (SELECT SUM(r) FROM al_player_review pr WHERE pr.team = ts.team) as runs_scored,
                (SELECT SUM(r) FROM nl_player_review pr WHERE pr.team = ts.team) as runs_scored_nl,
                (SELECT SUM(er) FROM al_pitcher_review pir WHERE pir.team = ts.team) as runs_allowed,
                (SELECT SUM(er) FROM nl_pitcher_review pir WHERE pir.team = ts.team) as runs_allowed_nl
            FROM ({}) ts
        ),
        CombinedTeamStats AS (
            SELECT
                team, league, actual_wins, actual_losses, games,
                COALESCE(runs_scored, runs_scored_nl, 650) as est_runs_scored,
                COALESCE(runs_allowed, runs_allowed_nl, 650) as est_runs_allowed
            FROM TeamStats
        ),
        PythagoreanCalc AS (
            SELECT
                *,
                POWER(est_runs_scored, 1.83) as RS_exp,
                POWER(est_runs_allowed, 1.83) as RA_exp
            FROM CombinedTeamStats
        )
        SELECT
            team, league, actual_wins,
            ROUND(games * (RS_exp / (RS_exp + RA_exp))) as expected_wins,
            (actual_wins - ROUND(games * (RS_exp / (RS_exp + RA_exp)))) as luck_factor
        FROM PythagoreanCalc
        ORDER BY luck_factor DESC
        """.format(
            self._get_combined_query_base("team_standings")
        )

        results = pd.read_sql_query(query, self.conn)
        print("\nTeam Performance vs. Pythagorean Expectation:")
        print(results.to_string(index=False))
        return results

    def run_all_analytics(self):
        """Run all advanced analytics."""
        print("=" * 80)
        print("ADVANCED BASEBALL ANALYTICS SUITE")
        print("=" * 80)

        self.sabermetric_calculations()
        input("\nPress Enter to continue to Pythagorean Expectation...")
        self.pythagorean_expectation()

        print("\n" + "=" * 80)
        print("Advanced analytics complete!")
        print("=" * 80)


def main():
    """Main function to run advanced analytics."""
    # Ensure the database file exists before running
    if not os.path.exists(DB_FILE):
        print(f"Error: Database '{DB_FILE}' not found.")
        print("Please run database_import.py first.")
        return

    analytics = AdvancedBaseballAnalytics(db_path=DB_FILE)
    try:
        analytics.run_all_analytics()
    except (sqlite3.Error, pd.errors.DatabaseError) as e:
        print(f"An error occurred during analysis: {e}")
    finally:
        analytics.close()


if __name__ == "__main__":
    main()
