#!/usr/bin/env python3
# year_coverage_analysis_v1.py
"""
Analyze year coverage in the baseball history database to identify gaps
and verify data completeness.
"""

import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def analyze_year_coverage(db_path):
    """
    Analyze which years are present in each table and identify gaps.

    Args:
        db_path: Path to the baseball_history.db file

    Returns:
        Dictionary with coverage analysis for each table
    """
    conn = sqlite3.connect(db_path)

    # Dictionary to store results
    coverage_report = {}

    # Tables to analyze
    tables = ["team_standings", "player_stats", "pitcher_stats"]

    for table in tables:
        # Get all years present in the table
        query = f"SELECT DISTINCT year FROM {table} ORDER BY year"
        df = pd.read_sql_query(query, conn)

        if not df.empty:
            years_present = df["year"].tolist()
            min_year = min(years_present)
            max_year = max(years_present)

            # Create complete range and find missing years
            expected_years = list(range(min_year, max_year + 1))
            missing_years = [
                year for year in expected_years if year not in years_present
            ]

            # Calculate coverage percentage
            coverage_pct = (len(years_present) / len(expected_years)) * 100

            coverage_report[table] = {
                "min_year": min_year,
                "max_year": max_year,
                "total_years": len(years_present),
                "expected_years": len(expected_years),
                "missing_years": missing_years,
                "coverage_percentage": coverage_pct,
                "years_present": years_present,
            }
        else:
            coverage_report[table] = {"error": "No data found in table"}

    conn.close()
    return coverage_report


def print_coverage_report(coverage_report):
    """
    Print a formatted coverage report showing year gaps and statistics.
    """
    print("=" * 80)
    print("BASEBALL DATABASE YEAR COVERAGE ANALYSIS")
    print("=" * 80)

    for table, data in coverage_report.items():
        print(f"\n{table.upper()}")
        print("-" * len(table))

        if "error" in data:
            print(f"ERROR: {data['error']}")
            continue

        print(f"Year Range: {data['min_year']} - {data['max_year']}")
        print(f"Total Years with Data: {data['total_years']}")
        print(f"Expected Years: {data['expected_years']}")
        print(f"Coverage: {data['coverage_percentage']:.1f}%")

        if data["missing_years"]:
            print(f"\nMissing Years ({len(data['missing_years'])} total):")
            # Group consecutive missing years for cleaner display
            missing_ranges = []
            start = data["missing_years"][0]
            end = start

            for i in range(1, len(data["missing_years"])):
                if data["missing_years"][i] == end + 1:
                    end = data["missing_years"][i]
                else:
                    if start == end:
                        missing_ranges.append(str(start))
                    else:
                        missing_ranges.append(f"{start}-{end}")
                    start = data["missing_years"][i]
                    end = start

            # Add the last range
            if start == end:
                missing_ranges.append(str(start))
            else:
                missing_ranges.append(f"{start}-{end}")

            print("  " + ", ".join(missing_ranges))
        else:
            print("\nNo missing years - complete coverage!")


def create_coverage_visualization(coverage_report, save_path=None):
    """
    Create a visual representation of year coverage across tables.
    """
    # Prepare data for visualization
    all_years = set()
    for table_data in coverage_report.values():
        if "years_present" in table_data:
            all_years.update(table_data["years_present"])

    if not all_years:
        print("No data available for visualization")
        return

    min_year = min(all_years)
    max_year = max(all_years)

    # Create figure
    fig, axes = plt.subplots(
        len(coverage_report), 1, figsize=(15, 2 * len(coverage_report))
    )
    if len(coverage_report) == 1:
        axes = [axes]

    for idx, (table, data) in enumerate(coverage_report.items()):
        ax = axes[idx]

        if "error" in data:
            ax.text(
                0.5,
                0.5,
                f"{table}: {data['error']}",
                ha="center",
                va="center",
                transform=ax.transAxes,
            )
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            continue

        # Create binary array for visualization
        years_array = []
        years_labels = []

        for year in range(min_year, max_year + 1):
            years_labels.append(str(year))
            years_array.append(1 if year in data["years_present"] else 0)

        # Create heatmap
        heatmap_data = [years_array]

        # Plot with color coding: green for present, red for missing
        sns.heatmap(
            heatmap_data,
            ax=ax,
            cmap=["#ffcccc", "#ccffcc"],
            cbar=False,
            linewidths=0.5,
            linecolor="gray",
            xticklabels=[
                y if i % 5 == 0 else "" for i, y in enumerate(years_labels)
            ],
            yticklabels=[table],
        )

        ax.set_title(f"{table} - Coverage: {data['coverage_percentage']:.1f}%")

        # Rotate x-axis labels for better readability
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    plt.suptitle(
        "Baseball Database Year Coverage Visualization", fontsize=16, y=1.02
    )
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        print(f"\nVisualization saved to: {save_path}")

    plt.show()


def check_data_consistency(db_path):
    """
    Check for data consistency across tables for overlapping years.
    """
    conn = sqlite3.connect(db_path)

    print("\n" + "=" * 80)
    print("DATA CONSISTENCY CHECK")
    print("=" * 80)

    # Get years from each table
    tables_years = {}
    for table in ["team_standings", "player_stats", "pitcher_stats"]:
        query = f"SELECT DISTINCT year FROM {table} ORDER BY year"
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            tables_years[table] = set(df["year"].tolist())

    # Find common years across all tables
    if len(tables_years) > 0:
        common_years = set.intersection(*tables_years.values())
        print(f"\nYears present in ALL tables: {len(common_years)}")
        if len(common_years) < 20:
            print(f"Common years: {sorted(common_years)}")
        else:
            print(
                f"Common years range: {min(common_years)} - {max(common_years)}"
            )

        # Find years unique to each table
        for table, years in tables_years.items():
            unique_years = years - common_years
            if unique_years:
                print(f"\nYears ONLY in {table}: {len(unique_years)}")
                if len(unique_years) < 10:
                    print(f"  {sorted(unique_years)}")
                else:
                    print(f"  Too many to list ({len(unique_years)} years)")

    # Check record counts by year for suspicious gaps
    print("\n" + "-" * 80)
    print("RECORD COUNT ANALYSIS (checking for suspiciously low counts)")
    print("-" * 80)

    for table in ["team_standings", "player_stats", "pitcher_stats"]:
        query = f"""
        SELECT year, COUNT(*) as record_count
        FROM {table}
        GROUP BY year
        ORDER BY year
        """
        df = pd.read_sql_query(query, conn)

        if not df.empty:
            avg_count = df["record_count"].mean()
            suspicious_years = df[df["record_count"] < avg_count * 0.5]

            if not suspicious_years.empty:
                print(f"\n{table}: Years with suspiciously low record counts:")
                print(f"  (Average: {avg_count:.1f} records/year)")
                for _, row in suspicious_years.iterrows():
                    print(f"  {row['year']}: {row['record_count']} records")

    conn.close()


# Main execution
if __name__ == "__main__":
    # Define the database path
    db_path = Path(
        r"C:\Users\LENOVO\python_homework\assignment14\baseball_history.db"
    )

    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        exit(1)

    # Run the analysis
    coverage_report = analyze_year_coverage(db_path)
    print_coverage_report(coverage_report)

    # Check data consistency
    check_data_consistency(db_path)

    # Create visualization (optional - comment out if matplotlib not installed)
    try:
        create_coverage_visualization(
            coverage_report, save_path="year_coverage.png"
        )
    except ImportError:
        print(
            "\nNote: Install matplotlib and seaborn for visualization support"
        )
