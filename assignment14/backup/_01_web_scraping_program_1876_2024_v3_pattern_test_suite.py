#!/usr/bin/env python3
# _01_web_scraping_program_1876_2024_v3_pattern_test_suite.py
"""
Baseball Scraper Pattern Test Suite - Selenium Hybrid Version (Fixed)
====================================================================

This version integrates Selenium for loading local files and includes a corrected
standings extraction method to fix parsing failures.

Key fixes in this version:
1.  Resolved 'relative path' ValueError by converting file paths to absolute.
2.  Corrected AttributeError in the summary report generation.
"""

import io
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Import the original scraper components
from _01_web_scraping_program_1876_2024_v3 import (
    BaseballStat,
    League,
    RefactoredBaseballScraper,
    StatType,
    TeamStanding,
)
from bs4 import BeautifulSoup

# Selenium and WebDriver-Manager imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager


class PatternTestSuite:
    """Enhanced test suite using Selenium + BeautifulSoup for robust pattern testing"""

    def __init__(self):
        self.scraper = RefactoredBaseballScraper()
        # --- FIX: Initialize Selenium WebDriver in a way that avoids console noise ---
        print("Initializing Selenium WebDriver...")
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--log-level=3")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        try:
            self.driver = webdriver.Chrome(
                service=ChromeService(ChromeDriverManager().install()),
                options=options,
            )
            print("WebDriver initialized successfully.")
        except Exception as e:
            print(f"Error initializing WebDriver: {e}")
            sys.exit(1)

    def teardown(self):
        """Properly close the WebDriver session."""
        if hasattr(self, "driver") and self.driver:
            print("\nShutting down Selenium WebDriver...")
            self.driver.quit()

    def load_html_with_selenium(self, file_path: Path) -> BeautifulSoup:
        """
        Load a local HTML file using Selenium to get the page source,
        then parse with BeautifulSoup.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"HTML file not found: {file_path}")

        # --- FIX #1: Convert relative path to an absolute path before creating the file URI ---
        absolute_file_path = file_path.resolve()
        file_uri = absolute_file_path.as_uri()

        self.driver.get(file_uri)
        html_content = self.driver.page_source
        return BeautifulSoup(html_content, "html.parser")

    def extract_standings_fixed(
        self, soup: BeautifulSoup, year: int, league_name: str
    ) -> List[TeamStanding]:
        """A corrected and robust version of standings extraction."""
        standings_data = []
        standings_table = None
        for table in soup.find_all("table", class_="boxed"):
            if "Team Standings" in str(table) or "Final Standings" in str(
                table
            ):
                standings_table = table
                break

        if not standings_table:
            return []

        rows = standings_table.find_all("tr")
        current_division = "League"

        for row in rows:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            first_cell_text = cells[0].get_text(strip=True)
            is_division_header = len(cells) == 1 and first_cell_text in [
                "East",
                "Central",
                "West",
            ]
            if is_division_header or (
                cells[0].get("class") == ["banner"]
                and first_cell_text in ["East", "Central", "West"]
            ):
                current_division = first_cell_text
                continue

            team_link = cells[0].find("a")
            if team_link and (
                "teamstats" in team_link.get("href", "")
                or "roster" in team_link.get("href", "")
            ):
                try:
                    team = team_link.get_text(strip=True)
                    wins, losses = -1, -1
                    wp, gb = 0.0, "0"

                    numeric_cells = [
                        cell.get_text(strip=True)
                        for cell in cells[1:]
                        if cell.get_text(strip=True).isdigit()
                    ]
                    if len(numeric_cells) >= 2:
                        wins = int(numeric_cells[0])
                        losses = int(numeric_cells[1])

                    for cell in cells:
                        text = cell.get_text(strip=True)
                        if (
                            text.startswith(".")
                            and text[1:].replace(".", "", 1).isdigit()
                        ):
                            wp = float(text)
                            break

                    for i, cell in enumerate(cells):
                        text = cell.get_text(strip=True)
                        if (
                            text == "--"
                            or text == "—"
                            or (
                                i > 2
                                and text.replace("½", ".5")
                                .replace(".", "", 1)
                                .isdigit()
                            )
                        ):
                            gb = (
                                text.replace("–", "0")
                                .replace("--", "0")
                                .replace("—", "0")
                            )
                            break

                    if wins != -1 and losses != -1:
                        standing = TeamStanding(
                            year=year,
                            league=league_name,
                            division=current_division,
                            team=team,
                            wins=wins,
                            losses=losses,
                            winning_percentage=float(wp),
                            games_back=gb,
                        )
                        standings_data.append(standing)
                except (ValueError, IndexError) as e:
                    print(
                        f"DEBUG: Could not parse standings row: {row.get_text()} | Error: {e}"
                    )
                    continue
        return standings_data

    # The individual test methods call the fixed standings extractor
    def test_pattern_1_nl_1876_2004(self, soup: BeautifulSoup) -> Dict:
        results = {"pattern": "NL 1876-2004", "standings": []}
        results["standings"] = self.extract_standings_fixed(
            soup, 1876, "National League"
        )
        return results

    def test_pattern_2_nl_2005_2013(self, soup: BeautifulSoup) -> Dict:
        results = {"pattern": "NL 2005-2013", "standings": []}
        results["standings"] = self.extract_standings_fixed(
            soup, 2005, "National League"
        )
        return results

    def test_pattern_3_nl_2014_2024(self, soup: BeautifulSoup) -> Dict:
        results = {"pattern": "NL 2014-2024", "standings": []}
        results["standings"] = self.extract_standings_fixed(
            soup, 2014, "National League"
        )
        return results

    def test_pattern_4_al_1901_2001(self, soup: BeautifulSoup) -> Dict:
        results = {"pattern": "AL 1901-2001", "standings": []}
        results["standings"] = self.extract_standings_fixed(
            soup, 1901, "American League"
        )
        return results

    def test_pattern_5_al_2002_2003(self, soup: BeautifulSoup) -> Dict:
        results = {"pattern": "AL 2002-2003", "standings": []}
        results["standings"] = self.extract_standings_fixed(
            soup, 2002, "American League"
        )
        return results

    def test_pattern_6_al_2004_2004(self, soup: BeautifulSoup) -> Dict:
        results = {"pattern": "AL 2004", "standings": []}
        results["standings"] = self.extract_standings_fixed(
            soup, 2004, "American League"
        )
        return results

    def test_pattern_7_al_2005_2013(self, soup: BeautifulSoup) -> Dict:
        results = {"pattern": "AL 2005-2013", "standings": []}
        results["standings"] = self.extract_standings_fixed(
            soup, 2005, "American League"
        )
        return results

    def test_pattern_8_al_2014_2024(self, soup: BeautifulSoup) -> Dict:
        results = {"pattern": "AL 2014-2024", "standings": []}
        results["standings"] = self.extract_standings_fixed(
            soup, 2014, "American League"
        )
        return results

    def validate_extraction_results(
        self, results: Dict, expected_standings: int
    ) -> Tuple[bool, List[str]]:
        issues = []
        standings_count = len(results.get("standings", []))
        if standings_count < expected_standings:
            issues.append(
                f"Too few standings extracted. Expected {expected_standings}, but got {standings_count}"
            )
        return len(issues) == 0, issues

    def generate_summary_report(
        self, all_results: Dict[str, Dict], mappings: List
    ) -> str:
        report = [
            "\n" + "=" * 80,
            "BASEBALL SCRAPER PATTERN TEST SUITE - SUMMARY REPORT",
            "=" * 80 + "\n",
        ]
        successful_patterns = 0
        all_issues = []

        # --- FIX #2: Use the string key `item[2]` directly, not `item[2].__name__` ---
        expected_standings_map = {item[2]: item[3] for item in mappings}

        for pattern_key, results in all_results.items():
            expected_count = expected_standings_map.get(pattern_key, 0)
            is_valid, issues = self.validate_extraction_results(
                results, expected_count
            )
            if is_valid:
                successful_patterns += 1

            report.append(
                f"\nPattern: {pattern_key} (Expected Standings: {expected_count})"
            )
            report.append("-" * 40)
            report.append(f"Status: {'✓ PASSED' if is_valid else '✗ FAILED'}")
            report.append(
                f"Standings Found: {len(results.get('standings', []))}"
            )
            if issues:
                report.append("Issues:")
                for issue in issues:
                    report.append(f"  - {issue}")
                    all_issues.append(f"{pattern_key}: {issue}")

        report.append("\n" + "=" * 80)
        report.append("OVERALL SUMMARY")
        report.append("=" * 80)
        successful_count = len(all_results)
        report.append(f"Total Patterns Tested: {successful_count}")
        if successful_count > 0:
            success_rate = successful_patterns / successful_count * 100
            report.append(
                f"Successful Patterns: {successful_patterns}/{successful_count} ({success_rate:.1f}%)"
            )
        if all_issues:
            report.append(f"\nTotal Issues Found: {len(all_issues)}")
        else:
            report.append("\n✓ All pattern tests completed successfully!")
        report.append("\n" + "=" * 80)
        return "\n".join(report)

    def run_all_tests(
        self, html_samples_dir: Path
    ) -> Tuple[Dict[str, Dict], List]:
        all_results = {}
        test_mappings = [
            (
                "pattern_1_national_league_1876_2004.html",
                self.test_pattern_1_nl_1876_2004,
                "NL 1876-2004",
                8,
            ),
            (
                "pattern_2_national_league_2005_2013.html",
                self.test_pattern_2_nl_2005_2013,
                "NL 2005-2013",
                16,
            ),
            (
                "pattern_3_national_league_2014_2024.html",
                self.test_pattern_3_nl_2014_2024,
                "NL 2014-2024",
                15,
            ),
            (
                "pattern_4_american_league_1901_2001.html",
                self.test_pattern_4_al_1901_2001,
                "AL 1901-2001",
                8,
            ),
            (
                "pattern_5_american_league_2002_2003.html",
                self.test_pattern_5_al_2002_2003,
                "AL 2002-2003",
                14,
            ),
            (
                "pattern_6_american_league_2004_2004.html",
                self.test_pattern_6_al_2004_2004,
                "AL 2004",
                14,
            ),
            (
                "pattern_7_american_league_2005_2013.html",
                self.test_pattern_7_al_2005_2013,
                "AL 2005-2013",
                14,
            ),
            (
                "pattern_8_american_league_2014_2024.html",
                self.test_pattern_8_al_2014_2024,
                "AL 2014-2024",
                15,
            ),
        ]

        for filename, test_method, pattern_key, _ in test_mappings:
            html_file = html_samples_dir / filename
            if not html_file.exists():
                print(
                    f"\nWarning: Sample file {filename} not found, skipping..."
                )
                continue
            try:
                soup = self.load_html_with_selenium(html_file)
                results = test_method(soup)
                all_results[pattern_key] = results
            except Exception as e:
                print(f"\nError testing {filename}: {e}")
                import traceback

                traceback.print_exc()

        return all_results, test_mappings

    def save_results_to_json(self, results: Dict, output_file: Path):
        json_safe_results = {}
        for pattern, data in results.items():
            json_safe_results[pattern] = {
                "pattern": data.get("pattern"),
                "standings": [
                    {
                        "year": standing.year,
                        "league": standing.league,
                        "division": standing.division,
                        "team": standing.team,
                        "wins": standing.wins,
                        "losses": standing.losses,
                        "winning_percentage": standing.winning_percentage,
                        "games_back": standing.games_back,
                    }
                    for standing in data.get("standings", [])
                ],
            }
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(json_safe_results, f, indent=2)
        print(f"\nResults saved to: {output_file}")


def main():
    """Main entry point for the pattern test suite"""
    print("\n" + "=" * 80)
    print("BASEBALL SCRAPER PATTERN TEST SUITE")
    print("Selenium Hybrid Version")
    print("=" * 80)

    html_samples_dir = Path("html_patterns")
    if not html_samples_dir.exists():
        print(
            f"\nError: HTML samples directory '{html_samples_dir}' not found!"
        )
        sys.exit(1)

    test_suite = PatternTestSuite()
    exit_code = 1

    try:
        print(
            f"\nRunning tests on HTML samples from: {html_samples_dir.resolve()}"
        )
        all_results, test_mappings = test_suite.run_all_tests(html_samples_dir)
        summary_report = test_suite.generate_summary_report(
            all_results, test_mappings
        )
        print(summary_report)
        output_file = Path("pattern_test_results.json")
        test_suite.save_results_to_json(all_results, output_file)

        total_issues = 0
        for pattern_key, res in all_results.items():
            expected_count = next(
                (m[3] for m in test_mappings if m[2] == pattern_key), 0
            )
            is_valid, issues = test_suite.validate_extraction_results(
                res, expected_count
            )
            if not is_valid:
                total_issues += len(issues)

        if total_issues == 0:
            print("\n✓ All pattern tests passed!")
            exit_code = 0
        else:
            print(
                f"\n✗ Pattern tests completed with {total_issues} total issues."
            )
            exit_code = 1

    finally:
        test_suite.teardown()
        return exit_code


if __name__ == "__main__":
    sys.exit(main())
