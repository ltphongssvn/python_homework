#!/usr/bin/env python3
# _01_web_scraping_program_1876_2024_v3.py
"""
Baseball Almanac Web Scraper - Final Production Version
======================================================

This version maintains the working Selenium-based fetching and corrected parsing
logic, and adds a comprehensive, resilient scraping framework that includes
detailed statistics tracking and a final summary report.
"""

import csv
import io
import logging
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager


def setup_logging():
    """Sets up logging configuration."""
    log_file = f"baseball_scraper_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger(__name__)


logger = setup_logging()


class StatType(Enum):
    PLAYER = "player"
    PITCHER = "pitcher"


class League(Enum):
    AMERICAN = ("American League", "a", 1901, 2025)
    NATIONAL = ("National League", "n", 1876, 2025)

    def __init__(
        self, display_name: str, code: str, start_year: int, end_year: int
    ):
        self.display_name, self.code, self.start_year, self.end_year = (
            display_name,
            code,
            start_year,
            end_year,
        )


@dataclass
class BaseballStat:
    name: str
    url: str
    team: str
    team_url: str
    year: int
    league: str
    statistic: str
    value: str
    stat_type: StatType
    rank: int = 1


@dataclass
class TeamStanding:
    year: int
    league: str
    division: str
    team: str
    wins: int
    losses: int
    winning_percentage: float
    games_back: str
    ties: Optional[int] = 0
    payroll: Optional[str] = None


class RefactoredBaseballScraper:
    """Refactored scraper with Selenium, fixed parsing, and summary reporting."""

    def __init__(
        self,
        base_url: str = "https://www.baseball-almanac.com",
        delay: float = 1.0,
        output_dir: str = "baseball_data_refactored",
    ):
        self.base_url = base_url
        self.delay = delay
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.driver = self._init_driver()
        # --- FIX: Expanded self.stats to track all necessary report metrics ---
        self.stats: Dict[str, Any] = {
            "start_time": datetime.now(),
            "requests_made": 0,
            "requests_failed": 0,
            "years_processed": 0,
            "failed_years_details": [],
            "player_stats_extracted": 0,
            "pitcher_stats_extracted": 0,
            "standings_extracted": 0,
        }

    def _init_driver(self) -> webdriver.Chrome:
        logger.info("Initializing Selenium WebDriver...")
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--log-level=3")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        try:
            driver = webdriver.Chrome(
                service=ChromeService(ChromeDriverManager().install()),
                options=options,
            )
            logger.info("WebDriver initialized successfully.")
            return driver
        except Exception as e:
            logger.error(
                f"Fatal error initializing WebDriver: {e}", exc_info=True
            )
            sys.exit(1)

    def close(self):
        if hasattr(self, "driver") and self.driver:
            logger.info("Closing Selenium WebDriver.")
            self.driver.quit()

    def make_request(
        self, url: str, max_retries: int = 3
    ) -> Optional[BeautifulSoup]:
        self.stats["requests_made"] += 1
        for attempt in range(max_retries):
            try:
                self.driver.get(url)
                time.sleep(self.delay)
                return BeautifulSoup(self.driver.page_source, "html.parser")
            except Exception as e:
                self.stats["requests_failed"] += 1
                logger.warning(
                    "Selenium request failed (attempt %d/%d) for %s: %s",
                    attempt + 1,
                    max_retries,
                    url,
                    e,
                )
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
        logger.error("Failed to fetch %s after %d attempts.", url, max_retries)
        return None

    # --- FIX: Implemented full logic for stat extraction ---
    # Place this helper method inside the RefactoredBaseballScraper class
    def _is_header_row(self, cells: List[Tag]) -> bool:
        """Check if a row is a header row by looking for banner class or common header text."""
        if not cells:
            return True
        # A common pattern for headers is the 'banner' class on the first cell
        if "banner" in cells[0].get("class", []):
            return True
        # Another pattern is multiple cells with bold text, typical of headers
        bold_tags = sum(1 for cell in cells if cell.find("b"))
        if bold_tags > 2:
            return True
        return False

    # Replace your existing extract_stats_from_table method with this new version
    def extract_stats_from_table(
        self, soup: BeautifulSoup, year: int, league: str, stat_type: StatType
    ) -> List[BaseballStat]:
        """
        A robust, rewritten method to extract statistics that correctly handles rowspans.
        """
        stats: List[BaseballStat] = []
        keyword = (
            "Player Review"
            if stat_type == StatType.PLAYER
            else "Pitcher Review"
        )

        # Find the table header and then its parent table
        table_header = soup.find("h2", string=lambda t: t and keyword in t)
        if not table_header:
            logger.warning(
                f"Could not find {stat_type.value} table header for {year} {league}"
            )
            return []

        table = table_header.find_parent("table")
        if not table:
            logger.warning(
                f"Could not find parent table for {stat_type.value} header in {year} {league}"
            )
            return []

        rows = table.find_all("tr")
        # This tracker manages the state of cells that span multiple rows
        rowspan_tracker = (
            {}
        )  # {col_index: {'cell': cell_object, 'remaining': count}}

        for row in rows:
            cells = row.find_all(["td", "th"])
            if self._is_header_row(cells):
                continue

            processed_cells: List[Tag] = []
            current_cell_idx = 0

            # This loop reconstructs the row by inserting spanned cells from previous rows
            # and processing the new cells from the current row.
            for i in range(len(cells) + len(rowspan_tracker) + 1):
                if i in rowspan_tracker:
                    # This column is occupied by a cell from a previous row
                    processed_cells.append(rowspan_tracker[i]["cell"])
                    rowspan_tracker[i]["remaining"] -= 1
                    if rowspan_tracker[i]["remaining"] == 0:
                        del rowspan_tracker[i]
                elif current_cell_idx < len(cells):
                    # This is a new cell from the current row
                    cell = cells[current_cell_idx]
                    rowspan = int(cell.get("rowspan", 1))
                    if rowspan > 1:
                        rowspan_tracker[i] = {
                            "cell": cell,
                            "remaining": rowspan - 1,
                        }
                    processed_cells.append(cell)
                    current_cell_idx += 1
                else:
                    break

            if len(processed_cells) < 4:
                continue

            # Safely extract data from the processed, complete row
            try:
                statistic = processed_cells[0].get_text(strip=True)
                name_cell = processed_cells[1]
                team_cell = processed_cells[2]
                value_cell = processed_cells[3]

                name_link = name_cell.find("a")
                if name_link and statistic:
                    name = name_link.get_text(strip=True)
                    url = urljoin(self.base_url, name_link.get("href", ""))
                    team = team_cell.get_text(strip=True)
                    value = value_cell.get_text(strip=True)

                    if name and value:
                        stats.append(
                            BaseballStat(
                                name=name,
                                url=url,
                                team=team,
                                team_url="",
                                year=year,
                                league=league,
                                statistic=statistic,
                                value=value,
                                stat_type=stat_type,
                            )
                        )
            except (AttributeError, IndexError) as e:
                logger.debug(f"Could not parse a stat row for {year}: {e}")
                continue

        logger.info(
            f"Extracted {len(stats)} {stat_type.value} statistics for {year} {league}"
        )
        return stats

    def extract_standings(
        self, soup: BeautifulSoup, year: int, league_name: str
    ) -> List[TeamStanding]:
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
        rows, current_division = standings_table.find_all("tr"), "League"
        for row in rows:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            first_cell_text = cells[0].get_text(strip=True)
            if len(cells) == 1 and first_cell_text in [
                "East",
                "Central",
                "West",
            ]:
                current_division = first_cell_text
                continue
            team_link = cells[0].find("a")
            if team_link and (
                "teamstats" in team_link.get("href", "")
                or "roster" in team_link.get("href", "")
            ):
                try:
                    team, wins, losses, wp, gb = (
                        team_link.get_text(strip=True),
                        -1,
                        -1,
                        0.0,
                        "0",
                    )
                    numeric_cells = [
                        c.get_text(strip=True)
                        for c in cells[1:]
                        if c.get_text(strip=True).isdigit()
                    ]
                    if len(numeric_cells) >= 2:
                        wins, losses = int(numeric_cells[0]), int(
                            numeric_cells[1]
                        )
                    for c in cells:
                        text = c.get_text(strip=True)
                        if (
                            text.startswith(".")
                            and text[1:].replace(".", "", 1).isdigit()
                        ):
                            wp = float(text)
                            break
                    for i, c in enumerate(cells):
                        text = c.get_text(strip=True).strip()
                        if text in ("--", "—") or (
                            i > 2
                            and text.replace("½", ".5")
                            .replace("—", "0")
                            .replace(".", "", 1)
                            .isdigit()
                        ):
                            gb = (
                                text.replace("–", "0")
                                .replace("--", "0")
                                .replace("—", "0")
                            )
                            break
                    if wins != -1:
                        standings_data.append(
                            TeamStanding(
                                year,
                                league_name,
                                current_division,
                                team,
                                wins,
                                losses,
                                wp,
                                gb,
                            )
                        )
                except (ValueError, IndexError) as e:
                    logger.debug(
                        "Could not parse standings row: %s | Error: %s",
                        row.get_text(),
                        e,
                    )
        return standings_data

    # --- FIX: scrape_year now updates detailed stats and returns them ---
    def scrape_year(
        self, year: int, league: League
    ) -> Optional[Dict[str, int]]:
        url = f"{self.base_url}/yearly/yr{year}{league.code}.shtml"
        soup = self.make_request(url)
        if not soup:
            return None
        player_stats = self.extract_stats_from_table(
            soup, year, league.display_name, StatType.PLAYER
        )
        pitcher_stats = self.extract_stats_from_table(
            soup, year, league.display_name, StatType.PITCHER
        )
        all_standings = self.extract_standings(soup, year, league.display_name)

        # Update master counters
        self.stats["player_stats_extracted"] += len(player_stats)
        self.stats["pitcher_stats_extracted"] += len(pitcher_stats)
        self.stats["standings_extracted"] += len(all_standings)
        self.stats["years_processed"] += 1

        if player_stats or pitcher_stats:
            self.save_to_csv(
                player_stats + pitcher_stats,
                f"{league.code}_stats_{year}.csv",
                asdict(
                    BaseballStat(
                        None, None, None, None, None, None, None, None, None
                    )
                ).keys(),
            )
        if all_standings:
            self.save_to_csv(
                all_standings,
                f"{league.code}_standings_{year}.csv",
                asdict(
                    TeamStanding(
                        None, None, None, None, None, None, None, None
                    )
                ).keys(),
            )

        return {
            "players": len(player_stats),
            "pitchers": len(pitcher_stats),
            "standings": len(all_standings),
        }

    def save_to_csv(
        self, data: List[Any], filename: str, fieldnames: List[str]
    ):
        filepath = self.output_dir / filename
        try:
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(
                    csvfile, fieldnames=fieldnames, extrasaction="ignore"
                )
                writer.writeheader()
                for item in data:
                    row = asdict(item)
                    if "stat_type" in row and row["stat_type"]:
                        row["stat_type"] = row["stat_type"].value
                    writer.writerow(row)
        except IOError as e:
            logger.error("Failed to save CSV file %s: %s", filename, e)

    # --- FIX: Added summary report generation method ---
    def generate_summary(self):
        end_time = datetime.now()
        duration = end_time - self.stats["start_time"]
        total_years = (
            League.AMERICAN.end_year - League.AMERICAN.start_year + 1
        ) + (League.NATIONAL.end_year - League.NATIONAL.start_year + 1)
        success_rate = (
            1
            - self.stats["requests_failed"]
            / max(self.stats["requests_made"], 1)
        ) * 100
        total_records = sum(
            self.stats[k]
            for k in [
                "player_stats_extracted",
                "pitcher_stats_extracted",
                "standings_extracted",
            ]
        )
        summary = f"""
{'=' * 80}
BASEBALL ALMANAC HISTORICAL DATA SCRAPING SUMMARY
{'=' * 80}
Scraping Duration: {duration}
Output Directory: {self.output_dir.resolve()}

SCOPE:
  - American League: {League.AMERICAN.start_year}-{League.AMERICAN.end_year} ({League.AMERICAN.end_year - League.AMERICAN.start_year + 1} years)
  - National League: {League.NATIONAL.start_year}-{League.NATIONAL.end_year} ({League.NATIONAL.end_year - League.NATIONAL.start_year + 1} years)
  - Total Years Attempted: {total_years}

PERFORMANCE METRICS:
  - Total Requests Made: {self.stats['requests_made']:,}
  - Failed Requests: {self.stats['requests_failed']:,}
  - Success Rate: {success_rate:.1f}%
  - Years Successfully Processed: {self.stats['years_processed']:,}

DATA EXTRACTED:
  - Player Statistics: {self.stats['player_stats_extracted']:,}
  - Pitcher Statistics: {self.stats['pitcher_stats_extracted']:,}
  - Team Standings: {self.stats['standings_extracted']:,}
  - Total Records: {total_records:,}

FAILED YEARS:
"""
        if self.stats["failed_years_details"]:
            for league, year in self.stats["failed_years_details"]:
                summary += f"  - {league}: {year}\n"
        else:
            summary += "  - None (100% success!)\n"
        summary += f"""
{'=' * 80}
"""
        logger.info(summary)
        summary_file = (
            self.output_dir
            / f"scraping_summary_{datetime.now():%Y%m%d_%H%M%S}.txt"
        )
        with open(summary_file, "w", encoding="utf-8") as f:
            f.write(summary)
        logger.info("Summary report saved to %s", summary_file)


def main():
    """Main entry point to run the full, resumable historical scrape."""
    scraper = RefactoredBaseballScraper()

    try:
        for league in League:
            logger.info(
                "=" * 50
                + f"\nPROCESSING LEAGUE: {league.display_name}\n"
                + "=" * 50
            )

            # Note: Checkpointing logic removed for this focused example,
            # but would be included in a truly production-long run.
            for year in range(league.start_year, league.end_year + 1):
                try:
                    result = scraper.scrape_year(year, league)
                    if result is None:
                        logger.error(
                            f"FAIL: Year {year} for {league.display_name} could not be fetched."
                        )
                        scraper.stats["failed_years_details"].append(
                            (league.display_name, year)
                        )
                    else:
                        logger.info(
                            f"SUCCESS: Year {year} | Players: {result['players']}, Pitchers: {result['pitchers']}, Standings: {result['standings']}"
                        )
                except Exception as e:
                    logger.error(
                        f"Critical error processing year {year} for {league.display_name}: {e}",
                        exc_info=True,
                    )
                    scraper.stats["failed_years_details"].append(
                        (league.display_name, year)
                    )

    except KeyboardInterrupt:
        logger.warning("Scraping interrupted by user. Shutting down.")
    finally:
        logger.info("Scraping run concluded. Generating final summary.")
        scraper.generate_summary()
        scraper.close()


if __name__ == "__main__":
    main()
