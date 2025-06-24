#!/usr/bin/env python3
# csv_web_scraping_program_2000_2024.py
"""
Baseball Almanac Web Scraper - Data Collection Module
====================================================

This script focuses exclusively on web scraping functionality,
demonstrating separation of concerns and clean architecture.

Key design decisions:
- Outputs data directly to CSV files without database dependencies
- Uses dataclasses for type safety and clarity
- Implements robust error handling and retry logic
- Maintains clean interfaces for future extensibility
"""

import csv
import logging
import time
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Configure logging with structured output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("baseball_scraper.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class StatType(Enum):
    """Enumeration for statistic types"""

    PLAYER = "player"
    PITCHER = "pitcher"


@dataclass
class BaseballStat:
    """Unified data model for baseball statistics

    Using a dataclass provides:
    - Automatic __init__, __repr__, and __eq__ methods
    - Type hints for better IDE support
    - Easy conversion to dict for CSV export
    """

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
    """Data model for team standings"""

    year: int
    league: str
    division: str
    team: str
    wins: int
    losses: int
    winning_percentage: float
    games_back: str


class BaseballWebScraper:
    """Web scraper for Baseball Almanac data

    This class focuses solely on web scraping responsibilities:
    - Making HTTP requests
    - Parsing HTML content
    - Extracting structured data
    - Saving to CSV files
    """

    def __init__(
        self,
        base_url: str = "https://www.baseball-almanac.com",
        delay: float = 1.5,
        output_dir: str = "scraped_data",
    ):
        self.base_url = base_url
        self.delay = delay
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Configure session with proper headers
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Educational Baseball Data Scraper 2.0)"
                )
            }
        )

        # Track scraping statistics
        self.stats: Dict[str, int] = {
            "requests_made": 0,
            "requests_failed": 0,
            "data_extracted": 0,
        }

    def make_request(
        self, url: str, max_retries: int = 3
    ) -> Optional[BeautifulSoup]:
        """Make HTTP request with exponential backoff retry logic

        This demonstrates proper API etiquette and error handling.
        """
        self.stats["requests_made"] += 1

        for attempt in range(max_retries):
            try:
                logger.info(
                    "Requesting: %s (attempt %d/%d)",
                    url,
                    attempt + 1,
                    max_retries,
                )
                response = self.session.get(url, timeout=30)
                response.raise_for_status()

                # Parse with BeautifulSoup
                soup = BeautifulSoup(response.content, "html.parser")

                # Respect rate limits
                time.sleep(self.delay)

                return soup

            except requests.RequestException as e:
                self.stats["requests_failed"] += 1
                logger.warning("Request failed: %s", str(e))

                if attempt < max_retries - 1:
                    # Exponential backoff: 2^attempt seconds
                    wait_time = 2attempt
                    logger.info(
                        "Waiting %d seconds before retry...", wait_time
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        "Failed to fetch %s after %d attempts",
                        url,
                        max_retries,
                    )

        return None

    def extract_review_data(
        self, soup: BeautifulSoup, year: int, league: str, stat_type: StatType
    ) -> List[BaseballStat]:
        """Extract statistics from review tables

        This method demonstrates:
        - Defensive programming with null checks
        - Clear variable naming
        - Comprehensive logging for debugging
        """
        stats: List[BaseballStat] = []

        # Determine which table to look for
        table_identifier = (
            "Player Review"
            if stat_type == StatType.PLAYER
            else "Pitcher Review"
        )

        # Find the appropriate review table
        tables = soup.find_all("table", class_="boxed")
        review_table = None

        for table in tables:
            h2 = table.find("h2")
            if h2 and table_identifier in h2.get_text():
                review_table = table
                break

        if not review_table:
            logger.warning(
                "No %s table found for %d %s", table_identifier, year, league
            )
            return stats

        # Extract statistics from the table
        rows = review_table.find_all("tr")
        current_statistic = None

        for row in rows:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            # Check if this row defines a statistic category
            first_cell = cells[0]
            if "datacolBlue" in first_cell.get("class", []):
                stat_link = first_cell.find("a")
                if stat_link:
                    current_statistic = stat_link.get_text(strip=True)
                    logger.debug(
                        "Found statistic category: %s", current_statistic
                    )

            # Extract data if we have a current statistic
            if current_statistic and len(cells) >= 4:
                # Extract player/pitcher name
                name_cell = cells[1] if len(cells) > 1 else None
                if name_cell and "datacolBox" in name_cell.get("class", []):
                    name_link = name_cell.find("a")
                    if name_link:
                        name = name_link.get_text(strip=True)
                        url = urljoin(self.base_url, name_link.get("href", ""))

                        # Extract team information
                        team_cell = cells[2] if len(cells) > 2 else None
                        team_name = ""
                        team_url = ""
                        if team_cell:
                            team_link = team_cell.find("a")
                            if team_link:
                                team_name = team_link.get_text(strip=True)
                                team_url = urljoin(
                                    self.base_url, team_link.get("href", "")
                                )

                        # Extract statistical value
                        value_cell = cells[3] if len(cells) > 3 else None
                        value = (
                            value_cell.get_text(strip=True)
                            if value_cell
                            else ""
                        )

                        # Create stat object if all required data is present
                        if name and team_name and value:
                            stat = BaseballStat(
                                name=name,
                                url=url,
                                team=team_name,
                                team_url=team_url,
                                year=year,
                                league=league,
                                statistic=current_statistic,
                                value=value,
                                stat_type=stat_type,
                            )
                            stats.append(stat)
                            self.stats["data_extracted"] += 1
                            logger.debug(
                                "Extracted %s: %s - %s - %s: %s",
                                stat_type.value,
                                name,
                                team_name,
                                current_statistic,
                                value,
                            )

        logger.info(
            "Extracted %d %s statistics for %d %s",
            len(stats),
            stat_type.value,
            year,
            league,
        )
        return stats

    def extract_team_standings(
        self, soup: BeautifulSoup, year: int, league: str
    ) -> List[TeamStanding]:
        """Extract team standings data from the page

        This method handles the complex structure of standings tables,
        demonstrating pattern recognition and robust parsing.
        """
        standings_data: List[TeamStanding] = []

        logger.info("Extracting team standings for %d %s", year, league)

        # Find tables that might contain standings
        tables = soup.find_all("table")

        for table in tables:
            # Look for indicators that this is a standings table
            table_text = table.get_text().lower()

            # Check if table contains division markers
            has_divisions = any(
                marker in table_text for marker in ["east", "central", "west"]
            )

            # Check if table has wins/losses columns
            headers = [
                th.get_text(strip=True).upper() for th in table.find_all("th")
            ]
            has_standings_cols = "W" in headers and "L" in headers

            if has_divisions or has_standings_cols:
                logger.debug("Found potential standings table")
                rows = table.find_all("tr")
                current_division = None

                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if not cells:
                        continue

                    first_cell_text = cells[0].get_text(strip=True)

                    # Check if this row indicates a division
                    if first_cell_text in ["East", "Central", "West"]:
                        current_division = first_cell_text
                        logger.debug("Found division: %s", current_division)
                        continue

                    # Skip header rows
                    header_indicators = ["TEAM", "W", "L", "PCT", "GB"]
                    if any(
                        header in first_cell_text.upper()
                        for header in header_indicators
                    ):
                        continue

                    # Process team data if we have a division set
                    if current_division and len(cells) >= 5:
                        # Extract team name
                        team_cell = cells[0]
                        if "banner" in cells[0].get("class", []):
                            team_cell = (
                                cells[1] if len(cells) > 1 else cells[0]
                            )

                        team_link = team_cell.find("a")
                        team_name = (
                            team_link.get_text(strip=True)
                            if team_link
                            else team_cell.get_text(strip=True)
                        )

                        # Skip if not a valid team name
                        if not team_name or len(team_name) > 50:
                            continue

                        try:
                            # Parse wins, losses, and other stats
                            wins_val = None
                            losses_val = None
                            wp_val = None
                            gb_val = None

                            # Look for numeric values that could be W-L
                            for i, cell in enumerate(cells[1:], 1):
                                text = cell.get_text(strip=True)

                                # Look for wins (typically 40-110 range)
                                if (
                                    wins_val is None
                                    and text.isdigit()
                                    and 40 <= int(text) <= 110
                                ):
                                    wins_val = int(text)

                                    # Losses usually follow wins
                                    if i < len(cells) - 1:
                                        next_text = cells[i + 1].get_text(
                                            strip=True
                                        )
                                        if next_text.isdigit():
                                            losses_val = int(next_text)

                                    # Look for winning pct and GB after W-L
                                    if i + 2 < len(cells):
                                        wp_text = cells[i + 2].get_text(
                                            strip=True
                                        )
                                        if wp_text.startswith("."):
                                            wp_val = float(wp_text)

                                    if i + 3 < len(cells):
                                        gb_val = cells[i + 3].get_text(
                                            strip=True
                                        )
                                        if gb_val in ["-", "—"]:
                                            gb_val = "0"
                                    break

                            # Validate we have wins and losses
                            if wins_val and losses_val:
                                # Calculate winning percentage if not provided
                                if wp_val is None:
                                    total_games = wins_val + losses_val
                                    if total_games > 0:
                                        wp_val = wins_val / total_games

                                standing = TeamStanding(
                                    year=year,
                                    league=league,
                                    division=current_division,
                                    team=team_name,
                                    wins=wins_val,
                                    losses=losses_val,
                                    winning_percentage=wp_val or 0.0,
                                    games_back=gb_val or "0",
                                )
                                standings_data.append(standing)
                                self.stats["data_extracted"] += 1
                                logger.debug(
                                    "Extracted: %s %s - %s (%d-%d)",
                                    league,
                                    current_division,
                                    team_name,
                                    wins_val,
                                    losses_val,
                                )

                        except (ValueError, IndexError) as e:
                            logger.debug(
                                "Failed to parse team row: %s", str(e)
                            )
                            continue

                # If we found standings data, we can stop looking
                if standings_data:
                    break

        logger.info(
            "Extracted %d team standings for %d %s",
            len(standings_data),
            year,
            league,
        )
        return standings_data

    def save_to_csv(
        self, data: List[Any], filename: str, fieldnames: List[str]
    ):
        """Generic CSV saving method with proper error handling

        This demonstrates DRY principle and error handling best practices.
        """
        filepath = self.output_dir / filename

        try:
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for item in data:
                    # Convert dataclass to dict, handling enum values
                    row = asdict(item)
                    if "stat_type" in row:
                        row["stat_type"] = row["stat_type"].value
                    writer.writerow(row)

            logger.info("Saved %d records to %s", len(data), filepath)

        except IOError as e:
            logger.error("Failed to save CSV file %s: %s", filename, str(e))
            raise

    def save_pivoted_stats(
        self, stats: List[BaseballStat], stat_type: StatType
    ):
        """Save statistics in pivoted format for easier analysis

        This method transforms the normalized data into a wide format
        that's more familiar to analysts and easier to read.
        """
        if not stats:
            logger.warning("No %s statistics to save", stat_type.value)
            return

        # Filter stats by type
        filtered_stats = [s for s in stats if s.stat_type == stat_type]

        # Group by player/pitcher and create pivoted data
        pivoted_data: Dict[tuple, Dict[str, Any]] = {}

        for stat in filtered_stats:
            key = (stat.name, stat.team, stat.year, stat.league)

            if key not in pivoted_data:
                pivoted_data[key] = {
                    f"{stat_type.value}_name": stat.name,
                    "team": stat.team,
                    "year": stat.year,
                    "league": stat.league,
                }

            # Add statistic as a column
            pivoted_data[key][stat.statistic] = stat.value

        # Convert to list of dicts
        rows = list(pivoted_data.values())

        if rows:
            # Get all unique statistic names for column headers
            all_stats: set[str] = set()
            for row in rows:
                all_stats.update(
                    k
                    for k in row.keys()
                    if k
                    not in [
                        f"{stat_type.value}_name",
                        "team",
                        "year",
                        "league",
                    ]
                )

            # Define field order
            fieldnames = [
                f"{stat_type.value}_name",
                "team",
                "year",
                "league",
            ] + sorted(all_stats)

            # Save to CSV
            filename = f"{stat_type.value}_statistics.csv"
            filepath = self.output_dir / filename

            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            logger.info(
                "Saved %d %s records to %s",
                len(rows),
                stat_type.value,
                filepath,
            )

    def scrape_year(self, year: int) -> Dict[str, int]:
        """Scrape data for a specific year

        Returns statistics about the scraping operation.
        """
        all_stats: List[BaseballStat] = []
        all_standings: List[TeamStanding] = []

        leagues = [("a", "American League"), ("n", "National League")]

        for league_code, league_name in leagues:
            url = f"{self.base_url}/yearly/yr{year}{league_code}.shtml"
            soup = self.make_request(url)

            if not soup:
                logger.error(
                    "Failed to fetch data for %d %s", year, league_name
                )
                continue

            # Extract player statistics
            player_stats = self.extract_review_data(
                soup, year, league_name, StatType.PLAYER
            )
            all_stats.extend(player_stats)

            # Extract pitcher statistics
            pitcher_stats = self.extract_review_data(
                soup, year, league_name, StatType.PITCHER
            )
            all_stats.extend(pitcher_stats)

            # Extract team standings
            team_standings = self.extract_team_standings(
                soup, year, league_name
            )
            all_standings.extend(team_standings)

        # Save raw data to CSV
        if all_stats:
            stat_fields = [
                "name",
                "url",
                "team",
                "team_url",
                "year",
                "league",
                "statistic",
                "value",
                "stat_type",
                "rank",
            ]
            self.save_to_csv(all_stats, f"raw_stats_{year}.csv", stat_fields)

        if all_standings:
            standing_fields = [
                "year",
                "league",
                "division",
                "team",
                "wins",
                "losses",
                "winning_percentage",
                "games_back",
            ]
            self.save_to_csv(
                all_standings, f"standings_{year}.csv", standing_fields
            )

        # Count stats by type
        player_count = sum(
            1 for s in all_stats if s.stat_type == StatType.PLAYER
        )
        pitcher_count = sum(
            1 for s in all_stats if s.stat_type == StatType.PITCHER
        )

        return {
            "players": player_count,
            "pitchers": pitcher_count,
            "standings": len(all_standings),
        }

    def scrape_range(self, start_year: int = 2000, end_year: int = 2024):
        """Scrape multiple years of data and create consolidated files"""
        logger.info("Starting scrape for years %d-%d", start_year, end_year)

        all_stats: List[BaseballStat] = []
        all_standings: List[TeamStanding] = []

        total_players = 0
        total_pitchers = 0
        total_teams = 0

        for year in range(start_year, end_year + 1):
            try:
                counts = self.scrape_year(year)
                total_players += counts["players"]
                total_pitchers += counts["pitchers"]
                total_teams += counts["standings"]

                logger.info(
                    "Year %d complete: %d player, %d pitcher stats, "
                    "%d standings",
                    year,
                    counts["players"],
                    counts["pitchers"],
                    counts["standings"],
                )

                # Load year data for consolidation
                year_stats_file = self.output_dir / f"raw_stats_{year}.csv"
                year_standings_file = self.output_dir / f"standings_{year}.csv"

                # Read stats
                if year_stats_file.exists():
                    with open(year_stats_file, "r", encoding="utf-8") as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            # Reconstruct BaseballStat object
                            stat = BaseballStat(
                                name=row["name"],
                                url=row["url"],
                                team=row["team"],
                                team_url=row["team_url"],
                                year=int(row["year"]),
                                league=row["league"],
                                statistic=row["statistic"],
                                value=row["value"],
                                stat_type=StatType(row["stat_type"]),
                                rank=int(row["rank"]),
                            )
                            all_stats.append(stat)

                # Read standings
                if year_standings_file.exists():
                    with open(year_standings_file, "r", encoding="utf-8") as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            standing = TeamStanding(
                                year=int(row["year"]),
                                league=row["league"],
                                division=row["division"],
                                team=row["team"],
                                wins=int(row["wins"]),
                                losses=int(row["losses"]),
                                winning_percentage=float(
                                    row["winning_percentage"]
                                ),
                                games_back=row["games_back"],
                            )
                            all_standings.append(standing)

            except (requests.RequestException, IOError, ValueError) as e:
                # These are the expected exceptions from our scraping operations:
                # - requests.RequestException: Network/HTTP errors
                # - IOError: File system errors when reading/writing CSVs
                # - ValueError: Data parsing errors (e.g., invalid year format)
                logger.error("Error scraping year %d: %s", year, str(e))
                continue
            except KeyboardInterrupt:
                # Allow user to interrupt long-running scrapes gracefully
                logger.info("Scraping interrupted by user")
                raise

        # Save consolidated pivoted files
        self.save_pivoted_stats(all_stats, StatType.PLAYER)
        self.save_pivoted_stats(all_stats, StatType.PITCHER)

        # Save consolidated standings
        if all_standings:
            standing_fields = [
                "year",
                "league",
                "division",
                "team",
                "wins",
                "losses",
                "winning_percentage",
                "games_back",
            ]
            self.save_to_csv(
                all_standings, "team_standings.csv", standing_fields
            )

        # Generate summary report
        self.generate_summary_report(
            total_players, total_pitchers, total_teams
        )

    def generate_summary_report(self, players: int, pitchers: int, teams: int):
        """Generate a summary of the scraping operation"""
        success_rate = (
            1
            - self.stats["requests_failed"]
            / max(self.stats["requests_made"], 1)
        )  100

        summary = f"""
{'='  60}
BASEBALL ALMANAC WEB SCRAPING SUMMARY
{'='  60}
Output Directory: {self.output_dir}
Total Requests Made: {self.stats['requests_made']}
Failed Requests: {self.stats['requests_failed']}
Success Rate: {success_rate:.1f}%

Data Extracted:
  - Player Statistics: {players}
  - Pitcher Statistics: {pitchers}
  - Team Standings: {teams}
  - Total Records: {self.stats['data_extracted']}

Output Files:
  - player_statistics.csv (pivoted format)
  - pitcher_statistics.csv (pivoted format)
  - team_standings.csv
  - raw_stats_YYYY.csv (per year)
  - standings_YYYY.csv (per year)
{'='  60}
"""
        print(summary)

        # Save summary to file
        summary_file = self.output_dir / "scraping_summary.txt"
        with open(summary_file, "w", encoding="utf-8") as f:
            f.write(summary)


def main():
    """Main execution function

    This demonstrates clean script organization with clear entry points.
    """
    # Create scraper instance
    scraper = BaseballWebScraper(output_dir="scraped_data")

    # You can test with a single year first
    # scraper.scrape_year(2024)

    # Or scrape a range of years
    scraper.scrape_range(2020, 2024)

    logger.info(
        "Web scraping complete. CSV files saved to 'scraped_data' directory."
    )


if __name__ == "__main__":
    main()
