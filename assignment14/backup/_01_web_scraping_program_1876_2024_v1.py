#!/usr/bin/env python3
# _01_web_scraping_program_1876_2024_v1.py
"""
Baseball Almanac Web Scraper - Enhanced Historical Data Collection
================================================================

This enhanced version scrapes the complete historical data:
- American League: 1901-2025 (125 years)
- National League: 1876-2025 (150 years)

Key enhancements by seasoned developers:
- Robust handling of historical data variations
- Memory-efficient chunk processing
- Comprehensive error recovery and checkpointing
- Adaptive parsing for different era formats
"""

import csv
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Configure logging with more detailed formatting for long runs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.FileHandler(
            f"baseball_scraper_{datetime.now():%Y%m%d_%H%M%S}.log"
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class StatType(Enum):
    """Enumeration for statistic types"""

    PLAYER = "player"
    PITCHER = "pitcher"


class League(Enum):
    """Enumeration for leagues with their historical context"""

    AMERICAN = ("American League", "a", 1901, 2025)
    NATIONAL = ("National League", "n", 1876, 2025)

    def __init__(
        self, display_name: str, code: str, start_year: int, end_year: int
    ):
        self.display_name = display_name
        self.code = code
        self.start_year = start_year
        self.end_year = end_year


@dataclass
class BaseballStat:
    """Unified data model for baseball statistics"""

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


@dataclass
class ScrapingCheckpoint:
    """Tracks scraping progress for resumability"""

    last_completed_year: int
    league: str
    total_scraped: int
    failed_years: List[int]
    timestamp: str


class HistoricalBaseballScraper:
    """Enhanced web scraper for complete Baseball Almanac historical data

    This class implements patterns seasoned developers use for large-scale scraping:
    - Checkpoint-based resumability
    - Adaptive parsing for different eras
    - Memory-efficient processing
    - Comprehensive error tracking
    """

    def __init__(
        self,
        base_url: str = "https://www.baseball-almanac.com",
        delay: float = 1.5,
        output_dir: str = "historical_baseball_data",
        checkpoint_dir: str = "scraping_checkpoints",
    ):
        self.base_url = base_url
        self.delay = delay
        self.output_dir = Path(output_dir)
        self.checkpoint_dir = Path(checkpoint_dir)

        # Create directories
        self.output_dir.mkdir(exist_ok=True)
        self.checkpoint_dir.mkdir(exist_ok=True)

        # Configure session with enhanced headers
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Educational Historical Baseball Data Scraper 3.0)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            }
        )

        # Enhanced statistics tracking
        self.stats: Dict[str, Any] = {
            "requests_made": 0,
            "requests_failed": 0,
            "data_extracted": 0,
            "years_processed": 0,
            "start_time": datetime.now(),
            "failed_years": {},  # Track failures by league
        }

        # Era-specific parsing hints (discovered through experience)
        self.era_configs = {
            "pre_1900": {
                "table_class": ["boxed", "data"],
                "strict_parsing": False,
            },
            "early_1900s": {"table_class": ["boxed"], "strict_parsing": False},
            "modern": {"table_class": ["boxed"], "strict_parsing": True},
        }

    def get_era_config(self, year: int) -> Dict[str, Any]:
        """Determine parsing configuration based on year

        Seasoned developers know that HTML structures evolved over time.
        This method encapsulates that knowledge.
        """
        if year < 1900:
            return self.era_configs["pre_1900"]
        elif year < 1950:
            return self.era_configs["early_1900s"]
        else:
            return self.era_configs["modern"]

    def save_checkpoint(
        self, league: League, last_year: int, failed_years: List[int]
    ):
        """Save scraping progress for resumability

        This is crucial for long-running scrapers - allows recovery from failures.
        """
        checkpoint = ScrapingCheckpoint(
            last_completed_year=last_year,
            league=league.display_name,
            total_scraped=self.stats["years_processed"],
            failed_years=failed_years,
            timestamp=datetime.now().isoformat(),
        )

        checkpoint_file = (
            self.checkpoint_dir / f"checkpoint_{league.code}.json"
        )
        with open(checkpoint_file, "w") as f:
            json.dump(asdict(checkpoint), f, indent=2)

        logger.info(
            "Checkpoint saved for %s at year %d",
            league.display_name,
            last_year,
        )

    def load_checkpoint(self, league: League) -> Optional[ScrapingCheckpoint]:
        """Load previous scraping progress if it exists"""
        checkpoint_file = (
            self.checkpoint_dir / f"checkpoint_{league.code}.json"
        )

        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, "r") as f:
                    data = json.load(f)
                    return ScrapingCheckpoint(**data)
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("Failed to load checkpoint: %s", str(e))

        return None

    def make_request(
        self, url: str, max_retries: int = 3
    ) -> Optional[BeautifulSoup]:
        """Enhanced request method with better error handling and retry logic"""
        self.stats["requests_made"] += 1

        # Adaptive delay based on request count (be extra nice after many requests)
        adaptive_delay = self.delay
        if self.stats["requests_made"] > 100:
            adaptive_delay = self.delay * 1.5
        elif self.stats["requests_made"] > 200:
            adaptive_delay = self.delay * 2

        for attempt in range(max_retries):
            try:
                logger.info(
                    "Requesting: %s (attempt %d/%d, request #%d)",
                    url,
                    attempt + 1,
                    max_retries,
                    self.stats["requests_made"],
                )

                response = self.session.get(url, timeout=30)
                response.raise_for_status()

                # Check for soft 404s (pages that return 200 but have no content)
                if len(response.content) < 1000:
                    logger.warning("Suspiciously small response for %s", url)
                    return None

                soup = BeautifulSoup(response.content, "html.parser")

                # Respect rate limits with adaptive delay
                time.sleep(adaptive_delay)

                return soup

            except requests.exceptions.Timeout:
                self.stats["requests_failed"] += 1
                logger.warning("Request timeout for %s", url)
                wait_time = 5 * (attempt + 1)  # Longer waits for timeouts

            except requests.exceptions.ConnectionError:
                self.stats["requests_failed"] += 1
                logger.warning("Connection error for %s", url)
                wait_time = 10 * (
                    attempt + 1
                )  # Even longer for connection issues

            except requests.RequestException as e:
                self.stats["requests_failed"] += 1
                logger.warning("Request failed: %s", str(e))
                wait_time = 2**attempt

            if attempt < max_retries - 1:
                logger.info("Waiting %d seconds before retry...", wait_time)
                time.sleep(wait_time)
            else:
                logger.error(
                    "Failed to fetch %s after %d attempts", url, max_retries
                )

        return None

    def extract_review_data_adaptive(
        self, soup: BeautifulSoup, year: int, league: str, stat_type: StatType
    ) -> List[BaseballStat]:
        """Enhanced extraction with era-aware parsing

        This method demonstrates how experienced developers handle varying data formats
        across different time periods.
        """
        stats: List[BaseballStat] = []
        era_config = self.get_era_config(year)

        # Try multiple table identification strategies
        table_identifier = (
            "Player Review"
            if stat_type == StatType.PLAYER
            else "Pitcher Review"
        )

        # Strategy 1: Look for tables with specific classes
        tables = []
        for table_class in era_config["table_class"]:
            tables.extend(soup.find_all("table", class_=table_class))

        # Strategy 2: If no tables found, try without class restriction
        if not tables:
            tables = soup.find_all("table")
            logger.debug(
                "Falling back to classless table search for year %d", year
            )

        review_table = None

        # Look for the review table with flexible matching
        for table in tables:
            # Check multiple locations for table identifiers
            table_text = table.get_text()
            h2 = table.find("h2")
            caption = table.find("caption")
            first_row = table.find("tr")

            # Check various possible locations for the identifier
            if (
                (h2 and table_identifier in h2.get_text())
                or (caption and table_identifier in caption.get_text())
                or (first_row and table_identifier in first_row.get_text())
                or (
                    table_identifier in table_text[:500]
                )  # Check first 500 chars
            ):
                review_table = table
                logger.debug(
                    "Found %s table using flexible matching", table_identifier
                )
                break

        if not review_table:
            # For very old years, this might be expected
            if year < 1920:
                logger.info(
                    "No %s table found for %d %s (possibly not tracked yet)",
                    table_identifier,
                    year,
                    league,
                )
            else:
                logger.warning(
                    "No %s table found for %d %s",
                    table_identifier,
                    year,
                    league,
                )
            return stats

        # Extract statistics with flexible parsing
        rows = review_table.find_all("tr")
        current_statistic = None

        for row_idx, row in enumerate(rows):
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            # Enhanced statistic detection
            first_cell = cells[0]
            cell_classes = first_cell.get("class", [])

            # Check multiple indicators for statistic rows
            if any(
                cls in cell_classes
                for cls in ["datacolBlue", "statHeader", "category"]
            ):
                stat_link = first_cell.find("a")
                if stat_link:
                    current_statistic = stat_link.get_text(strip=True)
                elif first_cell.get_text(strip=True):
                    # Some older years might not have links
                    current_statistic = first_cell.get_text(strip=True)

                logger.debug("Found statistic category: %s", current_statistic)
                continue

            # Extract player/pitcher data with flexible column detection
            if (
                current_statistic and len(cells) >= 3
            ):  # Minimum: rank, name, value
                try:
                    # Find name cell (usually has a link)
                    name_cell = None
                    name_idx = None

                    for idx, cell in enumerate(cells):
                        if cell.find("a") and "player" in str(
                            cell.find("a").get("href", "")
                        ):
                            name_cell = cell
                            name_idx = idx
                            break

                    if not name_cell:
                        # Fallback: assume second cell is name
                        if len(cells) > 1 and "datacolBox" in cells[1].get(
                            "class", []
                        ):
                            name_cell = cells[1]
                            name_idx = 1

                    if name_cell:
                        name_link = name_cell.find("a")
                        if name_link:
                            name = name_link.get_text(strip=True)
                            url = urljoin(
                                self.base_url, name_link.get("href", "")
                            )

                            # Team is usually next cell after name
                            team_name = ""
                            team_url = ""
                            if (
                                name_idx is not None
                                and len(cells) > name_idx + 1
                            ):
                                team_cell = cells[name_idx + 1]
                                team_link = team_cell.find("a")
                                if team_link:
                                    team_name = team_link.get_text(strip=True)
                                    team_url = urljoin(
                                        self.base_url,
                                        team_link.get("href", ""),
                                    )
                                else:
                                    team_name = team_cell.get_text(strip=True)

                            # Value is usually last cell or after team
                            value = ""
                            if len(cells) > name_idx + 2:
                                value = cells[name_idx + 2].get_text(
                                    strip=True
                                )
                            elif len(cells) > 0:
                                value = cells[-1].get_text(strip=True)

                            # Validate and create stat
                            if (
                                name
                                and value
                                and value not in ["", "-", "N/A"]
                            ):
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

                except Exception as e:
                    logger.debug("Failed to parse row %d: %s", row_idx, str(e))
                    if era_config["strict_parsing"]:
                        raise

        logger.info(
            "Extracted %d %s statistics for %d %s",
            len(stats),
            stat_type.value,
            year,
            league,
        )
        return stats

    def extract_team_standings_adaptive(
        self, soup: BeautifulSoup, year: int, league: str
    ) -> List[TeamStanding]:
        """Extract standings with historical format awareness"""
        standings_data: List[TeamStanding] = []
        era_config = self.get_era_config(year)

        logger.info("Extracting team standings for %d %s", year, league)

        # Note: Divisions were introduced in 1969
        has_divisions = year >= 1969

        tables = soup.find_all("table")

        for table in tables:
            table_text = table.get_text().lower()

            # Different eras use different indicators
            standings_indicators = [
                "wins",
                "losses",
                "w-l",
                "pct",
                "standings",
            ]
            if not any(
                indicator in table_text for indicator in standings_indicators
            ):
                continue

            rows = table.find_all("tr")
            current_division = "League" if not has_divisions else None

            for row in rows:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue

                first_cell_text = cells[0].get_text(strip=True)

                # Division detection (post-1969)
                if has_divisions and first_cell_text in [
                    "East",
                    "Central",
                    "West",
                ]:
                    current_division = first_cell_text
                    logger.debug("Found division: %s", current_division)
                    continue

                # Skip headers
                if any(
                    header in first_cell_text.upper()
                    for header in ["TEAM", "W", "L"]
                ):
                    continue

                # Process team data
                if (not has_divisions or current_division) and len(cells) >= 3:
                    try:
                        # Extract team name (handle different formats)
                        team_cell = cells[0]
                        team_link = team_cell.find("a")
                        team_name = (
                            team_link.get_text(strip=True)
                            if team_link
                            else team_cell.get_text(strip=True)
                        )

                        # Skip invalid entries
                        if (
                            not team_name
                            or len(team_name) > 50
                            or team_name.isdigit()
                        ):
                            continue

                        # Find wins and losses (flexible position detection)
                        wins_val = None
                        losses_val = None

                        for i in range(1, len(cells)):
                            cell_text = cells[i].get_text(strip=True)

                            # Look for wins (typically 20-116 range historically)
                            if wins_val is None and cell_text.isdigit():
                                val = int(cell_text)
                                if 20 <= val <= 116:  # Reasonable wins range
                                    wins_val = val
                                    # Losses often follow wins
                                    if i + 1 < len(cells):
                                        next_text = cells[i + 1].get_text(
                                            strip=True
                                        )
                                        if next_text.isdigit():
                                            losses_val = int(next_text)
                                    break

                        if wins_val and losses_val:
                            # Calculate winning percentage
                            total_games = wins_val + losses_val
                            wp_val = (
                                wins_val / total_games
                                if total_games > 0
                                else 0.0
                            )

                            # Games back might not exist in older years
                            gb_val = "0"
                            for i in range(len(cells) - 1, 0, -1):
                                gb_text = cells[i].get_text(strip=True)
                                if gb_text and not gb_text.isdigit():
                                    if gb_text in ["-", "—"]:
                                        gb_val = "0"
                                    else:
                                        gb_val = gb_text
                                    break

                            standing = TeamStanding(
                                year=year,
                                league=league,
                                division=current_division or "League",
                                team=team_name,
                                wins=wins_val,
                                losses=losses_val,
                                winning_percentage=wp_val,
                                games_back=gb_val,
                            )
                            standings_data.append(standing)
                            self.stats["data_extracted"] += 1

                    except (ValueError, IndexError) as e:
                        logger.debug("Failed to parse team row: %s", str(e))
                        continue

            # If we found standings, we're done
            if standings_data:
                break

        logger.info(
            "Extracted %d team standings for %d %s",
            len(standings_data),
            year,
            league,
        )
        return standings_data

    def scrape_year_with_retry(
        self, year: int, league: League, max_attempts: int = 2
    ) -> Tuple[bool, Dict[str, int]]:
        """Scrape a single year with retry logic

        Returns (success, counts) tuple
        """
        for attempt in range(max_attempts):
            try:
                counts = self.scrape_year(year, league)
                return True, counts
            except Exception as e:
                logger.error(
                    "Error scraping year %d (attempt %d/%d): %s",
                    year,
                    attempt + 1,
                    max_attempts,
                    str(e),
                )
                if attempt < max_attempts - 1:
                    time.sleep(10)  # Wait before retry

        return False, {"players": 0, "pitchers": 0, "standings": 0}

    def scrape_year(self, year: int, league: League) -> Dict[str, int]:
        """Scrape data for a specific year and league"""
        url = f"{self.base_url}/yearly/yr{year}{league.code}.shtml"
        soup = self.make_request(url)

        if not soup:
            raise Exception(
                f"Failed to fetch data for {year} {league.display_name}"
            )

        all_stats: List[BaseballStat] = []
        all_standings: List[TeamStanding] = []

        # Extract player statistics
        player_stats = self.extract_review_data_adaptive(
            soup, year, league.display_name, StatType.PLAYER
        )
        all_stats.extend(player_stats)

        # Extract pitcher statistics
        pitcher_stats = self.extract_review_data_adaptive(
            soup, year, league.display_name, StatType.PITCHER
        )
        all_stats.extend(pitcher_stats)

        # Extract team standings
        team_standings = self.extract_team_standings_adaptive(
            soup, year, league.display_name
        )
        all_standings.extend(team_standings)

        # Save year-specific files
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
            self.save_to_csv(
                all_stats, f"{league.code}_stats_{year}.csv", stat_fields
            )

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
                all_standings,
                f"{league.code}_standings_{year}.csv",
                standing_fields,
            )

        # Count by type
        player_count = sum(
            1 for s in all_stats if s.stat_type == StatType.PLAYER
        )
        pitcher_count = sum(
            1 for s in all_stats if s.stat_type == StatType.PITCHER
        )

        self.stats["years_processed"] += 1

        return {
            "players": player_count,
            "pitchers": pitcher_count,
            "standings": len(all_standings),
        }

    def save_to_csv(
        self, data: List[Any], filename: str, fieldnames: List[str]
    ):
        """Save data to CSV with proper error handling"""
        filepath = self.output_dir / filename

        try:
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for item in data:
                    row = asdict(item)
                    if "stat_type" in row:
                        row["stat_type"] = row["stat_type"].value
                    writer.writerow(row)

            logger.debug("Saved %d records to %s", len(data), filepath)

        except IOError as e:
            logger.error("Failed to save CSV file %s: %s", filename, str(e))
            raise

    def consolidate_league_data(self, league: League):
        """Consolidate all years for a league into master files"""
        logger.info("Consolidating data for %s", league.display_name)

        all_stats: List[BaseballStat] = []
        all_standings: List[TeamStanding] = []

        # Read all year files for this league
        for year in range(league.start_year, league.end_year + 1):
            # Stats file
            stats_file = self.output_dir / f"{league.code}_stats_{year}.csv"
            if stats_file.exists():
                with open(stats_file, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
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

            # Standings file
            standings_file = (
                self.output_dir / f"{league.code}_standings_{year}.csv"
            )
            if standings_file.exists():
                with open(standings_file, "r", encoding="utf-8") as f:
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

        # Save consolidated files
        if all_stats:
            # Save by stat type
            for stat_type in StatType:
                filtered_stats = [
                    s for s in all_stats if s.stat_type == stat_type
                ]
                if filtered_stats:
                    self.save_pivoted_stats(
                        filtered_stats,
                        stat_type,
                        f"{league.code}_{stat_type.value}_statistics_historical.csv",
                    )

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
                all_standings,
                f"{league.code}_standings_historical.csv",
                standing_fields,
            )

        logger.info(
            "Consolidated %s: %d stats, %d standings",
            league.display_name,
            len(all_stats),
            len(all_standings),
        )

    def save_pivoted_stats(
        self, stats: List[BaseballStat], stat_type: StatType, filename: str
    ):
        """Save statistics in pivoted format"""
        if not stats:
            return

        # Group by player/pitcher
        pivoted_data: Dict[tuple, Dict[str, Any]] = {}

        for stat in stats:
            key = (stat.name, stat.team, stat.year, stat.league)

            if key not in pivoted_data:
                pivoted_data[key] = {
                    f"{stat_type.value}_name": stat.name,
                    "team": stat.team,
                    "year": stat.year,
                    "league": stat.league,
                }

            pivoted_data[key][stat.statistic] = stat.value

        rows = list(pivoted_data.values())

        if rows:
            # Get all unique statistics
            all_stats: Set[str] = set()
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

            fieldnames = [
                f"{stat_type.value}_name",
                "team",
                "year",
                "league",
            ] + sorted(all_stats)

            filepath = self.output_dir / filename
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            logger.info(
                "Saved pivoted data to %s (%d records)", filename, len(rows)
            )

    def scrape_historical_data(self):
        """Main method to scrape all historical data"""
        logger.info("Starting historical baseball data scraping")
        logger.info(
            "American League: %d-%d",
            League.AMERICAN.start_year,
            League.AMERICAN.end_year,
        )
        logger.info(
            "National League: %d-%d",
            League.NATIONAL.start_year,
            League.NATIONAL.end_year,
        )

        total_stats = {
            "players": 0,
            "pitchers": 0,
            "standings": 0,
            "failed_years": [],
        }

        # Process each league
        for league in League:
            logger.info("\n" + "=" * 60)
            logger.info("Processing %s", league.display_name)
            logger.info("=" * 60)

            # Check for existing checkpoint
            checkpoint = self.load_checkpoint(league)
            start_year = league.start_year
            failed_years = []

            if checkpoint:
                logger.info(
                    "Resuming from checkpoint: last completed year %d",
                    checkpoint.last_completed_year,
                )
                start_year = checkpoint.last_completed_year + 1
                failed_years = checkpoint.failed_years

            # Process years in chunks to manage memory
            chunk_size = 10
            years_to_process = list(range(start_year, league.end_year + 1))

            for chunk_start in range(0, len(years_to_process), chunk_size):
                chunk_end = min(
                    chunk_start + chunk_size, len(years_to_process)
                )
                chunk_years = years_to_process[chunk_start:chunk_end]

                logger.info(
                    "Processing years %d-%d for %s",
                    chunk_years[0],
                    chunk_years[-1],
                    league.display_name,
                )

                for year in chunk_years:
                    success, counts = self.scrape_year_with_retry(year, league)

                    if success:
                        total_stats["players"] += counts["players"]
                        total_stats["pitchers"] += counts["pitchers"]
                        total_stats["standings"] += counts["standings"]

                        logger.info(
                            "Year %d complete: %d player, %d pitcher stats, %d standings",
                            year,
                            counts["players"],
                            counts["pitchers"],
                            counts["standings"],
                        )
                    else:
                        failed_years.append(year)
                        total_stats["failed_years"].append(
                            (league.display_name, year)
                        )
                        logger.error(
                            "Failed to scrape year %d for %s",
                            year,
                            league.display_name,
                        )

                    # Save checkpoint after each year
                    self.save_checkpoint(league, year, failed_years)

                # Consolidate after each chunk to free memory
                logger.info("Consolidating chunk data...")
                self.consolidate_league_data(league)

            # Retry failed years once more with longer delays
            if failed_years:
                logger.info(
                    "Retrying %d failed years for %s",
                    len(failed_years),
                    league.display_name,
                )
                for year in failed_years[
                    :
                ]:  # Copy list to modify during iteration
                    time.sleep(30)  # Longer delay for retry
                    success, counts = self.scrape_year_with_retry(
                        year, league, max_attempts=1
                    )
                    if success:
                        failed_years.remove(year)
                        total_stats["players"] += counts["players"]
                        total_stats["pitchers"] += counts["pitchers"]
                        total_stats["standings"] += counts["standings"]

            # Final consolidation for the league
            self.consolidate_league_data(league)

            # Log league summary
            logger.info(
                "%s complete. Failed years: %s",
                league.display_name,
                failed_years if failed_years else "None",
            )

        # Generate comprehensive summary
        self.generate_historical_summary(total_stats)

    def generate_historical_summary(self, total_stats: Dict[str, Any]):
        """Generate a comprehensive summary of the historical scraping operation"""
        end_time = datetime.now()
        duration = end_time - self.stats["start_time"]

        success_rate = (
            1
            - self.stats["requests_failed"]
            / max(self.stats["requests_made"], 1)
        ) * 100

        summary = f"""
{'=' * 80}
BASEBALL ALMANAC HISTORICAL DATA SCRAPING SUMMARY
{'=' * 80}
Scraping Duration: {duration}
Output Directory: {self.output_dir}

SCOPE:
  - American League: {League.AMERICAN.start_year}-{League.AMERICAN.end_year} ({League.AMERICAN.end_year - League.AMERICAN.start_year + 1} years)
  - National League: {League.NATIONAL.start_year}-{League.NATIONAL.end_year} ({League.NATIONAL.end_year - League.NATIONAL.start_year + 1} years)
  - Total Years Attempted: {League.AMERICAN.end_year - League.AMERICAN.start_year + 1 + League.NATIONAL.end_year - League.NATIONAL.start_year + 1}

PERFORMANCE METRICS:
  - Total Requests Made: {self.stats['requests_made']:,}
  - Failed Requests: {self.stats['requests_failed']:,}
  - Success Rate: {success_rate:.1f}%
  - Years Successfully Processed: {self.stats['years_processed']:,}

DATA EXTRACTED:
  - Player Statistics: {total_stats['players']:,}
  - Pitcher Statistics: {total_stats['pitchers']:,}
  - Team Standings: {total_stats['standings']:,}
  - Total Records: {self.stats['data_extracted']:,}

FAILED YEARS:
"""

        if total_stats["failed_years"]:
            for league, year in total_stats["failed_years"]:
                summary += f"  - {league}: {year}\n"
        else:
            summary += "  - None (100% success!)\n"

        summary += f"""
OUTPUT FILES:
  League-Specific Historical Files:
    - a_player_statistics_historical.csv (American League players)
    - a_pitcher_statistics_historical.csv (American League pitchers)
    - a_standings_historical.csv (American League standings)
    - n_player_statistics_historical.csv (National League players)
    - n_pitcher_statistics_historical.csv (National League pitchers)
    - n_standings_historical.csv (National League standings)

  Year-Specific Files:
    - [league]_stats_[year].csv (individual year statistics)
    - [league]_standings_[year].csv (individual year standings)

  Checkpoint Files:
    - checkpoint_a.json (American League progress)
    - checkpoint_n.json (National League progress)

NOTES:
  - Data availability varies by era (older years may have fewer statistics)
  - Some statistics were not tracked in early baseball history
  - Failed years can be retried by running the scraper again (checkpoints will resume)
{'=' * 80}
"""
        print(summary)

        # Save summary to file
        summary_file = (
            self.output_dir
            / f"scraping_summary_{datetime.now():%Y%m%d_%H%M%S}.txt"
        )
        with open(summary_file, "w", encoding="utf-8") as f:
            f.write(summary)

        logger.info("Summary saved to %s", summary_file)


def main():
    """Main execution function demonstrating professional patterns"""
    # Parse command line arguments (in production, use argparse)
    import sys

    # Allow command line override of year ranges for testing
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Test mode: scrape just a few recent years
        logger.info("Running in test mode - limited year range")

        # Temporarily modify league year ranges for testing
        League.AMERICAN.end_year = League.AMERICAN.start_year + 2
        League.NATIONAL.end_year = League.NATIONAL.start_year + 2

    # Create scraper instance
    scraper = HistoricalBaseballScraper(
        output_dir="historical_baseball_data",
        checkpoint_dir="scraping_checkpoints",
    )

    try:
        # Run the historical scraping
        scraper.scrape_historical_data()

        logger.info("Historical scraping complete!")

    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user - checkpoints saved")
        print("\nScraping interrupted. Run again to resume from checkpoint.")
    except Exception as e:
        logger.error("Unexpected error: %s", str(e), exc_info=True)
        print(f"\nError occurred: {e}")
        print("Check the log file for details. Checkpoints have been saved.")


if __name__ == "__main__":
    main()
