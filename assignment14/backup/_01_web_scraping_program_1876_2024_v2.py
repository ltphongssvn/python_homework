#!/usr/bin/env python3
# _01_web_scraping_program_1876_2024_v1.py
"""
Baseball Almanac Web Scraper - Multi-Pattern Historical Data Collection
=====================================================================

This enhanced version handles 8 different HTML patterns
across baseball history:
- Pattern 1: National League 1876-2004 (older structure)
- Pattern 2: National League 2005-2013 (mid-period structure)
- Pattern 3: National League 2014-2024 (modern structure)
- Pattern 4: American League 1901-2001 (older structure)
- Pattern 5: American League 2002-2003 (transitional structure)
- Pattern 6: American League 2004-2004 (single year variant)
- Pattern 7: American League 2005-2013 (mid-period structure)
- Pattern 8: American League 2014-2024 (modern structure)

Key architectural decisions:
- Pattern detection system to identify HTML structure
- Era-specific parsers for different time periods
- Flexible field mapping for varying column names
- Robust error handling for missing elements
"""

import csv
import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Configure detailed logging for debugging different patterns
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s "
    "- [%(funcName)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.FileHandler(
            f"baseball_multipattern_{datetime.now():%Y%m%d_%H%M%S}.log"
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


class HTMLPattern(Enum):
    """Enumeration for different HTML patterns across eras"""

    PATTERN_1 = "National League 1876-2004"  # Older structure
    PATTERN_2 = "National League 2005-2013"  # Mid-period
    PATTERN_3 = "National League 2014-2024"  # Modern
    PATTERN_4 = "American League 1901-2001"  # Older structure
    PATTERN_5 = "American League 2002-2003"  # Transitional
    PATTERN_6 = "American League 2004-2004"  # Single year variant
    PATTERN_7 = "American League 2005-2013"  # Mid-period
    PATTERN_8 = "American League 2014-2024"  # Modern


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
    # Additional fields for pattern tracking
    pattern_used: Optional[str] = None
    extraction_confidence: float = 1.0


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
    # Additional fields for modern eras
    ties: Optional[int] = None
    payroll: Optional[str] = None
    pattern_used: Optional[str] = None


@dataclass
class PatternConfig:
    """Configuration for parsing different HTML patterns"""

    pattern: HTMLPattern
    year_range: Tuple[int, int]
    league: League

    # Table identification patterns
    player_table_identifiers: List[str] = field(default_factory=list)
    pitcher_table_identifiers: List[str] = field(default_factory=list)
    standings_table_identifiers: List[str] = field(default_factory=list)

    # Class name patterns
    data_cell_classes: List[str] = field(default_factory=list)
    stat_name_classes: List[str] = field(default_factory=list)
    value_cell_classes: List[str] = field(default_factory=list)

    # Column mapping for different eras
    column_mapping: Dict[str, str] = field(default_factory=dict)

    # Special parsing rules
    has_payroll: bool = False
    has_ties_column: bool = False
    has_top25_links: bool = False
    uses_middle_class: bool = False  # For rowspan cells


class MultiPatternBaseballScraper:
    """Enhanced scraper that handles multiple HTML patterns
    across baseball history

    This class demonstrates how experienced developers handle
    evolving data formats:
    - Pattern detection and classification
    - Era-specific parsing strategies
    - Flexible field mapping
    - Graceful degradation for missing data
    """

    def __init__(
        self,
        base_url: str = "https://www.baseball-almanac.com",
        delay: float = 1.5,
        output_dir: str = "multipattern_baseball_data",
    ):
        self.base_url = base_url
        self.delay = delay
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Configure session
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 "
                "(Educational Multi-Pattern Baseball Scraper 4.0)",
                "Accept": "text/html,application/xhtml+xml,"
                "application/xml;q=0.9,*/*;q=0.8",
            }
        )

        # Initialize pattern configurations
        self.pattern_configs = self._initialize_pattern_configs()

        # Track statistics
        self.stats = {
            "requests_made": 0,
            "requests_failed": 0,
            "data_extracted": 0,
            "patterns_detected": {},
            "pattern_failures": {},
        }

    def _initialize_pattern_configs(self) -> Dict[HTMLPattern, PatternConfig]:
        """Initialize configuration for each HTML pattern

        This method encapsulates knowledge about how the HTML evolved
        over time.
        Each pattern configuration tells the scraper
        how to handle that era's HTML.
        """
        configs = {}

        # Pattern 1: National League 1876-2004 (Older structure)
        configs[HTMLPattern.PATTERN_1] = PatternConfig(
            pattern=HTMLPattern.PATTERN_1,
            year_range=(1876, 2004),
            league=League.NATIONAL,
            player_table_identifiers=["Player Review", "Hitting Statistics"],
            pitcher_table_identifiers=[
                "Pitcher Review",
                "Pitching Statistics",
            ],
            standings_table_identifiers=["Team Standings"],
            data_cell_classes=["datacolBox", "datacolBoxR", "datacolBoxC"],
            stat_name_classes=["datacolBlue"],
            value_cell_classes=["datacolBoxR", "datacolBox"],
            column_mapping={
                "Base on Balls": "Base on Balls",
                "Batting Average": "Batting Average",
                "Doubles": "Doubles",
                "Hits": "Hits",
                "Home Runs": "Home Runs",
                "RBI": "RBI",
                "Runs": "Runs",
                "ERA": "ERA",
                "Wins": "Wins",
            },
            has_payroll=False,
            has_ties_column=False,
            has_top25_links=True,
            uses_middle_class=True,
        )

        # Pattern 2: National League 2005-2013
        configs[HTMLPattern.PATTERN_2] = PatternConfig(
            pattern=HTMLPattern.PATTERN_2,
            year_range=(2005, 2013),
            league=League.NATIONAL,
            player_table_identifiers=["Player Review", "Hitting Statistics"],
            pitcher_table_identifiers=[
                "Pitcher Review",
                "Pitching Statistics",
            ],
            standings_table_identifiers=["Team Standings"],
            data_cell_classes=["datacolBox"],
            stat_name_classes=["datacolBlue"],
            value_cell_classes=["datacolBox"],
            column_mapping={
                "Base on Balls": "Base on Balls",
                "Batting Average": "Batting Average",
                "On Base Percentage": "On Base Percentage",
                "Stolen Bases": "Stolen Bases",
            },
            has_payroll=True,
            has_ties_column=True,
            has_top25_links=True,
            uses_middle_class=True,
        )

        # Pattern 3: National League 2014-2024 (Modern)
        configs[HTMLPattern.PATTERN_3] = PatternConfig(
            pattern=HTMLPattern.PATTERN_3,
            year_range=(2014, 2024),
            league=League.NATIONAL,
            player_table_identifiers=["Player Review", "Hitting Statistics"],
            pitcher_table_identifiers=[
                "Pitcher Review",
                "Pitching Statistics",
            ],
            standings_table_identifiers=["Team Standings", "Final Standings"],
            data_cell_classes=["datacolBox", "datacolBoxR", "datacolBoxC"],
            stat_name_classes=["datacolBlue"],
            value_cell_classes=["datacolBoxR", "datacolBox"],
            has_payroll=True,
            has_ties_column=True,
            has_top25_links=True,
            uses_middle_class=True,
        )

        # Pattern 4: American League 1901-2001
        configs[HTMLPattern.PATTERN_4] = PatternConfig(
            pattern=HTMLPattern.PATTERN_4,
            year_range=(1901, 2001),
            league=League.AMERICAN,
            player_table_identifiers=["Player Review", "Hitting Statistics"],
            pitcher_table_identifiers=[
                "Pitcher Review",
                "Pitching Statistics",
            ],
            standings_table_identifiers=["Team Standings"],
            data_cell_classes=["datacolBox", "datacolBoxR", "datacolBoxC"],
            stat_name_classes=["datacolBlue"],
            value_cell_classes=["datacolBoxR", "datacolBox"],
            has_payroll=False,
            has_ties_column=False,
            has_top25_links=True,
            uses_middle_class=True,
        )

        # Pattern 5: American League 2002-2003
        # (Transitional with detailed team stats)
        configs[HTMLPattern.PATTERN_5] = PatternConfig(
            pattern=HTMLPattern.PATTERN_5,
            year_range=(2002, 2003),
            league=League.AMERICAN,
            player_table_identifiers=["Player Review", "Hitting Statistics"],
            pitcher_table_identifiers=[
                "Pitcher Review",
                "Pitching Statistics",
            ],
            standings_table_identifiers=["Team Standings"],
            data_cell_classes=["datacolBox"],
            stat_name_classes=["datacolBlue"],
            value_cell_classes=["datacolBox"],
            has_payroll=True,
            has_ties_column=False,
            has_top25_links=True,
            uses_middle_class=False,
        )

        # Pattern 6: American League 2004 (Single year with specific format)
        configs[HTMLPattern.PATTERN_6] = PatternConfig(
            pattern=HTMLPattern.PATTERN_6,
            year_range=(2004, 2004),
            league=League.AMERICAN,
            player_table_identifiers=["Player Review", "Hitting Statistics"],
            pitcher_table_identifiers=[
                "Pitcher Review",
                "Pitching Statistics",
            ],
            standings_table_identifiers=["Team Standings", "Final Standings"],
            data_cell_classes=["datacolBox"],
            stat_name_classes=["datacolBlue"],
            value_cell_classes=["datacolBox"],
            has_payroll=True,
            has_ties_column=True,
            has_top25_links=True,
            uses_middle_class=True,
        )

        # Pattern 7: American League 2005-2013
        configs[HTMLPattern.PATTERN_7] = PatternConfig(
            pattern=HTMLPattern.PATTERN_7,
            year_range=(2005, 2013),
            league=League.AMERICAN,
            player_table_identifiers=["Player Review", "Hitting Statistics"],
            pitcher_table_identifiers=[
                "Pitcher Review",
                "Pitching Statistics",
            ],
            standings_table_identifiers=["Team Standings", "Final Standings"],
            data_cell_classes=["datacolBox"],
            stat_name_classes=["datacolBlue"],
            value_cell_classes=["datacolBox"],
            has_payroll=True,
            has_ties_column=True,
            has_top25_links=True,
            uses_middle_class=True,
        )

        # Pattern 8: American League 2014-2024 (Modern)
        configs[HTMLPattern.PATTERN_8] = PatternConfig(
            pattern=HTMLPattern.PATTERN_8,
            year_range=(2014, 2024),
            league=League.AMERICAN,
            player_table_identifiers=["Player Review", "Hitting Statistics"],
            pitcher_table_identifiers=[
                "Pitcher Review",
                "Pitching Statistics",
            ],
            standings_table_identifiers=["Team Standings", "Final Standings"],
            data_cell_classes=["datacolBox", "datacolBoxR", "datacolBoxC"],
            stat_name_classes=["datacolBlue"],
            value_cell_classes=["datacolBoxR", "datacolBoxC"],
            has_payroll=True,
            has_ties_column=True,
            has_top25_links=True,
            uses_middle_class=True,
        )

        return configs

    def detect_pattern(
        self, soup: BeautifulSoup, year: int, league: League
    ) -> Optional[HTMLPattern]:
        """Detect which HTML pattern is used in the page

        This method demonstrates pattern recognition - a key skill for handling
        evolving data formats. It examines the HTML structure to determine
        which parsing strategy to use.
        """
        logger.info("Detecting pattern for %d %s", year, league.display_name)

        # First, try to match by year range and league
        candidates = []
        for pattern, config in self.pattern_configs.items():
            if (
                config.league == league
                and config.year_range[0] <= year <= config.year_range[1]
            ):
                candidates.append((pattern, config))

        if not candidates:
            logger.warning(
                "No pattern candidates found for %d %s",
                year,
                league.display_name,
            )
            return None

        # If multiple candidates, examine HTML structure
        # to determine best match
        if len(candidates) > 1:
            logger.debug(
                "Multiple pattern candidates found, examining HTML structure"
            )

            # Look for distinguishing features
            for pattern, config in candidates:
                score = 0

                # Check for specific class names
                for class_name in config.data_cell_classes:
                    if soup.find(class_=class_name):
                        score += 1

                # Check for table identifiers
                for identifier in config.player_table_identifiers:
                    if soup.find(text=re.compile(identifier)):
                        score += 2

                # Check for payroll data (distinguishes newer patterns)
                if config.has_payroll and soup.find(
                    text=re.compile("Payroll")
                ):
                    score += 3

                # Check for middle class usage
                if config.uses_middle_class and soup.find(class_="middle"):
                    score += 2

                logger.debug("Pattern %s score: %d", pattern.value, score)

            # Return the highest scoring pattern
            # For now, return the first candidate
            # (can be enhanced with scoring)
            pattern = candidates[0][0]
        else:
            pattern = candidates[0][0]

        logger.info("Detected pattern: %s", pattern.value)
        self.stats["patterns_detected"][pattern.value] = (
            self.stats["patterns_detected"].get(pattern.value, 0) + 1
        )

        return pattern

    def extract_stats_with_pattern(
        self,
        soup: BeautifulSoup,
        year: int,
        league: str,
        stat_type: StatType,
        pattern: HTMLPattern,
    ) -> List[BaseballStat]:
        """Extract statistics using the appropriate pattern configuration

        This method shows how to apply different parsing strategies based on
        the detected pattern.
        Each era's HTML requires slightly different handling.
        """
        config = self.pattern_configs[pattern]
        stats: List[BaseballStat] = []

        logger.info(
            "Extracting %s stats for %d %s using pattern %s",
            stat_type.value,
            year,
            league,
            pattern.value,
        )

        # Select appropriate table identifiers
        if stat_type == StatType.PLAYER:
            identifiers = config.player_table_identifiers
        else:
            identifiers = config.pitcher_table_identifiers

        # Find the review table
        review_table = None
        tables = soup.find_all("table", class_="boxed")

        for table in tables:
            # Check various locations for identifiers
            table_text = table.get_text()

            # Look in h2 tags
            h2 = table.find("h2")
            if h2:
                h2_text = h2.get_text()
                for identifier in identifiers:
                    if identifier in h2_text:
                        review_table = table
                        logger.debug("Found table via h2: %s", identifier)
                        break

            # Look in header cells
            if not review_table:
                header = table.find("td", class_="header")
                if header:
                    header_text = header.get_text()
                    for identifier in identifiers:
                        if identifier in header_text:
                            review_table = table
                            logger.debug(
                                "Found table via header: %s", identifier
                            )
                            break

            # Check table text content
            if not review_table:
                for identifier in identifiers:
                    if identifier in table_text:
                        review_table = table
                        logger.debug(
                            "Found table via text search: %s", identifier
                        )
                        break

            if review_table:
                break

        if not review_table:
            logger.warning(
                "No %s table found for %d %s with pattern %s",
                stat_type.value,
                year,
                league,
                pattern.value,
            )
            return stats

        # Extract statistics based on pattern configuration
        rows = review_table.find_all("tr")
        current_statistic = None

        for row_idx, row in enumerate(rows):
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            # Skip header rows
            if any(cell.get("class") == ["banner"] for cell in cells):
                continue

            # Check for statistic name row
            first_cell = cells[0] if cells else None
            if first_cell and any(
                cls in first_cell.get("class", [])
                for cls in config.stat_name_classes
            ):
                # This is a statistic category row
                stat_link = first_cell.find("a")
                if stat_link:
                    current_statistic = stat_link.get_text(strip=True)
                    logger.debug("Found statistic: %s", current_statistic)
                continue

            # Extract player/pitcher data
            if current_statistic and len(cells) >= 3:
                try:
                    # Handle different cell arrangements based on pattern
                    name_cell = None
                    team_cell = None
                    value_cell = None

                    # Pattern-specific cell extraction
                    if config.uses_middle_class and any(
                        cell.get("class") == ["middle"] for cell in cells
                    ):
                        # Handle rowspan cells
                        # (multiple players with same value)
                        # This is common in tie situations
                        name_cell = cells[0] if len(cells) > 0 else None
                        team_cell = cells[1] if len(cells) > 1 else None
                        # Value might be in a rowspan cell from previous row
                        value_cell = cells[2] if len(cells) > 2 else None
                    else:
                        # Standard row layout
                        if len(cells) >= 4:
                            name_cell = cells[1]
                            team_cell = cells[2]
                            value_cell = cells[3]
                        elif len(cells) == 3:
                            # Some patterns have only 3 cells
                            name_cell = cells[0]
                            team_cell = cells[1]
                            value_cell = cells[2]

                    # Extract data from cells
                    if name_cell and team_cell and value_cell:
                        # Extract name and URL
                        name_link = name_cell.find("a")
                        if name_link:
                            name = name_link.get_text(strip=True)
                            url = urljoin(
                                self.base_url, name_link.get("href", "")
                            )

                            # Extract team
                            team_link = team_cell.find("a")
                            team_name = ""
                            team_url = ""
                            if team_link:
                                team_name = team_link.get_text(strip=True)
                                team_url = urljoin(
                                    self.base_url, team_link.get("href", "")
                                )
                            else:
                                team_name = team_cell.get_text(strip=True)

                            # Extract value
                            value = value_cell.get_text(strip=True)

                            # Create stat object
                            if name and value:
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
                                    pattern_used=pattern.value,
                                )
                                stats.append(stat)
                                self.stats["data_extracted"] += 1

                    # Handle the special case of multiple players
                    # with same value
                    elif name_cell and not value_cell:
                        # Look for value in cells with "middle" class
                        # n same row
                        middle_cells = [
                            c for c in cells if "middle" in c.get("class", [])
                        ]
                        if middle_cells:
                            value = middle_cells[-1].get_text(strip=True)
                            name_link = name_cell.find("a")
                            if name_link and value:
                                name = name_link.get_text(strip=True)
                                url = urljoin(
                                    self.base_url, name_link.get("href", "")
                                )

                                # Try to get team from next cell
                                team_name = ""
                                team_url = ""
                                if team_cell:
                                    team_link = team_cell.find("a")
                                    if team_link:
                                        team_name = team_link.get_text(
                                            strip=True
                                        )
                                        team_url = urljoin(
                                            self.base_url,
                                            team_link.get("href", ""),
                                        )
                                    else:
                                        team_name = team_cell.get_text(
                                            strip=True
                                        )

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
                                    pattern_used=pattern.value,
                                )
                                stats.append(stat)
                                self.stats["data_extracted"] += 1

                except Exception as e:
                    logger.debug("Error parsing row %d: %s", row_idx, str(e))
                    continue

        logger.info(
            "Extracted %d %s statistics using pattern %s",
            len(stats),
            stat_type.value,
            pattern.value,
        )
        return stats

    def extract_standings_with_pattern(
        self, soup: BeautifulSoup, year: int, league: str, pattern: HTMLPattern
    ) -> List[TeamStanding]:
        """Extract team standings using pattern-specific logic

        This method handles the various standings table formats
        across different eras.
        """
        config = self.pattern_configs[pattern]
        standings_data: List[TeamStanding] = []

        logger.info(
            "Extracting standings for %d %s using pattern %s",
            year,
            league,
            pattern.value,
        )

        # Find standings table
        standings_table = None
        tables = soup.find_all("table", class_="boxed")

        for table in tables:
            # Check for standings identifiers
            table_text = table.get_text()

            for identifier in config.standings_table_identifiers:
                if identifier in table_text:
                    standings_table = table
                    logger.debug("Found standings table via: %s", identifier)
                    break

            if standings_table:
                break

        if not standings_table:
            logger.warning("No standings table found for %d %s", year, league)
            return standings_data

        # Parse standings based on pattern
        rows = standings_table.find_all("tr")
        current_division = None

        # Determine if this year has divisions
        has_divisions = year >= 1969

        for row in rows:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            # Skip banner rows
            if any(cell.get("class") == ["banner"] for cell in cells):
                # Check if this is a division marker
                first_cell_text = (
                    cells[0].get_text(strip=True) if cells else ""
                )
                if first_cell_text in ["East", "Central", "West"]:
                    current_division = first_cell_text
                    logger.debug("Found division: %s", current_division)
                continue

            # Skip header rows
            first_cell_text = cells[0].get_text(strip=True) if cells else ""
            if any(
                header in first_cell_text.upper()
                for header in ["TEAM", "W", "L", "PCT"]
            ):
                continue

            # Process team data
            if len(cells) >= 4:  # Minimum: Team, W, L, PCT
                try:
                    # Extract team name
                    team_cell = cells[0]
                    team_link = team_cell.find("a")
                    team_name = ""

                    if team_link:
                        team_name = team_link.get_text(strip=True)
                    else:
                        team_name = team_cell.get_text(strip=True)

                    # Skip invalid entries
                    if not team_name or len(team_name) > 50:
                        continue

                    # Extract wins, losses, and other data
                    wins = None
                    losses = None
                    wp = None
                    gb = None
                    ties = None
                    payroll = None

                    # Pattern-specific parsing
                    if config.has_ties_column and config.has_payroll:
                        # Modern format: Team, W, L, T, WP, GB, Payroll
                        if len(cells) >= 7:
                            wins = int(cells[1].get_text(strip=True))
                            losses = int(cells[2].get_text(strip=True))
                            ties = int(cells[3].get_text(strip=True))
                            wp = float(
                                cells[4].get_text(strip=True).strip(".")
                            )
                            gb = cells[5].get_text(strip=True)
                            payroll = cells[6].get_text(strip=True)
                    elif config.has_payroll:
                        # Mid-2000s format: Team, W, L, WP, GB, Payroll
                        if len(cells) >= 6:
                            wins = int(cells[1].get_text(strip=True))
                            losses = int(cells[2].get_text(strip=True))
                            wp = float(
                                cells[3].get_text(strip=True).strip(".")
                            )
                            gb = cells[4].get_text(strip=True)
                            payroll = cells[5].get_text(strip=True)
                    else:
                        # Older format: Team, W, L, WP, GB
                        if len(cells) >= 5:
                            wins = int(cells[1].get_text(strip=True))
                            losses = int(cells[2].get_text(strip=True))
                            wp = float(
                                cells[3].get_text(strip=True).strip(".")
                            )
                            gb = cells[4].get_text(strip=True)

                    # Handle games back
                    if gb in ["-", "--", "—"]:
                        gb = "0"

                    # Create standing object
                    if wins is not None and losses is not None:
                        standing = TeamStanding(
                            year=year,
                            league=league,
                            division=current_division or "League",
                            team=team_name,
                            wins=wins,
                            losses=losses,
                            winning_percentage=wp or (wins / (wins + losses)),
                            games_back=gb or "0",
                            ties=ties,
                            payroll=payroll,
                            pattern_used=pattern.value,
                        )
                        standings_data.append(standing)
                        self.stats["data_extracted"] += 1

                except (ValueError, IndexError, AttributeError) as e:
                    logger.debug("Error parsing standings row: %s", str(e))
                    continue

        logger.info(
            "Extracted %d standings using pattern %s",
            len(standings_data),
            pattern.value,
        )
        return standings_data

    def make_request(
        self, url: str, max_retries: int = 3
    ) -> Optional[BeautifulSoup]:
        """Make HTTP request with retry logic"""
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

                soup = BeautifulSoup(response.content, "html.parser")
                time.sleep(self.delay)

                return soup

            except requests.RequestException as e:
                self.stats["requests_failed"] += 1
                logger.warning("Request failed: %s", str(e))

                if attempt < max_retries - 1:
                    wait_time = 2**attempt
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

    def scrape_year(self, year: int, league: League) -> Dict[str, int]:
        """Scrape data for a specific year,
        detecting and using the appropriate pattern"""
        url = f"{self.base_url}/yearly/yr{year}{league.code}.shtml"
        soup = self.make_request(url)

        if not soup:
            raise Exception(
                f"Failed to fetch data for {year} {league.display_name}"
            )

        # Detect which pattern to use
        pattern = self.detect_pattern(soup, year, league)

        if not pattern:
            logger.error(
                "Could not detect pattern for %d %s", year, league.display_name
            )
            self.stats["pattern_failures"][
                f"{year}-{league.code}"
            ] = "No pattern detected"
            return {"players": 0, "pitchers": 0, "standings": 0}

        all_stats: List[BaseballStat] = []
        all_standings: List[TeamStanding] = []

        try:
            # Extract player statistics
            player_stats = self.extract_stats_with_pattern(
                soup, year, league.display_name, StatType.PLAYER, pattern
            )
            all_stats.extend(player_stats)

            # Extract pitcher statistics
            pitcher_stats = self.extract_stats_with_pattern(
                soup, year, league.display_name, StatType.PITCHER, pattern
            )
            all_stats.extend(pitcher_stats)

            # Extract team standings
            team_standings = self.extract_standings_with_pattern(
                soup, year, league.display_name, pattern
            )
            all_standings.extend(team_standings)

        except Exception as e:
            logger.error(
                "Error scraping %d %s with pattern %s: %s",
                year,
                league.display_name,
                pattern.value,
                str(e),
            )
            self.stats["pattern_failures"][f"{year}-{league.code}"] = str(e)

        # Save year-specific files with pattern information
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
                "pattern_used",
            ]
            self.save_to_csv(
                all_stats,
                f"{league.code}_stats_{year}_pattern.csv",
                stat_fields,
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
                "ties",
                "payroll",
                "pattern_used",
            ]
            self.save_to_csv(
                all_standings,
                f"{league.code}_standings_{year}_pattern.csv",
                standing_fields,
            )

        # Count by type
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
            "pattern": pattern.value,
        }

    def save_to_csv(
        self, data: List[Any], filename: str, fieldnames: List[str]
    ):
        """Save data to CSV with pattern tracking"""
        filepath = self.output_dir / filename

        try:
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for item in data:
                    row = asdict(item)
                    if "stat_type" in row and hasattr(
                        row["stat_type"], "value"
                    ):
                        row["stat_type"] = row["stat_type"].value
                    writer.writerow(row)

            logger.debug("Saved %d records to %s", len(data), filepath)

        except IOError as e:
            logger.error("Failed to save CSV file %s: %s", filename, str(e))
            raise

    def save_pattern_analysis(self):
        """Save detailed analysis of pattern usage and failures"""
        analysis_file = self.output_dir / "pattern_analysis.json"

        analysis = {
            "timestamp": datetime.now().isoformat(),
            "patterns_detected": self.stats["patterns_detected"],
            "pattern_failures": self.stats["pattern_failures"],
            "total_requests": self.stats["requests_made"],
            "failed_requests": self.stats["requests_failed"],
            "total_data_extracted": self.stats["data_extracted"],
            "pattern_configs": {
                pattern.value: {
                    "year_range": config.year_range,
                    "league": config.league.display_name,
                    "has_payroll": config.has_payroll,
                    "has_ties_column": config.has_ties_column,
                    "uses_middle_class": config.uses_middle_class,
                }
                for pattern, config in self.pattern_configs.items()
            },
        }

        with open(analysis_file, "w") as f:
            json.dump(analysis, f, indent=2)

        logger.info("Pattern analysis saved to %s", analysis_file)

    def scrape_historical_data(self):
        """Main method to scrape all historical data with pattern detection"""
        logger.info("Starting multi-pattern historical baseball data scraping")

        # Define year ranges for each pattern
        pattern_ranges = [
            (League.NATIONAL, 1876, 2004, "Pattern 1"),
            (League.NATIONAL, 2005, 2013, "Pattern 2"),
            (League.NATIONAL, 2014, 2024, "Pattern 3"),
            (League.AMERICAN, 1901, 2001, "Pattern 4"),
            (League.AMERICAN, 2002, 2003, "Pattern 5"),
            (League.AMERICAN, 2004, 2004, "Pattern 6"),
            (League.AMERICAN, 2005, 2013, "Pattern 7"),
            (League.AMERICAN, 2014, 2024, "Pattern 8"),
        ]

        total_stats = {
            "players": 0,
            "pitchers": 0,
            "standings": 0,
            "patterns_used": {},
            "failed_years": [],
        }

        for league, start_year, end_year, pattern_name in pattern_ranges:
            logger.info("\n" + "=" * 60)
            logger.info(
                "Processing %s: %d-%d (%s)",
                league.display_name,
                start_year,
                end_year,
                pattern_name,
            )
            logger.info("=" * 60)

            for year in range(start_year, end_year + 1):
                try:
                    counts = self.scrape_year(year, league)

                    total_stats["players"] += counts["players"]
                    total_stats["pitchers"] += counts["pitchers"]
                    total_stats["standings"] += counts["standings"]

                    pattern_used = counts.get("pattern", "Unknown")
                    total_stats["patterns_used"][pattern_used] = (
                        total_stats["patterns_used"].get(pattern_used, 0) + 1
                    )

                    logger.info(
                        "Year %d complete: %d player, %d pitcher stats, "
                        "%d standings (Pattern: %s)",
                        year,
                        counts["players"],
                        counts["pitchers"],
                        counts["standings"],
                        pattern_used,
                    )

                except Exception as e:
                    logger.error(
                        "Failed to scrape year %d for %s: %s",
                        year,
                        league.display_name,
                        str(e),
                    )
                    total_stats["failed_years"].append(
                        (league.display_name, year, str(e))
                    )

        # Save pattern analysis
        self.save_pattern_analysis()

        # Generate comprehensive summary
        self.generate_summary(total_stats)

    def generate_summary(self, total_stats: Dict[str, Any]):
        """Generate a comprehensive summary of the multi-pattern scraping"""
        success_rate = (
            1
            - self.stats["requests_failed"]
            / max(self.stats["requests_made"], 1)
        ) * 100

        summary = f"""
{'=' * 80}
MULTI-PATTERN BASEBALL ALMANAC SCRAPING SUMMARY
{'=' * 80}
Output Directory: {self.output_dir}

PERFORMANCE METRICS:
  - Total Requests Made: {self.stats['requests_made']:,}
  - Failed Requests: {self.stats['requests_failed']:,}
  - Success Rate: {success_rate:.1f}%

DATA EXTRACTED:
  - Player Statistics: {total_stats['players']:,}
  - Pitcher Statistics: {total_stats['pitchers']:,}
  - Team Standings: {total_stats['standings']:,}
  - Total Records: {self.stats['data_extracted']:,}

PATTERNS USED:
"""

        for pattern, count in sorted(total_stats["patterns_used"].items()):
            summary += f"  - {pattern}: {count} years\n"

        summary += """
PATTERN DETECTION:
"""

        for pattern, count in sorted(self.stats["patterns_detected"].items()):
            summary += f"  - {pattern}: {count} times\n"

        if self.stats["pattern_failures"]:
            summary += """
PATTERN FAILURES:
"""
            for year_league, reason in self.stats["pattern_failures"].items():
                summary += f"  - {year_league}: {reason}\n"

        if total_stats["failed_years"]:
            summary += """
FAILED YEARS:
"""
            for league, year, error in total_stats["failed_years"]:
                summary += f"  - {league} {year}: {error}\n"
        else:
            summary += """
FAILED YEARS:
  - None (100% success!)
"""

        summary += f"""
OUTPUT FILES:
  - Pattern-specific yearly files: [league]_stats_[year]_pattern.csv
  - Pattern-specific standings: [league]_standings_[year]_pattern.csv
  - Pattern analysis: pattern_analysis.json

KEY INSIGHTS:
  - Successfully handled {len(self.stats['patterns_detected'])}
  different HTML patterns
  - Adapted parsing logic for evolving website structure from 1876-2024
  - Maintained data consistency across different eras
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


def demonstrate_pattern_differences():
    """Demonstrate the differences between patterns for educational purposes"""
    print("\n" + "=" * 80)
    print("PATTERN DIFFERENCES IN BASEBALL ALMANAC HTML STRUCTURE")
    print("=" * 80)

    patterns = [
        (
            "Pattern 1 (NL 1876-2004)",
            "Basic structure, no payroll, simple classes",
        ),
        ("Pattern 2 (NL 2005-2013)", "Added payroll data, ties column"),
        ("Pattern 3 (NL 2014-2024)", "Modern structure, enhanced classes"),
        ("Pattern 4 (AL 1901-2001)", "Similar to Pattern 1"),
        (
            "Pattern 5 (AL 2002-2003)",
            "Transitional, detailed team stats tables",
        ),
        (
            "Pattern 6 (AL 2004)",
            "Single year variant with specific formatting",
        ),
        ("Pattern 7 (AL 2005-2013)", "Similar to Pattern 2"),
        ("Pattern 8 (AL 2014-2024)", "Modern structure like Pattern 3"),
    ]

    for pattern, description in patterns:
        print(f"\n{pattern}:")
        print(f"  {description}")

    print("\nKey differences handled by the scraper:")
    print("  1. Payroll data: Added in 2002+ for some leagues")
    print("  2. Ties column: Present in some years but not others")
    print("  3. Cell classes: Evolution from simple to complex")
    print("  4. Rowspan handling: 'middle' class for tied statistics")
    print("  5. Table identification: Different header structures")
    print("=" * 80 + "\n")


def main():
    """Main execution function"""
    # Show pattern differences
    demonstrate_pattern_differences()

    # Create scraper instance
    scraper = MultiPatternBaseballScraper(
        output_dir="multipattern_baseball_data"
    )

    # You can test specific patterns
    # scraper.scrape_year(1876, League.NATIONAL)  # Pattern 1
    # scraper.scrape_year(2005, League.NATIONAL)  # Pattern 2
    # scraper.scrape_year(2014, League.NATIONAL)  # Pattern 3
    # scraper.scrape_year(1901, League.AMERICAN)  # Pattern 4
    # scraper.scrape_year(2002, League.AMERICAN)  # Pattern 5

    # Or run the full historical scrape
    try:
        scraper.scrape_historical_data()
        logger.info("Multi-pattern scraping complete!")
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        print("\nScraping interrupted. Partial data has been saved.")
    except Exception as e:
        logger.error("Unexpected error: %s", str(e), exc_info=True)
        print(f"\nError occurred: {e}")


if __name__ == "__main__":
    main()
