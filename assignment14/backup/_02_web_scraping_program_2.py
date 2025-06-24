#!/usr/bin/env python3
# 2_web_scraping_program_2.py
"""
Baseball Almanac Enhanced Web Scraper with SQLite Database
==========================================================

This version demonstrates professional Python development practices:
- Unified data model to prevent type confusion
- Clear separation of concerns
- Robust error handling
- Efficient database operations
"""

import logging
import sqlite3
import time
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Configure logging with lazy formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("baseball_scraper_enhanced.log"),
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

    This single class handles both player and pitcher statistics,
    preventing type confusion and reducing code duplication.
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


class EnhancedBaseballScraper:
    """Enhanced scraper with unified data model"""

    def __init__(
        self,
        base_url: str = "https://www.baseball-almanac.com",
        delay: float = 1.5,
    ):
        self.base_url = base_url
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Educational Baseball Data Scraper 2.0)"
                )
            }
        )

        # Initialize database
        self.db_path = "baseball_stats.db"
        self._init_database()

    def _init_database(self):
        """Initialize SQLite database with unified schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Single table for all baseball statistics
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS baseball_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                url TEXT,
                team TEXT NOT NULL,
                team_url TEXT,
                year INTEGER NOT NULL,
                league TEXT NOT NULL,
                statistic TEXT NOT NULL,
                value TEXT NOT NULL,
                stat_type TEXT NOT NULL,
                rank INTEGER DEFAULT 1,
                UNIQUE(name, team, year, league, statistic, stat_type)
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS team_standings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL,
                league TEXT NOT NULL,
                division TEXT NOT NULL,
                team TEXT NOT NULL,
                wins INTEGER NOT NULL,
                losses INTEGER NOT NULL,
                winning_percentage REAL,
                games_back TEXT,
                UNIQUE(year, league, division, team)
            )
        """
        )

        # Create indexes for performance
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_stats_year "
            "ON baseball_stats(year)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_stats_name "
            "ON baseball_stats(name)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_stats_type "
            "ON baseball_stats(stat_type)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_standings_year "
            "ON team_standings(year)"
        )

        conn.commit()
        conn.close()
        logger.info("Database initialized at %s", self.db_path)

    def make_request(
        self, url: str, max_retries: int = 3
    ) -> Optional[BeautifulSoup]:
        """Make HTTP request with retry logic"""
        for attempt in range(max_retries):
            try:
                logger.info("Requesting: %s", url)
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, "html.parser")
                time.sleep(self.delay)
                return soup
            except requests.RequestException as e:
                logger.warning(
                    "Request failed (attempt %d): %s", attempt + 1, str(e)
                )
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
        return None

    def extract_review_data(
        self, soup: BeautifulSoup, year: int, league: str, stat_type: StatType
    ) -> List[BaseballStat]:
        """Extract statistics from review tables

        This unified method handles both player and pitcher data,
        reducing code duplication and preventing type confusion.
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
                # Look for name in the second cell
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

                        # Create unified stat object
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

        Team standings have a different structure than player/pitcher stats.
        They're organized by division (East, Central, West) with columns
        for wins, losses, winning percentage, and games back.
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
                    if any(
                        header in first_cell_text.upper()
                        for header in ["TEAM", "W", "L", "PCT", "GB"]
                    ):
                        continue

                    # Process team data if we have a division set
                    if current_division and len(cells) >= 5:
                        # Extract team name (might be in first or second cell)
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
                            # Find wins and losses columns
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
                                        if gb_val == "-" or gb_val == "—":
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

    def save_to_database(
        self,
        baseball_stats: List[BaseballStat],
        team_standings: List[TeamStanding],
    ):
        """Save all extracted data to SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Save baseball statistics (both players and pitchers)
        for stat in baseball_stats:
            try:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO baseball_stats
                    (name, url, team, team_url, year, league,
                     statistic, value, stat_type, rank)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        stat.name,
                        stat.url,
                        stat.team,
                        stat.team_url,
                        stat.year,
                        stat.league,
                        stat.statistic,
                        stat.value,
                        stat.stat_type.value,
                        stat.rank,
                    ),
                )
            except sqlite3.Error as e:
                logger.error(
                    "Error inserting %s stat: %s", stat.stat_type.value, str(e)
                )

        # Save team standings
        for standing in team_standings:
            try:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO team_standings
                    (year, league, division, team, wins, losses,
                     winning_percentage, games_back)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        standing.year,
                        standing.league,
                        standing.division,
                        standing.team,
                        standing.wins,
                        standing.losses,
                        standing.winning_percentage,
                        standing.games_back,
                    ),
                )
            except sqlite3.Error as e:
                logger.error("Error inserting team standing: %s", str(e))

        conn.commit()
        conn.close()
        logger.info("Data saved to database")

    def export_to_csv(self):
        """Export database tables to CSV files"""
        conn = sqlite3.connect(self.db_path)

        # Export player statistics
        player_df = pd.read_sql_query(
            """
            SELECT name AS player_name, team, year, league,
                   statistic, value
            FROM baseball_stats
            WHERE stat_type = 'player'
            ORDER BY year DESC, name
        """,
            conn,
        )

        if not player_df.empty:
            # Pivot to traditional format
            player_pivot = player_df.pivot_table(
                index=["player_name", "team", "year", "league"],
                columns="statistic",
                values="value",
                aggfunc="first",
            ).reset_index()

            player_pivot.to_csv("player_statistics.csv", index=False)
            logger.info("Exported %d player records to CSV", len(player_pivot))

        # Export pitcher statistics
        pitcher_df = pd.read_sql_query(
            """
            SELECT name AS pitcher_name, team, year, league,
                   statistic, value
            FROM baseball_stats
            WHERE stat_type = 'pitcher'
            ORDER BY year DESC, name
        """,
            conn,
        )

        if not pitcher_df.empty:
            pitcher_pivot = pitcher_df.pivot_table(
                index=["pitcher_name", "team", "year", "league"],
                columns="statistic",
                values="value",
                aggfunc="first",
            ).reset_index()

            pitcher_pivot.to_csv("pitcher_statistics.csv", index=False)
            logger.info(
                "Exported %d pitcher records to CSV", len(pitcher_pivot)
            )

        # Export team standings
        standings_df = pd.read_sql_query(
            """
            SELECT * FROM team_standings
            ORDER BY year DESC, league, division, wins DESC
        """,
            conn,
        )

        if not standings_df.empty:
            standings_df.to_csv("team_standings.csv", index=False)
            logger.info("Exported %d team standings to CSV", len(standings_df))

        conn.close()

    def scrape_year(self, year: int):
        """Scrape data for a specific year"""
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

        # Save all data to database
        self.save_to_database(all_stats, all_standings)

        # Count stats by type for reporting
        player_count = sum(
            1 for s in all_stats if s.stat_type == StatType.PLAYER
        )
        pitcher_count = sum(
            1 for s in all_stats if s.stat_type == StatType.PITCHER
        )

        return player_count, pitcher_count, len(all_standings)

    def scrape_range(self, start_year: int = 2000, end_year: int = 2024):
        """Scrape multiple years of data"""
        logger.info("Starting scrape for years %d-%d", start_year, end_year)

        total_players = 0
        total_pitchers = 0
        total_teams = 0

        for year in range(start_year, end_year + 1):
            try:
                players, pitchers, teams = self.scrape_year(year)
                total_players += players
                total_pitchers += pitchers
                total_teams += teams

                logger.info(
                    "Year %d complete: %d player, %d pitcher stats",
                    year,
                    players,
                    pitchers,
                )

            except (requests.RequestException, sqlite3.Error) as e:
                logger.error("Error scraping year %d: %s", year, str(e))
                continue

        logger.info(
            "Scraping complete! Total: %d player, %d pitcher stats",
            total_players,
            total_pitchers,
        )

        # Export to CSV files
        self.export_to_csv()

        # Generate summary report
        self.generate_summary_report()

    def generate_summary_report(self):
        """Generate a summary of the scraped data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get summary statistics
        cursor.execute(
            """
            SELECT COUNT(DISTINCT name)
            FROM baseball_stats
            WHERE stat_type = 'player'
        """
        )
        unique_players = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(DISTINCT name)
            FROM baseball_stats
            WHERE stat_type = 'pitcher'
        """
        )
        unique_pitchers = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM baseball_stats
            WHERE stat_type = 'player'
        """
        )
        total_player_records = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM baseball_stats
            WHERE stat_type = 'pitcher'
        """
        )
        total_pitcher_records = cursor.fetchone()[0]

        cursor.execute("SELECT MIN(year), MAX(year) FROM baseball_stats")
        year_range = cursor.fetchone()

        conn.close()

        print("\n" + "=" * 60)
        print("BASEBALL ALMANAC SCRAPING SUMMARY")
        print("=" * 60)
        print(f"Database: {self.db_path}")
        print(f"Year Range: {year_range[0]} - {year_range[1]}")
        print(f"Unique Players: {unique_players}")
        print(f"Unique Pitchers: {unique_pitchers}")
        print(f"Total Player Statistical Records: {total_player_records}")
        print(f"Total Pitcher Statistical Records: {total_pitcher_records}")
        print("\nExported Files:")
        print("  - player_statistics.csv")
        print("  - pitcher_statistics.csv")
        print("  - team_standings.csv")
        print("=" * 60)


def main():
    """Main execution function"""
    scraper = EnhancedBaseballScraper()

    # You can test with a single year first
    # scraper.scrape_year(2024)

    # Or scrape a range of years
    scraper.scrape_range(2020, 2024)


if __name__ == "__main__":
    main()
