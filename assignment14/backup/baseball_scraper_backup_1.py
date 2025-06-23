#!/usr/bin/env python3
# baseball_scraper.py
"""
Baseball Almanac Web Scraper - Fixed for Team Standings
========================================================

This script demonstrates approaches to complex web scraping challenges,
specifically updated to correctly extract team standings data.

Key improvements:
1. Enhanced table detection specifically for standings tables
2. Proper extraction of division-structured data
3. Correct parsing of wins, losses, and games back
"""

import logging
import os
import re
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

# --- Centralized Directory Configuration ---
# Define the specific directory for the CSV data files
DATA_DIR = "baseball_data"

# Ensure the data directory exists in the current working directory
os.makedirs(DATA_DIR, exist_ok=True)
# --- End of Configuration ---

# Configure comprehensive logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        # Log files will be saved in the current working directory
        logging.FileHandler("baseball_scraper.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Console output logger setup
output_log_path = "console_output.log"
output_file_handler = logging.FileHandler(output_log_path, mode="w")
output_file_handler.setLevel(logging.INFO)
simple_formatter = logging.Formatter("%(message)s")
output_file_handler.setFormatter(simple_formatter)
output_logger = logging.getLogger("console_output")
output_logger.addHandler(output_file_handler)
output_logger.propagate = False


class BaseballAlmanacScraper:
    """
    A comprehensive scraper for Baseball Almanac historical data.
    Updated to correctly extract team standings data.
    """

    def __init__(
        self,
        base_url: str = "https://www.baseball-almanac.com",
        delay: float = 1.5,
    ):
        """Initialize the scraper with configuration parameters."""
        self.base_url = base_url
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "Educational Baseball Data Scraper 1.0"}
        )

        # Storage for our extracted data
        self.data_storage: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
            "american_league": {
                "player_review": [],
                "pitcher_review": [],
                "team_standings": [],
            },
            "national_league": {
                "player_review": [],
                "pitcher_review": [],
                "team_standings": [],
            },
        }

    def make_request(
        self, url: str, max_retries: int = 3
    ) -> Optional[BeautifulSoup]:
        """Make a web request with retry logic and error handling."""
        for attempt in range(max_retries):
            try:
                logger.info("Requesting: %s (attempt %d)", url, attempt + 1)
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, "html.parser")
                time.sleep(self.delay)
                return soup
            except requests.RequestException as e:
                logger.warning(
                    "Request failed (attempt %d): %s", attempt + 1, e
                )
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
        logger.error("Failed to fetch %s after %d attempts", url, max_retries)
        return None

    def extract_table_data(
        self, soup: BeautifulSoup, table_title: str
    ) -> List[Dict[str, Any]]:
        """
        Extract data from HTML tables with special handling for team standings.
        """
        if "team standings" in table_title.lower():
            return self.extract_team_standings(soup, table_title)
        return self.extract_regular_table_data(soup, table_title)

    def extract_team_standings(
        self, soup: BeautifulSoup, table_title: str
    ) -> List[Dict[str, Any]]:
        """
        Specialized extraction for team standings tables.
        """
        logger.info("Extracting team standings for: %s", table_title)

        year_match = re.search(r"\b(19|20)\d{2}\b", table_title)
        year = year_match.group() if year_match else None
        league = (
            "American" if "american" in table_title.lower() else "National"
        )
        standings_data = []
        tables = soup.find_all("table")

        for table in tables:
            table_text = table.get_text().lower()
            has_markers = all(
                m in table_text for m in ["east", "central", "west"]
            )
            headers = [
                th.get_text(strip=True).upper() for th in table.find_all("th")
            ]
            has_cols = "W" in headers and "L" in headers

            if has_markers or has_cols:
                logger.info("Found potential standings table")
                rows = table.find_all("tr")
                current_division = None

                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if not cells:
                        continue

                    first_cell_text = cells[0].get_text(strip=True)
                    if first_cell_text in ["East", "Central", "West"]:
                        current_division = first_cell_text
                        logger.debug("Found division: %s", current_division)
                        continue

                    if len(cells) >= 6 and current_division:
                        team_cell = (
                            cells[1]
                            if "banner" in cells[0].get("class", [])
                            else cells[0]
                        )
                        team_link = team_cell.find("a")
                        team_name = (
                            team_link.get_text(strip=True)
                            if team_link
                            else team_cell.get_text(strip=True)
                        )

                        skip_headers = [
                            "TEAM | ROSTER",
                            "W",
                            "L",
                            "T",
                            "WP",
                            "GB",
                        ]
                        if not team_name or team_name.upper() in skip_headers:
                            continue

                        try:
                            w_val, l_val, wp_val, gb_val = (
                                None,
                                None,
                                None,
                                None,
                            )
                            for i, cell in enumerate(cells[1:], 1):
                                text = cell.get_text(strip=True)
                                if (
                                    w_val is None
                                    and text.isdigit()
                                    and 40 <= int(text) <= 110
                                ):
                                    w_val = text
                                    if i + 1 < len(cells):
                                        l_val = cells[i + 1].get_text(
                                            strip=True
                                        )
                                    if i + 3 < len(cells):
                                        wp_text = cells[i + 3].get_text(
                                            strip=True
                                        )
                                        if wp_text.startswith("."):
                                            wp_val = wp_text
                                    if i + 4 < len(cells):
                                        gb_val = cells[i + 4].get_text(
                                            strip=True
                                        )
                                    break

                            if w_val and l_val:
                                w_int, l_int = int(w_val), int(l_val)
                                if 50 <= w_int + l_int <= 165:
                                    standings_data.append(
                                        {
                                            "year": year,
                                            "league": league,
                                            "division": current_division,
                                            "team": team_name,
                                            "w": w_int,
                                            "l": l_int,
                                            "wp": wp_val or "",
                                            "gb": gb_val or "-",
                                        }
                                    )
                        except (ValueError, IndexError) as e:
                            logger.debug("Failed to parse row: %s", e)
                            continue
                if standings_data:
                    break
        logger.info("Extracted %d teams from standings", len(standings_data))
        return standings_data

    def extract_regular_table_data(
        self, soup: BeautifulSoup, table_title: str
    ) -> List[Dict[str, Any]]:
        """
        Original extraction method for player and pitcher review tables.
        """
        table: Optional[Tag] = None
        title_parts = self._parse_table_title(table_title)
        logger.debug("Searching for table with components: %s", title_parts)

        # Try multiple strategies to find the table
        table = self._find_table_by_heading(soup, table_title, title_parts)
        if not table:
            table = self._find_table_by_attributes(soup, table_title)
        if not table:
            table = self._find_table_by_content(soup, title_parts)
        if not table:
            table = self._find_table_by_structure(soup, title_parts)
        if not table:
            table = self._find_table_by_proximity(soup, title_parts)

        if not table:
            logger.warning("Could not find table: %s", table_title)
            return []
        return self._extract_data_from_table(table)

    def _parse_table_title(self, table_title: str) -> Dict[str, str]:
        """Break down table title into searchable components."""
        components = {}
        year_match = re.search(r"\b(19|20)\d{2}\b", table_title)
        if year_match:
            components["year"] = year_match.group()
        if "american" in table_title.lower():
            components["league"] = "american"
        elif "national" in table_title.lower():
            components["league"] = "national"
        if "player review" in table_title.lower():
            components["data_type"] = "player"
        elif "pitcher review" in table_title.lower():
            components["data_type"] = "pitcher"
        return components

    def _find_table_by_heading(
        self,
        soup: BeautifulSoup,
        table_title: str,
        title_parts: Dict[str, str],
    ) -> Optional[Tag]:
        """Find table by nearby heading elements."""
        heading = soup.find(
            ["h1", "h2", "h3", "h4"],
            string=re.compile(re.escape(table_title), re.IGNORECASE),
        )
        if not heading and "year" in title_parts and "league" in title_parts:
            year, league = title_parts["year"], title_parts["league"]
            headings = soup.find_all(["h1", "h2", "h3", "h4", "div", "span"])
            for h in headings:
                text = h.get_text(strip=True).lower()
                if year in text and league in text:
                    heading = h
                    break
        if heading and (parent := heading.find_parent()):
            table = heading.find_next_sibling("table")
            if not table:
                table = parent.find("table")
            if not table:
                table = heading.find_next("table")
            return table if isinstance(table, Tag) else None
        return None

    def _find_table_by_attributes(
        self, soup: BeautifulSoup, table_title: str
    ) -> Optional[Tag]:
        """Find table by HTML attributes."""
        table = soup.find(
            "table", summary=re.compile(table_title, re.IGNORECASE)
        )
        if isinstance(table, Tag):
            return table
        for caption in soup.find_all("caption"):
            if table_title.lower() in caption.get_text(strip=True).lower():
                if isinstance(table := caption.find_parent("table"), Tag):
                    return table
        return None

    def _find_table_by_content(
        self, soup: BeautifulSoup, title_parts: Dict[str, str]
    ) -> Optional[Tag]:
        """Find table by content matching."""
        for table in soup.find_all("table"):
            text = table.get_text(strip=True).lower()
            total = sum(
                1 for k, v in title_parts.items() if k != "full_title" and v
            )
            matches = sum(
                1 for v in title_parts.values() if v and v.lower() in text
            )
            if total > 0 and matches >= (total * 0.7):
                return table
        return None

    def _find_table_by_structure(
        self, soup: BeautifulSoup, title_parts: Dict[str, str]
    ) -> Optional[Tag]:
        """Find tables by expected structural patterns."""
        if "data_type" not in title_parts:
            return None
        data_type = title_parts["data_type"]
        for table in soup.find_all("table"):
            headers = [
                th.text.lower() for th in table.find_all(["th", "td"])[:10]
            ]
            header_text = " ".join(headers)
            if data_type == "player" and any(
                k in header_text for k in ["player", "avg", "hits"]
            ):
                return table
            elif data_type == "pitcher" and any(
                k in header_text for k in ["pitcher", "era", "wins"]
            ):
                return table
        return None

    def _find_table_by_proximity(
        self, soup: BeautifulSoup, title_parts: Dict[str, str]
    ) -> Optional[Tag]:
        """Find tables near relevant text content."""
        if "year" not in title_parts or "league" not in title_parts:
            return None
        year, league = title_parts["year"], title_parts["league"]
        regex = re.compile(f"{year}.*{league}|{league}.*{year}", re.IGNORECASE)
        for element in soup.find_all(text=regex):
            parent = element.find_parent()
            while parent and parent.name != "body":
                if isinstance(parent, Tag) and (table := parent.find("table")):
                    if isinstance(table, Tag):
                        return table
                parent = parent.find_parent()
        return None

    def _extract_data_from_table(self, table: Tag) -> List[Dict[str, Any]]:
        """Extract and clean data from the identified table."""
        rows = table.find_all("tr")
        if len(rows) < 2:
            return []

        header_row, header_idx = next(
            (
                (r, i)
                for i, r in enumerate(rows)
                if any(
                    c.get_text(strip=True)
                    and not c.get_text(strip=True).isdigit()
                    for c in r.find_all(["th", "td"])
                )
            ),
            (None, 0),
        )
        if not header_row:
            return []

        headers = [
            self.clean_header(h.get_text(strip=True))
            for h in header_row.find_all(["th", "td"])
            if h.get_text(strip=True)
        ]
        table_data = []
        for row in rows[header_idx + 1 :]:
            cells = row.find_all(["td", "th"])
            if len(cells) >= len(headers):
                row_data = {
                    headers[i]: c.get_text(strip=True)
                    for i, c in enumerate(cells[: len(headers)])
                }
                if any(str(v).strip() for v in row_data.values()):
                    table_data.append(row_data)
        return table_data

    def clean_header(self, header: str) -> str:
        """Clean header text for use as CSV column names."""
        header = re.sub(r"[^\w\s]", "", header)
        return re.sub(r"\s+", "_", header.strip()).lower()

    def scrape_year_data(self, year: int, league: str) -> Dict[str, Any]:
        """Scrape all data for a specific year and league."""
        url = f"{self.base_url}/yearly/yr{year}{league}.shtml"
        soup = self.make_request(url)
        if not soup:
            return {}
        league_name = "American" if league == "a" else "National"
        logger.info("Scraping %d %s League data", year, league_name)
        table_mappings = {
            "player_review": f"{year} {league_name} League Player Review",
            "pitcher_review": f"{year} {league_name} League Pitcher Review",
            "team_standings": f"{year} {league_name} League Team Standings",
        }
        year_data: Dict[str, Any] = {}
        for data_type, title in table_mappings.items():
            table_data = self.extract_table_data(soup, title)
            if data_type != "team_standings":
                for row in table_data:
                    row.update({"year": year, "league": league_name})
            year_data[data_type] = table_data
        return year_data

    def scrape_all_years(
        self, start_year: int = 2000, end_year: int = 2024
    ) -> None:
        """Scrape data for all years and both leagues."""
        total_ops = (end_year - start_year + 1) * 2
        logger.info("Starting scrape for years %d-%d", start_year, end_year)
        leagues = [("a", "american_league"), ("n", "national_league")]
        jobs = [
            (y, l_tuple)
            for y in range(start_year, end_year + 1)
            for l_tuple in leagues
        ]
        for i, (year, (l_code, l_name)) in enumerate(jobs):
            try:
                year_data = self.scrape_year_data(year, l_code)
                for data_type, rows in year_data.items():
                    self.data_storage[l_name][data_type].extend(rows)
                progress = ((i + 1) / total_ops) * 100
                logger.info(
                    "Progress: %.1f%% (%d/%d)", progress, i + 1, total_ops
                )
            except KeyboardInterrupt:
                logger.warning("Scraping interrupted by user.")
                return
            except (AttributeError, KeyError, TypeError) as e:
                logger.error(
                    "Data processing error for %d %s: %s", year, l_name, e
                )
                continue
        logger.info("Scraping completed successfully!")

    def save_to_csv(self, output_dir: str = DATA_DIR) -> None:
        """Save all collected data to CSV files."""
        file_mappings = {
            (
                "american_league",
                "player_review",
            ): "Yearly_American_League_Player_Review.csv",
            (
                "american_league",
                "pitcher_review",
            ): "Yearly_American_League_Pitcher_Review.csv",
            (
                "american_league",
                "team_standings",
            ): "Yearly_American_League_Team_Standings.csv",
            (
                "national_league",
                "player_review",
            ): "Yearly_National_League_Player_Review.csv",
            (
                "national_league",
                "pitcher_review",
            ): "Yearly_National_League_Pitcher_Review.csv",
            (
                "national_league",
                "team_standings",
            ): "Yearly_National_League_Team_Standings.csv",
        }
        for (league, data_type), filename in file_mappings.items():
            data = self.data_storage[league][data_type]
            if data:
                df = pd.DataFrame(data)
                df = self.clean_dataframe(df, data_type)
                filepath = os.path.join(output_dir, filename)
                df.to_csv(filepath, index=False)
                logger.info("Saved %d rows to %s", len(df), filepath)
            else:
                logger.warning("No data found for %s %s", league, data_type)

    def clean_dataframe(
        self, df: pd.DataFrame, data_type: str
    ) -> pd.DataFrame:
        """Clean and optimize DataFrame for analysis."""
        df = df.dropna(how="all").drop_duplicates()
        if data_type == "team_standings" and {
            "year",
            "division",
            "gb",
        }.issubset(df.columns):

            def clean_gb(value):
                if pd.isna(value):
                    return 0.0
                val_str = str(value).strip()
                if val_str in ["-", "—"]:
                    return 0.0
                cleaned = "".join(
                    c for c in val_str if c.isdigit() or c == "."
                )
                try:
                    return float(cleaned) if cleaned else 0.0
                except ValueError:
                    logger.warning("Could not parse GB value: %s", val_str)
                    return 0.0

            df["gb_numeric"] = df["gb"].apply(clean_gb)
            df = df.sort_values(["year", "division", "gb_numeric"])
            df = df.drop("gb_numeric", axis=1)
            if "w" in df.columns and "l" in df.columns:
                mask = (
                    pd.to_numeric(df["w"], errors="coerce").notna()
                    & pd.to_numeric(df["l"], errors="coerce").notna()
                )
                if len(df) > len(df[mask]):
                    logger.warning(
                        "Removed %d invalid rows from standings",
                        len(df) - len(df[mask]),
                    )
                df = df[mask]
        elif "year" in df.columns:
            df = df.sort_values(["year"])

        for col in df.columns:
            if col in ["w", "l"] and data_type == "team_standings":
                df[col] = (
                    pd.to_numeric(df[col], errors="coerce")
                    .fillna(0)
                    .astype(int)
                )
            elif col not in [
                "player",
                "team",
                "pos",
                "league",
                "division",
                "gb",
                "wp",
            ] and not col.endswith("_link"):
                try:
                    df[col] = pd.to_numeric(df[col])
                except (ValueError, TypeError):
                    logger.debug("Column '%s' kept as non-numeric", col)
        return df.reset_index(drop=True)


def main():
    """Main execution function demonstrating the complete workflow."""
    try:
        scraper = BaseballAlmanacScraper()
        scraper.scrape_all_years(2000, 2024)
        scraper.save_to_csv()
        total_rows = sum(
            len(dt)
            for league_data in scraper.data_storage.values()
            for dt in league_data.values()
        )
        logger.info("Scraping complete! Total rows collected: %d", total_rows)
        summary_header = "\n" + "=" * 60 + "\nSCRAPING SUMMARY\n" + "=" * 60
        output_logger.info(summary_header.replace("\n", "", 1))
        print(summary_header)
        for league, data in scraper.data_storage.items():
            header = f"\n{league.title().replace('_', ' ')}:"
            output_logger.info(header.replace("\n", ""))
            print(header)
            for data_type, rows in data.items():
                summary = f"  {data_type.title().replace('_', ' ')}: {len(rows)} rows"
                output_logger.info(summary)
                print(summary)

        output_path = os.path.abspath(DATA_DIR)
        saved_to_msg = f"\nFiles saved to: {output_path}/"
        analysis_msg = "Ready for analysis!"
        output_logger.info(saved_to_msg.replace("\n", ""))
        output_logger.info(analysis_msg)
        print(saved_to_msg)
        print(analysis_msg)

    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user.")
    except Exception as e:
        logger.critical("An unexpected error occurred: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()
