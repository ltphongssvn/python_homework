-- List all distinct batting statistics available in the database.
SELECT DISTINCT statistic FROM player_stats ORDER BY statistic;

-- List all distinct pitching statistics available in the database.
SELECT DISTINCT statistic FROM pitcher_stats ORDER BY statistic;

-- Find the earliest and latest year recorded in each table.
SELECT 'player_stats' as table_name, MIN(year) as earliest_year, MAX(year) as latest_year FROM player_stats
UNION ALL
SELECT 'pitcher_stats' as table_name, MIN(year) as earliest_year, MAX(year) as latest_year FROM pitcher_stats
UNION ALL
SELECT 'team_standings' as table_name, MIN(year) as earliest_year, MAX(year) as latest_year FROM team_standings;

-- Count the unique players, pitchers, and teams.
SELECT
    (SELECT COUNT(DISTINCT name) FROM player_stats) as unique_batters,
    (SELECT COUNT(DISTINCT name) FROM pitcher_stats) as unique_pitchers,
    (SELECT COUNT(DISTINCT team) FROM team_standings) as unique_teams;

-- Find the all-time leaders for home runs in a single season.
SELECT year, name, team, CAST(value AS INTEGER) as home_runs
FROM player_stats
WHERE statistic = 'Home Runs'
ORDER BY home_runs DESC
LIMIT 10;

-- Find the all-time best single-season ERAs.
SELECT year, name, team, CAST(value AS REAL) as era
FROM pitcher_stats
WHERE statistic = 'ERA' AND era > 0
ORDER BY era ASC
LIMIT 10;

-- Find the best single-season batting averages.
SELECT year, name, team, CAST(value AS REAL) as batting_average
FROM player_stats
WHERE statistic = 'Batting Average'
ORDER BY batting_average DESC
LIMIT 10;

-- Show the top 10 team performances by number of wins.
SELECT year, team, league, wins, losses, winning_percentage
FROM team_standings
ORDER BY wins DESC
LIMIT 10;

-- Calculate total career home runs by grouping by player name.
SELECT
    name,
    SUM(CASE WHEN statistic = 'Home Runs' THEN CAST(value AS INTEGER) ELSE 0 END) as career_home_runs,
    MIN(year) as first_year,
    MAX(year) as last_year
FROM player_stats
WHERE statistic = 'Home Runs'
GROUP BY name
ORDER BY career_home_runs DESC
LIMIT 20;

-- Calculate total career wins for pitchers.
SELECT
    name,
    SUM(CAST(value AS INTEGER)) as career_wins
FROM pitcher_stats
WHERE statistic = 'Wins'
GROUP BY name
ORDER BY career_wins DESC
LIMIT 20;

-- Find players who appear in the most distinct number of seasons.
SELECT
    name,
    COUNT(DISTINCT year) as seasons_played,
    MIN(year) as first_season,
    MAX(year) as last_season
FROM player_stats
GROUP BY name
ORDER BY seasons_played DESC
LIMIT 20;

-- Show the total home runs and average leader's total for each decade.
SELECT
    (year / 10) * 10 || 's' as decade,
    SUM(CAST(value AS INTEGER)) as total_home_runs,
    ROUND(AVG(CAST(value AS REAL)), 1) as avg_hr_leader
FROM player_stats
WHERE statistic = 'Home Runs'
GROUP BY decade
ORDER BY decade;

-- Show the average ERA leader's performance for each decade.
SELECT
    (year / 10) * 10 || 's' as decade,
    ROUND(AVG(CAST(value AS REAL)), 2) as avg_era_leader
FROM pitcher_stats
WHERE statistic = 'ERA' AND CAST(value AS REAL) > 0
GROUP BY decade
ORDER BY decade;

-- Find players who had 40+ home runs on a team that won at least 60% of its games.
SELECT
    ps.year,
    ps.name,
    ps.team,
    CAST(ps.value AS INTEGER) as home_runs,
    ts.winning_percentage
FROM player_stats as ps
JOIN team_standings as ts ON ps.year = ts.year AND ps.team = ts.team
WHERE
    ps.statistic = 'Home Runs'
    AND home_runs >= 40
    AND ts.winning_percentage >= 0.600
ORDER BY home_runs DESC;

-- Find pitchers who had an excellent ERA (sub-2.50) while playing for a team with a losing record.
SELECT
    ps.year,
    ps.name,
    ps.team,
    CAST(ps.value AS REAL) as era,
    ts.winning_percentage
FROM pitcher_stats as ps
JOIN team_standings as ts ON ps.year = ts.year AND ps.team = ts.team
WHERE
    ps.statistic = 'ERA'
    AND era < 2.50
    AND ts.winning_percentage < 0.500
ORDER BY era ASC;

-- Calculate the standard deviation of winning percentages for each league each year to see how balanced the competition was.
SELECT
    year,
    league,
    COUNT(team) as teams,
    ROUND(AVG(winning_percentage), 3) as avg_win_pct,
    ROUND(STDDEV(winning_percentage), 4) as std_dev_win_pct
FROM team_standings
WHERE year >= 1950
GROUP BY year, league
ORDER BY year DESC, league;

-- List all tables in the database.
SELECT name
FROM sqlite_master
WHERE type='table'
ORDER BY name;

-- Show column details (schema) for each table.
-- For pitcher_stats
PRAGMA table_info('pitcher_stats');
-- For player_stats
PRAGMA table_info('player_stats');
-- For team_standings
PRAGMA table_info('team_standings');


-- Count rows per table.
SELECT 'pitcher_stats' AS table_name, COUNT(*) AS row_count FROM pitcher_stats
UNION ALL
SELECT 'player_stats',         COUNT(*) FROM player_stats
UNION ALL
SELECT 'team_standings',      COUNT(*) FROM team_standings;

-- Year range covered by each table.
SELECT MIN(year) AS min_year, MAX(year) AS max_year FROM pitcher_stats;
SELECT MIN(year), MAX(year)               FROM player_stats;
SELECT MIN(year), MAX(year)               FROM team_standings;

-- Exploring pitcher_stats, distinct statistics and stat types.
SELECT stat_type, statistic, COUNT(*) AS count_occurrences
FROM pitcher_stats
GROUP BY stat_type, statistic
ORDER BY stat_type, statistic;

-- Top 10 pitchers by value for a given stat (e.g., ERA) in a given year and league.
SELECT year, league, name, team, value
FROM pitcher_stats
WHERE statistic = 'ERA' AND year = 2024  -- change year/league as needed
ORDER BY CAST(value AS REAL)
LIMIT 10;

-- Which pitchers appeared most frequently across all stats in a specific year.
SELECT year, name, COUNT(*) AS stat_entries
FROM pitcher_stats
WHERE year = 2024
GROUP BY year, name
ORDER BY stat_entries DESC
LIMIT 10;

-- Compare pitcher performance across years (trending ERA).
SELECT year, AVG(CAST(value AS REAL)) AS avg_era
FROM pitcher_stats
WHERE statistic = 'ERA'
GROUP BY year
ORDER BY year;

-- Exploring player_stats, monthly or yearly leaders by stat type.
SELECT year, statistic, name, value
FROM player_stats
WHERE year = 2023 AND statistic = 'Home Runs'
ORDER BY CAST(value AS INTEGER) DESC
LIMIT 5;

-- Top batters by batting average over entire history.
SELECT name, MAX(CAST(value AS REAL)) AS best_batting_average, year
FROM player_stats
WHERE statistic = 'Batting Average'
GROUP BY name
ORDER BY best_batting_average DESC
LIMIT 10;

-- Number of distinct stat categories tracked per year.
SELECT year, COUNT(DISTINCT statistic) AS stat_categories
FROM player_stats
GROUP BY year
ORDER BY year;

-- Exploring team_standings, which teams won most games each year.
SELECT year, team, wins, losses, winning_percentage
FROM team_standings AS t
WHERE wins = (
  SELECT MAX(wins)
  FROM team_standings
  WHERE year = t.year
)
ORDER BY year;

-- Average number of wins per league per year.
SELECT year, league, AVG(wins) AS avg_wins
FROM team_standings
GROUP BY year, league
ORDER BY year, league;

-- Total championships by team (1st-place finishes).
SELECT team, COUNT(*) AS times_first
FROM (
  SELECT year, league, team, wins,
         RANK() OVER (PARTITION BY year, league ORDER BY wins DESC) AS rank_in_league
  FROM team_standings
) AS ranked
WHERE rank_in_league = 1
GROUP BY team
ORDER BY times_first DESC
LIMIT 10;

-- Correlate top teams and top pitchers in the same year.
WITH top_teams AS (
  SELECT year, team
  FROM (
    SELECT year, league, team, wins,
           RANK() OVER (PARTITION BY year, league ORDER BY wins DESC) AS rk
    FROM team_standings
  )
  WHERE rk = 1
),
top_pitchers AS (
  SELECT year, name, team, CAST(value AS REAL) AS era
  FROM pitcher_stats
  WHERE statistic = 'ERA'
)
SELECT tt.year, tt.team, tp.name, tp.era
FROM top_teams AS tt
LEFT JOIN top_pitchers AS tp
  ON tt.year = tp.year
  AND tt.team = tp.team
ORDER BY tt.year DESC;

-- See if leader in home runs is on a top team that year.
WITH top_teams AS (
  SELECT year, team
  FROM (
    SELECT year, league, team, wins,
           RANK() OVER (PARTITION BY year, league ORDER BY wins DESC) AS rk
    FROM team_standings
  ) WHERE rk = 1
),
hr_leaders AS (
  SELECT year, name, team,
         MAX(CAST(value AS INTEGER)) AS max_hr
  FROM player_stats
  WHERE statistic = 'Home Runs'
  GROUP BY year, name, team
)
SELECT hl.year, hl.name, hl.max_hr,
       CASE WHEN tl.team IS NOT NULL THEN '✔️' ELSE '❌' END AS on_top_team
FROM hr_leaders AS hl
LEFT JOIN top_teams AS tl
  ON hl.year = tl.year AND hl.team = tl.team
ORDER BY hl.year DESC;

-- Show all distinct teams (across all tables).
SELECT DISTINCT team FROM pitcher_stats
UNION
SELECT DISTINCT team FROM player_stats
UNION
SELECT DISTINCT team FROM team_standings
ORDER BY team;

-- Spot missing years in any table (e.g., pitcher_stats).
WITH
  bounds AS (
    SELECT MIN(year) AS min_year, MAX(year) AS max_year
    FROM pitcher_stats
  ),
  -- Generate all years between min_year and max_year
  years AS (
    SELECT min_year AS year
    FROM bounds
    UNION ALL
    SELECT year + 1
    FROM years
    JOIN bounds ON year < bounds.max_year
  )
SELECT years.year
FROM years
LEFT JOIN (
  SELECT DISTINCT year FROM pitcher_stats
) AS existing USING (year)
WHERE existing.year IS NULL
ORDER BY years.year;

-- Basic SELECT queries for each table
-- Preview few rows of pitcher_stats
SELECT * FROM pitcher_stats
LIMIT 5;

-- Preview few rows of player_stats
SELECT * FROM player_stats
LIMIT 5;

-- Preview few rows of team_standings
SELECT * FROM team_standings
LIMIT 5;

-- Select specific columns for pitcher stats
SELECT year, name, team, statistic, value FROM pitcher_stats;

-- Select specific columns for pitcher stats
SELECT year, name, team, statistic, value FROM pitcher_stats;

-- Filter pitcher stats by year (e.g., 1901)
SELECT * FROM pitcher_stats WHERE year = 1901;

-- Filter pitcher stats by league (e.g., American League)
SELECT * FROM pitcher_stats WHERE league = 'American League';

-- Find pitcher with lowest ERA in 1901 (casting value to REAL for comparison)
SELECT name, value FROM pitcher_stats
WHERE year = 1901 AND statistic = 'ERA'
ORDER BY CAST(value AS REAL) ASC LIMIT 1;

-- Select specific columns for player stats
SELECT year, name, team, statistic, value FROM player_stats;

-- Filter player stats by year (e.g., 1901)
SELECT * FROM player_stats WHERE year = 1901;

-- Filter player stats by league (e.g., American League)
SELECT * FROM player_stats WHERE league = 'American League';

-- Find player with highest batting average in 1901
SELECT name, value FROM player_stats
WHERE year = 1901 AND statistic = 'Batting Average'
ORDER BY CAST(value AS REAL) DESC LIMIT 1;

-- Select specific columns for team standings
SELECT year, team, wins, losses, winning_percentage FROM team_standings;

-- Filter team standings by year (e.g., 1901)
SELECT * FROM team_standings WHERE year = 1901;

-- Filter team standings by league (e.g., American League)
SELECT * FROM team_standings WHERE league = 'American League';

-- Find team with most wins in 1901
SELECT team, wins FROM team_standings
WHERE year = 1901 ORDER BY wins DESC LIMIT 1;

-- Join queries to combine data
-- Find league winner by highest winning percentage in 1901 American League
SELECT team FROM team_standings
WHERE year = 1901 AND league = 'American League'
ORDER BY winning_percentage DESC LIMIT 1;

-- Get batting averages for players on the 1901 American League winning team
SELECT name, value FROM player_stats
WHERE year = 1901 AND league = 'American League' AND team = 'Chicago White Sox'
AND statistic = 'Batting Average';

-- Aggregate queries for team totals
-- Total home runs by team in 1901 American League
SELECT team, SUM(CAST(value AS INTEGER)) AS total_home_runs
FROM player_stats
WHERE year = 1901 AND league = 'American League' AND statistic = 'Home Runs'
GROUP BY team;

-- Total pitcher wins by team in 1901 American League (assuming 'Wins' is a statistic)
SELECT team, SUM(CAST(value AS INTEGER)) AS total_wins
FROM pitcher_stats
WHERE year = 1901 AND league = 'American League' AND statistic = 'Wins'
GROUP BY team;

-- Find player with most home runs in 1901
SELECT name, value FROM player_stats
WHERE year = 1901 AND statistic = 'Home Runs'
ORDER BY CAST(value AS INTEGER) DESC LIMIT 1;

-- Trend analysis: Total home runs per year for American League
SELECT year, SUM(CAST(value AS INTEGER)) AS total_home_runs
FROM player_stats
WHERE league = 'American League' AND statistic = 'Home Runs'
GROUP BY year
ORDER BY year;

-- Find players who played for Chicago White Sox in both 1901 and 1902
SELECT DISTINCT p1.name
FROM player_stats p1
JOIN player_stats p2 ON p1.name = p2.name
WHERE p1.year = 1901 AND p2.year = 1902
AND p1.team = 'Chicago White Sox' AND p2.team = 'Chicago White Sox';

-- Career totals: Total home runs for each player
SELECT name, SUM(CAST(value AS INTEGER)) AS total_home_runs
FROM player_stats
WHERE statistic = 'Home Runs'
GROUP BY name
ORDER BY total_home_runs DESC;

-- Get all pitcher stats.
SELECT * FROM pitcher_stats;

-- Get pitcher stats for a specific year.
SELECT * FROM pitcher_stats WHERE year = 2024;

-- Get pitcher stats for a specific league
SELECT * FROM pitcher_stats WHERE league = 'American League';

-- Get pitcher stats for a specific statistic
SELECT * FROM pitcher_stats WHERE statistic = 'ERA';

-- Get top 10 pitchers with the lowest ERA
SELECT name, team, value
FROM pitcher_stats
WHERE statistic = 'ERA'
ORDER BY CAST(value AS REAL) ASC
LIMIT 10;

-- Get all player stats
SELECT * FROM player_stats;

-- Get player stats for a specific year
SELECT * FROM player_stats WHERE year = 2000;

-- Get player stats for a specific league
SELECT * FROM player_stats WHERE league = 'American League';

-- Get player stats for a specific statistic
SELECT * FROM player_stats WHERE statistic = 'Batting Average';

-- Get top 10 players with the highest batting average
SELECT name, team, value
FROM player_stats
WHERE statistic = 'Batting Average'
ORDER BY CAST(value AS REAL) DESC
LIMIT 10;

-- Get all team standings
SELECT * FROM team_standings;

-- Get team standings for a specific year
SELECT * FROM team_standings WHERE year = 2014;

-- Get team standings for a specific league
SELECT * FROM team_standings WHERE league = 'American League';

-- Get team standings for a specific division
SELECT * FROM team_standings WHERE division = 'League';

-- Get top 5 teams with the highest winning percentage
SELECT team, wins, losses, winning_percentage
FROM team_standings
ORDER BY winning_percentage DESC
LIMIT 5;

-- Count the number of rows in each table for an overview.
SELECT 'pitcher_stats' AS table_name, COUNT(*) AS total_rows FROM pitcher_stats
UNION ALL
SELECT 'player_stats', COUNT(*) FROM player_stats
UNION ALL
SELECT 'team_standings', COUNT(*) FROM team_standings;

-- Retrieve a small sample of data from each table.
-- Sample rows from pitcher_stats
SELECT * FROM pitcher_stats LIMIT 5;

-- Sample rows from player_stats
SELECT * FROM player_stats LIMIT 5;

-- Sample rows from team_standings
SELECT * FROM team_standings LIMIT 5;

-- Explore distinct years or leagues available.
-- Distinct years in pitcher_stats:
SELECT DISTINCT year FROM pitcher_stats ORDER BY year;

-- Distinct leagues in player_stats:
SELECT DISTINCT league FROM player_stats ORDER BY league;

-- Average Performance in Team Standings.
SELECT
  year,
  AVG(wins) AS avg_wins,
  AVG(losses) AS avg_losses,
  AVG(winning_percentage) AS avg_win_pct
FROM team_standings
GROUP BY year
ORDER BY year;

-- Identify the top team each season by winning percentage.
WITH RankedTeams AS (
  SELECT
    year,
    team,
    wins,
    losses,
    winning_percentage,
    RANK() OVER (PARTITION BY year ORDER BY winning_percentage DESC) AS rank
  FROM team_standings
)
SELECT *
FROM RankedTeams
WHERE rank = 1;

-- Highest Batting Averages (Player Stats).
SELECT
  year,
  name,
  team,
  CAST(value AS REAL) AS batting_average
FROM player_stats
WHERE statistic = 'Batting Average'
ORDER BY year, batting_average DESC;

-- Best Pitching Performances (ERA) in Pitcher Stats.
SELECT
  year,
  name,
  team,
  CAST(value AS REAL) AS ERA
FROM pitcher_stats
WHERE statistic = 'ERA'
ORDER BY year, ERA ASC;

-- Grouping by Statistic Types for Overview.
-- In player_stats:
SELECT stat_type, COUNT(*) AS count
FROM player_stats
GROUP BY stat_type;

-- In pitcher_stats:
SELECT stat_type, COUNT(*) AS count
FROM pitcher_stats
GROUP BY stat_type;

-- Relate player statistics to team performance.
SELECT
  p.year,
  p.team,
  p.name,
  p.statistic,
  p.value,
  t.wins,
  t.losses,
  t.winning_percentage
FROM player_stats p
JOIN team_standings t ON p.team = t.team AND p.year = t.year
WHERE p.statistic = 'Hits'
ORDER BY p.year, p.team;

-- Compare a player’s combined stats (if they appear in both tables).
SELECT
  'Player' AS record_type,
  year,
  stat_type,
  statistic,
  value
FROM player_stats
WHERE name = 'Nap Lajoie'
UNION ALL
SELECT
  'Pitcher',
  year,
  stat_type,
  statistic,
  value
FROM pitcher_stats
WHERE name = 'Nap Lajoie'
ORDER BY year;

-- Retrieve all team standings for the 1901 season.
SELECT *
FROM team_standings
WHERE year = 1901;

-- Count rows in each table
SELECT COUNT(*) AS total_rows FROM pitcher_stats;
SELECT COUNT(*) AS total_rows FROM player_stats;
SELECT COUNT(*) AS total_rows FROM team_standings;

-- Find the range of years in pitcher_stats
SELECT MIN(year) AS start_year, MAX(year) AS end_year
FROM pitcher_stats;

-- Find the range of years in player_stats
SELECT MIN(year) AS start_year, MAX(year) AS end_year
FROM player_stats;

-- Find the range of years in team_standings
SELECT MIN(year) AS start_year, MAX(year) AS end_year
FROM player_stats;

-- Find the range of years in team_standings
SELECT MIN(year) AS start_year, MAX(year) AS end_year
FROM team_standings;

-- List distinct leagues in each table
SELECT DISTINCT league
FROM pitcher_stats;

SELECT DISTINCT league
FROM player_stats;

SELECT DISTINCT league
FROM team_standings;

-- Group the pitcher statistics by the type of stat (e.g., Complete Games, ERA, etc.).
SELECT stat_type, COUNT(*) AS occurrences
FROM pitcher_stats
GROUP BY stat_type
ORDER BY occurrences DESC;

-- Frequency distribution of various player statistics
SELECT statistic, COUNT(*) AS occurrences
FROM player_stats
GROUP BY statistic
ORDER BY occurrences DESC;

-- Average winning percentages along with counts of seasons per league.
SELECT league, AVG(winning_percentage) AS avg_win_pct, COUNT(*) AS seasons
FROM team_standings
GROUP BY league;

-- Join: Pitchers with Team Standings: to link individual player or pitcher statistics with broader team performance data
SELECT
    p.year,
    p.name AS pitcher,
    p.value AS pitcher_value,
    t.team,
    t.wins,
    t.losses,
    t.winning_percentage
FROM pitcher_stats p
JOIN team_standings t
    ON p.year = t.year AND p.team = t.team
LIMIT 10;

-- Join: Players with Team Standings.
SELECT
    p.year,
    p.name AS player,
    p.value AS stat_value,
    t.team,
    t.wins,
    t.losses
FROM player_stats p
JOIN team_standings t
    ON p.year = t.year AND p.team = t.team
LIMIT 10;

-- Inspect how SQLite will execute a given query.
EXPLAIN QUERY PLAN
SELECT * FROM player_stats;

-- Total number of pitchers.
SELECT COUNT(DISTINCT name) FROM pitcher_stats;

-- Top 5 pitchers with the most complete games.
SELECT name, value
FROM pitcher_stats
WHERE statistic = 'Complete Games'
ORDER BY CAST(value AS INTEGER) DESC
LIMIT 5;

-- Average ERA for each league.
SELECT league, AVG(CAST(value AS REAL))
FROM pitcher_stats
WHERE statistic = 'ERA'
GROUP BY league;

-- Total number of players.
SELECT COUNT(DISTINCT name) FROM player_stats;

-- Top 5 players with the highest batting average.
SELECT name, value
FROM player_stats
WHERE statistic = 'Batting Average'
ORDER BY CAST(value AS REAL) DESC
LIMIT 5;

-- Total hits for each team.
SELECT team, SUM(CAST(value AS INTEGER))
FROM player_stats
WHERE statistic = 'Hits'
GROUP BY team;

-- Total number of teams.
SELECT COUNT(DISTINCT team) FROM team_standings;

-- Top 5 teams with the highest winning percentage.
SELECT team, winning_percentage
FROM team_standings
ORDER BY winning_percentage DESC
LIMIT 5;

-- Average wins and losses for each league.
SELECT league, AVG(wins), AVG(losses)
FROM team_standings
GROUP BY league;

-- Players who are both pitchers and batters.
SELECT DISTINCT ps.name
FROM player_stats ps
JOIN pitcher_stats pits
ON ps.name = pits.name;

-- Teams with the highest winning percentage and most complete games.
SELECT ts.team, ts.winning_percentage, pits.value
FROM team_standings ts
JOIN pitcher_stats pits
ON ts.team = pits.team
WHERE pits.statistic = 'Complete Games'
ORDER BY ts.winning_percentage DESC, CAST(pits.value AS INTEGER) DESC
LIMIT 5;

-- Count total records in each table.
SELECT 'pitcher_stats' AS table_name, COUNT(*) AS row_count FROM pitcher_stats
UNION
SELECT 'player_stats', COUNT(*) FROM player_stats
UNION
SELECT 'team_standings', COUNT(*) FROM team_standings;

-- List distinct years covered.
SELECT DISTINCT year FROM pitcher_stats
UNION
SELECT DISTINCT year FROM player_stats
UNION
SELECT DISTINCT year FROM team_standings
ORDER BY year;

-- List distinct leagues.
SELECT DISTINCT league FROM pitcher_stats
UNION
SELECT DISTINCT league FROM player_stats
UNION
SELECT DISTINCT league FROM team_standings
ORDER BY league;

-- Top 5 pitchers by ERA in a specific year (e.g., 2024).
SELECT name, team, value
FROM pitcher_stats
WHERE statistic = 'ERA' AND year = 2024
ORDER BY CAST(value AS REAL) ASC
LIMIT 5;

-- Most frequent pitching statistics tracked.
SELECT statistic, COUNT(*) AS count
FROM pitcher_stats
GROUP BY statistic
ORDER BY count DESC;

-- Pitchers with multiple awards in a single year.
SELECT year, league, name, team, COUNT(*) AS stat_count
FROM pitcher_stats
GROUP BY year, league, name, team
HAVING stat_count > 1
ORDER BY stat_count DESC, year, name
LIMIT 10;

-- Top 5 batting averages in a specific league (e.g., American League, 2024).
SELECT name, team, value
FROM player_stats
WHERE statistic = 'Batting Average' AND year = 2024 AND league = 'American League'
ORDER BY CAST(value AS REAL) DESC
LIMIT 5;

-- Players with home runs and stolen bases in the same year.
SELECT p1.year, p1.league, p1.name, p1.team, p1.value AS home_runs, p2.value AS stolen_bases
FROM player_stats p1
JOIN player_stats p2
  ON p1.year = p2.year AND p1.league = p2.league AND p1.name = p2.name AND p1.team = p2.team
WHERE p1.statistic = 'Home Runs' AND p2.statistic = 'Stolen Bases'
  AND CAST(p1.value AS INTEGER) > 0 AND CAST(p2.value AS INTEGER) > 0
ORDER BY p1.year DESC, CAST(p1.value AS INTEGER) DESC
LIMIT 10;

-- Historical trends in a statistic (e.g., Home Runs).
SELECT year, AVG(CAST(value AS REAL)) AS avg_home_runs
FROM player_stats
WHERE statistic = 'Home Runs'
GROUP BY year
ORDER BY year;

-- Top 5 teams by winning percentage in a specific year (e.g., 2024).
SELECT team, league, division, wins, losses, winning_percentage
FROM team_standings
WHERE year = 2024
ORDER BY winning_percentage DESC
LIMIT 5;

-- Teams with consistent success (multiple years with high winning percentage).
SELECT team, COUNT(*) AS high_win_years
FROM team_standings
WHERE winning_percentage >= 0.600
GROUP BY team
ORDER BY high_win_years DESC
LIMIT 10;

-- Average games back by division.
SELECT league, division, AVG(CAST(games_back AS REAL)) AS avg_games_back
FROM team_standings
WHERE games_back NOT IN ('-', '') AND games_back IS NOT NULL
GROUP BY league, division
ORDER BY league, division;

-- Teams with both top pitchers and hitters in the same year.
SELECT DISTINCT p.year, p.league, p.team
FROM pitcher_stats p
JOIN player_stats b
  ON p.year = b.year AND p.league = b.league AND p.team = b.team
WHERE p.statistic IN ('ERA', 'Wins', 'Saves')
  AND b.statistic IN ('Batting Average', 'Home Runs', 'RBI')
ORDER BY p.year DESC, p.league, p.team
LIMIT 10;

-- Correlating team success with individual performance.
SELECT t.year, t.league, t.team, t.winning_percentage, COUNT(DISTINCT p.name) AS star_players
FROM team_standings t
LEFT JOIN (
  SELECT year, league, team, name FROM pitcher_stats
  UNION
  SELECT year, league, team, name FROM player_stats
) p ON t.year = p.year AND t.league = p.league AND t.team = p.team
WHERE t.winning_percentage >= 0.500
GROUP BY t.year, t.league, t.team
ORDER BY t.winning_percentage DESC, star_players DESC
LIMIT 10;

-- Check for missing or invalid values.
SELECT 'pitcher_stats' AS table_name, COUNT(*) AS null_values
FROM pitcher_stats
WHERE value IS NULL OR value = ''
UNION
SELECT 'player_stats', COUNT(*)
FROM player_stats
WHERE value IS NULL OR value = ''
UNION
SELECT 'team_standings', COUNT(*)
FROM team_standings
WHERE games_back IS NULL OR games_back = '';

-- Duplicate records check.
SELECT year, league, stat_type, statistic, name, team, COUNT(*) AS count
FROM pitcher_stats
GROUP BY year, league, stat_type, statistic, name, team
HAVING COUNT(*) > 1
UNION
SELECT year, league, stat_type, statistic, name, team, COUNT(*)
FROM player_stats
GROUP BY year, league, stat_type, statistic, name, team
HAVING COUNT(*) > 1;

-- List all tables.
SELECT name
FROM sqlite_master
WHERE type='table'
  AND name NOT LIKE 'sqlite_%';

-- Check each table column-level details.
PRAGMA table_info(pitcher_stats);
PRAGMA table_info(player_stats);
PRAGMA table_info(team_standings);

-- View original CREATE statements for tables
SELECT sql
FROM sqlite_schema
WHERE tbl_name='pitcher_stats' AND type='table';

SELECT sql
FROM sqlite_schema
WHERE tbl_name='player_stats' AND type='table';

SELECT sql
FROM sqlite_schema
WHERE tbl_name='team_standings' AND type='table';

-- Inspect indexes.
SELECT name, tbl_name, sql
FROM sqlite_schema
WHERE type='index' AND tbl_name='pitcher_stats';

SELECT name, tbl_name, sql
FROM sqlite_schema
WHERE type='index' AND tbl_name='player_stats';

SELECT name, tbl_name, sql
FROM sqlite_schema
WHERE type='index' AND tbl_name='team_standings';

-- List triggers and views.
SELECT name, type, sql
FROM sqlite_schema
WHERE type IN ('trigger', 'view');

-- Quick data sanity checks.
-- Get row counts
SELECT 'pitcher_stats' AS tbl, COUNT(*) FROM pitcher_stats
UNION ALL
SELECT 'player_stats' AS tbl, COUNT(*) FROM player_stats
UNION ALL
SELECT 'team_standings' AS tbl, COUNT(*) FROM team_standings;

-- Peek at sample data
SELECT * FROM pitcher_stats LIMIT 5;
SELECT * FROM player_stats LIMIT 5;
SELECT * FROM team_standings LIMIT 5;

-- Summary statistics
SELECT
  MIN(year), MAX(year) AS year_range,
  COUNT(DISTINCT league) AS num_leagues
FROM player_stats;

SELECT
  AVG(wins) AS avg_wins, AVG(losses) AS avg_losses
FROM team_standings;

-- Validate team names appearing across tables.
SELECT DISTINCT team
FROM pitcher_stats
EXCEPT
SELECT DISTINCT team
FROM team_standings;

-- Identify all the different types of batting statistics available in the player_stats table.
SELECT DISTINCT statistic FROM player_stats ORDER BY statistic;

-- Identifies all the different types of pitching statistics available in the pitcher_stats table.
SELECT DISTINCT statistic FROM pitcher_stats ORDER BY statistic;

-- Finds the earliest and latest year recorded in each table to understand the historical span of the dataset.
SELECT 'player_stats' as table_name, MIN(year) as earliest_year, MAX(year) as latest_year FROM player_stats
UNION ALL
SELECT 'pitcher_stats' as table_name, MIN(year) as earliest_year, MAX(year) as latest_year FROM pitcher_stats
UNION ALL
SELECT 'team_standings' as table_name, MIN(year) as earliest_year, MAX(year) as latest_year FROM team_standings;

-- Finds the all-time leaders for home runs in a single season.
SELECT year, name, team, CAST(value AS INTEGER) as home_runs
FROM player_stats
WHERE statistic = 'Home Runs'
ORDER BY home_runs DESC
LIMIT 10;

-- Finds the best single-season ERAs, casting value to REAL for floating-point sorting.
SELECT year, name, team, CAST(value AS REAL) as era
FROM pitcher_stats
WHERE statistic = 'ERA' AND era > 0
ORDER BY era ASC
LIMIT 10;

-- Shows the top 10 team performances by the number of wins.
SELECT year, team, league, wins, losses, winning_percentage
FROM team_standings
ORDER BY wins DESC
LIMIT 10;

-- Calculates total career home runs by grouping by player name.
SELECT
    name,
    SUM(CASE WHEN statistic = 'Home Runs' THEN CAST(value AS INTEGER) ELSE 0 END) as career_home_runs,
    MIN(year) as first_year,
    MAX(year) as last_year
FROM player_stats
WHERE statistic = 'Home Runs'
GROUP BY name
ORDER BY career_home_runs DESC
LIMIT 20;

-- Finds players who appear in the most distinct number of seasons.
SELECT
    name,
    COUNT(DISTINCT year) as seasons_played,
    MIN(year) as first_season,
    MAX(year) as last_season
FROM player_stats
GROUP BY name
ORDER BY seasons_played DESC
LIMIT 20;

-- Shows the total home runs for each decade to analyze historical trends.
SELECT
    (year / 10) * 10 || 's' as decade,
    SUM(CAST(value AS INTEGER)) as total_home_runs,
    ROUND(AVG(CAST(value AS REAL)), 1) as avg_hr_leader
FROM player_stats
WHERE statistic = 'Home Runs'
GROUP BY decade
ORDER BY decade;

-- Finds players who hit 40+ home runs while playing for a team that won at least 60% of its games.
SELECT
    ps.year,
    ps.name,
    ps.team,
    CAST(ps.value AS INTEGER) as home_runs,
    ts.winning_percentage
FROM player_stats as ps
JOIN team_standings as ts ON ps.year = ts.year AND ps.team = ts.team
WHERE
    ps.statistic = 'Home Runs'
    AND home_runs >= 40
    AND ts.winning_percentage >= 0.600
ORDER BY home_runs DESC;

-- Finds pitchers who had an excellent ERA (sub-2.50) while playing for a team with a losing record (winning percentage &lt; .500).
SELECT
    ps.year,
    ps.name,
    ps.team,
    CAST(ps.value AS REAL) as era,
    ts.winning_percentage
FROM pitcher_stats as ps
JOIN team_standings as ts ON ps.year = ts.year AND ps.team = ts.team
WHERE
    ps.statistic = 'ERA'
    AND era < 2.50
    AND ts.winning_percentage < 0.500
ORDER BY era ASC;

-- Calculates the standard deviation of winning percentages for each league each year. A lower number suggests a more balanced league.
SELECT
    year,
    league,
    COUNT(team) as teams,
    ROUND(AVG(winning_percentage), 3) as avg_win_pct,
    ROUND(STDDEV(winning_percentage), 4) as std_dev_win_pct
FROM team_standings
WHERE year >= 1950
GROUP BY year, league
ORDER BY year DESC, league;

-- Initial Data Quality and Overview Queries.
-- Understanding the temporal scope of our data
SELECT
    MIN(year) as earliest_year,
    MAX(year) as latest_year,
    COUNT(DISTINCT year) as total_years,
    COUNT(DISTINCT league) as total_leagues
FROM pitcher_stats

UNION ALL

SELECT
    MIN(year) as earliest_year,
    MAX(year) as latest_year,
    COUNT(DISTINCT year) as total_years,
    COUNT(DISTINCT league) as total_leagues
FROM player_stats

UNION ALL

SELECT
    MIN(year) as earliest_year,
    MAX(year) as latest_year,
    COUNT(DISTINCT year) as total_years,
    COUNT(DISTINCT league) as total_leagues
FROM team_standings;

-- Checking for data gaps or missing years
WITH all_years AS (
    -- First, gather all years that actually exist in the database
    SELECT DISTINCT year FROM pitcher_stats
    UNION
    SELECT DISTINCT year FROM player_stats
    UNION
    SELECT DISTINCT year FROM team_standings
),
year_range AS (
    -- Then, find the minimum and maximum of those existing years
    SELECT
        MIN(year) as min_year,
        MAX(year) as max_year
    FROM all_years
),
-- FIX: Use a recursive CTE to generate a complete, continuous sequence of years.
-- This is the standard SQL approach for generating a series.
expected_years(year) AS (
    -- This is the "Anchor": the starting point of the sequence.
    SELECT min_year FROM year_range
    UNION ALL
    -- This is the "Recursive" part: it takes the last year and adds 1,
    -- continuing until it reaches the max_year.
    SELECT year + 1 FROM expected_years
    WHERE year < (SELECT max_year FROM year_range)
)
-- Finally, find which of the expected years are missing from the actual years.
SELECT e.year as missing_year
FROM expected_years e
LEFT JOIN all_years a ON e.year = a.year
WHERE a.year IS NULL
ORDER BY e.year;

-- Understanding Data Distribution and Patterns.
-- Analyzing the types of statistics tracked over time.
SELECT
    stat_type,
    statistic,
    COUNT(DISTINCT year) as years_tracked,
    MIN(year) as first_year,
    MAX(year) as last_year,
    COUNT(*) as total_records
FROM pitcher_stats
GROUP BY stat_type, statistic
ORDER BY years_tracked DESC, statistic;

-- Examining league evolution and expansion.
SELECT
    year,
    league,
    COUNT(DISTINCT team) as team_count,
    COUNT(DISTINCT division) as division_count
FROM team_standings
GROUP BY year, league
ORDER BY year, league;

-- Advanced Statistical Analysis.
-- Finding dominant pitchers across eras (normalized by era).
WITH era_stats AS (
    SELECT
        year,
        league,
        name,
        team,
        CAST(value AS REAL) as era_value
    FROM pitcher_stats
    WHERE statistic = 'ERA'
      AND value NOT LIKE '%-%'  -- Excluding non-numeric values
      AND CAST(value AS REAL) > 0
),
era_rankings AS (
    SELECT
        year,
        league,
        name,
        team,
        era_value,
        RANK() OVER (PARTITION BY year, league ORDER BY era_value) as era_rank,
        AVG(era_value) OVER (PARTITION BY year, league) as league_avg_era
    FROM era_stats
)
SELECT
    year,
    league,
    name,
    team,
    era_value,
    ROUND(league_avg_era, 3) as league_avg,
    ROUND(era_value / league_avg_era, 3) as era_plus_inverse
FROM era_rankings
WHERE era_rank = 1
ORDER BY year, league;

-- Analyzing home run trends over decades.
WITH hr_data AS (
    SELECT
        year,
        league,
        name,
        team,
        CAST(value AS INTEGER) as home_runs
    FROM player_stats
    WHERE statistic = 'Home Runs'
      AND value NOT LIKE '%-%'
),
decade_analysis AS (
    SELECT
        (year / 10) * 10 as decade,
        league,
        SUM(home_runs) as total_hrs,
        AVG(home_runs) as avg_hrs_per_player,
        MAX(home_runs) as max_hrs,
        COUNT(DISTINCT name) as total_players
    FROM hr_data
    GROUP BY decade, league
)
SELECT
    decade,
    league,
    total_hrs,
    ROUND(avg_hrs_per_player, 2) as avg_hrs,
    max_hrs,
    total_players,
    ROUND(CAST(total_hrs AS REAL) / total_players, 2) as hrs_per_player
FROM decade_analysis
ORDER BY decade, league;

-- Cross-Table Relationship Analysis.
-- Correlating team success with individual performances.
WITH team_wins AS (
    SELECT
        year,
        league,
        team,
        wins,
        winning_percentage
    FROM team_standings
    WHERE games_back NOT LIKE '%.%'  -- Focusing on division winners
),
team_batting_leaders AS (
    SELECT
        p.year,
        p.league,
        p.team,
        COUNT(DISTINCT CASE WHEN p.statistic = 'Batting Average' THEN p.name END) as batting_leaders,
        COUNT(DISTINCT CASE WHEN p.statistic = 'Home Runs' THEN p.name END) as hr_leaders,
        COUNT(DISTINCT CASE WHEN p.statistic = 'RBIs' THEN p.name END) as rbi_leaders
    FROM player_stats p
    INNER JOIN (
        SELECT year, league, statistic, MAX(CAST(value AS REAL)) as max_value
        FROM player_stats
        WHERE value NOT LIKE '%-%'
        GROUP BY year, league, statistic
    ) max_stats ON p.year = max_stats.year
                AND p.league = max_stats.league
                AND p.statistic = max_stats.statistic
                AND CAST(p.value AS REAL) = max_stats.max_value
    GROUP BY p.year, p.league, p.team
)
SELECT
    tw.year,
    tw.league,
    tw.team,
    tw.wins,
    tw.winning_percentage,
    COALESCE(tbl.batting_leaders, 0) as batting_champs,
    COALESCE(tbl.hr_leaders, 0) as hr_champs,
    COALESCE(tbl.rbi_leaders, 0) as rbi_champs,
    (COALESCE(tbl.batting_leaders, 0) +
     COALESCE(tbl.hr_leaders, 0) +
     COALESCE(tbl.rbi_leaders, 0)) as total_offensive_leaders
FROM team_wins tw
LEFT JOIN team_batting_leaders tbl
    ON tw.year = tbl.year
    AND tw.league = tbl.league
    AND tw.team = tbl.team
WHERE tw.winning_percentage > 0.600
ORDER BY total_offensive_leaders DESC, tw.wins DESC
LIMIT 25;

-- Data Integrity and Anomaly Detection.
-- Finding potential data quality issues.
SELECT
    'pitcher_stats' as table_name,
    year,
    league,
    statistic,
    name,
    team,
    value,
    'Non-numeric value' as issue_type
FROM pitcher_stats
WHERE statistic IN ('ERA', 'Wins', 'Strikeouts', 'Saves')
  AND (value LIKE '%[a-zA-Z]%' OR value LIKE '%-%' OR value = '')

UNION ALL

SELECT
    'player_stats' as table_name,
    year,
    league,
    statistic,
    name,
    team,
    value,
    'Suspicious value' as issue_type
FROM player_stats
WHERE (statistic = 'Batting Average' AND CAST(value AS REAL) > 1.0)
   OR (statistic = 'Home Runs' AND CAST(value AS INTEGER) > 100)
   OR (statistic = 'Stolen Bases' AND CAST(value AS INTEGER) > 200);

-- Historical Record Analysis.
-- Finding all-time records for key statistics
WITH numeric_pitcher_stats AS (
    SELECT
        year,
        league,
        statistic,
        name,
        team,
        value,
        CASE
            WHEN statistic = 'ERA' THEN CAST(value AS REAL)
            ELSE CAST(value AS INTEGER)
        END as numeric_value
    FROM pitcher_stats
    WHERE value NOT LIKE '%-%'
      AND value != ''
      AND statistic IN ('Wins', 'Strikeouts', 'ERA', 'Saves', 'Complete Games')
),
record_holders AS (
    SELECT
        statistic,
        name,
        team,
        year,
        numeric_value,
        RANK() OVER (PARTITION BY statistic
                     ORDER BY
                        CASE WHEN statistic = 'ERA' THEN numeric_value END ASC,
                        CASE WHEN statistic != 'ERA' THEN numeric_value END DESC
                    ) as record_rank
    FROM numeric_pitcher_stats
    WHERE (statistic != 'ERA' OR numeric_value > 0)  -- Exclude ERA of 0
)
SELECT
    statistic,
    name,
    team,
    year,
    numeric_value as record_value
FROM record_holders
WHERE record_rank = 1
ORDER BY statistic;

-- 1. Get a quick overview of the data range in each table.
SELECT 'player_stats' as table_name,
       MIN(year) as earliest_year,
       MAX(year) as latest_year,
       COUNT(DISTINCT year) as total_years,
       COUNT(DISTINCT name) as unique_players,
       COUNT(*) as total_records
FROM player_stats
UNION ALL
SELECT 'pitcher_stats',
       MIN(year),
       MAX(year),
       COUNT(DISTINCT year),
       COUNT(DISTINCT name),
       COUNT(*)
FROM pitcher_stats
UNION ALL
SELECT 'team_standings',
       MIN(year),
       MAX(year),
       COUNT(DISTINCT year),
       COUNT(DISTINCT team),
       COUNT(*)
FROM team_standings;

-- List all available player statistics with their frequency.
SELECT statistic,
       COUNT(*) as occurrences,
       COUNT(DISTINCT year) as years_tracked,
       MIN(year) as first_year,
       MAX(year) as last_year
FROM player_stats
GROUP BY statistic
ORDER BY occurrences DESC;

SELECT statistic,
       COUNT(*) as occurrences,
       COUNT(DISTINCT year) as years_tracked,
       MIN(year) as first_year,
       MAX(year) as last_year
FROM pitcher_stats
GROUP BY statistic
ORDER BY occurrences DESC;

-- Basic Statistical Queries.
-- Find the highest single-season home run totals.
SELECT year, name, team, value as home_runs
FROM player_stats
WHERE statistic = 'Home Runs'
ORDER BY CAST(value AS INTEGER) DESC
LIMIT 10;

-- Find the lowest ERAs in history (minimum would typically be applied)
SELECT year, name, team, value as ERA
FROM pitcher_stats
WHERE statistic = 'ERA'
  AND CAST(value AS REAL) > 0  -- Exclude any zero or invalid values
ORDER BY CAST(value AS REAL) ASC
LIMIT 10;

-- Track home run trends by decade
SELECT
    (year / 10) * 10 as decade,
    COUNT(DISTINCT name) as players_with_hrs,
    SUM(CAST(value AS INTEGER)) as total_home_runs,
    ROUND(AVG(CAST(value AS REAL)), 2) as avg_home_runs,
    MAX(CAST(value AS INTEGER)) as max_home_runs
FROM player_stats
WHERE statistic = 'Home Runs'
GROUP BY decade
ORDER BY decade;

-- Evolution of pitching - ERA by decade
SELECT
    (year / 10) * 10 as decade,
    COUNT(DISTINCT name) as pitchers,
    ROUND(AVG(CAST(value AS REAL)), 3) as avg_era,
    ROUND(MIN(CAST(value AS REAL)), 3) as best_era
FROM pitcher_stats
WHERE statistic = 'ERA'
  AND CAST(value AS REAL) > 0
GROUP BY decade
ORDER BY decade;

-- Compare offensive production between leagues
SELECT
    year,
    league,
    statistic,
    COUNT(*) as player_count,
    SUM(CAST(value AS INTEGER)) as total,
    ROUND(AVG(CAST(value AS REAL)), 2) as average
FROM player_stats
WHERE statistic IN ('Home Runs', 'RBI', 'Hits')
  AND year >= 2000  -- Modern era comparison
GROUP BY year, league, statistic
ORDER BY year DESC, statistic, league;

-- Which league has been more competitive (closer standings)?
SELECT
    year,
    league,
    ROUND(AVG(winning_percentage), 3) as avg_win_pct,
    ROUND(STDDEV(winning_percentage), 3) as win_pct_stddev,
    MAX(wins) - MIN(wins) as win_spread
FROM team_standings
WHERE year >= 2000
GROUP BY year, league
ORDER BY year DESC, league;

-- Find players with the longest careers
SELECT
    name,
    COUNT(DISTINCT year) as seasons_played,
    MIN(year) as first_season,
    MAX(year) as last_season,
    COUNT(DISTINCT team) as teams_played_for
FROM player_stats
GROUP BY name
HAVING COUNT(DISTINCT year) >= 15
ORDER BY seasons_played DESC, first_season;

-- Career totals for counting stats (complex but powerful)
WITH career_totals AS (
    SELECT
        name,
        SUM(CASE WHEN statistic = 'Home Runs' THEN CAST(value AS INTEGER) ELSE 0 END) as career_hr,
        SUM(CASE WHEN statistic = 'RBI' THEN CAST(value AS INTEGER) ELSE 0 END) as career_rbi,
        SUM(CASE WHEN statistic = 'Hits' THEN CAST(value AS INTEGER) ELSE 0 END) as career_hits,
        COUNT(DISTINCT year) as seasons
    FROM player_stats
    WHERE statistic IN ('Home Runs', 'RBI', 'Hits')
    GROUP BY name
)
SELECT * FROM career_totals
WHERE career_hr > 300  -- 300+ home run club
ORDER BY career_hr DESC;

-- Find team dynasties (consecutive winning seasons)
WITH team_streaks AS (
    SELECT
        team,
        year,
        wins,
        losses,
        winning_percentage,
        CASE WHEN winning_percentage > 0.500 THEN 1 ELSE 0 END as winning_season,
        year - ROW_NUMBER() OVER (PARTITION BY team, CASE WHEN winning_percentage > 0.500 THEN 1 ELSE 0 END ORDER BY year) as streak_group
    FROM team_standings
)
SELECT
    team,
    MIN(year) as streak_start,
    MAX(year) as streak_end,
    COUNT(*) as consecutive_winning_seasons,
    ROUND(AVG(winning_percentage), 3) as avg_win_pct,
    SUM(wins) as total_wins
FROM team_streaks
WHERE winning_season = 1
GROUP BY team, streak_group
HAVING COUNT(*) >= 5  -- At least 5 consecutive winning seasons
ORDER BY consecutive_winning_seasons DESC, avg_win_pct DESC;

-- Cross-Table Analysis.
-- Best player seasons by team success.
SELECT
    ps.year,
    ps.name,
    ps.team,
    ps.value as home_runs,
    ts.wins,
    ts.winning_percentage,
    ts.division
FROM player_stats ps
JOIN team_standings ts ON ps.year = ts.year AND ps.team = ts.team
WHERE ps.statistic = 'Home Runs'
  AND ts.winning_percentage >= 0.600  -- Only successful teams
  AND CAST(ps.value AS INTEGER) >= 40  -- 40+ HR seasons
ORDER BY ps.year DESC, CAST(ps.value AS INTEGER) DESC;

-- Pitcher performance on winning vs losing teams.
SELECT
    CASE
        WHEN ts.winning_percentage >= 0.500 THEN 'Winning Team'
        ELSE 'Losing Team'
    END as team_type,
    COUNT(DISTINCT ps.name) as pitcher_count,
    ROUND(AVG(CAST(ps.value AS REAL)), 3) as avg_era
FROM pitcher_stats ps
JOIN team_standings ts ON ps.year = ts.year AND ps.team = ts.team
WHERE ps.statistic = 'ERA'
  AND CAST(ps.value AS REAL) BETWEEN 0.5 AND 10  -- Reasonable ERA range
GROUP BY team_type;

-- Find statistical anomalies (outliers).
WITH stats_with_zscore AS (
    SELECT
        year,
        name,
        team,
        statistic,
        CAST(value AS REAL) as numeric_value,
        AVG(CAST(value AS REAL)) OVER (PARTITION BY statistic) as avg_value,
        STDDEV(CAST(value AS REAL)) OVER (PARTITION BY statistic) as stddev_value
    FROM player_stats
    WHERE statistic = 'Batting Average'
      AND value NOT LIKE '.%'  -- Exclude malformed data
)
SELECT
    year,
    name,
    team,
    numeric_value as batting_avg,
    ROUND((numeric_value - avg_value) / NULLIF(stddev_value, 0), 2) as z_score
FROM stats_with_zscore
WHERE ABS((numeric_value - avg_value) / NULLIF(stddev_value, 0)) > 3  -- 3+ standard deviations
ORDER BY z_score DESC;

-- Year-over-year improvement leaders.
WITH yoy_comparison AS (
    SELECT
        p1.name,
        p1.year as year1,
        p2.year as year2,
        p1.value as value1,
        p2.value as value2,
        CAST(p2.value AS INTEGER) - CAST(p1.value AS INTEGER) as improvement
    FROM player_stats p1
    JOIN player_stats p2 ON p1.name = p2.name
                        AND p1.statistic = p2.statistic
                        AND p2.year = p1.year + 1
    WHERE p1.statistic = 'Home Runs'
)
SELECT * FROM yoy_comparison
WHERE improvement > 20  -- 20+ HR improvement
ORDER BY improvement DESC
LIMIT 20;

-- Data Quality Checks.
-- Check for data anomalies.
SELECT
    'Suspicious batting averages' as issue,
    year, name, team, value
FROM player_stats
WHERE statistic = 'Batting Average'
  AND (CAST(value AS REAL) > 1.0 OR value LIKE '%[a-zA-Z]%');

-- Find duplicate entries.
SELECT
    year, name, team, statistic, COUNT(*) as duplicate_count
FROM player_stats
GROUP BY year, name, team, statistic
HAVING COUNT(*) > 1;

-- Calculate career totals for specific statistics for each player and filter that list to find elite home run hitters.
WITH career_totals AS (
    SELECT
        name,
        SUM(CASE WHEN statistic = 'Home Runs' THEN CAST(value AS INTEGER) ELSE 0 END) as career_hr,
        SUM(CASE WHEN statistic = 'RBI' THEN CAST(value AS INTEGER) ELSE 0 END) as career_rbi,
        SUM(CASE WHEN statistic = 'Hits' THEN CAST(value AS INTEGER) ELSE 0 END) as career_hits,
        COUNT(DISTINCT year) as seasons
    FROM player_stats
    WHERE statistic IN ('Home Runs', 'RBI', 'Hits')
    GROUP BY name
)
SELECT * FROM career_totals
WHERE career_hr > 300  -- 300+ home run club
ORDER BY career_hr DESC;

-- Identifies teams with consecutive winning seasons in the team_standings table.
WITH team_streaks AS (
    SELECT
        team,
        year,
        wins,
        losses,
        winning_percentage,
        CASE WHEN winning_percentage > 0.500 THEN 1 ELSE 0 END as winning_season,
        year - ROW_NUMBER() OVER (PARTITION BY team, CASE WHEN winning_percentage > 0.500 THEN 1 ELSE 0 END ORDER BY year) as streak_group
    FROM team_standings
)
SELECT
    team,
    MIN(year) as streak_start,
    MAX(year) as streak_end,
    COUNT(*) as consecutive_winning_seasons,
    ROUND(AVG(winning_percentage), 3) as avg_win_pct,
    SUM(wins) as total_wins
FROM team_streaks
WHERE winning_season = 1
GROUP BY team, streak_group
HAVING COUNT(*) >= 5
ORDER BY consecutive_winning_seasons DESC, avg_win_pct DESC;

-- Analyzes team standings data in the team_standings table, focusing on the years 2000 and later.
SELECT
    -- FIX: Added the 'ts' alias to specify which 'year' and 'league' to use.
    ts.year,
    ts.league,
    COUNT(DISTINCT ts.team) as teams,
    -- FIX: Prefixed all ambiguous columns with the 'ts.' alias.
    ROUND(AVG(ts.winning_percentage), 3) as avg_win_pct,
    ROUND(MIN(ts.winning_percentage), 3) as worst_team,
    ROUND(MAX(ts.winning_percentage), 3) as best_team,
    ROUND(MAX(ts.winning_percentage) - MIN(ts.winning_percentage), 3) as win_pct_spread,
    -- Manual standard deviation calculation
    ROUND(SQRT(AVG((ts.winning_percentage - sub.league_avg) * (ts.winning_percentage - sub.league_avg))), 3) as win_pct_stddev
FROM team_standings ts
JOIN (
    SELECT year, league, AVG(winning_percentage) as league_avg
    FROM team_standings
    WHERE year >= 2000
    GROUP BY year, league
) sub ON ts.year = sub.year AND ts.league = sub.league
WHERE ts.year >= 2000
GROUP BY ts.year, ts.league
ORDER BY ts.year DESC, ts.league;

-- Calculates the Z-score for players' batting averages in the player_stats table and identifies outliers with a Z-score greater than 3 or less than -3.
-- Step 1: Calculate the average value for the statistic first.
WITH stats_with_avg AS (
    SELECT
        year,
        name,
        team,
        statistic,
        CAST(value AS REAL) as numeric_value,
        AVG(CAST(value AS REAL)) OVER (PARTITION BY statistic) as avg_value
    FROM player_stats
    WHERE statistic = 'Batting Average' AND value NOT LIKE '.%'
),
-- Step 2: Use the calculated average to find the standard deviation.
-- There is no nesting of window functions here.
stats_with_stddev AS (
    SELECT
        *, -- Select all columns from the previous CTE
        SQRT(AVG(POWER(numeric_value - avg_value, 2)) OVER (PARTITION BY statistic)) as stddev_value
    FROM stats_with_avg
)
-- Step 3: Now that all components are calculated, compute the Z-score and filter.
SELECT
    year,
    name,
    team,
    numeric_value as batting_avg,
    ROUND((numeric_value - avg_value) / NULLIF(stddev_value, 0), 2) as z_score
FROM stats_with_stddev
WHERE ABS((numeric_value - avg_value) / NULLIF(stddev_value, 0)) > 3
ORDER BY z_score DESC;

-- Analyzes team standings data in the team_standings table, focusing on the years 2000 and later.
SELECT
    ts.year,
    ts.league,
    COUNT(DISTINCT ts.team) as teams,
    ROUND(AVG(ts.winning_percentage), 3) as avg_win_pct,
    ROUND(MIN(ts.winning_percentage), 3) as worst_team,
    ROUND(MAX(ts.winning_percentage), 3) as best_team,
    ROUND(MAX(ts.winning_percentage) - MIN(ts.winning_percentage), 3) as win_pct_spread,
    -- Manual variance calculation (standard deviation squared)
    ROUND(AVG((ts.winning_percentage - sub.league_avg) *
              (ts.winning_percentage - sub.league_avg)) * 1000, 1) as variance_x1000
FROM team_standings ts
JOIN (
    SELECT year, league, AVG(winning_percentage) as league_avg
    FROM team_standings
    WHERE year >= 2000
    GROUP BY year, league
) sub ON ts.year = sub.year AND ts.league = sub.league
WHERE ts.year >= 2000
GROUP BY ts.year, ts.league
ORDER BY ts.year DESC, ts.league;

-- Ccalculates the batting averages of players in the player_stats table and identifies outliers based on their deviation from the mean batting average.
-- Step 1: Get the raw batting averages (your original CTE was correct)
WITH batting_stats AS (
    SELECT
        year,
        name,
        team,
        CAST(value AS REAL) as batting_avg
    FROM player_stats
    WHERE statistic = 'Batting Average'
      AND value NOT LIKE '.%'
      AND CAST(value AS REAL) > 0
      AND CAST(value AS REAL) <= 1.0
),
-- Step 2: Calculate the overall mean (average). This isolates the first window function.
stats_with_mean AS (
    SELECT
        year,
        name,
        team,
        batting_avg,
        AVG(batting_avg) OVER() as mean_avg
    FROM batting_stats
),
-- Step 3: Use the calculated mean to find the variance. This isolates the second window function.
stats_with_variance AS (
    SELECT
        year,
        name,
        team,
        batting_avg,
        mean_avg,
        AVG(POWER(batting_avg - mean_avg, 2)) OVER() as variance
    FROM stats_with_mean
)
-- Final Step: Now that all components are calculated, compute the deviation and filter.
SELECT
    year,
    name,
    team,
    ROUND(batting_avg, 3) as batting_avg,
    -- Show how many variances away from mean (squared z-score)
    ROUND(POWER(batting_avg - mean_avg, 2) / variance, 2) as deviation_squared
FROM stats_with_variance
WHERE POWER(batting_avg - mean_avg, 2) / variance > 6.25  -- (2.5)^2
ORDER BY batting_avg DESC;

-- Find outliers using range-based detection instead of standard deviation
WITH batting_stats AS (
    SELECT
        year, name, team,
        CAST(value AS REAL) as batting_avg
    FROM player_stats
    WHERE statistic = 'Batting Average'
      AND CAST(value AS REAL) BETWEEN 0.001 AND 1.0
),
percentiles AS (
    SELECT
        year, name, team, batting_avg,
        -- Calculate position in distribution
        PERCENT_RANK() OVER (ORDER BY batting_avg) as pct_rank,
        -- Get quartiles for comparison
        (SELECT batting_avg FROM batting_stats
         ORDER BY batting_avg
         -- FIX: Cast the result of the calculation to an INTEGER for OFFSET
         LIMIT 1 OFFSET CAST((SELECT COUNT(*) FROM batting_stats) * 0.25 AS INTEGER)) as q1,
        (SELECT batting_avg FROM batting_stats
         ORDER BY batting_avg
         -- FIX: Cast the result of the calculation to an INTEGER for OFFSET
         LIMIT 1 OFFSET CAST((SELECT COUNT(*) FROM batting_stats) * 0.75 AS INTEGER)) as q3
    FROM batting_stats
)
SELECT year, name, team,
       ROUND(batting_avg, 3) as batting_avg,
       CASE
           WHEN pct_rank >= 0.99 THEN 'Top 1%'
           WHEN pct_rank >= 0.95 THEN 'Top 5%'
           WHEN pct_rank <= 0.01 THEN 'Bottom 1%'
           WHEN pct_rank <= 0.05 THEN 'Bottom 5%'
       END as percentile_group
FROM percentiles
WHERE pct_rank >= 0.95 OR pct_rank <= 0.05
ORDER BY batting_avg DESC;