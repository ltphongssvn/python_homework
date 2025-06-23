#!/usr/bin/env python3
# dash_dashboard_1876_2024_v3_production.py
"""
Baseball History Dashboard - Production Ready Version

This module creates an interactive dashboard for exploring baseball statistics
from 1876 to 2025, including team standings, player statistics,
and pitcher data. Uses Dash and Plotly for visualization of historical baseball data.

Key improvements in this version:
- Comprehensive error handling and logging
- Fixed Plotly template issues
- Added health checks and monitoring
- Improved performance and stability
- Better user experience with loading states
"""

import logging
import sqlite3
import sys
import traceback
from functools import wraps
from pathlib import Path

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output, State, dash_table, dcc, html

# ============================================================================
# CONFIGURATION AND SETUP
# ============================================================================

# Configure logging for production debugging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # Uncomment for file logging:
        # logging.FileHandler('dashboard.log')
    ],
)
logger = logging.getLogger(__name__)

# Debug mode flag - set to False in production
DEBUG_MODE = False

# Initialize the Dash app with production settings
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,  # Prevents errors from dynamic layouts
    serve_locally=True,  # Serve assets locally to avoid CDN issues
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"}
    ],
)

app.title = "Baseball History Dashboard"

# Custom CSS for consistent styling
custom_style = {
    "backgroundColor": "#f8f9fa",
    "fontFamily": "Arial, sans-serif",
}

# ============================================================================
# ERROR HANDLING UTILITIES
# ============================================================================


def safe_callback(func):
    """
    Decorator to safely handle callback errors.

    This wrapper catches any exceptions that occur in callbacks and returns
    appropriate error visualizations instead of crashing the app.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Callback error in {func.__name__}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

            # Create an error figure that shows in the UI
            fig = go.Figure()

            if DEBUG_MODE:
                # Detailed error for development
                error_text = f"Error in {func.__name__}:<br>{str(e)}"
                fig.add_annotation(
                    text=error_text,
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(color="red", size=12),
                    align="center",
                )
            else:
                # User-friendly message for production
                fig.add_annotation(
                    text="Unable to load data. Please try refreshing the page.",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(color="#666", size=14),
                    align="center",
                )

            fig.update_layout(
                showlegend=False,
                xaxis=dict(visible=False),
                yaxis=dict(visible=False),
                template="plotly_white",
                height=400,
            )

            return fig

    return wrapper


# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================


class Config:
    """Centralized configuration for database paths and settings"""

    DATABASE_NAME = "baseball_history.db"

    @classmethod
    def get_database_path(cls):
        """
        Find the database file in multiple possible locations.

        Returns:
            Path: Path to the database file

        Raises:
            FileNotFoundError: If database cannot be found in any location
        """
        # List of paths to check, in order of preference
        possible_paths = [
            # Parent directory (when running from dashboards subdirectory)
            Path(__file__).parent.parent / cls.DATABASE_NAME,
            # Current directory
            Path(__file__).parent / cls.DATABASE_NAME,
            # Absolute path as fallback
            Path(r"C:\Users\LENOVO\python_homework\assignment14")
            / cls.DATABASE_NAME,
        ]

        # Check each path and return the first one that exists
        for db_path in possible_paths:
            if db_path.exists():
                logger.info(f"Found database at: {db_path}")
                return db_path

        # If no path works, provide helpful error message
        searched_paths = "\n".join(f"  - {p}" for p in possible_paths)
        error_msg = (
            f"Could not find '{cls.DATABASE_NAME}'\n"
            f"Searched in these locations:\n{searched_paths}\n"
            f"Current working directory: {Path.cwd()}\n"
            f"Script location: {Path(__file__).parent}"
        )
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)


# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================


def load_team_standings():
    """
    Load team standings data with comprehensive error handling.

    Returns:
        pd.DataFrame: Team standings data or empty DataFrame on error
    """
    try:
        db_path = Config.get_database_path()

        with sqlite3.connect(str(db_path)) as conn:
            # First verify the table exists
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='team_standings';"
            )

            if not cursor.fetchone():
                raise ValueError(
                    "Table 'team_standings' not found in database"
                )

            # Load the data
            query = """
            SELECT year, league, division, team, wins, losses,
                   winning_percentage, games_back
            FROM team_standings
            ORDER BY year DESC, winning_percentage DESC
            """
            df = pd.read_sql_query(query, conn)

        # Data cleaning and enhancement
        if "games_back" in df.columns:
            # Extract numeric values from games_back
            df["games_back_numeric"] = pd.to_numeric(
                df["games_back"].str.extract(r"(\d+\.?\d*)")[0],
                errors="coerce",
            )

        # Add calculated fields
        df["total_games"] = df["wins"] + df["losses"]

        logger.info(f"Successfully loaded {len(df)} team standings records")
        return df

    except Exception as e:
        logger.error(f"Error loading team standings: {str(e)}")
        # Return empty DataFrame with expected columns
        return pd.DataFrame(
            columns=[
                "year",
                "league",
                "division",
                "team",
                "wins",
                "losses",
                "winning_percentage",
                "games_back",
                "games_back_numeric",
                "total_games",
            ]
        )


def load_player_stats():
    """
    Load player statistics data with error handling.

    Returns:
        pd.DataFrame: Player statistics or empty DataFrame on error
    """
    try:
        db_path = Config.get_database_path()

        with sqlite3.connect(str(db_path)) as conn:
            # Verify table exists
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='player_stats';"
            )
            if not cursor.fetchone():
                raise ValueError("Table 'player_stats' not found in database")

            # Load common batting statistics
            query = """
            SELECT year, league, name, team, statistic, value
            FROM player_stats
            WHERE statistic IN ('Batting Average', 'Home Runs', 'Hits', 'RBI',
                              'Stolen Bases', 'Doubles', 'Triples')
            ORDER BY year DESC
            """
            df = pd.read_sql_query(query, conn)

        logger.info(f"Successfully loaded {len(df)} player stat records")
        return df

    except Exception as e:
        logger.error(f"Error loading player stats: {str(e)}")
        return pd.DataFrame(
            columns=["year", "league", "name", "team", "statistic", "value"]
        )


def load_pitcher_stats():
    """
    Load pitcher statistics data with error handling.

    Returns:
        pd.DataFrame: Pitcher statistics or empty DataFrame on error
    """
    try:
        db_path = Config.get_database_path()

        with sqlite3.connect(str(db_path)) as conn:
            # Verify table exists
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='pitcher_stats';"
            )
            if not cursor.fetchone():
                raise ValueError("Table 'pitcher_stats' not found in database")

            # Load common pitching statistics
            query = """
            SELECT year, league, name, team, statistic, value
            FROM pitcher_stats
            WHERE statistic IN ('ERA', 'Wins', 'Strikeouts', 'Complete Games',
                              'Saves', 'Shutouts', 'Games')
            ORDER BY year DESC
            """
            df = pd.read_sql_query(query, conn)

        logger.info(f"Successfully loaded {len(df)} pitcher stat records")
        return df

    except Exception as e:
        logger.error(f"Error loading pitcher stats: {str(e)}")
        return pd.DataFrame(
            columns=["year", "league", "name", "team", "statistic", "value"]
        )


# ============================================================================
# LOAD DATA AT STARTUP
# ============================================================================

print("=" * 80)
print("BASEBALL HISTORY DASHBOARD - STARTUP")
print("=" * 80)
print(f"Python version: {sys.version}")
print(f"Dash version: {dash.__version__}")
print(f"Plotly version: {plotly.__version__}")
print(f"Script location: {Path(__file__).parent}")
print(f"Current working directory: {Path.cwd()}")
print("-" * 80)

# Load all data
standings_df = load_team_standings()
player_df = load_player_stats()
pitcher_df = load_pitcher_stats()

# Check if we have any data
data_available = not standings_df.empty

if data_available:
    print("Data loading complete!")
    print(
        f"Total records loaded: {len(standings_df) + len(player_df) + len(pitcher_df):,}"
    )
else:
    print("WARNING: No data loaded. Dashboard will show error message.")
print("=" * 80)

# ============================================================================
# VISUALIZATION FUNCTIONS (WITH FIXES)
# ============================================================================


def create_team_performance_timeline(df, year_range, selected_teams):
    """Create time series visualization of team performance"""
    if df.empty:
        return create_empty_figure("No team standings data available")

    # Apply filters
    filtered_df = df[
        (df["year"] >= year_range[0]) & (df["year"] <= year_range[1])
    ]

    if selected_teams:
        filtered_df = filtered_df[filtered_df["team"].isin(selected_teams)]

    if filtered_df.empty:
        return create_empty_figure("No data matches your selection")

    # Create line chart
    fig = px.line(
        filtered_df,
        x="year",
        y="winning_percentage",
        color="team",
        title="Team Winning Percentage Over Time",
        markers=True,
        hover_data=["wins", "losses", "league"],
        template="plotly_white",  # Use safe built-in template
    )

    fig.update_layout(
        height=400, title_x=0.5, yaxis_tickformat=".1%", hovermode="x unified"
    )

    return fig


def create_wins_losses_scatter(df, year_range):
    """Create scatter plot of wins vs losses"""
    if df.empty:
        return create_empty_figure("No team standings data available")

    # Filter by year range
    filtered_df = df[
        (df["year"] >= year_range[0]) & (df["year"] <= year_range[1])
    ]

    if filtered_df.empty:
        return create_empty_figure("No data for selected years")

    # Create scatter plot
    fig = px.scatter(
        filtered_df,
        x="wins",
        y="losses",
        color="league",
        size="total_games",
        title="Team Wins vs Losses",
        hover_data=["team", "year", "winning_percentage"],
        labels={"wins": "Wins", "losses": "Losses"},
        template="plotly_white",
    )

    # Add diagonal line for .500 winning percentage
    max_games = (
        filtered_df["total_games"].max() if not filtered_df.empty else 162
    )
    fig.add_trace(
        go.Scatter(
            x=[0, max_games / 2],
            y=[max_games / 2, 0],
            mode="lines",
            name=".500 Line",
            line=dict(dash="dash", color="gray"),
            showlegend=True,
        )
    )

    fig.update_layout(height=400, title_x=0.5)

    return fig


def create_league_comparison(df, metric, year_range):
    """Create comparison between leagues"""
    if df.empty or metric not in ["wins", "losses", "winning_percentage"]:
        return create_empty_figure("Cannot create league comparison")

    # Filter and aggregate data
    filtered_df = df[
        (df["year"] >= year_range[0]) & (df["year"] <= year_range[1])
    ]

    if metric == "winning_percentage":
        league_stats = (
            filtered_df.groupby(["year", "league"])[metric]
            .mean()
            .reset_index()
        )
    else:
        league_stats = (
            filtered_df.groupby(["year", "league"])[metric].sum().reset_index()
        )

    # Create grouped bar chart
    fig = px.bar(
        league_stats,
        x="year",
        y=metric,
        color="league",
        title=f'{metric.replace("_", " ").title()} by League Over Time',
        barmode="group",
        template="plotly_white",
    )

    fig.update_layout(height=400, title_x=0.5)

    if metric == "winning_percentage":
        fig.update_yaxis(tickformat=".1%")

    return fig


def create_player_leaders_chart(player_df, year, statistic):
    """
    Show top players for a given statistic.

    This is the FIXED version that avoids Plotly template issues.
    """
    if player_df.empty:
        return create_empty_figure("No player statistics available")

    # Filter for specific year and statistic
    filtered_df = player_df[
        (player_df["year"] == year) & (player_df["statistic"] == statistic)
    ].copy()

    if filtered_df.empty:
        return create_empty_figure(f"No {statistic} data for {year}")

    # Convert value to numeric, handling batting averages
    if statistic == "Batting Average":
        # Handle batting averages stored as .426 instead of 0.426
        filtered_df["numeric_value"] = pd.to_numeric(
            filtered_df["value"].str.replace(".", "0.", n=1), errors="coerce"
        )
    else:
        filtered_df["numeric_value"] = pd.to_numeric(
            filtered_df["value"], errors="coerce"
        )

    # Remove any rows where conversion failed
    filtered_df = filtered_df.dropna(subset=["numeric_value"])

    if filtered_df.empty:
        return create_empty_figure(f"No valid {statistic} data for {year}")

    # Get top 10 players
    top_players = filtered_df.nlargest(10, "numeric_value")

    # Create figure using go.Figure() to avoid template issues
    fig = go.Figure()

    # Add horizontal bar chart
    fig.add_trace(
        go.Bar(
            y=top_players["name"],
            x=top_players["numeric_value"],
            orientation="h",
            text=top_players["value"],
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>"
            + "Value: %{text}<br>"
            + "Team: %{customdata[0]}<br>"
            + "League: %{customdata[1]}<extra></extra>",
            customdata=top_players[["team", "league"]].values,
            marker_color="#1f77b4",
        )
    )

    # Update layout
    fig.update_layout(
        title={
            "text": f"Top 10 Players - {statistic} ({year})",
            "x": 0.5,
            "xanchor": "center",
        },
        xaxis_title=statistic,
        yaxis_title="Player",
        template="plotly_white",
        height=400,
        margin=dict(l=150, r=80, t=50, b=50),
        showlegend=False,
    )

    # Reverse y-axis to show highest values at top
    fig.update_yaxes(autorange="reversed")

    return fig


def create_empty_figure(message):
    """Create a figure with just an error/info message"""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(size=14, color="#666"),
    )
    fig.update_layout(
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        template="plotly_white",
        height=400,
    )
    return fig


# ============================================================================
# DASHBOARD LAYOUT
# ============================================================================


def create_layout():
    """Create the main dashboard layout with error handling"""

    if not data_available:
        # Show detailed error message
        return dbc.Container(
            [
                dbc.Alert(
                    [
                        html.H4(
                            "Database Connection Error",
                            className="alert-heading",
                        ),
                        html.P(
                            "Could not load data from the baseball history database."
                        ),
                        html.P("Please check that:"),
                        html.Ul(
                            [
                                html.Li("The database file exists"),
                                html.Li(
                                    "The database contains the required tables"
                                ),
                                html.Li(
                                    "You have read permissions for the database file"
                                ),
                            ]
                        ),
                        html.Hr(),
                        html.P(
                            f"Script location: {Path(__file__).parent}",
                            className="mb-0",
                        ),
                    ],
                    color="danger",
                ),
            ],
            style={"marginTop": "50px"},
        )

    # Calculate summary statistics
    total_records = len(standings_df) + len(player_df) + len(pitcher_df)
    year_range = (
        (standings_df["year"].min(), standings_df["year"].max())
        if not standings_df.empty
        else (1900, 2024)
    )
    unique_teams = (
        standings_df["team"].nunique() if not standings_df.empty else 0
    )
    unique_players = player_df["name"].nunique() if not player_df.empty else 0

    return dbc.Container(
        [
            # Header
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H1(
                                "⚾ Baseball History Dashboard",
                                className="text-center mb-4",
                                style={"color": "#1f77b4"},
                            ),
                            html.P(
                                "Explore baseball statistics from 1876 to 2025",
                                className="text-center text-muted",
                            ),
                        ]
                    )
                ]
            ),
            # Summary statistics cards
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H4(
                                        f"{total_records:,}",
                                        className="card-title",
                                    ),
                                    html.P(
                                        "Total Records", className="card-text"
                                    ),
                                ]
                            )
                        ),
                        width=3,
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H4(
                                        f"{year_range[0]}-{year_range[1]}",
                                        className="card-title",
                                    ),
                                    html.P(
                                        "Year Range", className="card-text"
                                    ),
                                ]
                            )
                        ),
                        width=3,
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H4(
                                        f"{unique_teams}",
                                        className="card-title",
                                    ),
                                    html.P("Teams", className="card-text"),
                                ]
                            )
                        ),
                        width=3,
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H4(
                                        f"{unique_players:,}",
                                        className="card-title",
                                    ),
                                    html.P("Players", className="card-text"),
                                ]
                            )
                        ),
                        width=3,
                    ),
                ],
                className="mb-4",
            ),
            # Controls section
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Dashboard Controls"),
                                dbc.CardBody(
                                    [
                                        # Year range slider
                                        html.Label(
                                            "Year Range:", className="fw-bold"
                                        ),
                                        dcc.RangeSlider(
                                            id="year-slider",
                                            min=year_range[0],
                                            max=year_range[1],
                                            value=[
                                                max(
                                                    year_range[0],
                                                    year_range[1] - 20,
                                                ),
                                                year_range[1],
                                            ],
                                            marks={
                                                year: str(year)
                                                for year in range(
                                                    year_range[0],
                                                    year_range[1] + 1,
                                                    10,
                                                )
                                            },
                                            tooltip={
                                                "placement": "bottom",
                                                "always_visible": True,
                                            },
                                        ),
                                        html.Hr(),
                                        # Team selection
                                        html.Label(
                                            "Select Teams:",
                                            className="fw-bold",
                                        ),
                                        dcc.Dropdown(
                                            id="team-dropdown",
                                            options=(
                                                [
                                                    {
                                                        "label": team,
                                                        "value": team,
                                                    }
                                                    for team in sorted(
                                                        standings_df[
                                                            "team"
                                                        ].unique()
                                                    )
                                                ]
                                                if not standings_df.empty
                                                else []
                                            ),
                                            value=[],
                                            multi=True,
                                            placeholder="Select teams (leave empty for all)",
                                        ),
                                        html.Hr(),
                                        # League comparison metric
                                        html.Label(
                                            "League Comparison Metric:",
                                            className="fw-bold",
                                        ),
                                        dcc.RadioItems(
                                            id="league-metric",
                                            options=[
                                                {
                                                    "label": "Total Wins",
                                                    "value": "wins",
                                                },
                                                {
                                                    "label": "Total Losses",
                                                    "value": "losses",
                                                },
                                                {
                                                    "label": "Average Win %",
                                                    "value": "winning_percentage",
                                                },
                                            ],
                                            value="wins",
                                            inline=True,
                                        ),
                                        html.Hr(),
                                        # Player statistics controls
                                        html.Label(
                                            "Player Statistics:",
                                            className="fw-bold",
                                        ),
                                        dbc.Row(
                                            [
                                                dbc.Col(
                                                    [
                                                        html.Label("Year:"),
                                                        dcc.Dropdown(
                                                            id="player-year",
                                                            options=(
                                                                [
                                                                    {
                                                                        "label": year,
                                                                        "value": year,
                                                                    }
                                                                    for year in sorted(
                                                                        player_df[
                                                                            "year"
                                                                        ].unique(),
                                                                        reverse=True,
                                                                    )
                                                                ]
                                                                if not player_df.empty
                                                                else []
                                                            ),
                                                            value=(
                                                                player_df[
                                                                    "year"
                                                                ].max()
                                                                if not player_df.empty
                                                                else 2024
                                                            ),
                                                        ),
                                                    ],
                                                    width=6,
                                                ),
                                                dbc.Col(
                                                    [
                                                        html.Label(
                                                            "Statistic:"
                                                        ),
                                                        dcc.Dropdown(
                                                            id="player-stat",
                                                            options=(
                                                                [
                                                                    {
                                                                        "label": stat,
                                                                        "value": stat,
                                                                    }
                                                                    for stat in sorted(
                                                                        player_df[
                                                                            "statistic"
                                                                        ].unique()
                                                                    )
                                                                ]
                                                                if not player_df.empty
                                                                else []
                                                            ),
                                                            value=(
                                                                "Home Runs"
                                                                if not player_df.empty
                                                                and "Home Runs"
                                                                in player_df[
                                                                    "statistic"
                                                                ].values
                                                                else None
                                                            ),
                                                        ),
                                                    ],
                                                    width=6,
                                                ),
                                            ]
                                        ),
                                    ]
                                ),
                            ]
                        ),
                        width=12,
                    )
                ],
                className="mb-4",
            ),
            # Visualizations
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Team Performance Timeline"),
                                dbc.CardBody(
                                    [
                                        dcc.Loading(
                                            id="loading-timeline",
                                            type="default",
                                            children=[
                                                dcc.Graph(id="timeline-chart")
                                            ],
                                        )
                                    ]
                                ),
                            ]
                        ),
                        width=12,
                    )
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Wins vs Losses Analysis"),
                                dbc.CardBody(
                                    [
                                        dcc.Loading(
                                            id="loading-scatter",
                                            type="default",
                                            children=[
                                                dcc.Graph(id="scatter-chart")
                                            ],
                                        )
                                    ]
                                ),
                            ]
                        ),
                        width=6,
                    ),
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("League Comparison"),
                                dbc.CardBody(
                                    [
                                        dcc.Loading(
                                            id="loading-league",
                                            type="default",
                                            children=[
                                                dcc.Graph(id="league-chart")
                                            ],
                                        )
                                    ]
                                ),
                            ]
                        ),
                        width=6,
                    ),
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Player Statistical Leaders"),
                                dbc.CardBody(
                                    [
                                        dcc.Loading(
                                            id="loading-player",
                                            type="default",
                                            children=[
                                                dcc.Graph(id="player-chart")
                                            ],
                                        )
                                    ]
                                ),
                            ]
                        ),
                        width=12,
                    )
                ],
                className="mb-4",
            ),
            # Data table
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Team Standings Data Explorer"),
                                dbc.CardBody(
                                    [
                                        html.P(
                                            "Showing filtered team standings data:",
                                            className="text-muted",
                                        ),
                                        dcc.Loading(
                                            id="loading-table",
                                            type="default",
                                            children=[
                                                dash_table.DataTable(
                                                    id="data-table",
                                                    columns=(
                                                        [
                                                            {
                                                                "name": col,
                                                                "id": col,
                                                            }
                                                            for col in standings_df.columns
                                                        ]
                                                        if not standings_df.empty
                                                        else []
                                                    ),
                                                    data=(
                                                        standings_df.head(
                                                            50
                                                        ).to_dict("records")
                                                        if not standings_df.empty
                                                        else []
                                                    ),
                                                    page_size=10,
                                                    sort_action="native",
                                                    filter_action="native",
                                                    style_table={
                                                        "overflowX": "auto"
                                                    },
                                                    style_cell={
                                                        "textAlign": "left",
                                                        "padding": "10px",
                                                        "whiteSpace": "normal",
                                                        "height": "auto",
                                                    },
                                                    style_header={
                                                        "backgroundColor": "#1f77b4",
                                                        "color": "white",
                                                        "fontWeight": "bold",
                                                    },
                                                    style_data_conditional=[
                                                        {
                                                            "if": {
                                                                "row_index": "odd"
                                                            },
                                                            "backgroundColor": "#f8f9fa",
                                                        }
                                                    ],
                                                )
                                            ],
                                        ),
                                    ]
                                ),
                            ]
                        )
                    )
                ]
            ),
            # Footer
            html.Hr(className="mt-5"),
            html.P(
                "Baseball History Dashboard © 2025 | Data from baseball-reference.com",
                className="text-center text-muted mb-4",
            ),
        ],
        fluid=True,
        style=custom_style,
    )


# Set the layout
app.layout = create_layout()

# ============================================================================
# SERVER CONFIGURATION
# ============================================================================


# Add cache control headers to prevent chunk loading issues
@app.server.after_request
def add_header(response):
    """Add headers to prevent caching issues"""
    response.headers["Cache-Control"] = (
        "no-store, no-cache, must-revalidate, max-age=0"
    )
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# Add health check endpoint
@app.server.route("/health")
def health_check():
    """Health check endpoint for monitoring"""
    return {
        "status": "healthy",
        "timestamp": pd.Timestamp.now().isoformat(),
        "data_loaded": data_available,
        "records": len(standings_df) + len(player_df) + len(pitcher_df),
    }


# ============================================================================
# CALLBACKS (WITH ERROR HANDLING)
# ============================================================================


@app.callback(
    Output("timeline-chart", "figure"),
    [Input("year-slider", "value"), Input("team-dropdown", "value")],
)
@safe_callback
def update_timeline(year_range, selected_teams):
    """Update team performance timeline"""
    logger.info(
        f"Updating timeline: years={year_range}, teams={selected_teams}"
    )
    return create_team_performance_timeline(
        standings_df, year_range, selected_teams
    )


@app.callback(
    Output("scatter-chart", "figure"), [Input("year-slider", "value")]
)
@safe_callback
def update_scatter(year_range):
    """Update wins vs losses scatter plot"""
    logger.info(f"Updating scatter plot: years={year_range}")
    return create_wins_losses_scatter(standings_df, year_range)


@app.callback(
    Output("league-chart", "figure"),
    [Input("league-metric", "value"), Input("year-slider", "value")],
)
@safe_callback
def update_league_comparison(metric, year_range):
    """Update league comparison chart"""
    logger.info(
        f"Updating league comparison: metric={metric}, years={year_range}"
    )
    return create_league_comparison(standings_df, metric, year_range)


@app.callback(
    Output("player-chart", "figure"),
    [Input("player-year", "value"), Input("player-stat", "value")],
)
@safe_callback
def update_player_leaders(year, statistic):
    """Update player leaders chart with error handling"""
    logger.info(f"Updating player chart: year={year}, stat={statistic}")

    if not year or not statistic:
        return create_empty_figure("Select year and statistic")

    return create_player_leaders_chart(player_df, year, statistic)


@app.callback(
    Output("data-table", "data"),
    [Input("year-slider", "value"), Input("team-dropdown", "value")],
)
def update_data_table(year_range, selected_teams):
    """Update data table based on filters"""
    try:
        if standings_df.empty:
            return []

        filtered_df = standings_df[
            (standings_df["year"] >= year_range[0])
            & (standings_df["year"] <= year_range[1])
        ]

        if selected_teams:
            filtered_df = filtered_df[filtered_df["team"].isin(selected_teams)]

        logger.info(
            f"Updating table: {len(filtered_df)} records after filtering"
        )
        return filtered_df.head(100).to_dict("records")

    except Exception as e:
        logger.error(f"Error updating data table: {str(e)}")
        return []


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Baseball History Dashboard - Starting")
    print("=" * 60)
    print(f"Debug mode: {DEBUG_MODE}")
    print(f"Dashboard URL: http://localhost:8050/")
    print(f"Health check: http://localhost:8050/health")
    print("=" * 60)

    # Perform startup checks
    try:
        # Test database connection
        test_path = Config.get_database_path()
        print(f"✓ Database found at: {test_path}")

        # Report data loading status
        if data_available:
            print(f"✓ Data loaded successfully")
            print(f"  - Team standings: {len(standings_df)} records")
            print(f"  - Player stats: {len(player_df)} records")
            print(f"  - Pitcher stats: {len(pitcher_df)} records")
        else:
            print("✗ No data loaded - check database connection")

    except Exception as e:
        print(f"✗ Startup error: {e}")

    print("\nPress CTRL+C to stop the server")
    print("=" * 60 + "\n")

    # Run the app with production settings
    try:
        app.run(
            debug=DEBUG_MODE,
            host="0.0.0.0",
            port=8050,
            dev_tools_hot_reload=False,  # Disable hot reload to prevent chunk loading issues
        )
    except KeyboardInterrupt:
        print("\n\nShutting down dashboard...")
        logger.info("Dashboard shutdown by user")
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        print(f"\nError starting server: {e}")
        sys.exit(1)
