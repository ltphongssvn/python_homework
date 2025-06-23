#!/usr/bin/env python3
# dash_dashboard_1876_2024_v1_fixed.py
"""
Baseball History Dashboard

This module creates an interactive dashboard for exploring baseball statistics
from 1876 to 2025, including team standings, player statistics,
and pitcher data.
Uses Dash and Plotly for visualization of historical baseball data.
"""
import sqlite3
from pathlib import Path

import dash
import dash_bootstrap_components as dbc  # type: ignore
import pandas as pd
import plotly.express as px  # type: ignore
import plotly.graph_objects as go  # type: ignore
from dash import dash_table  # Fixed: Updated import
from dash import Input, Output, dcc, html

# Initialize the Dash app with Bootstrap theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Baseball History Dashboard"

# Define custom CSS for consistent styling
custom_style = {
    "backgroundColor": "#f8f9fa",
    "fontFamily": "Arial, sans-serif",
}


# Configuration class for better path management
class Config:
    """Centralized configuration for database paths"""

    DATABASE_NAME = "baseball_history.db"

    @classmethod
    def get_database_path(cls):
        """Find the database file in multiple possible locations"""
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
                print(f"Found database at: {db_path}")
                return db_path

        # If no path works, provide helpful error message
        searched_paths = "\n".join(f"  - {p}" for p in possible_paths)
        raise FileNotFoundError(
            f"Could not find '{cls.DATABASE_NAME}'\n"
            f"Searched in these locations: {searched_paths}\n"
            f"Current working directory: {Path.cwd()}\n"
            f"Script location: {Path(__file__).parent}"
        )


# Data loading functions with improved error handling
def load_team_standings():
    """Load team standings data with wins/losses information"""
    try:
        # Get database path using the configuration
        db_path = Config.get_database_path()
        conn = sqlite3.connect(str(db_path))

        # Verify table exists before querying
        cursor = conn.cursor()

        cursor.execute(
            (
                "SELECT name "
                "FROM sqlite_master "
                "WHERE type='table' "
                "AND name='team_standings';"
            )
        )

        if not cursor.fetchone():
            conn.close()
            raise ValueError("Table 'team_standings' not found in database")

        query = """
        SELECT year, league, division, team, wins, losses,
               winning_percentage, games_back
        FROM team_standings
        ORDER BY year DESC, winning_percentage DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        # Data cleaning - handle the messy games_back column
        if "games_back" in df.columns:
            # Extract numeric values from games_back where possible
            df["games_back_numeric"] = pd.to_numeric(
                df["games_back"].str.extract(r"(\d+\.?\d*)")[0],
                errors="coerce",
            )

        # Add calculated fields
        df["total_games"] = df["wins"] + df["losses"]

        print(f"Successfully loaded {len(df)} team standings records")
        return df

    except (sqlite3.Error, pd.errors.DatabaseError, ValueError) as e:
        print(f"Error loading team standings: {str(e)}")
        # Return empty DataFrame with expected columns
        # to prevent further errors
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
    """Load player statistics data"""
    try:
        db_path = Config.get_database_path()
        conn = sqlite3.connect(str(db_path))

        # Verify table exists
        cursor = conn.cursor()
        cursor.execute(
            (
                "SELECT name "
                "FROM sqlite_master "
                "WHERE type='table' "
                "AND name='player_stats';"
            )
        )
        if not cursor.fetchone():
            conn.close()
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
        conn.close()

        print(f"Successfully loaded {len(df)} player stat records")
        return df

    except (sqlite3.Error, pd.errors.DatabaseError, ValueError) as e:
        print(f"Error loading player stats: {str(e)}")
        return pd.DataFrame(
            columns=["year", "league", "name", "team", "statistic", "value"]
        )


def load_pitcher_stats():
    """Load pitcher statistics data"""
    try:
        db_path = Config.get_database_path()
        conn = sqlite3.connect(str(db_path))

        # Verify table exists
        cursor = conn.cursor()

        cursor.execute(
            (
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='team_standings';"
            )
        )

        if not cursor.fetchone():
            conn.close()
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
        conn.close()

        print(f"Successfully loaded {len(df)} pitcher stat records")
        return df

    except (sqlite3.Error, pd.errors.DatabaseError, ValueError) as e:
        print(f"Error loading pitcher stats: {str(e)}")
        return pd.DataFrame(
            columns=["year", "league", "name", "team", "statistic", "value"]
        )


# Load all data at startup with error handling
print("=" * 80)
print("BASEBALL HISTORY DASHBOARD - STARTUP")
print("=" * 80)
print(f"Script location: {Path(__file__).parent}")
print(f"Current working directory: {Path.cwd()}")
print("-" * 80)

standings_df = load_team_standings()
player_df = load_player_stats()
pitcher_df = load_pitcher_stats()

# Check if we have any data
data_available = not standings_df.empty
if data_available:
    print("-" * 80)
    print("Data loading complete!")

    print(
        f"Total records loaded: "
        f"{len(standings_df) + len(player_df) + len(pitcher_df):,}"
    )

else:
    print("-" * 80)
    print("WARNING: No data loaded. Dashboard will show error message.")
print("=" * 80)


# Helper functions for creating visualizations
def create_team_performance_timeline(df, year_range, selected_teams):
    """Create time series visualization of team performance"""
    if df.empty:
        return go.Figure().add_annotation(
            text="No team standings data available",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )

    # Apply filters
    filtered_df = df[
        (df["year"] >= year_range[0]) & (df["year"] <= year_range[1])
    ]

    if selected_teams:
        filtered_df = filtered_df[filtered_df["team"].isin(selected_teams)]

    if filtered_df.empty:
        return go.Figure().add_annotation(
            text="No data matches your selection",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )

    # Create line chart showing winning percentage over time
    fig = px.line(
        filtered_df,
        x="year",
        y="winning_percentage",
        color="team",
        title="Team Winning Percentage Over Time",
        markers=True,
        hover_data=["wins", "losses", "league"],
    )

    fig.update_layout(
        template="plotly_white",
        height=400,
        title_x=0.5,
        yaxis_tickformat=".1%",
        hovermode="x unified",
    )

    return fig


def create_wins_losses_scatter(df, year_range):
    """Create scatter plot of wins vs losses"""
    if df.empty:
        return go.Figure().add_annotation(
            text="No team standings data available",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )

    # Filter by year range
    filtered_df = df[
        (df["year"] >= year_range[0]) & (df["year"] <= year_range[1])
    ]

    if filtered_df.empty:
        return go.Figure().add_annotation(
            text="No data for selected years",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )

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

    fig.update_layout(template="plotly_white", height=400, title_x=0.5)

    return fig


def create_league_comparison(df, metric, year_range):
    """Create comparison between leagues"""
    if df.empty or metric not in ["wins", "losses", "winning_percentage"]:
        return go.Figure().add_annotation(
            text="Cannot create league comparison",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )

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
    )

    fig.update_layout(template="plotly_white", height=400, title_x=0.5)

    if metric == "winning_percentage":
        fig.update_yaxis(tickformat=".1%")

    return fig


def create_player_leaders_chart(_player_df, year, statistic):
    """Show top players for a given statistic"""
    if player_df.empty:
        return go.Figure().add_annotation(
            text="No player statistics available",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )

    # Filter for specific year and statistic
    filtered_df = player_df[
        (player_df["year"] == year) & (player_df["statistic"] == statistic)
    ].copy()

    if filtered_df.empty:
        return go.Figure().add_annotation(
            text=f"No {statistic} data for {year}",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )

    # Convert value to numeric, handling batting averages
    if statistic == "Batting Average":
        # Batting averages are stored as .426 instead of 0.426
        filtered_df["numeric_value"] = pd.to_numeric(
            filtered_df["value"].str.replace(".", "0.", n=1), errors="coerce"
        )
    else:
        filtered_df["numeric_value"] = pd.to_numeric(
            filtered_df["value"], errors="coerce"
        )

    # Get top 10 players
    top_players = filtered_df.nlargest(10, "numeric_value")

    # Create horizontal bar chart
    fig = px.bar(
        top_players,
        y="name",
        x="numeric_value",
        orientation="h",
        title=f"Top 10 Players - {statistic} ({year})",
        labels={"numeric_value": statistic, "name": "Player"},
        text="value",
        hover_data=["team", "league"],
    )

    fig.update_traces(textposition="outside")
    fig.update_layout(template="plotly_white", height=400, title_x=0.5)

    return fig


# Define the app layout
def create_layout():
    """Create the main dashboard layout"""

    if not data_available:
        # Show detailed error message with troubleshooting steps
        return dbc.Container(
            [
                dbc.Alert(
                    [
                        html.H4(
                            "Database Connection Error",
                            className="alert-heading",
                        ),
                        html.P(
                            "Could not load data from the baseball "
                            "history database. Please check the following:"
                        ),
                        html.Ul(
                            [
                                html.Li(
                                    f"Database file exists at: "
                                    f"C:\\Users\\LENOVO\\python_homework\\assignment14\\{Config.DATABASE_NAME}"
                                ),
                                html.Li(
                                    "The database contains tables: "
                                    "team_standings, player_stats, pitcher_stats"
                                ),
                                html.Li(
                                    "You have read permissions "
                                    "for the database file"
                                ),
                            ]
                        ),
                        html.Hr(),
                        html.P(
                            f"Current script location: {Path(__file__).parent}",
                            className="mb-0",
                        ),
                    ],
                    color="danger",
                ),
            ]
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
                            )
                        ]
                    )
                ]
            ),
            # Summary statistics cards
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H4(
                                                f"{total_records:,}",
                                                className="card-title",
                                            ),
                                            html.P(
                                                "Total Records",
                                                className="card-text",
                                            ),
                                        ]
                                    )
                                ]
                            )
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H4(
                                                f"{year_range[0]}-{year_range[1]}",
                                                className="card-title",
                                            ),
                                            html.P(
                                                "Year Range",
                                                className="card-text",
                                            ),
                                        ]
                                    )
                                ]
                            )
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H4(
                                                f"{unique_teams}",
                                                className="card-title",
                                            ),
                                            html.P(
                                                "Teams", className="card-text"
                                            ),
                                        ]
                                    )
                                ]
                            )
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H4(
                                                f"{unique_players:,}",
                                                className="card-title",
                                            ),
                                            html.P(
                                                "Players",
                                                className="card-text",
                                            ),
                                        ]
                                    )
                                ]
                            )
                        ],
                        width=3,
                    ),
                ],
                className="mb-4",
            ),
            # Controls section
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardHeader("Dashboard Controls"),
                                    dbc.CardBody(
                                        [
                                            # Year range slider
                                            html.Label(
                                                "Year Range:",
                                                className="fw-bold",
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
                                                            html.Label(
                                                                "Year:"
                                                            ),
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
                            )
                        ],
                        width=12,
                    )
                ],
                className="mb-4",
            ),
            # Visualizations
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardHeader(
                                        "Team Performance Timeline"
                                    ),
                                    dbc.CardBody(
                                        [dcc.Graph(id="timeline-chart")]
                                    ),
                                ]
                            )
                        ],
                        width=12,
                    )
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardHeader("Wins vs Losses Analysis"),
                                    dbc.CardBody(
                                        [dcc.Graph(id="scatter-chart")]
                                    ),
                                ]
                            )
                        ],
                        width=6,
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardHeader("League Comparison"),
                                    dbc.CardBody(
                                        [dcc.Graph(id="league-chart")]
                                    ),
                                ]
                            )
                        ],
                        width=6,
                    ),
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardHeader(
                                        "Player Statistical Leaders"
                                    ),
                                    dbc.CardBody(
                                        [dcc.Graph(id="player-chart")]
                                    ),
                                ]
                            )
                        ],
                        width=12,
                    )
                ],
                className="mb-4",
            ),
            # Data table
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardHeader(
                                        "Team Standings Data Explorer"
                                    ),
                                    dbc.CardBody(
                                        [
                                            html.P(
                                                "Showing filtered team standings data:",
                                                className="text-muted",
                                            ),
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
                                            ),
                                        ]
                                    ),
                                ]
                            )
                        ]
                    )
                ]
            ),
        ],
        fluid=True,
        style=custom_style,
    )


# Set the layout
app.layout = create_layout()


# Callbacks
@app.callback(
    Output("timeline-chart", "figure"),
    [Input("year-slider", "value"), Input("team-dropdown", "value")],
)
def update_timeline(year_range, selected_teams):
    """Update team performance timeline"""
    return create_team_performance_timeline(
        standings_df, year_range, selected_teams
    )


@app.callback(
    Output("scatter-chart", "figure"), [Input("year-slider", "value")]
)
def update_scatter(year_range):
    """Update wins vs losses scatter plot"""
    return create_wins_losses_scatter(standings_df, year_range)


@app.callback(
    Output("league-chart", "figure"),
    [Input("league-metric", "value"), Input("year-slider", "value")],
)
def update_league_comparison(metric, year_range):
    """Update league comparison chart"""
    return create_league_comparison(standings_df, metric, year_range)


@app.callback(
    Output("player-chart", "figure"),
    [Input("player-year", "value"), Input("player-stat", "value")],
)
def update_player_leaders(year, statistic):
    """Update player leaders chart"""
    if year and statistic:
        return create_player_leaders_chart(player_df, year, statistic)
    return go.Figure().add_annotation(
        text="Select year and statistic",
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
    )


@app.callback(
    Output("data-table", "data"),
    [Input("year-slider", "value"), Input("team-dropdown", "value")],
)
def update_data_table(year_range, selected_teams):
    """Update data table based on filters"""
    if standings_df.empty:
        return []

    filtered_df = standings_df[
        (standings_df["year"] >= year_range[0])
        & (standings_df["year"] <= year_range[1])
    ]

    if selected_teams:
        filtered_df = filtered_df[filtered_df["team"].isin(selected_teams)]

    return filtered_df.head(100).to_dict("records")


# Run the app
if __name__ == "__main__":
    print("\nStarting Baseball History Dashboard...")
    print("Dashboard URL: http://localhost:8050/")
    print("Press CTRL+C to stop the server\n")

    app.run(debug=True, host="0.0.0.0", port=8050)
