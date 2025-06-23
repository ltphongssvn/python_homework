#!/usr/bin/env python3
# streamlit_dashboard.py
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Page configuration
st.set_page_config(
    page_title="Baseball History Dashboard",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for better styling
st.markdown(
    """
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data
def load_team_standings():
    """Load team standings data from the database"""
    try:
        db_path = (
            Path(__file__).resolve().parent.parent / "baseball_history.db"
        )
        conn = sqlite3.connect(db_path)

        # Query team_standings table - this is where wins/losses live
        query = """
        SELECT * FROM team_standings
        ORDER BY year DESC, winning_percentage DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        # Clean the games_back column - it sometimes contains the winning percentage
        # Experienced developers always inspect and clean messy data
        if "games_back" in df.columns:
            df["games_back_clean"] = pd.to_numeric(
                df["games_back"].str.extract(r"(\d+\.?\d*)")[0],
                errors="coerce",
            )

        print(f"Loaded {len(df)} team standing records")
        print(f"Years available: {df['year'].min()} to {df['year'].max()}")

        return df

    except Exception as e:
        st.error(f"Error loading team standings: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def load_player_stats():
    """Load player statistics from the database"""
    try:
        db_path = (
            Path(__file__).resolve().parent.parent / "baseball_history.db"
        )
        conn = sqlite3.connect(db_path)

        query = """
        SELECT * FROM player_stats
        WHERE statistic IN ('Batting Average', 'Home Runs', 'Hits', 'RBI')
        ORDER BY year DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        return df

    except Exception as e:
        st.error(f"Error loading player stats: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def load_pitcher_stats():
    """Load pitcher statistics from the database"""
    try:
        db_path = (
            Path(__file__).resolve().parent.parent / "baseball_history.db"
        )
        conn = sqlite3.connect(db_path)

        query = """
        SELECT * FROM pitcher_stats
        WHERE statistic IN ('ERA', 'Wins', 'Strikeouts', 'Complete Games')
        ORDER BY year DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        return df

    except Exception as e:
        st.error(f"Error loading pitcher stats: {str(e)}")
        return pd.DataFrame()


def create_team_performance_timeline(df, selected_teams, selected_leagues):
    """Create a timeline showing team performance over the years"""
    # Apply filters
    filtered_df = df.copy()

    if selected_teams:
        filtered_df = filtered_df[filtered_df["team"].isin(selected_teams)]

    if selected_leagues:
        filtered_df = filtered_df[filtered_df["league"].isin(selected_leagues)]

    if filtered_df.empty:
        return go.Figure().add_annotation(
            text="No data available for selected filters",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )

    # Create the visualization
    fig = px.line(
        filtered_df,
        x="year",
        y="winning_percentage",
        color="team",
        title="Team Winning Percentage Over Time",
        labels={"winning_percentage": "Winning Percentage", "year": "Year"},
        hover_data=["wins", "losses", "league", "division"],
    )

    fig.update_layout(
        hovermode="x unified", yaxis_tickformat=",.1%", height=500
    )

    return fig


def create_wins_losses_scatter(df, year_range):
    """Create a scatter plot of wins vs losses"""
    # Filter by year range
    filtered_df = df[
        (df["year"] >= year_range[0]) & (df["year"] <= year_range[1])
    ]

    if filtered_df.empty:
        return go.Figure().add_annotation(
            text="No data available for selected years",
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
        size="winning_percentage",
        title="Team Wins vs Losses",
        hover_data=["team", "year", "division"],
        labels={"wins": "Wins", "losses": "Losses"},
    )

    # Add diagonal line for .500 winning percentage
    fig.add_trace(
        go.Scatter(
            x=[0, 162],
            y=[162, 0],
            mode="lines",
            name=".500 Line",
            line=dict(dash="dash", color="gray"),
            showlegend=True,
        )
    )

    fig.update_layout(height=500)

    return fig


def create_league_comparison(df):
    """Compare leagues over time"""
    # Aggregate by league and year
    league_stats = (
        df.groupby(["year", "league"])
        .agg({"wins": "sum", "losses": "sum", "winning_percentage": "mean"})
        .reset_index()
    )

    # Create subplots
    fig = go.Figure()

    for league in league_stats["league"].unique():
        league_data = league_stats[league_stats["league"] == league]

        fig.add_trace(
            go.Bar(
                name=league,
                x=league_data["year"],
                y=league_data["wins"],
                text=league_data["wins"],
                textposition="auto",
            )
        )

    fig.update_layout(
        title="Total Wins by League Over Time",
        xaxis_title="Year",
        yaxis_title="Total Wins",
        barmode="group",
        height=500,
    )

    return fig


def create_top_players_chart(player_df, year, stat_type):
    """Show top players for a given statistic in a specific year"""
    # Filter data
    filtered_df = player_df[
        (player_df["year"] == year) & (player_df["statistic"] == stat_type)
    ].copy()

    if filtered_df.empty:
        return go.Figure().add_annotation(
            text=f"No {stat_type} data available for {year}",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )

    # Convert value to numeric where possible
    # Handle batting average differently (it's already a decimal)
    if stat_type == "Batting Average":
        filtered_df["numeric_value"] = pd.to_numeric(
            filtered_df["value"].str.replace(".", "0.", n=1), errors="coerce"
        )
    else:
        filtered_df["numeric_value"] = pd.to_numeric(
            filtered_df["value"], errors="coerce"
        )

    # Get top 10 players
    top_players = filtered_df.nlargest(10, "numeric_value")

    # Create bar chart
    fig = px.bar(
        top_players,
        x="numeric_value",
        y="name",
        orientation="h",
        title=f"Top 10 Players - {stat_type} ({year})",
        labels={"numeric_value": stat_type, "name": "Player"},
        text="value",
        hover_data=["team", "league"],
    )

    fig.update_traces(textposition="outside")
    fig.update_layout(height=400)

    return fig


def create_division_standings_table(df, year, league):
    """Create a standings table for a specific year and league"""
    # Filter data
    standings = df[
        (df["year"] == year) & (df["league"] == league)
    ].sort_values("winning_percentage", ascending=False)

    if standings.empty:
        return None

    # Format the display
    display_df = standings[
        ["team", "wins", "losses", "winning_percentage", "division"]
    ].copy()
    display_df["W-L"] = (
        display_df["wins"].astype(str) + "-" + display_df["losses"].astype(str)
    )
    display_df["PCT"] = display_df["winning_percentage"].apply(
        lambda x: f"{x:.3f}"
    )

    return display_df[["team", "W-L", "PCT", "division"]]


def main():
    """Main dashboard function"""
    # Header
    st.markdown(
        '<h1 class="main-header">⚾ Baseball History Dashboard</h1>',
        unsafe_allow_html=True,
    )

    # Load all data
    with st.spinner("Loading baseball data..."):
        standings_df = load_team_standings()
        player_df = load_player_stats()
        pitcher_df = load_pitcher_stats()

    if standings_df.empty:
        st.error(
            "No team standings data available. Please check your database connection."
        )
        return

    # Sidebar controls
    st.sidebar.header("Dashboard Controls")

    # Year range for standings
    min_year = int(standings_df["year"].min())
    max_year = int(standings_df["year"].max())

    year_range = st.sidebar.slider(
        "Year Range for Analysis",
        min_value=min_year,
        max_value=max_year,
        value=(
            max(min_year, max_year - 20),
            max_year,
        ),  # Default to last 20 years
        help="Select the range of years to analyze",
    )

    # League filter
    leagues = sorted(standings_df["league"].unique())
    selected_leagues = st.sidebar.multiselect(
        "Select Leagues",
        options=leagues,
        default=leagues,
        help="Filter by league",
    )

    # Team filter - show only teams that played in selected years
    available_teams = sorted(
        standings_df[
            (standings_df["year"] >= year_range[0])
            & (standings_df["year"] <= year_range[1])
        ]["team"].unique()
    )

    selected_teams = st.sidebar.multiselect(
        "Select Teams (Optional)",
        options=available_teams,
        default=[],  # Default to all teams
        help="Leave empty to show all teams",
    )

    # Summary Statistics
    st.header("📊 Database Overview")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_records = len(standings_df) + len(player_df) + len(pitcher_df)
        st.metric("Total Records", f"{total_records:,}")

    with col2:
        st.metric("Years Covered", f"{min_year} - {max_year}")

    with col3:
        total_teams = standings_df["team"].nunique()
        st.metric("Unique Teams", total_teams)

    with col4:
        total_players = (
            player_df["name"].nunique() if not player_df.empty else 0
        )
        st.metric("Unique Players", f"{total_players:,}")

    # Main visualizations
    st.header("📈 Team Performance Analysis")

    # Tab layout for different views
    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "Performance Timeline",
            "Wins vs Losses",
            "League Comparison",
            "Player Leaders",
        ]
    )

    with tab1:
        st.subheader("Team Winning Percentage Over Time")
        fig1 = create_team_performance_timeline(
            standings_df, selected_teams, selected_leagues
        )
        st.plotly_chart(fig1, use_container_width=True)

    with tab2:
        st.subheader("Team Wins vs Losses Scatter Plot")
        fig2 = create_wins_losses_scatter(standings_df, year_range)
        st.plotly_chart(fig2, use_container_width=True)

        # Add explanation
        st.info(
            "Teams above the diagonal line have a winning record (.500+), "
            "while teams below have a losing record. The size of each point "
            "represents the winning percentage."
        )

    with tab3:
        st.subheader("League Performance Comparison")
        fig3 = create_league_comparison(standings_df)
        st.plotly_chart(fig3, use_container_width=True)

    with tab4:
        st.subheader("Player Statistical Leaders")

        if not player_df.empty:
            col1, col2 = st.columns(2)

            with col1:
                selected_year = st.selectbox(
                    "Select Year",
                    options=sorted(player_df["year"].unique(), reverse=True),
                    index=0,
                )

            with col2:
                available_stats = sorted(player_df["statistic"].unique())
                selected_stat = st.selectbox(
                    "Select Statistic",
                    options=available_stats,
                    index=(
                        available_stats.index("Home Runs")
                        if "Home Runs" in available_stats
                        else 0
                    ),
                )

            fig4 = create_top_players_chart(
                player_df, selected_year, selected_stat
            )
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("Player statistics not available")

    # Standings Table Section
    st.header("📋 Historical Standings")

    col1, col2 = st.columns(2)

    with col1:
        standings_year = st.selectbox(
            "Select Year for Standings",
            options=sorted(standings_df["year"].unique(), reverse=True),
            index=0,
            key="standings_year",
        )

    with col2:
        standings_league = st.selectbox(
            "Select League for Standings",
            options=sorted(standings_df["league"].unique()),
            index=0,
            key="standings_league",
        )

    # Display standings table
    standings_table = create_division_standings_table(
        standings_df, standings_year, standings_league
    )

    if standings_table is not None:
        st.dataframe(
            standings_table, use_container_width=True, hide_index=True
        )
    else:
        st.info("No standings data available for selected year and league")

    # Data exploration
    with st.expander("🔍 Explore Raw Data"):
        data_choice = st.selectbox(
            "Select Dataset",
            options=["Team Standings", "Player Stats", "Pitcher Stats"],
            index=0,
        )

        if data_choice == "Team Standings":
            display_df = standings_df[
                standings_df["year"].between(year_range[0], year_range[1])
            ]
            st.dataframe(display_df.head(100), use_container_width=True)

            csv_data = display_df.to_csv(index=False)
            st.download_button(
                label="Download Team Standings CSV",
                data=csv_data,
                file_name=f"team_standings_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

        elif data_choice == "Player Stats" and not player_df.empty:
            st.dataframe(player_df.head(100), use_container_width=True)

        elif data_choice == "Pitcher Stats" and not pitcher_df.empty:
            st.dataframe(pitcher_df.head(100), use_container_width=True)

    # Debug information
    with st.expander("🔧 Debug Information"):
        st.write("**Team Standings Columns:**", standings_df.columns.tolist())
        st.write("**Team Standings Shape:**", standings_df.shape)
        if not player_df.empty:
            st.write(
                "**Player Stats Unique Statistics:**",
                sorted(player_df["statistic"].unique()),
            )
        if not pitcher_df.empty:
            st.write(
                "**Pitcher Stats Unique Statistics:**",
                sorted(pitcher_df["statistic"].unique()),
            )


if __name__ == "__main__":
    main()
