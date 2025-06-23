import os
import sqlite3
import sys
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Page configuration - this sets up the basic app structure
st.set_page_config(
    page_title="Baseball History Dashboard",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for professional appearance
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
    .error-box {
        background-color: #ffebee;
        border-left: 5px solid #f44336;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""",
    unsafe_allow_html=True,
)


class DatabaseAdapter:
    """
    Smart database adapter that discovers schema and adapts to different structures
    This is how experienced developers handle unknown or varying data schemas
    """

    def __init__(self):
        self.db_path = self.find_database()
        self.table_info = {}
        self.main_table = None
        if self.db_path:
            self.analyze_schema()

    def find_database(self):
        """Find the baseball database file in common locations"""
        possible_paths = [
            "baseball_history.db",  # Current directory
            "../baseball_history.db",  # Parent directory
            "../../baseball_history.db",  # Two levels up
            "./db/baseball_history.db",  # In db subfolder
        ]

        for path in possible_paths:
            if os.path.exists(path):
                return path
        return None

    def analyze_schema(self):
        """Analyze the database schema to understand available tables and columns"""
        if not self.db_path:
            return

        try:
            conn = sqlite3.connect(self.db_path)

            # Get all table names
            tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
            tables_df = pd.read_sql_query(tables_query, conn)

            if tables_df.empty:
                conn.close()
                return

            # Analyze each table
            for table_name in tables_df["name"]:
                try:
                    # Get column information
                    schema_query = f"PRAGMA table_info({table_name});"
                    schema_df = pd.read_sql_query(schema_query, conn)

                    # Get row count
                    count_query = (
                        f"SELECT COUNT(*) as count FROM {table_name};"
                    )
                    count_result = pd.read_sql_query(count_query, conn)
                    row_count = count_result["count"].iloc[0]

                    # Store table information
                    self.table_info[table_name] = {
                        "columns": schema_df["name"].tolist(),
                        "types": dict(
                            zip(schema_df["name"], schema_df["type"])
                        ),
                        "row_count": row_count,
                    }

                except Exception as e:
                    st.warning(f"Could not analyze table {table_name}: {e}")

            # Identify the best table for our dashboard
            self.main_table = self.identify_main_table()
            conn.close()

        except Exception as e:
            st.error(f"Database analysis failed: {e}")

    def identify_main_table(self):
        """Identify the most suitable table for dashboard data"""
        if not self.table_info:
            return None

        # Score tables based on relevance and size
        relevant_keywords = [
            "year",
            "team",
            "wins",
            "losses",
            "games",
            "season",
        ]
        best_score = 0
        best_table = None

        for table_name, info in self.table_info.items():
            score = 0
            columns_lower = [col.lower() for col in info["columns"]]

            # Score based on relevant columns
            for keyword in relevant_keywords:
                if any(keyword in col for col in columns_lower):
                    score += 10

            # Bonus for having substantial data
            if info["row_count"] > 50:
                score += 20

            # Reasonable number of columns
            if 5 <= len(info["columns"]) <= 30:
                score += 5

            if score > best_score:
                best_score = score
                best_table = table_name

        return best_table

    @st.cache_data
    def load_data(_self, limit=1000):
        """Load data with intelligent column mapping and error handling"""
        if not _self.db_path or not _self.main_table:
            return pd.DataFrame(), "No suitable database or table found"

        try:
            conn = sqlite3.connect(_self.db_path)

            # Load data from the main table
            query = f"SELECT * FROM {_self.main_table} LIMIT {limit};"
            df = pd.read_sql_query(query, conn)
            conn.close()

            if df.empty:
                return df, "No data found in table"

            # Apply intelligent column mapping
            df = _self.standardize_columns(df)

            return df, None

        except Exception as e:
            return pd.DataFrame(), f"Error loading data: {str(e)}"

    def standardize_columns(self, df):
        """Standardize column names to match dashboard expectations"""
        df = df.copy()

        # Common column mappings
        column_mappings = {
            "year": ["year", "season", "yr", "year_id"],
            "team": ["team", "team_name", "franchise", "club", "team_id"],
            "wins": ["wins", "w", "victories"],
            "losses": ["losses", "l", "defeats"],
            "games": ["games", "g", "total_games"],
            "attendance": ["attendance", "attend"],
        }

        # Apply mappings
        original_columns = [col.lower() for col in df.columns]
        rename_dict = {}

        for standard_name, variations in column_mappings.items():
            for variation in variations:
                matching_cols = [
                    col for col in df.columns if variation in col.lower()
                ]
                if matching_cols:
                    # Use the first match
                    rename_dict[matching_cols[0]] = standard_name
                    break

        df = df.rename(columns=rename_dict)

        # Convert data types safely
        for col in ["year", "wins", "losses", "games", "attendance"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Create calculated fields
        if "wins" in df.columns and "losses" in df.columns:
            df["total_games"] = df["wins"] + df["losses"]
            df["win_percentage"] = df["wins"] / df["total_games"]

        return df


# Initialize the database adapter
@st.cache_resource
def get_database_adapter():
    return DatabaseAdapter()


def create_visualization_1(df):
    """Time series visualization with fallback options"""
    if df.empty:
        return go.Figure().add_annotation(
            text="No data available for visualization"
        )

    # Try different combinations based on available columns
    if "year" in df.columns and "wins" in df.columns:
        yearly_stats = df.groupby("year")["wins"].sum().reset_index()
        fig = px.line(
            yearly_stats,
            x="year",
            y="wins",
            title="Wins Over Time",
            markers=True,
        )
    elif "year" in df.columns:
        # Use the first numeric column
        numeric_cols = df.select_dtypes(include=["number"]).columns
        if len(numeric_cols) > 0:
            col = numeric_cols[0]
            yearly_stats = df.groupby("year")[col].sum().reset_index()
            fig = px.line(
                yearly_stats,
                x="year",
                y=col,
                title=f"{col.title()} Over Time",
                markers=True,
            )
        else:
            return go.Figure().add_annotation(
                text="No numeric data available for time series"
            )
    else:
        return go.Figure().add_annotation(text="No time-based data available")

    fig.update_layout(template="plotly_white", height=400)
    return fig


def create_visualization_2(df):
    """Correlation analysis with adaptive columns"""
    if df.empty:
        return go.Figure().add_annotation(text="No data available")

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

    if len(numeric_cols) < 2:
        return go.Figure().add_annotation(
            text="Need at least 2 numeric columns for correlation"
        )

    # Use meaningful column pairs if available
    x_col = numeric_cols[0]
    y_col = numeric_cols[1]

    # Prefer wins/attendance if available
    if "attendance" in numeric_cols and "wins" in numeric_cols:
        x_col, y_col = "attendance", "wins"
    elif "wins" in numeric_cols and "losses" in numeric_cols:
        x_col, y_col = "wins", "losses"

    fig = px.scatter(
        df,
        x=x_col,
        y=y_col,
        title=f"{y_col.title()} vs {x_col.title()}",
        trendline="ols",
    )

    fig.update_layout(template="plotly_white", height=400)
    return fig


def create_visualization_3(df):
    """Team comparison with available data"""
    if df.empty:
        return go.Figure().add_annotation(text="No data available")

    if "team" not in df.columns:
        return go.Figure().add_annotation(
            text="No team data available for comparison"
        )

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    if not numeric_cols:
        return go.Figure().add_annotation(
            text="No numeric data for comparison"
        )

    # Use wins if available, otherwise first numeric column
    metric_col = "wins" if "wins" in numeric_cols else numeric_cols[0]

    # Get top teams by the metric
    team_stats = (
        df.groupby("team")[metric_col]
        .mean()
        .sort_values(ascending=False)
        .head(10)
    )

    fig = px.bar(
        x=team_stats.index,
        y=team_stats.values,
        title=f"Top Teams by {metric_col.title()}",
        labels={"x": "Team", "y": metric_col.title()},
    )

    fig.update_layout(template="plotly_white", height=400)
    return fig


def main():
    """Main dashboard application"""

    # Header
    st.markdown(
        '<h1 class="main-header">⚾ Baseball History Dashboard</h1>',
        unsafe_allow_html=True,
    )

    # Initialize database adapter
    db_adapter = get_database_adapter()

    # Check database connection
    if not db_adapter.db_path:
        st.error("❌ Could not find baseball_history.db file")
        st.info(
            "💡 Please ensure the database file is in the correct location"
        )
        st.info(
            "📍 Expected locations: current directory, parent directory, or db/ subfolder"
        )
        return

    if not db_adapter.main_table:
        st.error("❌ No suitable data table found in database")
        if db_adapter.table_info:
            st.info("Available tables:")
            for table, info in db_adapter.table_info.items():
                st.write(
                    f"  • {table}: {info['row_count']} rows, {len(info['columns'])} columns"
                )
        return

    # Load data
    with st.spinner("Loading baseball data..."):
        df, error = db_adapter.load_data()

    if error:
        st.error(f"❌ {error}")
        return

    if df.empty:
        st.warning("⚠️ No data available")
        return

    # Success message with database info
    st.success(f"✅ Connected to database: {db_adapter.db_path}")
    st.info(f"📊 Using table: {db_adapter.main_table} ({len(df)} records)")

    # Display column information
    with st.expander("🔍 Data Information"):
        col1, col2 = st.columns(2)

        with col1:
            st.write("**Available Columns:**")
            for col in df.columns:
                st.write(f"  • {col} ({df[col].dtype})")

        with col2:
            st.write("**Data Summary:**")
            st.write(f"  • Total records: {len(df):,}")
            st.write(f"  • Columns: {len(df.columns)}")
            if "year" in df.columns:
                st.write(
                    f"  • Year range: {df['year'].min()} - {df['year'].max()}"
                )
            if "team" in df.columns:
                st.write(f"  • Teams: {df['team'].nunique()}")

    # Interactive controls
    st.sidebar.header("Dashboard Controls")

    # Year filter if available
    if "year" in df.columns:
        year_range = st.sidebar.slider(
            "Year Range",
            min_value=int(df["year"].min()),
            max_value=int(df["year"].max()),
            value=(int(df["year"].min()), int(df["year"].max())),
        )
        df = df[(df["year"] >= year_range[0]) & (df["year"] <= year_range[1])]

    # Team filter if available
    if "team" in df.columns:
        teams = st.sidebar.multiselect(
            "Select Teams",
            options=sorted(df["team"].unique()),
            default=sorted(df["team"].unique())[:5],  # Limit default selection
        )
        if teams:
            df = df[df["team"].isin(teams)]

    # Summary metrics
    st.header("📊 Overview")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Records", f"{len(df):,}")
    with col2:
        if "team" in df.columns:
            st.metric("Teams", df["team"].nunique())
        else:
            st.metric("Columns", len(df.columns))
    with col3:
        if "wins" in df.columns:
            st.metric("Total Wins", f"{df['wins'].sum():,}")
        elif "year" in df.columns:
            st.metric("Years", f"{df['year'].nunique()}")
        else:
            st.metric("Data Points", f"{df.size:,}")
    with col4:
        numeric_cols = df.select_dtypes(include=["number"]).columns
        if len(numeric_cols) > 0:
            st.metric(
                "Avg " + numeric_cols[0].title(),
                f"{df[numeric_cols[0]].mean():.1f}",
            )
        else:
            st.metric(
                "Text Columns",
                len(df.select_dtypes(include=["object"]).columns),
            )

    # Visualizations
    st.header("📈 Data Visualizations")

    # Visualization 1
    st.subheader("1. Time Series Analysis")
    st.plotly_chart(create_visualization_1(df), use_container_width=True)

    # Visualization 2 & 3 side by side
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("2. Correlation Analysis")
        st.plotly_chart(create_visualization_2(df), use_container_width=True)

    with col2:
        st.subheader("3. Team Comparison")
        st.plotly_chart(create_visualization_3(df), use_container_width=True)

    # Data table
    with st.expander("🔍 Explore Raw Data"):
        st.dataframe(df.head(100), use_container_width=True)

        # Download option
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download Data as CSV",
            data=csv,
            file_name=f"baseball_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()
