#!/usr/bin/env python3
"""
Professional Streamlit Dashboard Application
streamlit_app.py - Interactive data exploration using the Streamlit framework

This application demonstrates professional-grade dashboard development using Streamlit,
showcasing data cleaning processes, interactive visualizations, and deployment-ready code.

Educational concepts demonstrated:
- Streamlit's declarative programming model
- Interactive data filtering and visualization
- Professional dashboard layout and design
- Data cleaning documentation and transparency
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

# Configure Streamlit page settings for professional appearance
st.set_page_config(
    page_title="Global Economic Analysis Dashboard",
    page_icon="📊",
    layout="wide",  # Use full browser width for better data visualization
    initial_sidebar_state="expanded"  # Start with sidebar open for better UX
)

# Custom CSS for professional styling (optional enhancement)
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f4e79;
        text-align: center;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.5rem;
        color: #2c5aa0;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data  # Streamlit's caching decorator for performance optimization
def load_and_prepare_data():
    """
    Load and prepare the dataset for analysis.
    
    This function demonstrates professional data loading practices including
    error handling, data validation, and transformation documentation.
    
    Returns:
        pd.DataFrame: Cleaned and prepared dataset ready for visualization
    """
    try:
        # Load the gapminder dataset from plotly's built-in datasets
        # This represents a common pattern of using reliable, well-documented datasets
        import plotly.data as px_data
        df = px_data.gapminder()
        
        # Data cleaning and preparation steps
        # These operations demonstrate common data preparation tasks
        
        # 1. Handle missing values (check for any potential issues)
        missing_before = df.isnull().sum().sum()
        
        # 2. Create derived columns for enhanced analysis
        df['gdp_total'] = df['gdpPercap'] * df['pop']  # Calculate total GDP
        df['decade'] = (df['year'] // 10) * 10  # Group years into decades
        
        # 3. Create categorical variables for better visualization
        df['income_category'] = pd.cut(
            df['gdpPercap'], 
            bins=[0, 1000, 5000, 15000, float('inf')],
            labels=['Low Income', 'Lower Middle', 'Upper Middle', 'High Income']
        )
        
        # 4. Add data quality indicators
        df['data_quality'] = 'Complete'  # All gapminder data is complete
        
        # 5. Sort data for consistent presentation
        df = df.sort_values(['country', 'year']).reset_index(drop=True)
        
        # Store cleaning metadata for transparency
        cleaning_info = {
            'missing_values_before': missing_before,
            'missing_values_after': df.isnull().sum().sum(),
            'rows_processed': len(df),
            'columns_added': ['gdp_total', 'decade', 'income_category', 'data_quality'],
            'cleaning_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return df, cleaning_info
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None, None

def display_data_cleaning_summary(cleaning_info):
    """
    Display comprehensive information about data cleaning processes.
    
    This function demonstrates transparency in data preparation, which is
    crucial for building trust in data-driven applications.
    """
    st.markdown('<div class="section-header">📋 Data Cleaning & Preparation Summary</div>', 
                unsafe_allow_html=True)
    
    # Create informative columns for cleaning information
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="Records Processed",
            value=f"{cleaning_info['rows_processed']:,}",
            delta="Complete dataset"
        )
    
    with col2:
        st.metric(
            label="Missing Values",
            value=cleaning_info['missing_values_after'],
            delta=f"Reduced from {cleaning_info['missing_values_before']}"
        )
    
    with col3:
        st.metric(
            label="Columns Added",
            value=len(cleaning_info['columns_added']),
            delta="Enhanced analysis"
        )
    
    # Detailed cleaning explanation
    with st.expander("🔍 Detailed Cleaning Process"):
        st.markdown("""
        **Data Preparation Steps Performed:**
        
        1. **Missing Value Analysis**: Verified data completeness across all variables
        2. **Derived Variables**: Created total GDP calculation (GDP per capita × population)
        3. **Temporal Grouping**: Added decade categories for trend analysis
        4. **Income Classification**: Categorized countries by income levels using World Bank standards
        5. **Data Quality Flags**: Added indicators for data reliability assessment
        6. **Sorting & Indexing**: Organized data chronologically by country for consistent analysis
        
        **Why These Steps Matter:**
        - Total GDP provides absolute economic size perspective alongside per-capita measures
        - Decade grouping enables long-term trend analysis beyond year-to-year fluctuations
        - Income categories facilitate comparative analysis between similar economies
        - Quality indicators ensure transparency about data reliability
        """)

def create_interactive_visualizations(df):
    """
    Create comprehensive interactive visualizations that demonstrate
    Streamlit's capabilities for data exploration and insight generation.
    """
    st.markdown('<div class="section-header">📈 Interactive Data Exploration</div>', 
                unsafe_allow_html=True)
    
    # Sidebar controls for global filtering
    st.sidebar.markdown("### 🎛️ Analysis Controls")
    
    # Year range selector
    year_range = st.sidebar.slider(
        "Select Year Range",
        min_value=int(df['year'].min()),
        max_value=int(df['year'].max()),
        value=(int(df['year'].min()), int(df['year'].max())),
        help="Filter data to analyze specific time periods"
    )
    
    # Continent selector
    continents = ['All'] + sorted(df['continent'].unique().tolist())
    selected_continent = st.sidebar.selectbox(
        "Select Continent",
        continents,
        help="Focus analysis on specific geographical regions"
    )
    
    # Filter dataframe based on selections
    filtered_df = df[
        (df['year'] >= year_range[0]) & 
        (df['year'] <= year_range[1])
    ]
    
    if selected_continent != 'All':
        filtered_df = filtered_df[filtered_df['continent'] == selected_continent]
    
    # Display filtering results
    st.info(f"📊 Analyzing {len(filtered_df):,} records from {filtered_df['country'].nunique()} countries")
    
    # Create multiple visualization tabs for organized presentation
    tab1, tab2, tab3, tab4 = st.tabs([
        "🌍 Global Overview", 
        "📊 Economic Trends", 
        "🔄 Comparative Analysis",
        "📋 Raw Data Explorer"
    ])
    
    with tab1:
        st.subheader("Global Economic Development Overview")
        
        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        latest_year = filtered_df['year'].max()
        latest_data = filtered_df[filtered_df['year'] == latest_year]
        
        with col1:
            avg_gdp = latest_data['gdpPercap'].mean()
            st.metric(
                "Average GDP per Capita",
                f"${avg_gdp:,.0f}",
                help="Average GDP per capita across all countries in selected period"
            )
        
        with col2:
            total_pop = latest_data['pop'].sum()
            st.metric(
                "Total Population",
                f"{total_pop/1e9:.2f}B",
                help="Combined population of analyzed countries"
            )
        
        with col3:
            country_count = latest_data['country'].nunique()
            st.metric(
                "Countries Analyzed",
                f"{country_count}",
                help="Number of countries in current selection"
            )
        
        with col4:
            year_span = latest_year - filtered_df['year'].min() + 1
            st.metric(
                "Time Period",
                f"{year_span} years",
                help="Span of years in current analysis"
            )
        
        # World map visualization
        st.subheader("GDP per Capita by Country (Latest Year)")
        
        if not latest_data.empty:
            fig_map = px.choropleth(
                latest_data,
                locations="iso_alpha",
                color="gdpPercap",
                hover_name="country",
                hover_data={"pop": ":,", "lifeExp": ":.1f"},
                color_continuous_scale="Viridis",
                title=f"Global GDP per Capita Distribution ({latest_year})"
            )
            fig_map.update_layout(height=500)
            st.plotly_chart(fig_map, use_container_width=True)
    
    with tab2:
        st.subheader("Economic Development Trends")
        
        # Country selector for trend analysis
        countries_for_trends = st.multiselect(
            "Select Countries for Trend Analysis",
            sorted(filtered_df['country'].unique()),
            default=sorted(filtered_df['country'].unique())[:5],
            help="Choose specific countries to compare their economic development over time"
        )
        
        if countries_for_trends:
            trend_data = filtered_df[filtered_df['country'].isin(countries_for_trends)]
            
            # GDP per capita trends
            fig_trends = px.line(
                trend_data,
                x='year',
                y='gdpPercap',
                color='country',
                title="GDP per Capita Trends Over Time",
                labels={'gdpPercap': 'GDP per Capita (USD)', 'year': 'Year'}
            )
            fig_trends.update_layout(height=400)
            st.plotly_chart(fig_trends, use_container_width=True)
            
            # Life expectancy correlation
            fig_scatter = px.scatter(
                trend_data,
                x='gdpPercap',
                y='lifeExp',
                size='pop',
                color='continent',
                animation_frame='year',
                hover_name='country',
                title="GDP vs Life Expectancy Over Time",
                labels={
                    'gdpPercap': 'GDP per Capita (USD)',
                    'lifeExp': 'Life Expectancy (years)',
                    'pop': 'Population'
                }
            )
            fig_scatter.update_layout(height=500)
            st.plotly_chart(fig_scatter, use_container_width=True)
    
    with tab3:
        st.subheader("Comparative Economic Analysis")
        
        # Income category analysis
        if not filtered_df.empty:
            # income_summary = filtered_df.groupby(['income_category', 'year']).agg({
            #     'gdpPercap': 'mean',
            #     'lifeExp': 'mean',
            #     'pop': 'sum',
            #     'country': 'nunique'
            # }).reset_index()


            income_summary = filtered_df.groupby(['income_category', 'year'], observed=True).agg({
                'gdpPercap': 'mean',
                'lifeExp': 'mean',
                'pop': 'sum',
                'country': 'nunique'
            }).reset_index()



            
            # Income category trends
            fig_income = px.bar(
                income_summary[income_summary['year'] == income_summary['year'].max()],
                x='income_category',
                y='gdpPercap',
                title="Average GDP per Capita by Income Category (Latest Year)",
                color='income_category',
                labels={'gdpPercap': 'Average GDP per Capita (USD)'}
            )
            st.plotly_chart(fig_income, use_container_width=True)
            
            # Continental comparison
            continent_data = filtered_df.groupby(['continent', 'year']).agg({
                'gdpPercap': 'mean',
                'lifeExp': 'mean',
                'pop': 'sum'
            }).reset_index()
            
            fig_continent = px.box(
                filtered_df,
                x='continent',
                y='gdpPercap',
                title="GDP per Capita Distribution by Continent",
                labels={'gdpPercap': 'GDP per Capita (USD)'}
            )
            st.plotly_chart(fig_continent, use_container_width=True)
    
    with tab4:
        st.subheader("Raw Data Explorer")
        
        # Data filtering options
        col1, col2 = st.columns(2)
        
        with col1:
            show_columns = st.multiselect(
                "Select Columns to Display",
                filtered_df.columns.tolist(),
                default=['country', 'year', 'gdpPercap', 'lifeExp', 'pop', 'continent']
            )
        
        with col2:
            sort_column = st.selectbox(
                "Sort by Column",
                filtered_df.columns.tolist(),
                index=list(filtered_df.columns).index('gdpPercap')
            )
        
        # Display filtered and sorted data
        if show_columns:
            display_data = filtered_df[show_columns].sort_values(
                sort_column, ascending=False
            )
            
            st.dataframe(
                display_data,
                use_container_width=True,
                height=400
            )
            
            # Data summary statistics
            st.subheader("Summary Statistics")
            numeric_columns = display_data.select_dtypes(include=[np.number]).columns
            if len(numeric_columns) > 0:
                st.dataframe(
                    display_data[numeric_columns].describe(),
                    use_container_width=True
                )

def main():
    """
    Main application function that orchestrates the entire dashboard.
    
    This function demonstrates professional application structure with
    clear separation of concerns and logical flow.
    """
    # Application header
    st.markdown('<div class="main-header">🌍 Global Economic Development Dashboard</div>', 
                unsafe_allow_html=True)
    
    st.markdown("""
    **Explore global economic trends and development patterns using interactive data visualization.**
    
    This dashboard provides comprehensive analysis of economic indicators across countries and time periods,
    demonstrating the power of Streamlit for rapid development of data-driven applications.
    """)
    
    # Load and prepare data
    with st.spinner("Loading and preparing data..."):
        df, cleaning_info = load_and_prepare_data()
    
    if df is not None and cleaning_info is not None:
        # Display data overview
        st.markdown('<div class="section-header">📊 Dataset Overview</div>', 
                    unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown(f"""
            **Dataset Information:**
            - **Source**: Gapminder Foundation via Plotly Express
            - **Records**: {len(df):,} observations
            - **Countries**: {df['country'].nunique()} unique countries
            - **Time Period**: {df['year'].min()} - {df['year'].max()}
            - **Variables**: Economic indicators, demographics, and geographic data
            """)
        
        with col2:
            st.dataframe(
                df.head(),
                use_container_width=True,
                height=200
            )
        
        # Display cleaning summary
        display_data_cleaning_summary(cleaning_info)
        
        # Create interactive visualizations
        create_interactive_visualizations(df)
        
        # Footer with additional information
        st.markdown("---")
        st.markdown("""
        **About This Dashboard:**
        This application demonstrates professional Streamlit development practices including
        data preparation transparency, interactive visualization design, and deployment-ready code structure.
        
        **Technical Stack:** Python, Streamlit, Plotly, Pandas, NumPy
        """)
        
    else:
        st.error("Failed to load data. Please check your data source and try again.")

# Application entry point
if __name__ == "__main__":
    main()