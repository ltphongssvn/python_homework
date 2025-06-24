# Baseball History Dashboard Project

## 🎯 Project Overview

This project implements interactive dashboards for analyzing baseball history data using both **Streamlit** and **Dash** frameworks. The dashboards provide comprehensive data visualization and analysis capabilities with multiple interactive features.

## 🚀 Live Deployments

- **Streamlit Dashboard**: https://pythonhomework-nz6p6b9ecjgky2gt9unqiz.streamlit.app
- **Dash Dashboard**: https://baseball-history-dashboard.onrender.com/

## 📊 Features

### Interactive Visualizations
1. **Time Series Analysis**: Track team performance trends over time
2. **Correlation Analysis**: Explore relationships between attendance and performance
3. **Team Comparison**: Compare multiple teams across different metrics

### Interactive Controls
- **Year Range Slider**: Filter data by specific time periods
- **Team Selection**: Multi-select dropdown for team comparisons
- **Dynamic Metrics**: Choose different performance indicators
- **Real-time Updates**: All visualizations update automatically based on user input

## 🛠 Technology Stack

- **Backend**: Python 3.8+, SQLite
- **Frameworks**: Streamlit, Dash
- **Visualization**: Plotly Express, Plotly Graph Objects
- **Data Processing**: Pandas
- **Styling**: Dash Bootstrap Components, Custom CSS
- **Deployment**: Streamlit.io, Render

## 📁 Project Structure

```
assignment14/
├── dashboards/
│   ├── streamlit_dashboard_1876_2024_v2.py                    # Streamlit implementation
│   ├── dash_dashboard_1876_2024_v3.py                         # Dash implementation
│   ├── requirements.txt                                       # Python dependencies
│   └── README.md                                              # This file
├── .streamlit/
│    └── config.toml                                           # Streamlit configuration
├── _01_web_scraping_program_1876_2024_v3.py                   # Web Scraping Program
├── _02_database_import_program_1876_2024_v1.py                # Database Import Program
├── _03_database_query_program_1876_2024_v2.py                 # Database Query Program
├── baseball_history.db                                        # SQLite database
├── custom_sql_query_1876_2024_v6.py                           # Customed Database Query Program
├── custom_sql_query_1876_2024_v6.sql                          # Customed SQL statements
├── inspect_schema_1876_2024_v1.py                             # Find out about Database schema
```

## 🚦 Quick Start

### Prerequisites
- Python 3.8 or higher
- SQLite database with baseball data

### Local Development

1. **Clone the repository**
   ```bash
   git clone https://github.com/ltphongssvn/python_homework.git
   cd assignment14
   ```

2. **Install dependencies**
   ```bash
   pip install -r dashboards/requirements.txt
   ```

3. **Run Streamlit Dashboard**
   ```bash
   cd dashboards
   streamlit run streamlit_dashboard_1876_2024_v2.py
   ```

4. **Run Dash Dashboard**
   ```bash
   cd dashboards
   python dash_dashboard_1876_2024_v3.py
   ```

## 📊 Database Schema

The project uses a SQLite database (`baseball_history.db`) with the following expected structure:

```sql
-- Table structure (based on the schema)
CREATE TABLE pitcher_stats (
  id INTEGER PRIMARY KEY,
  year INTEGER,
  league TEXT,
  stat_type TEXT,
  statistic TEXT,
  name TEXT,
  team TEXT,
  value TEXT,
  UNIQUE (year, league, statistic, name, team)
);

CREATE TABLE player_stats (
  id INTEGER PRIMARY KEY,
  year INTEGER,
  league TEXT,
  stat_type TEXT,
  statistic TEXT,
  name TEXT,
  team TEXT,
  value TEXT,
  UNIQUE (year, league, statistic, name, team)
);

CREATE TABLE team_standings (
  id INTEGER PRIMARY KEY,
  year INTEGER,
  league TEXT,
  division TEXT,
  team TEXT,
  wins INTEGER,
  losses INTEGER,
  winning_percentage REAL,
  games_back TEXT,
  UNIQUE (year, league, team)
);
```
## 🚀 Deployment Instructions

### Streamlit.io Deployment

1. Push the code to GitHub
2. Connect the GitHub repo to Streamlit.io
3. Set the main file path: `python_homework ∙ assignment14 ∙ assignment14/dashboards/streamlit_dashboard_1876_2024_v2.py`
4. Deploy automatically

### Render Deployment

1. Create a new Web Service on Render
2. Connect the GitHub repository
3. Configure build settings:
   - **Build Command**: `pip install -r dashboards/requirements.txt`
   - **Start Command**: `python dashboards/dash_dashboard_1876_2024_v3.py`
4. Deploy

## 🔧 Configuration

### Environment Variables
- `PORT`: Server port (automatically set by deployment platforms)
- `ENVIRONMENT`: Set to 'production' for deployment

### Streamlit Configuration
Located in `.streamlit/config.toml`:
- Theme customization
- Server settings
- Upload limits

## 📈 Performance Considerations

- **Data Caching**: Both dashboards implement caching for improved performance
- **Data Limiting**: Database queries are limited to prevent memory issues
- **Lazy Loading**: Data is loaded only when needed
- **Error Handling**: Comprehensive error handling for production reliability

## 🐛 Troubleshooting

### Common Issues

1. **Database Connection Error**
   - Ensure `baseball_history.db` is in the correct location
   - Check database file permissions

2. **Missing Dependencies**
   - Run `pip install -r requirements.txt`
   - Verify Python version compatibility

3. **Port Conflicts**
   - Streamlit default: 8501
   - Dash default: 8050
   - Use different ports for simultaneous running

## 📚 Development Notes

### Code Organization
- **Modular Design**: Functions are separated by responsibility
- **Error Handling**: Defensive programming with comprehensive error handling
- **Documentation**: Inline comments and docstrings throughout
- **Best Practices**: Following Python and dashboard development best practices

### Testing Strategy
- Local testing before deployment
- Cross-browser compatibility
- Different data scenarios (empty, missing columns, edge cases)
- Performance testing with larger datasets

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make the changes
4. Test thoroughly
5. Submit a pull request

## 📝 License

This project is created for educational purposes as part of Assignment 14.

## 📞 Contact

Thanh Phong Le
ltphongssvn@gmail.com

---

**Note**: This dashboard project demonstrates advanced Python development practices including defensive programming, modular design, comprehensive error handling, and production-ready deployment strategies.