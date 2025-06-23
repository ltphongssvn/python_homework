# Baseball History Dashboard Project

## 🎯 Project Overview

This project implements interactive dashboards for analyzing baseball history data using both **Streamlit** and **Dash** frameworks. The dashboards provide comprehensive data visualization and analysis capabilities with multiple interactive features.

## 🚀 Live Deployments

- **Streamlit Dashboard**: [![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://your-app-name.streamlit.app)
- **Dash Dashboard**: [Coming Soon - Render Deployment]

> **Note**: Replace `your-app-name` with your actual Streamlit app URL after deployment

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
│   ├── streamlit_dashboard.py    # Streamlit implementation
│   ├── dash_dashboard.py         # Dash implementation
│   ├── requirements.txt          # Python dependencies
│   └── explore_data.py          # Data exploration utility
├── baseball_history.db          # SQLite database
├── .streamlit/
│   └── config.toml              # Streamlit configuration
└── README.md                    # This file
```

## 🚦 Quick Start

### Prerequisites
- Python 3.8 or higher
- SQLite database with baseball data

### Local Development

1. **Clone the repository**
   ```bash
   git clone [your-repo-url]
   cd assignment14
   ```

2. **Install dependencies**
   ```bash
   pip install -r dashboards/requirements.txt
   ```

3. **Run Streamlit Dashboard**
   ```bash
   cd dashboards
   streamlit run streamlit_dashboard.py
   ```

4. **Run Dash Dashboard**
   ```bash
   cd dashboards
   python dash_dashboard.py
   ```

## 📊 Database Schema

The project uses a SQLite database (`baseball_history.db`) with the following expected structure:

```sql
-- Example table structure (adapt based on your actual schema)
CREATE TABLE baseball_data (
    id INTEGER PRIMARY KEY,
    year INTEGER,
    team TEXT,
    wins INTEGER,
    losses INTEGER,
    attendance INTEGER,
    runs_scored INTEGER,
    runs_allowed INTEGER
);
```

## 🎨 Dashboard Features Comparison

| Feature | Streamlit | Dash |
|---------|-----------|------|
| Development Speed | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| Customization | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Interactive Controls | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Deployment Ease | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| Performance | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ |

## 🚀 Deployment Instructions

### Streamlit.io Deployment

1. Push your code to GitHub
2. Connect your GitHub repo to Streamlit.io
3. Set the main file path: `dashboards/streamlit_dashboard.py`
4. Deploy automatically

### Render Deployment

1. Create a new Web Service on Render
2. Connect your GitHub repository
3. Configure build settings:
   - **Build Command**: `pip install -r dashboards/requirements.txt`
   - **Start Command**: `python dashboards/dash_dashboard.py`
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
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📝 License

This project is created for educational purposes as part of Assignment 14.

## 📞 Contact

[Your contact information]

---

**Note**: This dashboard project demonstrates advanced Python development practices including defensive programming, modular design, comprehensive error handling, and production-ready deployment strategies.