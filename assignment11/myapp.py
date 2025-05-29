from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import plotly.data as pldata

# Load the gapminder dataset instead of stocks
df = pldata.gapminder()

# Initialize Dash app
app = Dash(__name__)
server = app.server  # <-- This exposes the Flask server for production deployment

# Create the countries Series with duplicates removed (as specified in requirements)
countries = df['country'].drop_duplicates().sort_values()

# Layout - following the exact requirements
app.layout = html.Div([
    dcc.Dropdown(
        id="country-dropdown",  # Specific ID as required
        options=[{"label": country, "value": country} for country in countries],
        value="Canada"  # Initial value as specified
    ),
    dcc.Graph(id="gdp-growth")  # Specific ID as required
])

# Callback decorator - associating input with dropdown and output with graph
@app.callback(
    Output("gdp-growth", "figure"),  # Output to the graph
    [Input("country-dropdown", "value")]  # Input from the dropdown
)
def update_graph(selected_country):  # Function name as specified
    # Filter the dataset for the selected country
    country_data = df[df['country'] == selected_country]
    
    # Create line plot for 'year' vs 'gdpPercap' as specified
    fig = px.line(country_data, x='year', y='gdpPercap', 
                  title=f'GDP per Capita: {selected_country}')
    
    return fig

# Run the app - line doesn't need to change as noted
if __name__ == "__main__":
    app.run(debug=True)
