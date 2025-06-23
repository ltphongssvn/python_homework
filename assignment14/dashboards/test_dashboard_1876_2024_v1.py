#!/usr/bin/env python3
# test_dashboard.py - Minimal working Dash example
import dash
import dash_bootstrap_components as dbc
from dash import html

# Create a simple test file to verify dash works
print("Dash version:", dash.__version__)
print("Dash imported successfully!")

# You can also check the version details
version_parts = dash.__version__.split(".")
major_version = int(version_parts[0])
print(f"Major version: {major_version}")

if major_version >= 3:
    print("You're using Dash 3.x - use app.run()")
else:
    print("You're using Dash 2.x or older - use app.run_server()")


# Create the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Simple layout
app.layout = dbc.Container(
    [
        html.H1("Test Dashboard"),
        html.P("If you can see this, Dash is working correctly!"),
    ]
)

# Run the app
if __name__ == "__main__":
    print("Starting test dashboard...")
    print("Open http://localhost:8050 in your browser")
    app.run(debug=True, port=8050)
