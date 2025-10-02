#!/usr/bin/env python3
"""
Databricks CSV to Delta Table Uploader
A Dash application for uploading, editing, and converting CSV files to Delta tables

Deployed as a Databricks App
"""

import sys
import traceback
import os

def main():
    try:
        print("🚀 Starting Databricks CSV Uploader App...")
        print(f"📍 Python version: {sys.version}")
        print(f"📍 Working directory: {os.getcwd()}")
        print(f"📍 Python path: {sys.path}")
        
        # Import with error handling
        print("📦 Importing main application...")
        from databricks_csv_uploader import app
        print("✅ Successfully imported main application")
        print("🔄 Authentication will be handled on-demand (lazy loading)")
        
        # Check if we're in Databricks Apps environment
        port = int(os.getenv('PORT', 8080))
        host = os.getenv('HOST', '0.0.0.0')
        
        print(f"🌐 Starting server on {host}:{port}")
        
        # For Databricks Apps deployment
        app.run_server(
            debug=False, 
            host=host, 
            port=port,
            dev_tools_ui=False,
            dev_tools_props_check=False,
            dev_tools_hot_reload=False
        )
        
    except ImportError as e:
        print(f"❌ Import Error: {e}")
        print(f"📝 Full traceback: {traceback.format_exc()}")
        # Create a simple error page
        create_error_app(f"Import Error: {e}")
    except Exception as e:
        print(f"❌ Startup Error: {e}")
        print(f"📝 Full traceback: {traceback.format_exc()}")
        create_error_app(f"Startup Error: {e}")

def create_error_app(error_message):
    """Create a simple error page when main app fails to load"""
    import dash
    from dash import html
    
    error_app = dash.Dash(__name__)
    
    error_app.layout = html.Div([
        html.H1("❌ Databricks CSV Uploader - Startup Error", style={'color': 'red'}),
        html.Div([
            html.H3("Error Details:"),
            html.Pre(error_message, style={'backgroundColor': '#f8f8f8', 'padding': '10px'}),
            html.Hr(),
            html.H3("Troubleshooting:"),
            html.Ul([
                html.Li("Check that all required packages are installed"),
                html.Li("Verify Databricks authentication is properly configured"),
                html.Li("Ensure all Python files are uploaded correctly"),
                html.Li("Check app logs for detailed error information")
            ])
        ])
    ])
    
    port = int(os.getenv('PORT', 8080))
    host = os.getenv('HOST', '0.0.0.0')
    
    error_app.run_server(
        debug=True,
        host=host,
        port=port,
        dev_tools_ui=False
    )

if __name__ == "__main__":
    main()
