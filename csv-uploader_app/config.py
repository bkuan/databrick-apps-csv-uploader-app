"""
Configuration settings for Databricks CSV to Delta Table Uploader
Modify these settings to match your Databricks environment
"""

import os
from typing import Optional

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ Loaded environment variables from .env file")
except ImportError:
    print("⚠️  python-dotenv not installed. Using system environment variables only.")
    pass

# Databricks Connection Settings
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', 'https://your-workspace.cloud.databricks.com')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN', '')
DATABRICKS_OAUTH_TOKEN = os.getenv('DATABRICKS_OAUTH_TOKEN', '')
# To find your warehouse ID: Go to Databricks > SQL Warehouses > Click your warehouse > Copy the Server Hostname path
# Example: /sql/1.0/warehouses/abcd1234567890ef (replace 'your-warehouse-id' with your actual warehouse ID)
DATABRICKS_HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH', '/sql/1.0/warehouses/a1a5ed85eea63273')

# Default Unity Catalog Settings
DEFAULT_CATALOG = os.getenv('DEFAULT_CATALOG', 'ingest_demo')
DEFAULT_SCHEMA = os.getenv('DEFAULT_SCHEMA', 'medical_providers')
DEFAULT_VOLUME_PATH = os.getenv('DEFAULT_VOLUME_PATH', '/Volumes/ingest_demo/medical_providers/providers/')

# Application Settings
APP_PORT = int(os.getenv('PORT', '8050'))
APP_HOST = os.getenv('HOST', '0.0.0.0')
DEBUG_MODE = os.getenv('DEBUG', 'true').lower() == 'true'

# File Upload Settings
MAX_FILE_SIZE_MB = int(os.getenv('MAX_FILE_SIZE_MB', '100'))
ALLOWED_EXTENSIONS = ['csv']

# CSV Parsing Defaults
DEFAULT_DELIMITER = ','
DEFAULT_HAS_HEADER = True

# Table Creation Defaults  
DEFAULT_WRITE_MODE = 'create'  # options: 'create', 'overwrite', 'append'

# UI Settings
APP_TITLE = "Databricks CSV to Delta Table"
APP_DESCRIPTION = "Upload CSV files to Databricks volumes and create Delta tables with ease"

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

class DatabaseConfig:
    """Configuration for Databricks authentication and connection"""
    
    def __init__(self):
        self.host = DATABRICKS_HOST
        self.token = DATABRICKS_TOKEN
        self.oauth_token = DATABRICKS_OAUTH_TOKEN
        self.http_path = DATABRICKS_HTTP_PATH
    
    def is_valid(self) -> bool:
        """Check if configuration is valid for authentication"""
        return bool(self.host and (self.token or self.oauth_token))
    
    def get_auth_method(self) -> str:
        """Return the authentication method being used"""
        if self.oauth_token:
            return "OAuth Token"
        elif self.token:
            return "Personal Access Token"
        else:
            return "None"

def validate_config() -> list[str]:
    """Validate configuration settings"""
    errors = []
    
    db_config = DatabaseConfig()
    
    if not db_config.is_valid():
        errors.append("Please set DATABRICKS_HOST and either DATABRICKS_TOKEN or DATABRICKS_OAUTH_TOKEN")
    
    if DATABRICKS_HOST == 'https://your-workspace.cloud.databricks.com':
        errors.append("Please set your actual Databricks workspace URL in DATABRICKS_HOST")
    
    if not DEFAULT_VOLUME_PATH.startswith('/Volumes/'):
        errors.append("DEFAULT_VOLUME_PATH should start with '/Volumes/'")
    
    return errors

def print_config(hide_sensitive=True):
    """Print current configuration (excluding sensitive data by default)"""
    db_config = DatabaseConfig()
    
    print("🔧 Current Configuration:")
    print(f"  Databricks Host: {db_config.host}")
    print(f"  Auth Method: {db_config.get_auth_method()}")
    
    if not hide_sensitive:
        print(f"  Token: {db_config.token[:10] + '...' if db_config.token else 'Not set'}")
        print(f"  OAuth Token: {db_config.oauth_token[:10] + '...' if db_config.oauth_token else 'Not set'}")
    
    print(f"  Default Catalog: {DEFAULT_CATALOG}")
    print(f"  Default Schema: {DEFAULT_SCHEMA}")  
    print(f"  Default Volume Path: {DEFAULT_VOLUME_PATH}")
    print(f"  App Port: {APP_PORT}")
    print(f"  Debug Mode: {DEBUG_MODE}")
    print(f"  Max File Size: {MAX_FILE_SIZE_MB}MB")

def get_environment_template() -> str:
    """Return environment variables template"""
    return """
# Databricks Configuration
# Set these environment variables or create a .env file

export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi1234567890abcdef...

# Optional: OAuth token for Databricks Apps (takes precedence)
# export DATABRICKS_OAUTH_TOKEN=your-oauth-token

# Optional: SQL Warehouse HTTP path
# export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id

# App Configuration
export PORT=8050
export HOST=0.0.0.0
export DEBUG=true

# Default Unity Catalog settings
export DEFAULT_CATALOG=ingest_demo
export DEFAULT_SCHEMA=medical_providers
export DEFAULT_VOLUME_PATH=/Volumes/ingest_demo/medical_providers/providers/
"""

if __name__ == '__main__':
    print_config()
    errors = validate_config()
    if errors:
        print("\n❌ Configuration Errors:")
        for error in errors:
            print(f"  - {error}")
        print("\n💡 Environment Variables Template:")
        print(get_environment_template())
    else:
        print("\n✅ Configuration looks good!")