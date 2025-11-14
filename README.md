# Databricks CSV to Delta Table Uploader

A Databricks App built with Dash that enables seamless upload of CSV files to Databricks volumes and automated creation of Delta tables. This tool streamlines the data ingestion process for Databricks Unity Catalog environments.
<img width="3386" height="1118" alt="image" src="https://github.com/user-attachments/assets/70e86a65-92b7-42bd-a871-a5ff22304e4b" />

<img width="1656" height="1818" alt="image" src="https://github.com/user-attachments/assets/bb3e6850-51d8-4520-ac2d-217704241d3b" />


## Features

### Core Functionality
- **CSV File Upload**: Drag-and-drop interface for uploading CSV files up to 100MB
- **Data Preview & Editing**: Interactive table editor with real-time preview
- **Flexible CSV Parsing**: Support for various delimiters (comma, semicolon, tab, pipe)
- **Header Management**: Toggle between using first row as headers or auto-generated column names
- **Data Manipulation**: Add/remove rows and columns, edit cell values directly
- **Undo/Redo Operations**: Multi-step undo functionality with revert to original data
- **Volume Upload**: Direct upload to Databricks Unity Catalog volumes
- **Delta Table Creation**: Automated Delta table creation with schema inference

### Data Management
- **Column Operations**: Delete columns via dropdown selection
- **Row Operations**: Add new rows or delete existing ones
- **Real-time Editing**: Edit data directly in the preview table
- **Data Validation**: Error handling for malformed CSV files
- **Schema Inference**: Automatic data type detection for Delta table creation

### Databricks Integration
- **Unity Catalog Support**: Full integration with Databricks Unity Catalog
- **Volume Management**: Upload files to specified catalog/schema/volume paths
- **SQL Generation**: Automatic SQL generation for Delta table creation
- **Warehouse Execution**: Execute SQL commands via Databricks SQL warehouses
- **Authentication**: Support for both Personal Access Tokens and OAuth tokens

## Installation

### Prerequisites
- Python 3.8 or higher
- Databricks workspace with Unity Catalog enabled
- Databricks SQL warehouse (for table creation)
- Valid Databricks authentication credentials

### Setup
1. Clone or download the application files
2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure your Databricks environment by setting environment variables or creating a `.env` file:
   ```bash
   export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   export DATABRICKS_TOKEN=your-personal-access-token
   export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
   ```

## Configuration

### Environment Variables
The application supports configuration through environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABRICKS_HOST` | Your Databricks workspace URL | Required |
| `DATABRICKS_TOKEN` | Personal Access Token | Required (or OAuth) |
| `DATABRICKS_OAUTH_TOKEN` | OAuth token (alternative to PAT) | Optional |
| `DATABRICKS_HTTP_PATH` | SQL warehouse HTTP path | Required for table creation |
| `DEFAULT_CATALOG` | Default Unity Catalog name | `ingest_demo` |
| `DEFAULT_SCHEMA` | Default schema name | `medical_providers` |
| `PORT` | Application port | `8050` |
| `HOST` | Application host | `0.0.0.0` |
| `DEBUG` | Enable debug mode | `true` |

### Configuration File
Modify `config.py` to set default values and validate your configuration:
```python
python config.py
```

## Usage

### Starting the Application
1. **Local Development**:
   ```bash
   python app.py
   ```
   Access the application at `http://localhost:8050`

2. **Databricks Apps Deployment**:
   The application is designed to run as a Databricks App with automatic port and host configuration.

### Workflow
1. **Upload CSV File**: Use the drag-and-drop interface to upload your CSV file
2. **Configure Parsing**: Set delimiter and header options as needed
3. **Preview & Edit**: Review data in the interactive table, make edits as necessary
4. **Set Databricks Configuration**: Specify catalog, schema, volume, and table names
5. **Upload to Volume**: Click "Upload to Volume" to store the file in Databricks
6. **Create Delta Table**: Click "Create Delta Table" to generate the SQL
7. **Execute SQL**: Run the generated SQL to create your Delta table

### Data Editing Features
- **Cell Editing**: Click any cell to edit its value
- **Row Management**: Use the X button to delete rows, or "Add Row" to insert new ones
- **Column Management**: Add columns with "Add Column" or delete via dropdown
- **Header Editing**: When headers are enabled, edit the first row to change column names
- **Undo Operations**: Use "Undo" for step-by-step reversal or "Revert" to restore original data

## Architecture

### File Structure
```
csv-uploader_app/
├── app.py                    # Main Dash application
├── databricks_csv_uploader.py # Core application logic and UI
├── config.py                 # Configuration management
├── requirements.txt          # Python dependencies
└── README.md                # This documentation
```

### Key Components
- **Frontend**: Dash-based responsive web interface with modern CSS styling
- **Backend**: Python data processing with pandas and Databricks SDK integration
- **Authentication**: Lazy-loading Databricks authentication with error handling
- **Data Storage**: In-memory data stores with undo/redo functionality
- **SQL Generation**: Dynamic SQL generation based on data schema inference

## Dependencies

### Core Dependencies
- **dash**: Web application framework
- **pandas**: Data manipulation and analysis
- **databricks-sdk**: Official Databricks SDK for Python
- **plotly**: Interactive visualizations and data tables

### Optional Dependencies
- **python-dotenv**: Environment variable management
- **black**: Code formatting
- **flake8**: Code linting
- **pytest**: Testing framework

## Error Handling

The application includes comprehensive error handling for:
- Invalid CSV file formats
- Databricks authentication failures
- Network connectivity issues
- SQL execution errors
- File upload failures
- Configuration validation errors

## Security Considerations

- Credentials are loaded from environment variables
- No sensitive data is logged or displayed in the UI
- File uploads are processed in memory without persistent storage
- SQL injection protection through parameterized queries
- HTTPS support for Databricks connections

## Troubleshooting

### Common Issues
1. **Authentication Errors**: Verify your `DATABRICKS_TOKEN` and `DATABRICKS_HOST` are correct
2. **Warehouse Not Found**: Ensure `DATABRICKS_HTTP_PATH` points to a valid SQL warehouse
3. **Permission Errors**: Check that your token has access to the specified catalog/schema/volume
4. **Large File Upload**: Files over 100MB may cause memory issues
5. **CSV Parsing Errors**: Verify delimiter settings match your file format

### Debug Mode
Enable debug mode by setting `DEBUG=true` in your environment variables for detailed error messages and logging.

## License

This project is provided as-is for educational and development purposes. Please review your organization's policies regarding data handling and Databricks usage before deploying in production environments.

## Support

For issues related to:
- **Databricks SDK**: Refer to the [official Databricks SDK documentation](https://docs.databricks.com/dev-tools/sdk-python.html)
- **Dash Framework**: See the [Dash documentation](https://dash.plotly.com/)
- **Unity Catalog**: Check the [Unity Catalog documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
