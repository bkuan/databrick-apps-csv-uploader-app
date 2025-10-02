import dash
from dash import dcc, html, Input, Output, State, callback, dash_table, no_update, ctx, ALL
from dash.exceptions import PreventUpdate
import pandas as pd
import base64
import io
import os
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import Databricks SDK
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementExecutionAPI, StatementState
    DATABRICKS_AVAILABLE = True
    logger.info("Databricks SDK available")
except ImportError:
    DATABRICKS_AVAILABLE = False
    logger.warning("Databricks SDK not available")

# Import config
try:
    import config
except ImportError:
    logger.warning("config.py not found, using defaults")
    class Config:
        DEFAULT_CATALOG = 'main'
        DEFAULT_SCHEMA = 'default'
        DEFAULT_VOLUME_PATH = '/Volumes/main/default/csv_uploads/'
        DATABRICKS_HTTP_PATH = '/sql/1.0/warehouses/your-warehouse-id'
    config = Config()

# Undo configuration
UNDO_LIMIT = 10  # Maximum number of undo steps to keep

# Databricks client - will be initialized when needed
w = None
_auth_attempted = False

def get_databricks_client():
    """Get Databricks client with lazy initialization"""
    global w, _auth_attempted
    
    if not DATABRICKS_AVAILABLE:
        return None
        
    if w is not None:
        return w
        
    if _auth_attempted:
        return None
        
    try:
        logger.info("Attempting Databricks authentication (lazy load)")
        w = WorkspaceClient()
        logger.info("✅ Successfully authenticated with Databricks")
        _auth_attempted = True
        return w
    except Exception as e:
        logger.warning(f"❌ Databricks authentication failed: {e}")
        _auth_attempted = True
        return None

def push_to_undo_stack(undo_stack, new_state):
    """Push new state to undo stack, maintaining size limit"""
    if not undo_stack:
        undo_stack = []
    
    # Add new state to the stack
    undo_stack.append(new_state)
    
    # Keep only the last UNDO_LIMIT states
    if len(undo_stack) > UNDO_LIMIT:
        undo_stack = undo_stack[-UNDO_LIMIT:]
    
    return undo_stack

def pop_from_undo_stack(undo_stack):
    """Pop the most recent state from undo stack"""
    if not undo_stack or len(undo_stack) == 0:
        return None, []
    
    # Pop the most recent state
    previous_state = undo_stack.pop()
    return previous_state, undo_stack

def get_undo_count(undo_stack):
    """Get the number of available undo steps"""
    if not undo_stack:
        return 0
    return len(undo_stack)

# Initialize the Dash app
app = dash.Dash(__name__, title="Databricks CSV Ingest to Volume and Delta Table")

# Custom CSS
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }
            .main-container {
                max-width: 1600px;
                margin: 0 auto;
                background: white;
                padding: 30px;
                border-radius: 12px;
                box-shadow: 0 8px 32px rgba(0,0,0,0.1);
            }
            .section-header {
                color: #2c3e50;
                border-bottom: 3px solid #3498db;
                padding-bottom: 8px;
                margin-bottom: 20px;
            }
            .upload-area {
                border: 2px dashed #3498db;
                border-radius: 8px;
                padding: 40px;
                text-align: center;
                background: #f8f9ff;
                transition: all 0.3s ease;
                cursor: pointer;
            }
            .upload-area:hover {
                border-color: #2980b9;
                background: #e3f2fd;
            }
            .btn-primary {
                background: #3498db;
                color: white;
                border: none;
                padding: 12px 24px;
                border-radius: 6px;
                cursor: pointer;
                margin: 5px;
                font-weight: 500;
            }
            .btn-secondary {
                background: #95a5a6;
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 6px;
                cursor: pointer;
                margin: 5px;
            }
            .status-success {
                background: #d4edda;
                color: #155724;
                padding: 12px;
                border-radius: 6px;
                border-left: 4px solid #28a745;
                margin: 16px 0;
            }
            .status-error {
                background: #f8d7da;
                color: #721c24;
                padding: 12px;
                border-radius: 6px;
                border-left: 4px solid #dc3545;
                margin: 16px 0;
            }
            #action-buttons {
                text-align: center;
                margin: 30px 0;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# App layout
app.layout = html.Div(className="main-container", children=[
    # Data stores
    dcc.Store(id='file-data-store'),
    dcc.Store(id='csv-data-store'),
    dcc.Store(id='sql-query-store'),
    dcc.Store(id='original-data-store'),  # Store original uploaded data for revert
    dcc.Store(id='undo-stack-store'),     # Store stack of previous states for multi-step undo
    
    # Header
    html.H1([
        html.I(className="fas fa-database", style={"marginRight": "12px"}),
        "Databricks CSV to Delta Table"
    ], style={"textAlign": "center", "color": "#2c3e50", "marginBottom": "30px"}),
    
    # Authentication status
    html.Div(id='auth-status'),
    
    # File upload section
    html.Div(id='upload-section', children=[
        html.H3("📁 Upload CSV File", className="section-header"),
        dcc.Upload(
            id='upload-data',
            children=html.Div([
                html.I(className="fas fa-cloud-upload-alt", style={"fontSize": "48px", "color": "#3498db", "marginBottom": "16px"}),
                html.Div("Drag and Drop CSV files here or click to browse"),
                html.Small("Supports .csv files up to 100MB", style={"color": "#666"})
            ]),
            className="upload-area",
            multiple=False,
            accept='.csv'
        ),
    ]),
    
    # CSV configuration
    html.Div(id='config-section', style={'display': 'none'}, children=[
        html.H3("⚙️ CSV Configuration", className="section-header"),
        html.Div([
            html.Label("Delimiter:", style={"fontWeight": "500", "marginRight": "10px"}),
            dcc.Dropdown(
                id='delimiter',
                options=[
                    {'label': 'Comma (,)', 'value': ','},
                    {'label': 'Semicolon (;)', 'value': ';'},
                    {'label': 'Tab', 'value': '\\t'},
                    {'label': 'Pipe (|)', 'value': '|'}
                ],
                value=',',
                style={'width': '200px', 'display': 'inline-block'}
            ),
        ], style={'marginBottom': '15px'}),
        
        html.Div([
            html.Label("Header Options:", style={"fontWeight": "500", "display": "block", "marginBottom": "8px"}),
            dcc.Checklist(
                id='has-header',
                options=[{'label': ' ✅ First row contains headers', 'value': 'header'}],
                value=['header'],
                style={"fontSize": "16px", "marginBottom": "10px"}
            )
        ], style={'marginBottom': '20px', 'padding': '15px', 'border': '2px solid #3498db', 'borderRadius': '8px', 'backgroundColor': '#f8f9fa'}),
    ]),
    
    # CSV preview section
    html.Div(id='preview-section'),
    
    # Hidden placeholder components to prevent callback errors
    html.Div([
        dcc.Dropdown(id='column-delete-dropdown', options=[], style={'display': 'none'}),
        html.Button("Delete", id='delete-column-btn', n_clicks=0, style={'display': 'none'})
    ], style={'display': 'none'}),
    
    # Databricks configuration
    html.Div(id='databricks-config-section', style={'display': 'none'}, children=[
        html.H3("🏢 Databricks Configuration", className="section-header"),
        
        # Row 1: Catalog and Schema side by side
        html.Div([
            html.Div([
                html.Label("Catalog:", style={"fontWeight": "500", "display": "block", "marginBottom": "8px"}),
                dcc.Input(
                    id='catalog',
                    type='text',
                    value=getattr(config, 'DEFAULT_CATALOG', 'ingest_demo'),
                    style={"width": "100%", "padding": "10px", "borderRadius": "6px", "border": "1px solid #ddd"}
                ),
            ], style={'width': '48%', 'display': 'inline-block', 'marginRight': '4%'}),
            
            html.Div([
                html.Label("Schema:", style={"fontWeight": "500", "display": "block", "marginBottom": "8px"}),
                dcc.Input(
                    id='schema',
                    type='text',
                    value=getattr(config, 'DEFAULT_SCHEMA', 'medical_practice'),
                    style={"width": "100%", "padding": "10px", "borderRadius": "6px", "border": "1px solid #ddd"}
                ),
            ], style={'width': '48%', 'display': 'inline-block'}),
        ], style={'marginBottom': '20px'}),
        
        # Row 2: Volume and Volume Path side by side
        html.Div([
            html.Div([
                html.Label("Volume:", style={"fontWeight": "500", "display": "block", "marginBottom": "8px"}),
                dcc.Input(
                    id='volume',
                    type='text',
                    value='providers',
                    placeholder='e.g., providers',
                    style={"width": "100%", "padding": "10px", "borderRadius": "6px", "border": "1px solid #ddd"}
                ),
            ], style={'width': '35%', 'display': 'inline-block', 'marginRight': '4%'}),
            
            html.Div([
                html.Label("Volume Path:", style={"fontWeight": "500", "display": "block", "marginBottom": "8px", "color": "#666"}),
                dcc.Input(
                    id='volume-path',
                    type='text',
                    value='/Volumes/ingest_demo/medical_practice/providers/',
                    readOnly=True,
                    style={
                        "width": "100%", 
                        "padding": "10px", 
                        "borderRadius": "6px", 
                        "border": "1px solid #ddd",
                        "backgroundColor": "#f8f9fa",
                        "color": "#666"
                    }
                ),
            ], style={'width': '61%', 'display': 'inline-block'}),
        ], style={'marginBottom': '20px'}),
        
        # Row 3: Upload File Name (full width)
        html.Div([
            html.Label("📄 Upload File Name:", style={"fontWeight": "600", "display": "block", "marginBottom": "8px", "fontSize": "16px", "color": "#28a745"}),
            dcc.Input(
                id='upload-filename',
                type='text',
                placeholder='Enter filename for volume (e.g., customer_data.csv)',
                style={
                    "width": "100%", 
                    "padding": "12px", 
                    "borderRadius": "6px", 
                    "border": "2px solid #28a745",
                    "fontSize": "15px"
                }
            ),
        ], style={'marginBottom': '15px'}),
        
        # Row 4: Upload to Volume button
        html.Div([
            html.Button([
                html.I(className="fas fa-upload", style={"marginRight": "8px"}),
                "📤 Upload to Volume"
            ], id='upload-btn', n_clicks=0, className="btn-primary", 
               style={'marginBottom': '20px'}),
        ], style={'textAlign': 'left'}),
        
        # Row 5: Table Name (full width)
        html.Div([
            html.Label("🏷️ Table Name:", style={"fontWeight": "600", "display": "block", "marginBottom": "8px", "fontSize": "16px", "color": "#3498db"}),
            dcc.Input(
                id='table-name',
                type='text',
                placeholder='Enter table name (e.g., customer_data)',
                style={
                    "width": "100%", 
                    "padding": "12px", 
                    "borderRadius": "6px", 
                    "border": "2px solid #3498db",
                    "fontSize": "15px"
                }
            ),
        ], style={'marginBottom': '15px'}),
        
        # Row 6: Create Delta Table button
        html.Div([
            html.Button([
                html.I(className="fas fa-table", style={"marginRight": "8px"}),
                "🏗️ Create Delta Table"
            ], id='create-table-btn', n_clicks=0, className="btn-primary"),
        ], style={'textAlign': 'left'}),
    ]),
    
    # SQL execution button (separate from configuration)
    html.Div(id='action-buttons', style={'display': 'none'}, children=[
        html.Button([
            html.I(className="fas fa-play-circle", style={"marginRight": "8px"}),
            "▶️ Execute SQL"
        ], id='execute-sql-btn', n_clicks=0, className="btn-secondary", style={"display": "none"}),
    ]),
    
    # Status messages
    html.Div(id='status-messages'),
    
    # Hidden placeholder components for callbacks (prevent connection errors)
    html.Button(id='remove-file-btn', style={'display': 'none'}),
    html.Button(id='undo-btn', style={'display': 'none'}),
    html.Button(id='revert-btn', style={'display': 'none'}),
    dash_table.DataTable(id='preview-table', data=[], columns=[], style_table={'display': 'none'}),
    html.Button(id='add-row-btn', style={'display': 'none'}),
    html.Button(id='add-col-btn', style={'display': 'none'}),
])

# Authentication status callback
@callback(
    Output('auth-status', 'children'),
    Input('auth-status', 'id')
)
def show_auth_status(_):
    """Display Databricks authentication status"""
    if not DATABRICKS_AVAILABLE:
        return html.Div([
            html.I(className="fas fa-exclamation-triangle", style={"marginRight": "8px"}),
            "⚠️ Databricks SDK not installed. Run: pip install databricks-sdk"
        ], className="status-error")
    
    if w is None: # This check is for the initial state before any Databricks action
        return html.Div([
            html.I(className="fas fa-info-circle", style={"marginRight": "8px"}),
            "🔄 Databricks ready (will authenticate when uploading or creating tables)"
        ], style={
            'backgroundColor': '#d1ecf1', 
            'color': '#0c5460',
            'padding': '12px',
            'borderRadius': '6px',
            'border': '1px solid #bee5eb',
            'margin': '16px 0'
        })
    
    return html.Div([
        html.I(className="fas fa-check-circle", style={"marginRight": "8px"}),
        "✅ Connected to Databricks successfully"
    ], className="status-success")

# Volume Path auto-population callback
@callback(
    Output('volume-path', 'value'),
    [Input('catalog', 'value'),
     Input('schema', 'value'),
     Input('volume', 'value')]
)
def update_volume_path(catalog, schema, volume):
    """Auto-populate volume path based on catalog, schema, and volume inputs"""
    if catalog and schema and volume:
        return f"/Volumes/{catalog}/{schema}/{volume}/"
    elif catalog and schema:
        return f"/Volumes/{catalog}/{schema}/"
    elif catalog:
        return f"/Volumes/{catalog}/"
    else:
        return "/Volumes/"

# File upload callback
@callback(
    [Output('file-data-store', 'data'),
     Output('csv-data-store', 'data'),
     Output('original-data-store', 'data'),
     Output('undo-stack-store', 'data'),
     Output('upload-section', 'style'),
     Output('config-section', 'style'),
     Output('databricks-config-section', 'style'),
     Output('action-buttons', 'style'),
     Output('preview-section', 'children'),
     Output('table-name', 'value'),
     Output('upload-filename', 'value')],
    Input('upload-data', 'contents'),
    State('upload-data', 'filename'),
    State('delimiter', 'value'),
    State('has-header', 'value')
)
def process_upload(contents, filename, delimiter, has_header):
    if contents is None:
        raise PreventUpdate
    
    try:
        # Parse delimiter
        actual_delimiter = delimiter if delimiter != '\\t' else '\t'
        has_header_bool = 'header' in (has_header or [])
        
        # Decode file content
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        
        # Read CSV
        df = pd.read_csv(
            io.StringIO(decoded.decode('utf-8')),
            delimiter=actual_delimiter,
            header=0 if has_header_bool else None
        )
        
        if not has_header_bool:
            df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
        
        # Generate table name
        table_name = os.path.splitext(filename)[0].lower().replace(' ', '_').replace('-', '_')
        table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
        
        # Store data
        file_data = {'contents': contents, 'filename': filename}
        csv_data = {'data': df.to_dict('records'), 'columns': df.columns.tolist()}
        
        # Create preview with header setting
        has_header_bool = 'header' in (has_header or [])
        preview = create_preview_table(df, filename=filename, use_first_row_as_header=has_header_bool)
        
        # Hide upload section, show all other sections
        return (
            file_data,
            csv_data,
            csv_data,  # original-data-store (save original data for revert)
            [],        # undo-stack-store (initialize empty undo stack)
            {'display': 'none'},  # upload-section (hide after successful upload)
            {'display': 'block'},  # config-section
            {'display': 'block'},  # databricks-config-section  
            {'display': 'block'},  # action-buttons
            preview,
            table_name,
            filename  # upload-filename
        )
        
    except Exception as e:
        error_preview = html.Div([
            html.H4("Error reading CSV file", style={"color": "#dc3545"}),
            html.P(f"Error: {str(e)}", style={"color": "#721c24"}),
        ], className="status-error")
        
        return no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update, error_preview, no_update, no_update

def create_preview_table(df, filename=None, use_first_row_as_header=True):
    """Create a preview table with editing capabilities"""
    
    # Limit preview to first 20 rows  
    display_df = df.head(20)
    
    # Use first row values as column headers if enabled
    if use_first_row_as_header and len(df) > 0:
        # Get first row values as header names
        header_names = []
        first_row = df.iloc[0]
        for i, col in enumerate(df.columns):
            header_value = str(first_row[col]) if pd.notna(first_row[col]) else f"Column_{i+1}"
            # Truncate very long headers
            if len(header_value) > 50:
                header_value = header_value[:47] + "..."
            header_names.append(header_value)
        
        columns = [{"name": header_names[i], "id": col, "editable": True} for i, col in enumerate(df.columns)]
        print(f"DEBUG: Created headers from first row: {header_names}")
        print(f"DEBUG: First row KEPT in data so headers can be edited by editing first row")
    else:
        columns = [{"name": col, "id": col, "editable": True} for col in df.columns]
        print(f"DEBUG: Using original column names: {list(df.columns)}")
    
    # Create editable data table
    table = dash_table.DataTable(
        id='preview-table',
        data=display_df.to_dict('records'),
        columns=columns,
        editable=True,
        row_deletable=True,
        sort_action="native",
        page_size=20,
        style_cell={
            'textAlign': 'left', 
            'padding': '12px', 
            'fontFamily': 'Arial, sans-serif',
            'fontSize': '14px',
            'minWidth': '120px',
            'width': '120px',
            'maxWidth': '300px',
            'whiteSpace': 'normal',
            'height': 'auto',
        },
        style_header={
            'backgroundColor': '#3498db', 
            'color': 'white', 
            'fontWeight': 'bold',
            'textAlign': 'center',
            'padding': '15px',
            'fontSize': '15px'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': '#f8f9fa'
            }
        ] + ([{
            'if': {'row_index': 0},
            'backgroundColor': '#fff3cd',
            'border': '2px solid #ffc107',
            'fontWeight': 'bold'
        }] if use_first_row_as_header else []),
        style_table={'overflowX': 'auto'}
    )
    
    # Build the preview content
    preview_content = [
        # Header section with filename and remove button
        html.Div([
            html.Div([
                html.H3("📊 CSV Preview & Editor", className="section-header", style={'margin': '0', 'display': 'inline-block'}),
                html.Span(f" - {filename}" if filename else "", style={'color': '#666', 'fontSize': '1.1rem', 'marginLeft': '10px'})
            ], style={'display': 'inline-block'}),
            html.Div([
                html.Button("↶ Undo", id='undo-btn', n_clicks=0, className="btn-secondary", 
                           title="Undo last change",
                           style={'fontSize': '14px', 'padding': '8px 16px', 'marginRight': '8px', 'width': '180px', 'height': '36px'}),
                html.Button("🔄 Revert to Original", id='revert-btn', n_clicks=0, className="btn-secondary", 
                           title="Restore original uploaded data",
                           style={'fontSize': '14px', 'padding': '8px 16px', 'marginRight': '8px', 'width': '180px', 'height': '36px'}),
                html.Button("🗑️ Remove File", id='remove-file-btn', n_clicks=0, className="btn-secondary", 
                           style={'fontSize': '14px', 'padding': '8px 16px', 'width': '180px', 'height': '36px'})
            ], style={'display': 'inline-block', 'float': 'right'})
        ], style={'marginBottom': '15px', 'overflow': 'hidden'}),
        
        # Instructions  
        html.P([
            "✏️ Edit cells directly, delete rows with X, or add new data. ",
            "🎯 Edit the FIRST ROW (highlighted in yellow) to change column headers!" if use_first_row_as_header 
            else "The first row automatically serves as column headers."
        ], style={'backgroundColor': '#e7f3ff', 'padding': '10px', 'borderRadius': '5px', 'border': '1px solid #3498db'} if use_first_row_as_header else {}),
        
        # The data table
        table,
        
        # Action buttons for editing
        html.Div([
            # Left spacer to help center the add buttons
            html.Div(style={'width': '33%', 'display': 'inline-block'}),
            
            # Center: Add Row and Add Column buttons
            html.Div([
                html.Button("➕ Add Row", id='add-row-btn', n_clicks=0, className="btn-secondary",
                           style={'fontSize': '14px', 'padding': '8px 16px', 'marginRight': '8px', 'height': '36px'}),
                html.Button("➕ Add Column", id='add-col-btn', n_clicks=0, className="btn-secondary",
                           style={'fontSize': '14px', 'padding': '8px 16px', 'height': '36px'}),
            ], style={'width': '34%', 'display': 'inline-block', 'textAlign': 'center'}),
            
            # Right: Column deletion section
            html.Div([
                html.Div([
                    html.Span("Select Column to Delete", style={
                        'fontSize': '14px', 
                        'color': '#666', 
                        'marginRight': '10px',
                        'lineHeight': '36px',  # Match dropdown height
                        'verticalAlign': 'middle'
                    }),
                    dcc.Dropdown(
                        id='column-delete-dropdown',
                        placeholder="Select column...",
                        style={'width': '200px', 'marginRight': '8px'},
                        options=[{'label': col, 'value': col} for col in df.columns]  # Populate with current columns
                    ),
                    html.Button("Delete", id='delete-column-btn', n_clicks=0, className="btn-danger", 
                               style={'fontSize': '14px', 'padding': '8px 16px', 'height': '36px', 'verticalAlign': 'middle'})
                ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'flex-end'})
            ], style={'width': '33%', 'display': 'inline-block'})
        ], style={'margin': '15px 0', 'width': '100%'})
    ]
    
    return html.Div(preview_content)

# Update CSV when delimiter or header settings change
@callback(
    [Output('csv-data-store', 'data', allow_duplicate=True),
     Output('preview-section', 'children', allow_duplicate=True)],
    [Input('delimiter', 'value'),
     Input('has-header', 'value')],
    State('file-data-store', 'data'),
    prevent_initial_call=True
)
def update_on_delimiter_change(delimiter, has_header, file_data):
    """Re-process CSV when delimiter or header settings change"""
    trigger_id = ctx.triggered[0]['prop_id'] if ctx.triggered else 'unknown'
    print(f"DEBUG: Header update callback triggered by {trigger_id} - delimiter={delimiter}, has_header={has_header}, file_data={file_data is not None}")
    
    # Skip initial call when both inputs are None
    if delimiter is None and has_header is None:
        print("DEBUG: Initial call, skipping")
        raise PreventUpdate
        
    if not file_data:
        print("DEBUG: No file data, preventing update")
        raise PreventUpdate
    
    try:
        # Parse delimiter
        actual_delimiter = delimiter if delimiter != '\\t' else '\t'
        has_header_bool = 'header' in (has_header or [])
        print(f"DEBUG: Processing with delimiter='{actual_delimiter}', has_header={has_header_bool}")
        
        # Decode file content
        content_type, content_string = file_data['contents'].split(',')
        decoded = base64.b64decode(content_string)
        
        # Read CSV with new settings
        df = pd.read_csv(
            io.StringIO(decoded.decode('utf-8')),
            delimiter=actual_delimiter,
            header=0 if has_header_bool else None
        )
        
        if not has_header_bool:
            df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
        
        # Store updated data
        csv_data = {'data': df.to_dict('records'), 'columns': df.columns.tolist()}
        
        # Create updated preview with header setting
        preview = create_preview_table(df, filename=file_data['filename'], use_first_row_as_header=has_header_bool)
        print(f"DEBUG: Successfully updated preview with {len(df)} rows, {len(df.columns)} columns, headers={has_header_bool}")
        
        return csv_data, preview
        
    except Exception as e:
        print(f"DEBUG: Error in header update callback: {str(e)}")
        error_preview = html.Div([
            html.H4("Error processing CSV", style={"color": "#dc3545"}),
            html.P(f"Error: {str(e)}", style={"color": "#721c24"}),
        ], className="status-error")
        
        return no_update, error_preview

# Add row callback
@callback(
    [Output('csv-data-store', 'data', allow_duplicate=True),
     Output('undo-stack-store', 'data', allow_duplicate=True),
     Output('preview-section', 'children', allow_duplicate=True)],
    Input('add-row-btn', 'n_clicks'),
    [State('csv-data-store', 'data'),
     State('undo-stack-store', 'data'),
     State('file-data-store', 'data')],
    prevent_initial_call=True
)
def add_row(n_clicks, csv_data, undo_stack, file_data):
    """Add a new empty row to the data"""
    if n_clicks == 0 or not csv_data:
        raise PreventUpdate
    
    # Push current state to undo stack
    updated_stack = push_to_undo_stack(undo_stack, csv_data.copy() if csv_data else {})
    
    # Add empty row
    new_row = {col: '' for col in csv_data['columns']}
    csv_data['data'].append(new_row)
    
    # Create updated preview (default to headers enabled for consistency)
    df = pd.DataFrame(csv_data['data'])
    preview = create_preview_table(df, filename=file_data.get('filename'), use_first_row_as_header=True)
    
    return csv_data, updated_stack, preview

# Add column callback
@callback(
    [Output('csv-data-store', 'data', allow_duplicate=True),
     Output('undo-stack-store', 'data', allow_duplicate=True),
     Output('preview-section', 'children', allow_duplicate=True)],
    Input('add-col-btn', 'n_clicks'),
    [State('csv-data-store', 'data'),
     State('undo-stack-store', 'data'),
     State('file-data-store', 'data')],
    prevent_initial_call=True
)
def add_column(n_clicks, csv_data, undo_stack, file_data):
    """Add a new empty column to the data"""
    if n_clicks == 0 or not csv_data:
        raise PreventUpdate
    
    # Push current state to undo stack
    updated_stack = push_to_undo_stack(undo_stack, csv_data.copy() if csv_data else {})
    
    # Create new column name
    new_col_name = f"New_Column_{len(csv_data['columns']) + 1}"
    
    # Add column to schema
    csv_data['columns'].append(new_col_name)
    
    # Add empty values to all rows
    for row in csv_data['data']:
        row[new_col_name] = ''
    
    # Create updated preview (default to headers enabled for consistency)
    df = pd.DataFrame(csv_data['data'])
    preview = create_preview_table(df, filename=file_data.get('filename'), use_first_row_as_header=True)
    
    return csv_data, updated_stack, preview

# Update CSV data when table is edited and refresh headers if needed
@callback(
    [Output('csv-data-store', 'data', allow_duplicate=True),
     Output('undo-stack-store', 'data', allow_duplicate=True),
     Output('preview-section', 'children', allow_duplicate=True)],
    Input('preview-table', 'data'),
    [State('csv-data-store', 'data'),
     State('undo-stack-store', 'data'),
     State('file-data-store', 'data'),
     State('has-header', 'value')],
    prevent_initial_call=True
)
def update_csv_data_with_headers(table_data, csv_data, undo_stack, file_data, has_header):
    """Update CSV data when table is edited and refresh headers if first row changed"""
    print(f"DEBUG: Table edit callback triggered - rows: {len(table_data) if table_data else 0}")
    
    if not table_data or not csv_data:
        print("DEBUG: No table data or csv data, preventing update")
        raise PreventUpdate
    
    # Push current state to undo stack before making changes
    updated_stack = push_to_undo_stack(undo_stack, csv_data.copy() if csv_data else {})
    
    # Convert table data back to CSV data format
    df = pd.DataFrame(table_data)
    print(f"DEBUG: Updated CSV data - {len(df)} rows, {len(df.columns)} columns")
    
    csv_data['data'] = df.to_dict('records')
    csv_data['columns'] = df.columns.tolist()
    
    # If headers are enabled, refresh the preview AND update column names based on first row
    has_header_bool = 'header' in (has_header or [])
    if has_header_bool and len(df) > 0:
        print("DEBUG: Headers enabled - updating column names AND refreshing preview")
        
        # Update column names in stored CSV data to match first row values
        if len(df) > 0:
            first_row = df.iloc[0]
            new_column_names = []
            for i, col in enumerate(df.columns):
                header_value = str(first_row[col]) if pd.notna(first_row[col]) else f"Column_{i+1}"
                new_column_names.append(header_value)
            
            # Update the DataFrame with new column names
            df_renamed = df.copy()
            df_renamed.columns = new_column_names
            
            # Update stored data with new column names
            csv_data['data'] = df_renamed.to_dict('records')
            csv_data['columns'] = new_column_names
            
            print(f"DEBUG: Updated column names to: {new_column_names[:5]}...") # Show first 5
        
        filename = file_data.get('filename', 'data.csv') if file_data else 'data.csv'
        preview = create_preview_table(df, filename=filename, use_first_row_as_header=True)
        return csv_data, updated_stack, preview  # Store previous state in undo stack
    else:
        # Headers disabled or no data - just update CSV data
        return csv_data, updated_stack, no_update  # Store previous state in undo stack

# Remove file callback
@callback(
    [Output('upload-section', 'style', allow_duplicate=True),
     Output('config-section', 'style', allow_duplicate=True),
     Output('databricks-config-section', 'style', allow_duplicate=True),
     Output('action-buttons', 'style', allow_duplicate=True),
     Output('preview-section', 'children', allow_duplicate=True),
     Output('file-data-store', 'data', allow_duplicate=True),
     Output('csv-data-store', 'data', allow_duplicate=True),
     Output('original-data-store', 'data', allow_duplicate=True),
     Output('undo-stack-store', 'data', allow_duplicate=True),
     Output('upload-data', 'contents', allow_duplicate=True),
     Output('status-messages', 'children', allow_duplicate=True),
     Output('upload-filename', 'value', allow_duplicate=True),
     Output('table-name', 'value', allow_duplicate=True)],
    Input('remove-file-btn', 'n_clicks'),
    prevent_initial_call=True
)
def remove_file(n_clicks):
    """Remove the current file and reset to upload state"""
    if n_clicks == 0:
        raise PreventUpdate
    
    return (
        {'display': 'block'},   # upload-section (show)
        {'display': 'none'},    # config-section (hide)
        {'display': 'none'},    # databricks-config-section (hide)
        {'display': 'none'},    # action-buttons (hide)
        html.Div(),             # preview-section (clear)
        {},                     # file-data-store (clear)
        {},                     # csv-data-store (clear)
        {},                     # original-data-store (clear)
        [],                     # undo-stack-store (clear undo stack)
        None,                   # upload-data contents (clear file input)
        html.Div(),             # status-messages (clear)
        '',                     # upload-filename (clear)
        ''                      # table-name (clear)
    )

# Undo callback - multi-step undo using undo stack
@callback(
    [Output('csv-data-store', 'data', allow_duplicate=True),
     Output('undo-stack-store', 'data', allow_duplicate=True),
     Output('preview-section', 'children', allow_duplicate=True),
     Output('status-messages', 'children', allow_duplicate=True)],
    Input('undo-btn', 'n_clicks'),
    [State('csv-data-store', 'data'),
     State('undo-stack-store', 'data'),
     State('file-data-store', 'data')],
    prevent_initial_call=True
)
def undo_changes(n_clicks, current_data, undo_stack, file_data):
    """Undo last change by restoring previous state from stack"""
    if n_clicks == 0:
        raise PreventUpdate
    
    # Pop the most recent state from the stack
    previous_state, updated_stack = pop_from_undo_stack(undo_stack)
    
    if previous_state is None:
        return no_update, no_update, no_update, html.Div("⚠️ No previous state to undo", className="status-warning")
    
    try:
        # Restore the previous state
        df = pd.DataFrame(previous_state['data'])
        filename = file_data.get('filename') if file_data else None
        preview = create_preview_table(df, filename=filename, use_first_row_as_header=True)
        
        # Show remaining undo steps
        remaining_steps = get_undo_count(updated_stack)
        if remaining_steps > 0:
            status_msg = html.Div(f"↶ Changes undone successfully ({remaining_steps} more undo steps available)", className="status-success")
        else:
            status_msg = html.Div("↶ Changes undone successfully (no more undo steps)", className="status-success")
        
        return (
            previous_state,  # Restore previous CSV data
            updated_stack,   # Updated undo stack (with one item removed)
            preview,         # Updated preview
            status_msg       # Status with remaining steps info
        )
        
    except Exception as e:
        return no_update, no_update, no_update, html.Div(f"❌ Undo failed: {str(e)}", className="status-error")

# Revert to original callback - restores the originally uploaded data
@callback(
    [Output('csv-data-store', 'data', allow_duplicate=True),
     Output('undo-stack-store', 'data', allow_duplicate=True),
     Output('preview-section', 'children', allow_duplicate=True),
     Output('status-messages', 'children', allow_duplicate=True)],
    Input('revert-btn', 'n_clicks'),
    [State('csv-data-store', 'data'),
     State('undo-stack-store', 'data'),
     State('original-data-store', 'data'),
     State('file-data-store', 'data')],
    prevent_initial_call=True
)
def revert_to_original(n_clicks, current_data, undo_stack, original_data, file_data):
    """Revert to the original uploaded data"""
    if n_clicks == 0 or not original_data or not original_data.get('data'):
        return no_update, no_update, no_update, html.Div("⚠️ No original data to revert to", className="status-warning")
    
    try:
        # Push current state to undo stack before reverting
        updated_stack = push_to_undo_stack(undo_stack, current_data) if current_data else undo_stack
        
        # Restore the original data
        df = pd.DataFrame(original_data['data'])
        filename = file_data.get('filename') if file_data else None
        preview = create_preview_table(df, filename=filename, use_first_row_as_header=True)
        
        return (
            original_data,  # Restore original CSV data
            updated_stack,  # Updated undo stack (with current state added)
            preview,        # Updated preview
            html.Div("🔄 Reverted to original data successfully", className="status-success")
        )
        
    except Exception as e:
        return no_update, no_update, no_update, html.Div(f"❌ Revert failed: {str(e)}", className="status-error")

# Update undo button text with available steps
@callback(
    Output('undo-btn', 'children'),
    Input('undo-stack-store', 'data'),
    prevent_initial_call=False
)
def update_undo_button_text(undo_stack):
    """Update undo button text to show available steps"""
    steps_available = get_undo_count(undo_stack)
    if steps_available > 0:
        return f"↶ Undo ({steps_available})"
    else:
        return "↶ Undo"

# Populate column deletion dropdown options
@callback(
    Output('column-delete-dropdown', 'options'),
    Input('csv-data-store', 'data'),
    prevent_initial_call=True
)
def update_column_dropdown_options(csv_data):
    """Update dropdown options with current column names"""
    if not csv_data or not csv_data.get('columns'):
        return []
    
    # Create options from column names
    options = [{'label': col, 'value': col} for col in csv_data['columns']]
    return options

# Column deletion callback
@callback(
    [Output('csv-data-store', 'data', allow_duplicate=True),
     Output('undo-stack-store', 'data', allow_duplicate=True),
     Output('preview-section', 'children', allow_duplicate=True),
     Output('status-messages', 'children', allow_duplicate=True),
     Output('column-delete-dropdown', 'value')],
    Input('delete-column-btn', 'n_clicks'),
    [State('column-delete-dropdown', 'value'),
     State('csv-data-store', 'data'),
     State('undo-stack-store', 'data'),
     State('file-data-store', 'data')],
    prevent_initial_call=True
)
def delete_column_dropdown(n_clicks, selected_column, csv_data, undo_stack, file_data):
    """Delete selected column from dropdown"""
    if not n_clicks or not selected_column or not csv_data:
        raise PreventUpdate
    
    # Prevent removing the last column
    if len(csv_data['columns']) <= 1:
        return no_update, no_update, no_update, html.Div(
            "❌ Cannot remove the last remaining column!", 
            className="status-error"
        ), None
    
    # Check if selected column exists
    if selected_column not in csv_data['columns']:
        return no_update, no_update, no_update, html.Div(
            f"❌ Column '{selected_column}' not found!", 
            className="status-error"
        ), None
    
    try:
        # Push current state to undo stack BEFORE making changes
        updated_stack = push_to_undo_stack(undo_stack, csv_data.copy() if csv_data else {})
        
        # Remove column from schema
        csv_data['columns'] = [col for col in csv_data['columns'] if col != selected_column]
        
        # Remove column from all data rows
        for row in csv_data['data']:
            if selected_column in row:
                del row[selected_column]
        
        # Create updated preview
        df = pd.DataFrame(csv_data['data'])
        preview = create_preview_table(df, filename=file_data.get('filename') if file_data else None, use_first_row_as_header=True)
        
        # Success message
        success_msg = html.Div(
            f"✅ Successfully removed column '{selected_column}' (undo available)", 
            className="status-success"
        )
        
        return csv_data, updated_stack, preview, success_msg, None  # Clear dropdown selection
        
    except Exception as e:
        return no_update, no_update, no_update, html.Div(f"❌ Column deletion failed: {str(e)}", className="status-error"), no_update

# Upload to volume callback
@callback(
    Output('status-messages', 'children', allow_duplicate=True),
    Input('upload-btn', 'n_clicks'),
    [State('csv-data-store', 'data'),
     State('upload-filename', 'value'),
     State('volume-path', 'value')],
    prevent_initial_call=True
)
def upload_to_volume(n_clicks, csv_data, upload_filename, volume_path):
    """Upload processed CSV to Databricks volume"""
    print(f"DEBUG: Upload button clicked - csv_data has {len(csv_data.get('data', [])) if csv_data else 0} rows")
    
    if n_clicks == 0 or not csv_data:
        raise PreventUpdate
    
    w = get_databricks_client()
    if not w:
        return html.Div("❌ Databricks connection not available", className="status-error")
    
    try:
        # Convert CSV data back to DataFrame and then to CSV content
        df = pd.DataFrame(csv_data['data'])
        print(f"DEBUG: Uploading DataFrame with {len(df)} rows, {len(df.columns)} columns")
        print(f"DEBUG: First row data: {df.iloc[0].to_dict() if len(df) > 0 else 'No data'}")
        
        csv_content = df.to_csv(index=False)
        
        # Determine filename
        filename = upload_filename or "uploaded_data"
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        # Construct full path
        full_path = f"{volume_path.rstrip('/')}/{filename}"
        
        # Upload to volume
        w.files.upload(full_path, csv_content.encode('utf-8'))
        
        return html.Div(f"✅ Successfully uploaded {filename} to {volume_path}", className="status-success")
        
    except Exception as e:
        return html.Div(f"❌ Upload failed: {str(e)}", className="status-error")

# Create delta table SQL callback  
@callback(
    [Output('status-messages', 'children', allow_duplicate=True),
     Output('execute-sql-btn', 'style', allow_duplicate=True)],
    Input('create-table-btn', 'n_clicks'),
    [State('csv-data-store', 'data'),
     State('table-name', 'value'),
     State('upload-filename', 'value'),
     State('volume-path', 'value')],
    prevent_initial_call=True
)
def create_delta_table_sql(n_clicks, csv_data, table_name, upload_filename, volume_path):
    """Generate SQL to create Delta table"""
    if n_clicks == 0 or not csv_data:
        raise PreventUpdate
    
    try:
        # Generate DataFrame for schema inference
        df = pd.DataFrame(csv_data['data'])
        
        # Determine filename and table name
        filename = upload_filename or "uploaded_data"
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        table_name_final = table_name or filename.replace('.csv', '').replace(' ', '_').replace('-', '_').lower()
        
        # Infer schema from DataFrame
        columns_sql = []
        for col_name, dtype in zip(df.columns, df.dtypes):
            if dtype == 'object':
                sql_type = 'STRING'
            elif dtype in ['int64', 'int32']:
                sql_type = 'BIGINT'
            elif dtype in ['float64', 'float32']:
                sql_type = 'DOUBLE'
            else:
                sql_type = 'STRING'
            
            columns_sql.append(f"`{col_name}` {sql_type}")
        
        # Build SQL query
        columns_def = ',\n  '.join(columns_sql)
        location_path = f"{volume_path.rstrip('/')}/{filename}"
        
        sql_query = f"""CREATE TABLE {table_name_final} (
  {columns_def}
)
USING DELTA
LOCATION '{location_path}'"""
        
        # Store the SQL query for execution
        app.server.sql_query = sql_query
        
        return (
            html.Div([
                html.H4("🎯 Generated SQL Query:", style={"color": "#28a745"}),
                html.Pre(sql_query, style={
                    "backgroundColor": "#f8f9fa", 
                    "padding": "15px", 
                    "borderRadius": "5px",
                    "border": "1px solid #dee2e6",
                    "overflow": "auto",
                    "fontSize": "14px"
                }),
                html.P("👆 Click 'Execute SQL' to create the Delta table", style={"marginTop": "10px", "color": "#666"})
            ], className="status-info"),
            {"display": "inline-block"}  # Show execute button
        )
        
    except Exception as e:
        return (
            html.Div(f"❌ Error generating SQL: {str(e)}", className="status-error"),
            {"display": "none"}  # Hide execute button
        )

# Execute SQL callback
@callback(
    Output('status-messages', 'children', allow_duplicate=True),
    Input('execute-sql-btn', 'n_clicks'),
    prevent_initial_call=True
)
def execute_sql_query(n_clicks):
    """Execute the generated SQL query"""
    if n_clicks == 0:
        raise PreventUpdate
    
    w = get_databricks_client()
    if not w:
        return html.Div("❌ Databricks connection not available", className="status-error")
    
    # Check if warehouse ID is configured
    http_path = getattr(config, 'DATABRICKS_HTTP_PATH', '')
    if not http_path or http_path == 'your-warehouse-id':
        return html.Div([
            html.H4("❌ Warehouse ID Configuration Required", style={"color": "#dc3545"}),
            html.P("Please configure your Databricks warehouse ID in config.py:"),
            html.Ul([
                html.Li("Go to your Databricks workspace"),
                html.Li("Navigate to 'SQL Warehouses' in the sidebar"),
                html.Li("Click on your warehouse name"),
                html.Li("Copy the 'Server hostname' and 'HTTP path'"),
                html.Li("Update DATABRICKS_HTTP_PATH in config.py with the HTTP path")
            ]),
            html.P(f"Current value: '{http_path}'", style={"fontStyle": "italic", "color": "#666"})
        ], className="status-error")
    
    try:
        # Get the stored SQL query
        sql_query = getattr(app.server, 'sql_query', None)
        if not sql_query:
            return html.Div("❌ No SQL query available. Generate SQL first.", className="status-error")
        
        # Execute the SQL query
        result = w.statement_execution.execute_statement(
            warehouse_id=http_path.split('/')[-1],
            statement=sql_query
        )
        
        # Check execution status
        if result.status.state.name == 'SUCCEEDED':
            return html.Div([
                html.H4("✅ Delta Table Created Successfully!", style={"color": "#28a745"}),
                html.P("Your CSV data has been uploaded and the Delta table has been created in Databricks."),
                html.Pre(f"Executed: {sql_query}", style={
                    "backgroundColor": "#d4edda", 
                    "padding": "10px", 
                    "borderRadius": "5px",
                    "fontSize": "12px",
                    "marginTop": "10px"
                })
            ], className="status-success")
        else:
            error_msg = result.status.error.message if result.status.error else "Unknown error"
            return html.Div(f"❌ Execution error: {error_msg}", className="status-error")
            
    except Exception as e:
        return html.Div(f"❌ Execution failed: {str(e)}", className="status-error")

if __name__ == '__main__':
    print("🚀 Starting Databricks CSV Uploader...")
    print(f"📊 Databricks SDK Available: {DATABRICKS_AVAILABLE}")
    print("🔄 Databricks Authentication: Will authenticate when needed (lazy loading)")
    print("🌐 App running at: http://localhost:8050")
    
    app.run_server(debug=True, host='0.0.0.0', port=8050)