
import streamlit as st
import _snowflake  # For Snowflake-specific Streamlit functions

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col


import pandas as pd

import hashlib          # For creating cache keys from filter combinations
import time            # For performance monitoring and retry logic
import json            # For serializing/deserializing saved search filters
import re              # For phone number formatting and text validation
import urllib.parse    # For URL encoding address parameters
from datetime import datetime  # For timestamps in Salesforce integration

import pydeck as pdk   # For interactive maps with business locations
import math            # For map zoom calculations and coordinate math



TABLE_CONFIG = {
    "table_name": "sandbox.conklin.prospect_tool_table",  # Updated main table name
    "filter_table_name": "sandbox.conklin.VW_Prospector_filter_options_new",  # Updated filter table name
}


def get_table_name():
    return TABLE_CONFIG["table_name"]




def get_filter_table_name():
    return TABLE_CONFIG["filter_table_name"]

MAX_RESULTS = 300  # Maximum records to fetch per query
MAP_POINTS_LIMIT = 100  # Maximum businesses to display on map for performance
PAGE_SIZE_OPTIONS = [10, 25, 50, 100]  # Available pagination options for different screen sizes
DEFAULT_PAGE_SIZE = 100  # Default records per page (balanced for responsive design)
ROW_HEIGHT = 45  # Height of each row in data display (optimized for compact view)

CACHE_TTL = 600  # Cache time-to-live in seconds (10 minutes)

DEFAULT_MAP_ZOOM = 9  # Default zoom level for map view
SELECTED_BUSINESS_ZOOM = 15  # Zoom level when a single business is selected
CHIPS_PER_ROW = 3  # Number of filter chips per row for compact display

MIN_DISPLAY_ROWS = 2  # Minimum rows to display in data tables
MAX_DATAFRAME_HEIGHT = 1000  # Maximum height for dataframes
HEADER_BUFFER_HEIGHT = 50  # Buffer height for dataframe headers

PHONE_LENGTH_STANDARD = 10  # Standard US phone number length
PHONE_LENGTH_WITH_COUNTRY = 11  # US phone number with country code

DEFAULT_RADIUS_SCALE = 1.0  # Default radius scale for map markers
DEFAULT_STEP_SIZE = 1.0  # Default step size for numeric inputs

BUTTON_LABEL_VISIT_SITE = "Visit Site"
BUTTON_LABEL_CALL = "Call"
BUTTON_LABEL_EMAIL = "Email"
BUTTON_LABEL_GET_DIRECTIONS = "Get Directions"

STATIC_FILTERS = {
    # Text-based search filters
    "DBA_NAME": {"type": "text", "label": "Business Name", "column_name": "DBA_NAME"},
    # Location radius search (replaces ZIP, STATE, CITY filters)
    "LOCATION_RADIUS": {
        "type": "location_radius", 
        "label": "Location & Radius Search", 
        "address_column": "ADDRESS",
        "zip_column": "ZIP",
        "lat_column": "LATITUDE", 
        "lng_column": "LONGITUDE"
    },
    # Industry and classification filters
    "PRIMARY_INDUSTRY": {"type": "dropdown", "label": "Primary Industry", "column_name": "PRIMARY_INDUSTRY"},
    "SUB_INDUSTRY": {"type": "dropdown", "label": "Sub Industry", "column_name": "SUB_INDUSTRY"},
    "SIC_CODE": {"type": "dropdown", "label": "SIC Code", "column_name": "SIC"},
    # Range filters for numeric values (min/max sliders)
    "REVENUE": {"type": "range", "label": "Revenue", "column_name": "REVENUE"},
    "NUMBER_OF_EMPLOYEES": {"type": "range", "label": "Number of Employees", "column_name": "NUMBER_OF_EMPLOYEES"},
    "NUMBER_OF_LOCATIONS": {"type": "range", "label": "Number of Locations", "column_name": "NUMBER_OF_LOCATIONS"},
    # Boolean and selection filters
    "HAS_CONTACT_INFO": {
        "type": "selectbox", 
        "label": "Contact Info Filter", 
        "column_name": "HAS_CONTACT_INFO",
        "options": ["Show Only Prospects with Contact Info", "Show All Prospects"]
    },
    "B2B": {
        "type": "selectbox",
        "label": "B2B",
        "column_name": "IS_B2B",
        "options": [
            "Include B2B and non-B2B",
            "Exclude B2B",
            "Show only B2B"
        ]
    },
    "B2C": {
        "type": "selectbox",
        "label": "B2C",
        "column_name": "IS_B2C",
        "options": [
            "Include B2C and non-B2C",
            "Exclude B2C",
            "Show only B2C"
        ]
    }
}

retries = 3
for attempt in range(retries):
    try:
        session = get_active_session()
        break
    except SnowparkSQLException as e:
        if attempt < retries - 1:
            time.sleep(2 ** attempt)
        else:
            st.error(f"Failed to connect to Snowflake after {retries} retries: {str(e)}")
            st.stop()

st.set_page_config(page_title="Prospector POC", layout="wide")


def initialize_session_state():


    defaults = {
        "filters": {
            col: (
                [] if STATIC_FILTERS[col]["type"] == "dropdown" else
                [None, None] if STATIC_FILTERS[col]["type"] == "range" else
                {"address": "", "radius": 25} if STATIC_FILTERS[col]["type"] == "location_radius" else
                STATIC_FILTERS[col]["options"][0] if STATIC_FILTERS[col]["type"] == "selectbox" else
                ""  # Empty string for text inputs
            )
            for col in STATIC_FILTERS
        },
        # Data Management
        "last_update_time": 0,              # Timestamp of last data refresh
        "filtered_df": pd.DataFrame(),      # Currently filtered dataset
        "active_filters": {},               # Filters actually applied to data
        "total_records": 0,                 # Total count matching current filters
        # Pagination
        "current_page": 1,                  # Current page number for pagination
        "page_size": DEFAULT_PAGE_SIZE,     # Records per page
        # Search Management
        "search_name": "",                  # Name for saving current search
        "selected_search": "",              # Currently loaded saved search
        "confirm_delete_search": False,     # Confirmation state for search deletion
        "search_to_delete": None,           # Search marked for deletion
        # UI State Management
        "filter_update_trigger": {          # Tracks when dropdown filters need refreshing
            col: 0 for col in STATIC_FILTERS if STATIC_FILTERS[col]["type"] == "dropdown"
        },
        "reset_counter": 0,                 # Triggers filter reset when incremented
        "sidebar_collapsed": False,         # Controls sidebar visibility
        "data_editor_refresh_counter": 0,   # Forces data editor refresh
        # Map Interaction
        "map_style_selector": ":material/dark_mode:",  # Current map style
        "selected_business_indices": [],    # Businesses selected on map
        "business_search_term": "",         # Search term for business filtering
        # Salesforce Integration (Simple ID tracking approach)
        "sf_pushed_count": 0,              # Count of businesses marked for Salesforce
        "sf_business_ids": [],             # List of business IDs to push to Salesforce
        "sf_last_update": datetime.now().isoformat(),  # Timestamp of last Salesforce update
        # Cortex Analyst Results
        "cortex_analyst_results": None,     # Results from Cortex Analyst
        # Contact Info Filter dropdown state
        "has_contact_info": "Show Only Prospects with Contact Info"  # Default to show only those with contact info
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def get_current_user(session):
    try:
        user_queries = [
            "SELECT CURRENT_USER()",           # <-- Primary method
            "SELECT USER_NAME()",              # <-- Alternative 1
            "SELECT SYSTEM$GET_SESSION_PROPERTY('USER')",  # <-- Alternative 2
            "SELECT SESSION_USER"              # <-- Alternative 3
        ]
        
        for query in user_queries:
            try:
                result = session.sql(query).collect()
                user = result[0][0] if result and result[0][0] else None
                if user and user.lower() != 'none':
                    return user  # <-- Returns actual Snowflake username
            except:
                continue
        
        return "SIS_USER"  # <-- Fallback if all methods fail
        
    except Exception as e:
        return "SIS_USER"


def add_business_to_salesforce(business_id):
    """Track business ID for Salesforce integration."""
    business_id_str = str(business_id)
    
    
    # Check if already tracked to avoid duplicates
    if business_id_str in st.session_state["sf_business_ids"]:
        return False
    
    # Add the new business ID
    st.session_state["sf_business_ids"].append(business_id_str)
    
    # Update the counter and timestamp
    st.session_state["sf_pushed_count"] = len(st.session_state["sf_business_ids"])
    st.session_state["sf_last_update"] = datetime.now().isoformat()
    
    return True

@st.cache_data(ttl=CACHE_TTL)
def geocode_address(address_or_zip):
    """
    Geocode an address or ZIP code to latitude/longitude using Azure geocoding via Snowflake UDF.
    Returns (lat, lng) tuple or None if geocoding fails.
    
    Uses Snowflake UDF: python_workloads.data_engineering.geocode_address()
    The UDF now handles ZIP codes by automatically adding "USA" and using countrySet=US parameter.
    Expected format: JSON with latitude/longitude
    Example: '{"latitude": 35.4677, "longitude": -97.5095}'
    """
    # Clean and format the input
    query = address_or_zip.strip()
    if not query:
        return None
    
    try:
        # Get active Snowflake session
        session = get_active_session()
        
        # Escape single quotes in the address to prevent SQL injection
        escaped_query = query.replace("'", "''")
        
        # Call the Snowflake UDF for geocoding
        geocode_sql = f"""
        SELECT python_workloads.data_engineering.geocode_address('{escaped_query}') as geocode_result
        """
        
        result = session.sql(geocode_sql).collect()
        
        if result and len(result) > 0:
            geocode_result = result[0]['GEOCODE_RESULT']
            
            if geocode_result and isinstance(geocode_result, str):
                # Check for error messages from the UDF
                if "error" in geocode_result.lower():
                    return None
                
                # Parse JSON response
                try:
                    import json
                    coords_data = json.loads(geocode_result)
                    
                    if isinstance(coords_data, dict):
                        lat = coords_data.get('latitude')
                        lng = coords_data.get('longitude')
                        
                        if lat is not None and lng is not None:
                            # Convert to float (handles scientific notation automatically)
                            lat = float(lat)
                            lng = float(lng)
                            
                            # Basic validation for reasonable coordinates
                            if -90 <= lat <= 90 and -180 <= lng <= 180:
                                return (lat, lng)
                
                except (json.JSONDecodeError, ValueError, KeyError):
                    return None
        
        return None
        
    except Exception as e:
        st.warning(f"Geocoding error: {str(e)}")
        return None

def build_radius_filter_sql(center_lat, center_lng, radius_miles, lat_col="LATITUDE", lng_col="LONGITUDE"):
    """
    Build SQL for radius-based filtering using Snowflake's geospatial functions.
    
    Args:
        center_lat: Center latitude
        center_lng: Center longitude  
        radius_miles: Radius in miles
        lat_col: Name of latitude column in table
        lng_col: Name of longitude column in table
    
    Returns:
        SQL WHERE clause string
    """
    # Convert miles to meters (Snowflake uses meters)
    radius_meters = radius_miles * 1609.34
    
    sql = f"""
    ST_DWITHIN(
        ST_POINT({lng_col}, {lat_col}),
        ST_POINT({center_lng}, {center_lat}),
        {radius_meters}
    )
    """
    
    return sql.strip()

def run_cortex_analyst(sidebar_prompt, session, _snowflake, CORTEX_MODEL_PATH, API_ENDPOINT, API_TIMEOUT, rerun_on_success=True):
    """Run Cortex Analyst API and update session state. Returns True if successful."""
    cortex_messages = []
    cortex_messages.append({
        "role": "user",
        "content": [{"type": "text", "text": sidebar_prompt}],
    })
    request_body = {
        "messages": cortex_messages,
        "semantic_model_file": f"@{CORTEX_MODEL_PATH}",
    }
    try:
        resp = _snowflake.send_snow_api_request(
            "POST", API_ENDPOINT, {}, {}, request_body, None, API_TIMEOUT
        )
        parsed_content = json.loads(resp["content"])
        if resp["status"] < 400:
            analyst_message = {
                "role": "analyst",
                "content": parsed_content["message"]["content"],
                "request_id": parsed_content["request_id"],
            }
            st.session_state["cortex_messages"] = cortex_messages + [analyst_message]
            st.session_state["last_sidebar_cortex_prompt"] = sidebar_prompt
            for item in analyst_message["content"]:
                if item.get("type") == "sql" and "statement" in item:
                    try:
                        cortex_df = session.sql(item["statement"]).to_pandas()
                        st.session_state["filtered_df"] = cortex_df
                        st.session_state["total_records"] = len(cortex_df)
                    except Exception as e:
                        st.error(f"Error executing Cortex SQL for List/Map View: {e}")
                    break
            if rerun_on_success:
                st.session_state["analyst_running"] = False
            return True
        else:
            st.error(f"Cortex Analyst API error: {parsed_content.get('message', 'Unknown error')}")
    except Exception as e:
        st.error(f"Error running Cortex Analyst: {e}")
    st.session_state["analyst_running"] = False
    return False


initialize_session_state()


st.markdown("""
    <style>
    /* Import DM Sans font from Google Fonts - Global Payments brand typography */
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,100..1000;1,9..40,100..1000&display=swap');
    
    /* Root variables for responsive design and Global Payments colors */
    :root {
        /* Responsive Design Variables */
        --sidebar-base-width: 320px;
        --container-max-width: 100%;
        --container-padding: 1rem;
        --font-size-base: 1rem;
        --button-height: 44px;
        --card-gap: 1rem;
        --section-spacing: 1.5rem;
        --grid-columns: 3;
        
        /* Data Editor Variables */
        --data-editor-height: 500px;
        --data-editor-cell-padding: 8px;
        
        /* Map View Variables */
        --map-height: 60vh;
        --map-controls-width: 280px;
        
        /* Responsive Breakpoints */
        --tablet: 768px;
        --laptop: 1024px;
        
        /* Core Global Payments Colors - Essential Only */
        --gp-primary: #262AFF;          /* Global Blue - primary brand color */
        --gp-accent: #1CABFF;           /* Pulse Blue - accent color */
        --gp-deep-blue: #1B1EC6;       /* Hover states */
        --gp-white: #FFFFFF;
        --gp-black: #0C0C0C;
        --gp-charcoal: #595959;
        --gp-smoke: #C4C4C4;
        --gp-haze: #F4F4F4;
        
        /* Semantic Colors - Simplified */
        --gp-success: var(--gp-accent);
        --gp-warning: #FFCC00;
        --gp-error: #F4364C;
        --gp-background: var(--gp-white);
        --gp-surface: var(--gp-haze);
        --gp-border: var(--gp-smoke);
        --gp-text-primary: var(--gp-black);
        --gp-text-secondary: var(--gp-charcoal);
        
        /* Typography - DM Sans as primary font */
        --font-family-primary: 'DM Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
        --line-height-base: 1.5;
        --letter-spacing-base: 0;
        
        /* Elevation and Shadow System */
        --gp-shadow-sm: 0 1px 3px rgba(38, 42, 255, 0.08);
        --gp-shadow-md: 0 2px 8px rgba(38, 42, 255, 0.1);
        --gp-shadow-lg: 0 4px 16px rgba(38, 42, 255, 0.12);
        --gp-shadow-xl: 0 8px 32px rgba(38, 42, 255, 0.15);
        --gp-shadow-hover: 0 6px 20px rgba(38, 42, 255, 0.2);
        
        /* Border Radius System */
        --gp-radius-sm: 4px;
        --gp-radius-md: 6px;
        --gp-radius-lg: 8px;
        --gp-radius-xl: 12px;
        --gp-radius-2xl: 16px;
        --gp-radius-full: 50%;
        
        /* Spacing System */
        --gp-space-xs: 0.25rem;
        --gp-space-sm: 0.5rem;
        --gp-space-md: 1rem;
        --gp-space-lg: 1.5rem;
        --gp-space-xl: 2rem;
        --gp-space-2xl: 3rem;
        
        /* Animation and Transition System */
        --gp-transition-fast: 0.15s ease-out;
        --gp-transition-base: 0.2s ease-out;
        --gp-transition-slow: 0.3s ease-out;
        --gp-transition-slower: 0.5s ease-out;
        
        /* Animation Timing Functions */
        --gp-ease-in: cubic-bezier(0.4, 0, 1, 1);
        --gp-ease-out: cubic-bezier(0, 0, 0.2, 1);
        --gp-ease-in-out: cubic-bezier(0.4, 0, 0.2, 1);
        --gp-ease-bounce: cubic-bezier(0.68, -0.55, 0.265, 1.55);
        --gp-ease-back: cubic-bezier(0.34, 1.56, 0.64, 1);
    }

    /* Selective DM Sans font application - avoid overriding Material Icons */
    html, body, .stApp, [data-testid="stAppViewContainer"] {
        font-family: var(--font-family-primary) !important;
        line-height: var(--line-height-base);
    }
    
    /* Apply DM Sans to main content areas but not icon components */
    .main .block-container,
    [data-testid="stSidebar"],
    [data-testid="stHeader"],
    [data-testid="stToolbar"],
    [data-testid="stDecoration"],
    [data-testid="stMarkdown"],
    [data-testid="stText"],
    [data-testid="metric-container"],
    .stSelectbox label, 
    .stTextInput label, 
    .stNumberInput label,
    .stMultiSelect label,
    .stSlider label,
    .stCheckbox label,
    .stRadio label {
        font-family: var(--font-family-primary) !important;
        line-height: var(--line-height-base);
    }

    /* Targeted text styling - DM Sans for specific text elements */
    h1, h2, h3, h4, h5, h6, p, 
    .stMarkdown, .stText, .stHeader, .stSubheader, .stTitle,
    .stDataFrame, .stTable, .stMetric,
    [data-testid="stMarkdownContainer"], 
    [data-testid="stText"],
    [data-testid="metric-container"] {
        font-family: var(--font-family-primary) !important;
        line-height: var(--line-height-base);
    }
    
    /* Input and select styling */
    .stSelectbox select, .stMultiSelect select, 
    .stSelectbox option, .stMultiSelect option,
    .stTextInput input, .stNumberInput input {
        font-family: var(--font-family-primary) !important;
    }

    /* Base styles for main content */
    .main-container {
        max-width: var(--container-max-width);
        margin: 0 auto;
        padding: var(--container-padding);
        width: 100%;
        background-color: var(--gp-background);
        font-family: var(--font-family-primary) !important;
    }

    /* Make Streamlit main content area use full width */
    .main .block-container {
        max-width: 100% !important;
        padding-left: 1rem !important;
        padding-right: 1rem !important;
        background-color: var(--gp-background);
    }

    /* Responsive containers */
    div[data-testid="stHorizontalBlock"] {
        width: 100%;
    }

    /* =============================================================================
     * CORE ANIMATIONS - Consolidated and Optimized
     * ============================================================================= */
    
    @keyframes gp-fade-in {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    @keyframes gp-scale-in {
        from { opacity: 0; transform: scale(0.9); }
        to { opacity: 1; transform: scale(1); }
    }
    
    @keyframes gp-pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }
    
    @keyframes gp-shake {
        0%, 100% { transform: translateX(0); }
        10%, 30%, 50%, 70%, 90% { transform: translateX(-2px); }
        20%, 40%, 60%, 80% { transform: translateX(2px); }
    }
    
    @keyframes gp-rotate {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
    }

    /* =============================================================================
     * ESSENTIAL ANIMATION CLASSES - Streamlined
     * ============================================================================= */
    
    .gp-animate-fade-in {
        animation: gp-fade-in var(--gp-transition-base) var(--gp-ease-out) forwards;
    }
    
    .gp-animate-scale-in {
        animation: gp-scale-in var(--gp-transition-base) var(--gp-ease-back) forwards;
    }
    
    .gp-animate-pulse {
        animation: gp-pulse 2s infinite;
    }
    
    .gp-animate-spin {
        animation: gp-rotate 1s linear infinite;
    }
    
    .gp-animate-shake {
        animation: gp-shake 0.5s ease-in-out;
    }

    /* =============================================================================
     * CORE TRANSITIONS - Simplified
     * ============================================================================= */
    
    .gp-transition {
        transition: all var(--gp-transition-base);
    }
    
    .gp-transition-fast {
        transition: all var(--gp-transition-fast);
    }

    /* =============================================================================
     * ESSENTIAL HOVER EFFECTS
     * ============================================================================= */
    
    .gp-hover-lift:hover {
        transform: translateY(-2px);
        box-shadow: var(--gp-shadow-lg);
    }
    
    .gp-focus-ring:focus {
        outline: 2px solid var(--gp-primary);
        outline-offset: 2px;
        box-shadow: 0 0 0 4px rgba(38, 42, 255, 0.1);
    }

    /* =============================================================================
     * UTILITY CLASSES - Reusable UI Components
     * ============================================================================= */
    
    /* Essential Spacing Utilities - Most Used Only */
    .gp-mb-md { margin-bottom: var(--gp-space-md) !important; }
    .gp-p-md { padding: var(--gp-space-md) !important; }
    .gp-gap-md { gap: var(--gp-space-md) !important; }
    .gp-section-spacing { margin-top: var(--gp-space-lg) !important; }
    
    /* Common Gradient Utilities */
    .gp-gradient-primary { background: linear-gradient(135deg, var(--gp-primary) 0%, var(--gp-accent) 100%) !important; }
    .gp-gradient-surface { background: linear-gradient(135deg, var(--gp-surface) 0%, var(--gp-background) 100%) !important; }
    .gp-gradient-light { background: linear-gradient(135deg, #ffffff 0%, #f8f9ff 100%) !important; }
    .gp-gradient-light-alt { background: linear-gradient(135deg, #f6f8ff 0%, #ffffff 100%) !important; }
    .gp-gradient-muted { background: linear-gradient(135deg, #ffffff 0%, #fafbff 100%) !important; }
    .gp-gradient-dark { background: linear-gradient(135deg, #1a1b23 0%, #2e3748 100%) !important; }
    .gp-gradient-hover { background: linear-gradient(135deg, #1b1c6e 0%, #2d5a87 100%) !important; }
    
    /* Common Container Styles */
    .gp-container-elevated {
        background: var(--gp-background);
        border: 1px solid var(--gp-border);
        border-radius: var(--gp-radius-lg);
        box-shadow: var(--gp-shadow-md);
        padding: var(--gp-space-md);
    }
    
    /* Inline Style Utilities */
    .gp-center-text { text-align: center !important; }
    .gp-bold { font-weight: 600 !important; }
    .gp-color-primary { color: var(--gp-primary) !important; }
    .gp-color-white { color: var(--gp-white) !important; }
    .gp-font-sm { font-size: 0.8rem !important; }
    .gp-font-xs { font-size: 0.7rem !important; }
    
    /* Apply gradients to specific components */
    .business-details-card h3,
    .step-icon,
    .gp-progress-bar {
        background: var(--gp-gradient-primary) !important;
    }
    
    .business-data-timeline {
        background: var(--gp-gradient-surface) !important;
    }
    
    /* Apply gradient utility classes to other elements */
    .gp-apply-primary-gradient { background: var(--gp-gradient-primary) !important; }
    .gp-apply-surface-gradient { background: var(--gp-gradient-surface) !important; }
    .gp-card {
        background: var(--gp-background);
        border: 1px solid var(--gp-border);
        border-radius: var(--gp-radius-xl);
        padding: var(--gp-space-md);
        box-shadow: var(--gp-shadow-sm);
        transition: all var(--gp-transition-slow) var(--gp-ease-out);
        font-family: var(--font-family-primary);
        position: relative;
        overflow: hidden;
    }
    
    .gp-card:hover {
        box-shadow: var(--gp-shadow-md);
        transform: translateY(-2px) scale(1.01);
        border-color: var(--gp-accent);
    }
    
    .gp-card-header {
        border-bottom: 1px solid var(--gp-border);
        padding-bottom: var(--gp-space-sm);
        margin-bottom: var(--gp-space-md);
    }
    
    .gp-card-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--gp-text-primary);
        margin: 0;
        font-family: var(--font-family-primary);
    }
    

    

    
    /* Status Indicator Component */
    .gp-status {
        display: inline-flex;
        align-items: center;
        gap: var(--gp-space-xs);
        font-size: 0.8rem;
        font-family: var(--font-family-primary);
    }
    
    .gp-status-dot {
        width: 8px;
        height: 8px;
        border-radius: var(--gp-radius-full);
        flex-shrink: 0;
    }
    
    .gp-status-active .gp-status-dot {
        background-color: var(--gp-success);
        box-shadow: 0 0 0 2px rgba(28, 171, 255, 0.2);
    }
    
    .gp-status-inactive .gp-status-dot {
        background-color: var(--gp-border);
    }
    
    .gp-status-error .gp-status-dot {
        background-color: var(--gp-error);
        box-shadow: 0 0 0 2px rgba(244, 54, 76, 0.2);
    }
    
    .gp-status-warning .gp-status-dot {
        background-color: var(--gp-warning);
        box-shadow: 0 0 0 2px rgba(255, 204, 0, 0.2);
    }
    
    /* Layout Components */
    .gp-flex {
        display: flex;
    }
    
    .gp-flex-col {
        flex-direction: column;
    }
    
    .gp-flex-wrap {
        flex-wrap: wrap;
    }
    
    .gp-items-center {
        align-items: center;
    }
    
    .gp-items-start {
        align-items: flex-start;
    }
    
    .gp-items-end {
        align-items: flex-end;
    }
    
    .gp-justify-center {
        justify-content: center;
    }
    
    .gp-justify-between {
        justify-content: space-between;
    }
    
    .gp-justify-start {
        justify-content: flex-start;
    }
    
    .gp-justify-end {
        justify-content: flex-end;
    }
    
    .gp-grid {
        display: grid;
    }
    
    .gp-grid-cols-1 { grid-template-columns: repeat(1, 1fr); }
    .gp-grid-cols-2 { grid-template-columns: repeat(2, 1fr); }
    .gp-grid-cols-3 { grid-template-columns: repeat(3, 1fr); }
    .gp-grid-cols-4 { grid-template-columns: repeat(4, 1fr); }
    .gp-grid-cols-auto { grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); }
    
    .gp-w-full { width: 100%; }
    .gp-h-full { height: 100%; }
    
    /* Text Utilities */
    .gp-text-center { text-align: center; }
    .gp-text-left { text-align: left; }
    .gp-text-right { text-align: right; }
    
    .gp-text-sm { font-size: 0.8rem; }
    .gp-text-base { font-size: 1rem; }
    .gp-text-lg { font-size: 1.1rem; }
    .gp-text-xl { font-size: 1.25rem; }
    
    .gp-font-medium { font-weight: 500; }
    .gp-font-semibold { font-weight: 600; }
    .gp-font-bold { font-weight: 700; }
    
    .gp-text-primary { color: var(--gp-text-primary); }
    .gp-text-secondary { color: var(--gp-text-secondary); }
    .gp-text-success { color: var(--gp-success); }
    .gp-text-warning { color: var(--gp-warning); }
    .gp-text-error { color: var(--gp-error); }
    
    /* Metric Display Components */
    .gp-metric {
        background: var(--gp-background);
        padding: var(--gp-space-md);
        border-radius: var(--gp-radius-lg);
        border: 1px solid var(--gp-border);
        transition: all 0.2s ease;
        position: relative;
        overflow: hidden;
    }
    
    .gp-metric:hover {
        border-color: var(--gp-primary);
        box-shadow: var(--gp-shadow-md);
        transform: translateY(-1px);
    }
    
    .gp-metric::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        width: 3px;
        height: 100%;
        background: var(--gp-primary);
        opacity: 0;
        transition: opacity 0.2s ease;
    }
    
    .gp-metric:hover::before {
        opacity: 1;
    }
    
    .gp-metric-label {
        font-size: 0.7rem;
        font-weight: 500;
        color: var(--gp-text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: var(--gp-space-xs);
        font-family: var(--font-family-primary);
    }
    
    .gp-metric-value {
        font-size: 1.5rem;
        font-weight: 600;
        color: var(--gp-text-primary);
        line-height: 1.2;
        margin-bottom: var(--gp-space-xs);
        font-family: var(--font-family-primary);
    }
    
    .gp-metric-change {
        font-size: 0.8rem;
        font-weight: 500;
        display: flex;
        align-items: center;
        gap: var(--gp-space-xs);
        font-family: var(--font-family-primary);
    }
    
    .gp-metric-change.positive {
        color: var(--gp-success);
    }
    
    .gp-metric-change.negative {
        color: var(--gp-error);
    }
    
    .gp-metric-change.neutral {
        color: var(--gp-text-secondary);
    }
    
    .gp-metric-sm .gp-metric-value {
        font-size: 1.2rem;
    }
    
    .gp-metric-lg .gp-metric-value {
        font-size: 2rem;
    }
    
    .gp-metric-icon {
        position: absolute;
        top: var(--gp-space-md);
        right: var(--gp-space-md);
        font-size: 1.2rem;
        opacity: 0.6;
        color: var(--gp-primary);
    }
    
    /* Loading State Components - Enhanced with animations */
    .gp-loading {
        display: inline-flex;
        align-items: center;
        gap: var(--gp-space-sm);
        padding: var(--gp-space-sm) var(--gp-space-md);
        background: var(--gp-surface);
        border-radius: var(--gp-radius-md);
        color: var(--gp-text-secondary);
        font-family: var(--font-family-primary);
        font-size: 0.9rem;
        animation: gp-fade-in var(--gp-transition-base) var(--gp-ease-out);
    }
    
    .gp-spinner {
        width: 16px;
        height: 16px;
        border: 2px solid var(--gp-border);
        border-top: 2px solid var(--gp-primary);
        border-radius: var(--gp-radius-full);
        animation: gp-rotate 1s linear infinite;
    }
    
    .gp-spinner-lg {
        width: 24px;
        height: 24px;
        border-width: 3px;
    }
    
    .gp-spinner-sm {
        width: 12px;
        height: 12px;
        border-width: 1px;
    }
    
    .gp-skeleton {
        background: linear-gradient(90deg, var(--gp-surface) 25%, var(--gp-border) 50%, var(--gp-surface) 75%);
        background-size: 200% 100%;
        animation: gp-skeleton-loading 1.5s infinite;
        border-radius: var(--gp-radius-md);
    }
    
    @keyframes gp-skeleton-loading {
        0% { background-position: 200% 0; }
        100% { background-position: -200% 0; }
    }
    
    .gp-skeleton-text {
        height: 1em;
        margin: var(--gp-space-xs) 0;
    }
    
    .gp-skeleton-text.gp-skeleton-sm { height: 0.8em; }
    .gp-skeleton-text.gp-skeleton-lg { height: 1.2em; }
    
    /* Progress Bar Component */
    .gp-progress {
        width: 100%;
        height: 8px;
        background: var(--gp-surface);
        border-radius: var(--gp-radius-full);
        overflow: hidden;
        position: relative;
    }
    
    .gp-progress-bar {
        height: 100%;
        border-radius: var(--gp-radius-full);
        transition: width var(--gp-transition-slow) var(--gp-ease-out);
        position: relative;
        overflow: hidden;
    }
    
    .gp-progress-bar::after {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.3), transparent);
        animation: gp-progress-indeterminate 2s infinite linear;
    }
    
    .gp-progress-sm {
        height: 4px;
    }
    
    .gp-progress-lg {
        height: 12px;
    }
    

    

    

    

    
    /* =============================================================================
     * STREAMLIT SPECIFIC ANIMATIONS - Simplified
     * ============================================================================= */
    
    /* Animate Streamlit components on load */
    [data-testid="stMarkdown"] {
        animation: gp-fade-in var(--gp-transition-base) var(--gp-ease-out);
    }
    
    [data-testid="stMetric"] {
        animation: gp-fade-in var(--gp-transition-slow) var(--gp-ease-out);
    }
    
    [data-testid="stDataFrame"] {
        animation: gp-fade-in var(--gp-transition-slow) var(--gp-ease-out);
    }
    
    /* Sidebar animation */
    [data-testid="stSidebar"] {
        animation: gp-fade-in var(--gp-transition-slower) var(--gp-ease-out);
    }
    
    /* Main content animation */
    .main .block-container {
        animation: gp-fade-in var(--gp-transition-slower) var(--gp-ease-out);
    }
    
    /* =============================================================================
     * ACCESSIBILITY - Respect user's motion preferences
     * ============================================================================= */
    
    @media (prefers-reduced-motion: reduce) {
        *,
        *::before,
        *::after {
            animation-duration: 0.01ms !important;
            animation-iteration-count: 1 !important;
            transition-duration: 0.01ms !important;
            scroll-behavior: auto !important;
        }
        
        .gp-animate-pulse,
        .gp-animate-spin {
            animation: none;
        }
    }
    
    /* Responsive Utilities */
    @media (max-width: 768px) {
        .gp-hidden-mobile { display: none !important; }
        .gp-grid-cols-mobile-1 { grid-template-columns: repeat(1, 1fr) !important; }
        .gp-text-mobile-sm { font-size: 0.8rem !important; }
        .gp-p-mobile-sm { padding: var(--gp-space-sm) !important; }
    }
    
    @media (min-width: 769px) {
        .gp-hidden-desktop { display: none !important; }
    }
    
    /* =============================================================================
     * END UTILITY CLASSES
     * 
     * USAGE EXAMPLES:
     * 
     * Basic Card:
     * st.markdown('<div class="gp-card"><h3 class="gp-card-title">Title</h3><p>Content</p></div>', unsafe_allow_html=True)
     * 
     * Metric Display:
     * st.markdown('<div class="gp-metric"><div class="gp-metric-label">Revenue</div><div class="gp-metric-value">$1.2M</div></div>', unsafe_allow_html=True)
     * 
     * Flex Layout:
     * st.markdown('<div class="gp-flex gp-items-center gp-justify-between gp-gap-md">Content</div>', unsafe_allow_html=True)
     * 
     * Grid Layout:
     * st.markdown('<div class="gp-grid gp-grid-cols-3 gp-gap-md">Grid Items</div>', unsafe_allow_html=True)
     * 
     * Loading State:
     * st.markdown('<div class="gp-loading"><div class="gp-spinner"></div>Loading...</div>', unsafe_allow_html=True)
     * 
     * ============================================================================= */

    /* Button styling - Enhanced with animation system */
    .stButton > button {
        width: 100%;
        padding: 0.75rem;
        font-size: var(--font-size-base);
        border-radius: var(--gp-radius-lg);
        min-height: var(--button-height);
        background-color: var(--gp-primary);
        color: var(--gp-white);
        border: none;
        transition: all var(--gp-transition-base) var(--gp-ease-out);
        font-weight: 500;
        font-family: var(--font-family-primary) !important;
        box-shadow: var(--gp-shadow-sm);
        position: relative;
        overflow: hidden;
    }
    
    .stButton > button::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
        transition: left var(--gp-transition-slow) var(--gp-ease-out);
    }
    
    .stButton > button:hover::before {
        left: 100%;
    }
    
    .stButton > button[kind="secondary"] {
        background-color: var(--gp-surface);
        color: var(--gp-text-primary);
        border: 2px solid var(--gp-border);
        font-family: var(--font-family-primary) !important;
    }
    
    .stButton > button:hover {
        background-color: var(--gp-deep-blue);
        transform: translateY(-1px) scale(1.02);
        box-shadow: var(--gp-shadow-hover);
    }
    
    .stButton > button[kind="secondary"]:hover {
        background-color: var(--gp-surface);
        border-color: var(--gp-accent);
        transform: translateY(-1px);
        box-shadow: var(--gp-shadow-md);
    }
    
    .stButton > button:active {
        transform: translateY(0) scale(0.98);
        transition: all var(--gp-transition-fast);
    }

    /* Input styling - Enhanced with animations */
    .stTextInput > div > input, .stNumberInput > div > input {
        font-size: var(--font-size-base);
        padding: 0.5rem;
        width: 100%;
        border: 2px solid var(--gp-border);
        border-radius: var(--gp-radius-md);
        background-color: var(--gp-background);
        font-family: var(--font-family-primary) !important;
        transition: all var(--gp-transition-base) var(--gp-ease-out);
        position: relative;
    }
    
    .stTextInput > div > input:hover, .stNumberInput > div > input:hover {
        border-color: var(--gp-accent);
        box-shadow: 0 0 0 1px rgba(28, 171, 255, 0.1);
    }
    
    .stTextInput > div > input:focus, .stNumberInput > div > input:focus {
        border-color: var(--gp-primary);
        box-shadow: 0 0 0 3px rgba(38, 42, 255, 0.1);
        outline: none;
        transform: scale(1.01);
    }
    
    .stSelectbox > div, .stMultiSelect > div {
        font-size: var(--font-size-base);
        width: 100%;
        font-family: var(--font-family-primary) !important;
    }
    
    .stSelectbox > div > div {
        border: 2px solid var(--gp-border);
        border-radius: var(--gp-radius-md);
        background-color: var(--gp-background);
        font-family: var(--font-family-primary) !important;
        transition: all var(--gp-transition-base) var(--gp-ease-out);
    }
    
    .stSelectbox > div > div:hover {
        border-color: var(--gp-accent);
        box-shadow: var(--gp-shadow-sm);
    }
    
    .stSelectbox > div > div:focus-within {
        border-color: var(--gp-primary);
        box-shadow: 0 0 0 3px rgba(38, 42, 255, 0.1);
    }

    /* Data frame - responsive with Global Payments styling */
    .stDataFrame, .stDataEditor {
        width: 100%;
        overflow-x: auto;
        border: 1px solid var(--gp-border);
        border-radius: var(--gp-radius-lg);
        max-height: var(--data-editor-height) !important;
        overflow-y: auto !important;
        box-shadow: var(--gp-shadow-sm);
    }
    .stDataFrame > div, .stDataEditor > div {
        width: 100%;
    }
    .stDataFrame table, .stDataEditor table {
        width: 100% !important;
    }
    .stDataFrame th, .stDataEditor th {
        background-color: var(--gp-surface) !important;
        color: var(--gp-text-primary) !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        font-size: 0.8rem !important;
        letter-spacing: 0.5px !important;
        padding: var(--data-editor-cell-padding) !important;
        white-space: nowrap !important;
        border-bottom: 2px solid var(--gp-primary) !important;
    }
    
    /* Data editor responsiveness */
    .stDataEditor td {
        padding: var(--data-editor-cell-padding) !important;
    }
    
    /* Custom scrollbar for data frames */
    .stDataFrame::-webkit-scrollbar, .stDataEditor::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    .stDataFrame::-webkit-scrollbar-track, .stDataEditor::-webkit-scrollbar-track {
        background: var(--gp-surface);
        border-radius: var(--gp-radius-sm);
    }
    
    .stDataFrame::-webkit-scrollbar-thumb, .stDataEditor::-webkit-scrollbar-thumb {
        background: var(--gp-border);
        border-radius: var(--gp-radius-sm);
        transition: background 0.2s ease;
    }
    
    .stDataFrame::-webkit-scrollbar-thumb:hover, .stDataEditor::-webkit-scrollbar-thumb:hover {
        background: var(--gp-accent);
    }
        border-bottom: 2px solid var(--gp-primary) !important;
    }

    /* Pagination - Global Payments theme */
    .pagination-container {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        justify-content: center;
        align-items: center;
        margin-top: 1rem;
        width: 100%;
        padding: 1rem;
        background-color: var(--gp-surface);
        border-radius: var(--gp-radius-lg);
        box-shadow: var(--gp-shadow-sm);
    }
    
    /* Responsive pagination layout */
    .page-navigation-container {
        display: flex;
        flex-direction: column;
        width: 100%;
        margin: 0.5rem 0;
    }
    
    .page-size-controls {
        display: flex;
        align-items: center;
        flex-wrap: wrap;
        gap: 0.5rem;
    }
    
    .pagination-nav-buttons {
        display: flex;
        align-items: center;
        justify-content: flex-end;
    }
    
    .pagination-status {
        margin-top: 0.25rem;
        width: 100%;
        text-align: left;
        color: var(--gp-text-secondary);
        font-size: 0.8rem;
    }

    /* Business details card - Enhanced with new component system */
    .business-details-card {
        /* Use new component system as base */
        background: var(--gp-background);
        border: 1px solid var(--gp-border);
        border-radius: var(--gp-radius-xl);
        box-shadow: var(--gp-shadow-md);
        width: 100%;
        box-sizing: border-box;
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
        padding: 0;
        font-size: 0.75rem;
    }
    .business-details-card:hover {
        box-shadow: var(--gp-shadow-xl);
        transform: translateY(-2px);
    }
    .business-details-card h3 {
        font-size: 1rem;
        font-weight: 600;
        margin: 0;
        color: var(--gp-white);
        padding: 0.5rem 0.75rem;
        display: flex;
        align-items: center;
        border-radius: 12px;
        gap: 0.5rem;
        position: relative;
        flex-wrap: nowrap;
        justify-content: space-between;
        background-image: linear-gradient(135deg, var(--gp-primary) 0%, var(--gp-accent) 100%) !important;
        z-index: 1;
    }
    .business-details-card h3::before {
        content: '🏢';
        font-size: 0.9rem;
        background: rgba(255, 255, 255, 0.2);
        width: 28px;
        height: 28px;
        display: flex;
        align-items: center;
        justify-content: center;
        border-radius: 12px;
        backdrop-filter: blur(10px);
    }
    .business-details-card h3::after {
        content: '';
        position: absolute;
        bottom: 0;
        left: 0;
        right: 0;
        height: 1px;
        border-radius: 12px;
        background: rgba(255, 255, 255, 0.2);
    }
    
    /* Data Visualization Cards style - grouped dashboard sections */
    .business-data-dashboard {
        padding: 0.5rem;
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
    }
    .data-viz-section {
        background: var(--gp-surface);
        border-radius: var(--gp-radius-lg);
        padding: 0.4rem;
        position: relative;
        border-left: 4px solid var(--gp-accent);
        transition: all 0.2s ease;
        box-shadow: var(--gp-shadow-sm);
    }
    .data-viz-section:hover {
        border-left-color: var(--gp-primary);
        background: var(--gp-surface);
        box-shadow: var(--gp-shadow-md);
    }
    .data-viz-section:nth-child(2) {
        border-left-color: var(--gp-accent);
    }
    .data-viz-section:nth-child(2):hover {
        border-left-color: var(--gp-primary);
    }
    .data-viz-section:nth-child(3) {
        border-left-color: var(--gp-warning);
    }
    .data-viz-section:nth-child(3):hover {
        border-left-color: var(--gp-error);
    }
    .section-header {
        font-size: 0.65rem;
        font-weight: 600;
        color: var(--gp-text-secondary);
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 0.3rem;
        display: flex;
        align-items: center;
        gap: 0.4rem;
        font-family: var(--font-family-primary);
    }
    .section-header::before {
        content: '';
        width: 20px;
        height: 2px;
        background: var(--gp-primary);
        border-radius: var(--gp-radius-sm);
    }
    
    /* Salesforce push status in header */
    .business-name-container {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        flex-grow: 1;  /* Take up available space */
        text-align: left;  /* Ensure left alignment */
    }
    .sf-push-status {
        color: white;
        font-size: 11px;
        background-color: rgba(255, 255, 255, 0.25); 
        padding: 2px 6px;
        border-radius: 10px;
        margin-left: auto;  /* Push to the right */
        font-weight: 500;
        white-space: nowrap;
        backdrop-filter: blur(10px);
        text-align: right;  /* Ensure right alignment */
    }
    
    .data-viz-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 0.3rem;
    }
    .data-metric {
        /* Use new gp-metric component system */
        background: var(--gp-background);
        padding: 0.75rem;
        border-radius: var(--gp-radius-lg);
        border: 1px solid var(--gp-border);
        transition: all 0.2s ease;
        position: relative;
        min-height: 60px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .data-metric:hover {
        border-color: var(--gp-primary);
        box-shadow: var(--gp-shadow-md);
        transform: translateY(-1px);
    }
    .metric-icon {
        position: absolute;
        top: 0.5rem;
        right: 0.5rem;
        font-size: 0.85rem;
        opacity: 0.6;
        color: var(--gp-primary);
    }
    .metric-label {
        font-size: 0.7rem;
        font-weight: 500;
        color: var(--gp-text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.25rem;
        line-height: 1;
        font-family: var(--font-family-primary);
    }
    .metric-value {
        font-size: 0.95rem;
        color: var(--gp-text-primary);
        font-weight: 600;
        line-height: 1.2;
        word-break: break-word;
        font-family: var(--font-family-primary);
    }
    .metric-value a {
        color: var(--gp-primary);
        text-decoration: none;
        font-weight: 600;
    }
    .metric-value a:hover {
        color: var(--gp-accent);
        text-decoration: underline;
    }
    .metric-accent {
        position: absolute;
        top: 0;
        left: 0;
        width: 3px;
        height: 100%;
        background: var(--gp-accent);
        border-radius: 0 0 0 var(--gp-radius-lg);
        opacity: 0;
        transition: opacity 0.2s ease;
    }
    .data-metric:hover .metric-accent {
        opacity: 1;
    }

    /* Linear Timeline/Process Style - Enhanced with component system */
    .business-data-timeline {
        position: relative;
        padding: 30px 20px;
        border-radius: var(--gp-radius-2xl);
        border: 1px solid var(--gp-border);
        box-shadow: var(--gp-shadow-lg);
        overflow: hidden;
    }
    
    .business-data-timeline::before {
        content: '';
        position: absolute;
        left: 40px;
        top: 80px;
        bottom: 20px;
        width: 3px;
        background: linear-gradient(to bottom, var(--gp-primary), var(--gp-accent), var(--gp-primary));
        border-radius: var(--gp-radius-sm);
        box-shadow: 0 0 10px rgba(38, 42, 255, 0.3);
    }
    
    .timeline-header {
        text-align: center;
        margin-bottom: 40px;
        padding-bottom: 20px;
        border-bottom: 2px solid var(--gp-border);
    }
    
    .timeline-header h3 {
        font-family: var(--font-family-primary);
        font-weight: 700;
        color: var(--gp-text-secondary);
        margin: 0;
        font-size: 24px;
    }
    
    .timeline-step {
        position: relative;
        margin-left: 80px;
        margin-bottom: 32px;
        padding: 20px 24px;
        background: var(--gp-background);
        border-radius: var(--gp-radius-xl);
        border: 1px solid var(--gp-border);
        box-shadow: var(--gp-shadow-sm);
        transition: all 0.3s ease;
    }
    
    .timeline-step:hover {
        transform: translateX(8px);
        box-shadow: var(--gp-shadow-lg);
        border-color: var(--gp-primary);
    }
    
    .timeline-step::before {
        content: '';
        position: absolute;
        left: -59px;
        top: 50%;
        transform: translateY(-50%);
        width: 16px;
        height: 16px;
        background: var(--gp-primary);
        border: 4px solid var(--gp-background);
        border-radius: var(--gp-radius-full);
        box-shadow: 0 0 0 3px var(--gp-primary), 0 0 15px rgba(38, 42, 255, 0.4);
        z-index: 2;
    }
    
    .timeline-step::after {
        content: '';
        position: absolute;
        left: -45px;
        top: 50%;
        transform: translateY(-50%);
        width: 0;
        height: 0;
        border-top: 8px solid transparent;
        border-bottom: 8px solid transparent;
        border-left: 12px solid var(--gp-primary);
        z-index: 1;
    }
    
    .step-header {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 16px;
    }
    
    .step-icon {
        font-size: 24px;
        padding: 8px;
        border-radius: var(--gp-radius-full);
        display: flex;
        align-items: center;
        justify-content: center;
        min-width: 40px;
        height: 40px;
        color: var(--gp-white);
    }
    
    .step-title {
        font-family: var(--font-family-primary);
        font-weight: 600;
        color: var(--gp-text-secondary);
        font-size: 16px;
        margin: 0;
    }
    
    .step-content {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 12px;
    }
    
    .process-item {
        background: var(--gp-surface);
        padding: 12px 16px;
        border-radius: var(--gp-radius-lg);
        border-left: 3px solid var(--gp-primary);
        display: flex;
        flex-direction: column;
        gap: 4px;
        transition: all 0.2s ease;
        box-shadow: var(--gp-shadow-sm);
    }
    
    .process-item:hover {
        background: var(--gp-surface);
        transform: scale(1.02);
        border-left-color: var(--gp-accent);
        box-shadow: var(--gp-shadow-md);
    }
    
    .process-item-icon {
        font-size: 16px;
        opacity: 0.8;
        align-self: flex-start;
        color: var(--gp-primary);
    }
    
    .process-item-label {
        font-family: var(--font-family-primary);
        font-size: 11px;
        font-weight: 500;
        color: var(--gp-text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 2px;
    }
    
    .process-item-value {
        font-family: var(--font-family-primary);
        font-size: 14px;
        font-weight: 600;
        color: var(--gp-text-primary);
        line-height: 1.3;
    }
    
    .process-item-value a {
        color: var(--gp-primary);
        text-decoration: none;
    }
    
    .process-item-value a:hover {
        text-decoration: underline;
        color: var(--gp-accent);
    }
    
    .timeline-completion {
        position: relative;
        margin-left: 80px;
        text-align: center;
        padding: 20px;
        background: linear-gradient(135deg, var(--gp-primary), var(--gp-accent));
        color: var(--gp-white);
        border-radius: var(--gp-radius-xl);
        box-shadow: var(--gp-shadow-lg);
    }
    
    .timeline-completion::before {
        content: '✓';
        position: absolute;
        left: -67px;
        top: 50%;
        transform: translateY(-50%);
        width: 24px;
        height: 24px;
        background: var(--gp-primary);
        border: 4px solid var(--gp-background);
        border-radius: var(--gp-radius-full);
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: bold;
        font-size: 14px;
        color: var(--gp-white);
        box-shadow: 0 0 0 3px var(--gp-primary), 0 0 15px rgba(38, 42, 255, 0.4);
        z-index: 2;
    }

    /* Map container - Enhanced with component system */
    div[data-testid="stDeckGlJsonChart"] {
        width: 100% !important;
        height: 60vh !important;
        min-height: 300px !important;
        border: 2px solid var(--gp-border);
        border-radius: var(--gp-radius-lg);
        overflow: hidden;
        box-shadow: var(--gp-shadow-md);
        transition: all 0.2s ease;
    }
    
    div[data-testid="stDeckGlJsonChart"]:hover {
        border-color: var(--gp-primary);
        box-shadow: var(--gp-shadow-lg);
    }
    
    /* Force map height with higher specificity */
    div[data-testid="stDeckGlJsonChart"] > div {
        height: 100% !important;
    }
    
    div[data-testid="stDeckGlJsonChart"] iframe {
        height: 100% !important;
    }

    /* Tabs - use Streamlit default styling */
    .stTabs {
        width: 100%;
    }
    .stTabs [data-baseweb="tab-list"] {
        width: 100%;
        overflow-x: auto;
    }
    .stTabs [data-baseweb="tab"] {
        white-space: nowrap;
        min-width: fit-content;
    }
    
    /* Sidebar styling with enhanced component system */
    div[data-testid="stSidebar"] {
        background-color: var(--gp-surface) !important;
        border-right: 3px solid var(--gp-primary) !important;
    }
    
    div[data-testid="stSidebar"] .stButton > button {
        width: 100%;
        padding: 0.5rem;
        font-size: 0.9rem;
        min-height: 40px;
        border-radius: var(--gp-radius-md);
        background-color: var(--gp-primary);
        color: var(--gp-white);
        border: none;
        margin-bottom: 0.25rem;
        box-shadow: var(--gp-shadow-sm);
        transition: all 0.2s ease;
        font-family: var(--font-family-primary);
    }
    div[data-testid="stSidebar"] .stButton > button[kind="secondary"] {
        background-color: var(--gp-background);
        color: var(--gp-text-primary);
        border: 2px solid var(--gp-border);
    }
    div[data-testid="stSidebar"] .stButton > button:hover {
        background-color: var(--gp-deep-blue);
        box-shadow: var(--gp-shadow-md);
        transform: translateY(-1px);
    }
    div[data-testid="stSidebar"] .stButton > button[kind="secondary"]:hover {
        background-color: var(--gp-surface);
        border-color: var(--gp-accent);
        transform: translateY(-1px);
    }
    
    div[data-testid="stSidebar"] .stTextInput > div > input,
    div[data-testid="stSidebar"] .stNumberInput > div > input {
        font-size: 0.9rem;
        padding: 0.5rem;
        width: 100%;
        border: 2px solid var(--gp-border);
        border-radius: var(--gp-radius-md);
        background-color: var(--gp-background);
        transition: all 0.2s ease;
        font-family: var(--font-family-primary);
    }
    div[data-testid="stSidebar"] .stTextInput > div > input:focus,
    div[data-testid="stSidebar"] .stNumberInput > div > input:focus {
        border-color: var(--gp-primary);
        box-shadow: 0 0 0 3px rgba(38, 42, 255, 0.1);
        outline: none;
    }
    
    div[data-testid="stSidebar"] .stSelectbox > div,
    div[data-testid="stSidebar"] .stMultiSelect > div {
        font-size: 0.9rem;
        width: 100%;
        font-family: var(--font-family-primary);
    }
    div[data-testid="stSidebar"] .stSelectbox > div > div {
        border: 2px solid var(--gp-border);
        border-radius: var(--gp-radius-md);
        background-color: var(--gp-background);
        transition: all 0.2s ease;
    }
    
    div[data-testid="stSidebar"] .stMarkdown,
    div[data-testid="stSidebar"] label,
    div[data-testid="stSidebar"] .stCheckbox > label {
        font-size: 0.9rem;
        color: var(--gp-text-primary);
        font-family: var(--font-family-primary);
    }
    div[data-testid="stSidebar"] h2,
    div[data-testid="stSidebar"] h3 {
        font-size: 1.2rem;
        color: var(--gp-text-primary);
        border-bottom: 2px solid var(--gp-primary);
        padding-bottom: 0.5rem;
        margin-bottom: 1rem;
        font-family: var(--font-family-primary);
    }


        div[data-testid="stSidebar"] label,
        div[data-testid="stSidebar"] .stCheckbox > label {
            font-size: 0.8rem;
        }
        div[data-testid="stSidebar"] h2,
        div[data-testid="stSidebar"] h3 {
            font-size: 1.1rem;
        }
    }

    @media (max-width: 480px) {
        div[data-testid="stSidebar"] .stButton > button {
            font-size: 0.7rem;
            padding: 0.3rem;
            min-height: 32px;
        }
        div[data-testid="stSidebar"] .stTextInput > div > input,
        div[data-testid="stSidebar"] .stNumberInput > div > input {
            font-size: 0.7rem;
            padding: 0.3rem;
        }
        div[data-testid="stSidebar"] .stSelectbox > div,
        div[data-testid="stSidebar"] .stMultiSelect > div {
            font-size: 0.7rem;
        }
        div[data-testid="stSidebar"] .stMarkdown,
        div[data-testid="stSidebar"] label,
        div[data-testid="stSidebar"] .stCheckbox > label {
            font-size: 0.7rem;
        }
        div[data-testid="stSidebar"] h2,
        div[data-testid="stSidebar"] h3 {
            font-size: 1rem;
        }
    }

    /* Accessibility improvements */
    .stButton > button:focus,
    .stTextInput > div > input:focus,
    .stNumberInput > div > input:focus,
    .stSelectbox > div:focus,
    .stMultiSelect > div:focus {
        outline: 2px solid var(--gp-primary);
        outline-offset: 2px;
    }
    </style>
""", unsafe_allow_html=True)
def get_filtered_dataframe(df, filters, display_columns=None):
    filtered_df = df.copy()
    for key, value in filters.items():
        filter_cfg = STATIC_FILTERS.get(key)
        if not filter_cfg or value is None or value == "":
            continue
        
        # Handle location_radius filter separately (no column_name)
        if filter_cfg["type"] == "location_radius":
            # Location radius filtering is handled in SQL, skip DataFrame filtering here
            continue
            
        col_name = filter_cfg["column_name"]
        if col_name not in filtered_df.columns:
            continue  # Skip filter if column not present
        if filter_cfg["type"] == "text":
            if value:
                filtered_df = filtered_df[filtered_df[col_name].str.contains(str(value), case=False, na=False)]
        elif filter_cfg["type"] == "dropdown":
            if value:
                if isinstance(value, list):
                    filtered_df = filtered_df[filtered_df[col_name].isin(value)]
                else:
                    filtered_df = filtered_df[filtered_df[col_name] == value]
        elif filter_cfg["type"] == "range":
            min_val, max_val = value if isinstance(value, (list, tuple)) else (None, None)
            if min_val is not None:
                filtered_df = filtered_df[filtered_df[col_name] >= min_val]
            if max_val is not None:
                filtered_df = filtered_df[filtered_df[col_name] <= max_val]
        elif filter_cfg["type"] == "selectbox":
            # B2B/B2C logic
            if "Exclude" in value:
                filtered_df = filtered_df[filtered_df[col_name] == 0]
            elif "Show only" in value:
                filtered_df = filtered_df[filtered_df[col_name] == 1]
            # "Include" means no filter
        elif filter_cfg["type"] == "location_radius" and isinstance(value, dict) and value.get("address", "").strip():
            # Handle location radius filtering for DataFrame
            address = value.get("address", "").strip()
            radius = value.get("radius", 25)
            
            # Check if required columns exist
            if "LATITUDE" in filtered_df.columns and "LONGITUDE" in filtered_df.columns:
                geocoded = geocode_address(address)
                if geocoded:
                    center_lat, center_lng = geocoded
                    
                    # Calculate distance for each row
                    def haversine_distance(lat1, lon1, lat2, lon2):
                        """Calculate distance between two points in miles using Haversine formula"""
                        import math
                        R = 3959  # Earth's radius in miles
                        
                        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
                        dlat = lat2 - lat1
                        dlon = lon2 - lon1
                        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
                        c = 2 * math.asin(math.sqrt(a))
                        return R * c
                    
                    # Filter by distance
                    mask = filtered_df.apply(
                        lambda row: (
                            pd.notna(row['LATITUDE']) and pd.notna(row['LONGITUDE']) and
                            haversine_distance(center_lat, center_lng, row['LATITUDE'], row['LONGITUDE']) <= radius
                        ), axis=1
                    )
                    filtered_df = filtered_df[mask]
    # Ensure IS_B2B column is always present in output
    if "IS_B2B" not in filtered_df.columns and "IS_B2B" in df.columns:
        filtered_df["IS_B2B"] = df["IS_B2B"]
    # Always show all columns in the data editor, merging missing columns from the source DataFrame
    for col in df.columns:
        if col not in filtered_df.columns:
            filtered_df[col] = df[col]
    filtered_df = filtered_df[df.columns]
    filtered_df = filtered_df.reset_index(drop=True)
    return filtered_df

if not st.session_state["sidebar_collapsed"]:
    with st.sidebar:
        st.image("https://cdn.bfldr.com/ZGS6MXDP/at/2cs39569fprccp99mf97np54/global-logo-color", use_container_width=True)
        st.markdown("<h1 class='gp-center-text gp-font-sm gp-color-primary' style='margin: 0rem 0; font-family: var(--font-family-primary);'>Prospecting Portal</h1>", unsafe_allow_html=True)
        # Cortex Analyst Prompt above filters
        if "sidebar_cortex_prompt" not in st.session_state:
            st.session_state["sidebar_cortex_prompt"] = ""
        # Use a flag to reset the prompt value after reset
        if st.session_state.get("reset_sidebar_cortex_prompt", False):
            sidebar_prompt_value = ""
            st.session_state["reset_sidebar_cortex_prompt"] = False
        else:
            sidebar_prompt_value = st.session_state["sidebar_cortex_prompt"]

        with st.expander("Chat Agent", expanded=True):
            sidebar_prompt = st.text_area(
                "Ask a question about your prospects...",
                value=sidebar_prompt_value,
                key="sidebar_cortex_prompt",
                height=100,
                help="Enter your question or prompt for the Cortex Analyst"
            )
            run_analyst = st.button("Run Analyst", key="run_analyst_button", help="Run Cortex Analyst and update List/Map views with results.")
        st.markdown("""
        <div style="display: flex; align-items: center; width: 100%;">
          <hr style="flex:1; border:none; border-top:1px solid #ccc; margin:0 8px 0 0;">
          <span style="white-space:nowrap; font-weight:500;">Or</span>
          <hr style="flex:1; border:none; border-top:1px solid #ccc; margin:0 0 0 8px;">
        </div>
""", unsafe_allow_html=True)
        #st.markdown("<hr>", unsafe_allow_html=True)
        # Refactored: Use unified filter function for analyst prompt
        if run_analyst and st.session_state["sidebar_cortex_prompt"]:
            # Build filters from analyst prompt (if needed, parse prompt to filters)
            # For now, treat prompt as a text filter on DBA_NAME or similar
            analyst_filters = st.session_state["filters"].copy()
            analyst_filters["DBA_NAME"] = st.session_state["sidebar_cortex_prompt"]
            st.session_state["active_filters"] = analyst_filters
            # Standardize output columns for analyst prompt
            display_columns = [
                "DBA_NAME", "STATE", "CITY", "ZIP", "PRIMARY_INDUSTRY", "SUB_INDUSTRY", "SIC_CODE", "B2B", "B2C"
            ]
            st.session_state["filtered_df"] = get_filtered_dataframe(
                st.session_state["full_df"] if "full_df" in st.session_state else pd.DataFrame(),
                analyst_filters,
                display_columns
            )

if "cortex_analyst_results" in st.session_state:
    results = st.session_state["cortex_analyst_results"]
    if results is not None and len(results) > 0:
        import pandas as pd
        # Convert to DataFrame if needed
        if isinstance(results, pd.DataFrame):
            df_results = results
        else:
            df_results = pd.DataFrame(results)
        # Set the main filtered_df to the Cortex results and rerun to use the main table instance
        st.session_state["filtered_df"] = df_results
        st.session_state["cortex_results_active"] = True
        st.rerun()


def save_search(user_id, search_name, filters):

    try:
        if not filters:
            show_error_message("Filters dictionary is empty. Please apply at least one filter.")
            return
        cleaned_filters = {}
        valid_keys = set(STATIC_FILTERS.keys())
        for key, value in filters.items():
            if key not in valid_keys:
                show_error_message("Invalid filter key", f"{key} (expected one of {valid_keys})")
                return
            config = STATIC_FILTERS[key]
            if config.get("type") == "range":
                if isinstance(value, (tuple, list)) and len(value) == 2:
                    cleaned_filters[key] = [None if v is None else v for v in value]
                else:
                    show_error_message("Invalid range filter", f"for {key}: {value} (expected list/tuple of 2 values)")
                    return
            elif config.get("type") == "dropdown":
                if isinstance(value, list) and all(isinstance(v, (str, int, float)) and str(v).strip() and str(v).lower() not in ['d', 'i', 'ii', 'u', 'none', 'null', '[', ']', '', 'invalid'] for v in value):
                    cleaned_filters[key] = value
                else:
                    cleaned_filters[key] = []
            elif config.get("type") == "checkbox":
                if isinstance(value, bool):
                    cleaned_filters[key] = value
                else:
                    show_error_message("Invalid checkbox filter", f"for {key}: {value} (expected boolean)")
                    return
            elif config.get("type") == "text":
                if isinstance(value, str):
                    cleaned_filters[key] = value.strip()
                else:
                    show_error_message("Invalid text filter", f"for {key}: {value} (expected string)")
                    return
        try:
            filters_json = json.dumps(cleaned_filters, ensure_ascii=False)
            parsed_json = json.loads(filters_json)
            if not parsed_json:
                show_error_message("Generated JSON is empty", f"(Filters: {cleaned_filters})")
                return
            if len(filters_json) > 1048576:
                show_error_message("JSON string too large", f"{len(filters_json)} bytes (Filters: {cleaned_filters})")
                return
            if not re.match(r'^[\x20-\x7E\n\t]*$', filters_json):
                show_error_message("JSON contains invalid characters", f"(Filters: {cleaned_filters})")
                return
        except json.JSONDecodeError as e:
            show_error_message("Invalid JSON generated from filters", f"{str(e)} (Filters: {cleaned_filters})")
            return
        except TypeError as e:
            show_error_message("Filters contain non-serializable data", f"{str(e)} (Filters: {cleaned_filters})")
            return
        existing = session.table("SANDBOX.CONKLIN.SAVED_SEARCHES") \
                        .filter((col("USER_ID") == user_id) & (col("SEARCH_NAME") == search_name)) \
                        .count()
        if existing > 0:
            query = """
                UPDATE SANDBOX.CONKLIN.SAVED_SEARCHES
                SET FILTERS = PARSE_JSON(?), CREATED_AT = CURRENT_TIMESTAMP()
                WHERE USER_ID = ? AND SEARCH_NAME = ?
            """
            execute_sql_command(query, params=[filters_json, user_id, search_name], operation_name="update_saved_search")
        else:
            query = """
                INSERT INTO SANDBOX.CONKLIN.SAVED_SEARCHES (USER_ID, SEARCH_NAME, FILTERS, CREATED_AT)
                SELECT ?, ?, PARSE_JSON(?), CURRENT_TIMESTAMP()
            """
            execute_sql_command(query, params=[user_id, search_name, filters_json], operation_name="insert_saved_search")
        show_success_message(f"Saved search '{search_name}' successfully!")
    except Exception as e:
        show_error_message("Error saving search", f"{str(e)} (Filters: {cleaned_filters}, JSON: {filters_json})")
def load_saved_searches(user_id):

    try:
        df = session.table("SANDBOX.CONKLIN.SAVED_SEARCHES") \
                    .filter(col("USER_ID") == user_id) \
                    .select("SEARCH_NAME") \
                    .to_pandas()
        searches = df.to_dict("records")
        return searches
    except Exception as e:
        show_error_message("Error loading saved searches", str(e))
        return []

def load_search(user_id, search_name):
    try:
        query = """
            SELECT FILTERS
            FROM SANDBOX.CONKLIN.SAVED_SEARCHES
            WHERE USER_ID = ? AND SEARCH_NAME = ?
        """
        df = execute_sql_query(query, params=[user_id, search_name], operation_name="load_saved_search")
        if not df.empty:
            filters = json.loads(df["FILTERS"].iloc[0])
            for key in STATIC_FILTERS:
                if key not in st.session_state["filters"]:
                    if STATIC_FILTERS[key]["type"] == "range":
                        st.session_state["filters"][key] = [None, None]
                    elif STATIC_FILTERS[key]["type"] == "dropdown":
                        st.session_state["filters"][key] = []
                    elif STATIC_FILTERS[key]["type"] == "location_radius":
                        st.session_state["filters"][key] = {"address": "", "radius": 25}
                    elif STATIC_FILTERS[key]["type"] == "checkbox":
                        st.session_state["filters"][key] = False
                    elif STATIC_FILTERS[key]["type"] == "selectbox":
                        st.session_state["filters"][key] = STATIC_FILTERS[key]["options"][0]
                    else:
                        st.session_state["filters"][key] = ""
            for key, value in filters.items():
                if key in STATIC_FILTERS:
                    if STATIC_FILTERS[key]["type"] == "range":
                        if isinstance(value, (list, tuple)) and len(value) == 2:
                            try:
                                min_val = float(value[0]) if value[0] is not None else None
                                max_val = float(value[1]) if value[1] is not None else None
                                st.session_state["filters"][key] = [min_val, max_val]
                            except (ValueError, TypeError):
                                st.session_state["filters"][key] = [None, None]
                        else:
                            st.session_state["filters"][key] = [None, None]
                    elif STATIC_FILTERS[key]["type"] == "location_radius":
                        if isinstance(value, dict) and "address" in value and "radius" in value:
                            st.session_state["filters"][key] = value
                        else:
                            st.session_state["filters"][key] = {"address": "", "radius": 25}
                    else:
                        st.session_state["filters"][key] = value
            st.session_state["last_update_time"] = time.time()
            reset_to_first_page()
            increment_session_state_counter('load_search_counter')
            for col in get_filters_by_type("dropdown"):
                st.session_state["filter_update_trigger"][col] += 1
            show_success_message(f"Loaded search '{search_name}' successfully!")
            st.session_state["search_name"] = ""
            st.session_state["selected_search"] = ""
            st.rerun()
        else:
            st.warning(f"No saved search found with name '{search_name}'.")
            st.session_state["search_name"] = ""
            st.session_state["selected_search"] = ""
            st.rerun()
    except Exception as e:
        show_error_message("Error loading search", str(e))
        st.session_state["search_name"] = ""
        st.session_state["selected_search"] = ""
        st.rerun()

def create_cache_key(column, dependent_filters):

    filter_str = f"{column}:"
    for dep_col, dep_val in sorted(dependent_filters.items()):
        if dep_col in STATIC_FILTERS:
            if STATIC_FILTERS[dep_col]["type"] == "dropdown":
                val_str = ",".join(sorted(map(str, dep_val))) if dep_val else ""
            elif STATIC_FILTERS[dep_col]["type"] == "range":
                if isinstance(dep_val, (list, tuple)) and len(dep_val) == 2:
                    min_val, max_val = dep_val
                else:
                    min_val, max_val = None, None
                val_str = f"{min_val or ''}_{max_val or ''}"
            elif STATIC_FILTERS[dep_col]["type"] == "text":
                val_str = str(dep_val)
            else:
                val_str = str(dep_val)
        else:
            # Handle special filters that aren't in STATIC_FILTERS
            val_str = str(dep_val)
        filter_str += f"{dep_col}={val_str};"
    return hashlib.md5(filter_str.encode()).hexdigest()

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def fetch_unique_values(column, dependent_filters, cache_key, _trigger):
    """Fetch unique values for dropdown filters with dynamic filtering based on other active filters"""
    try:
        start_time = time.time()
        column_name = STATIC_FILTERS.get(column, {}).get("column_name", column)
        
        # Start with the filter options view
        query = f"SELECT DISTINCT {column_name} FROM {get_filter_table_name()} WHERE {column_name} IS NOT NULL"
        params = []
        
        # Process dependent filters
        dropdown_columns = [k for k, v in STATIC_FILTERS.items() if v["type"] == "dropdown"]
        
        for dep_col, dep_values in dependent_filters:
            # Handle special case filters
            if dep_col == "HAS_CONTACT_INFO":
                if dep_values is True:
                    query += " AND HAS_CONTACT_INFO = ?"
                    params.append(True)
                continue
            
            if dep_col == "location_filter":
                # Location-based filtering would require geocoding and distance calculations
                # For now, we'll note this but not implement complex location filtering in the filter view
                # This can be enhanced later if the filter view supports location-based queries
                continue
            
            # Handle dropdown filters
            if dep_col in dropdown_columns and STATIC_FILTERS[dep_col]["type"] == "dropdown":
                dep_column_name = STATIC_FILTERS.get(dep_col, {}).get("column_name", dep_col)
                
                # Skip empty or null values
                if isinstance(dep_values, (list, tuple)) and len(dep_values) == 2 and all(v is None for v in dep_values):
                    continue
                    
                if dep_values:
                    if isinstance(dep_values, list) and dep_values:
                        valid_values = [v for v in dep_values if v is not None and str(v).strip()]
                        if valid_values:
                            query += f" AND {dep_column_name} IN ({','.join(['?' for _ in valid_values])})"
                            params.extend(valid_values)
                    elif not isinstance(dep_values, list) and str(dep_values).strip():
                        query += f" AND {dep_column_name} = ?"
                        params.append(dep_values)
            
            # Handle range filters
            elif dep_col in STATIC_FILTERS and STATIC_FILTERS[dep_col]["type"] == "range":
                dep_column_name = STATIC_FILTERS.get(dep_col, {}).get("column_name", dep_col)
                if isinstance(dep_values, (list, tuple)) and len(dep_values) == 2:
                    min_val, max_val = dep_values
                    if min_val is not None and max_val is not None:
                        query += f" AND {dep_column_name} BETWEEN ? AND ?"
                        params.extend([min_val, max_val])
                    elif min_val is not None:
                        query += f" AND {dep_column_name} >= ?"
                        params.append(min_val)
                    elif max_val is not None:
                        query += f" AND {dep_column_name} <= ?"
                        params.append(max_val)
            
            # Handle text filters
            elif dep_col in STATIC_FILTERS and STATIC_FILTERS[dep_col]["type"] == "text":
                dep_column_name = STATIC_FILTERS.get(dep_col, {}).get("column_name", dep_col)
                if dep_values and str(dep_values).strip():
                    terms = [term.strip().lower() for term in str(dep_values).split() if term.strip()]
                    if terms:
                        for term in terms:
                            query += f" AND LOWER({dep_column_name}) LIKE ?"
                            params.append(f"%{term}%")
        
        if not validate_query_params(query, params, f"fetch_unique_values for {column}"):
            return []
        
        # Debug output for filter dependencies
        if st.session_state.get("filter_debug", False):
            st.write(f"**Debug - fetch_unique_values for {column}:**")
            st.write(f"- Dependencies: {dict(dependent_filters)}")
            st.write(f"- Query: {query}")
            st.write(f"- Params: {params}")
        
        df = execute_sql_query(query, params=params, operation_name=f"fetch_unique_values for {column}")
        values = sorted(df[column_name].dropna().tolist())
        
        invalid_values = ['d', 'i', 'ii', 'u', 'none', 'null', '[', ']', '', 'invalid']
        valid_values = [
            v for v in values
            if isinstance(v, (str, int, float))
            and str(v).strip()
            and str(v).lower() not in invalid_values
        ]
        
        MAX_OPTIONS = 100000
        if len(valid_values) > MAX_OPTIONS:
            valid_values = valid_values[:MAX_OPTIONS]
            st.warning(f"Showing top {MAX_OPTIONS} {column.lower()} options. Refine filters for more specific results.")
        
        query_time = time.time() - start_time
        return valid_values
        
    except Exception as e:
        show_error_message(f"Error fetching unique values for {column}", str(e))
        return []

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def fetch_filtered_data(filters, _cache_key, page_size, current_page, fetch_all=False):

    try:
        logical_columns = [
            "IDENTIFIER", "DBA_NAME", "ADDRESS", "CITY", "STATE", "ZIP", "PHONE", "CONTACT_NAME", "CONTACT_EMAIL", "CONTACT_PHONE", "CONTACT_MOBILE", "CONTACT_JOB_TITLE", "PRIMARY_INDUSTRY", "SUB_INDUSTRY", "SIC_CODE",
            "REVENUE", "NUMBER_OF_EMPLOYEES", "NUMBER_OF_LOCATIONS", "IS_B2B", "IS_B2C", "LONGITUDE", "LATITUDE", "FULL_ADDRESS", "WEBSITE", "IS_CURRENT_CUSTOMER", "DATA_AGG_UID", "PARENT_NAME", "PARENT_PHONE", "PARENT_WEBSITE",
            "TOP10_CONTACTS", "CONTACT_NATIONAL_DNC", "INTERNAL_DNC", "HAS_CONTACT_INFO"
            ]
        logical_to_actual = {"SIC_CODE": "SIC", "B2B": "IS_B2B", "B2C": "IS_B2C"}
        # All columns except TOP10_CONTACTS come from main table, TOP10_CONTACTS comes from tc
        columns = [
            f"main.{logical_to_actual.get(col, col)}" if col != "TOP10_CONTACTS" else "tc.TOP10_CONTACTS" for col in logical_columns
        ]
        count_query = f"SELECT COUNT(*) AS total FROM {get_table_name()} main"
        query = f"SELECT {', '.join(columns)} FROM {get_table_name()} main LEFT JOIN sandbox.conklin.top_contacts tc ON main.PROSPECT_ID = tc.PROSPECT_ID"
        where_clauses = []
        params = []
        for column, value in filters.items():
            if column == "customer_status":
                if value == "Current Customers Only":
                    where_clauses.append("IS_CURRENT_CUSTOMER = ?")
                    params.append(True)
                elif value == "Non-Customers Only":
                    where_clauses.append("IS_CURRENT_CUSTOMER = ?")
                    params.append(False)
                # If "Show both...", do not filter
            elif column == "HAS_CONTACT_INFO":
                # Handle the contact info filter (boolean value from dropdown conversion)
                if value is True:
                    where_clauses.append("main.HAS_CONTACT_INFO = ?")
                    params.append(True)
                # If value is False ("Show All Prospects"), do not filter
            elif column in STATIC_FILTERS:
                filter_type = STATIC_FILTERS[column]["type"]
                if filter_type == "dropdown" and value:
                    where_clauses.append(f"{STATIC_FILTERS[column]['column_name']} IN ({','.join(['?' for _ in value])})")
                    params.extend(value)
                elif filter_type == "range" and value != [None, None]:
                    min_val, max_val = value
                    if min_val is not None and max_val is not None and min_val > max_val:
                        st.warning(f"Min {STATIC_FILTERS[column]['label']} cannot be greater than Max")
                        return pd.DataFrame(), 0
                    if min_val is not None:
                        where_clauses.append(f"{STATIC_FILTERS[column]['column_name']} >= ?")
                        params.append(min_val)
                    if max_val is not None:
                        where_clauses.append(f"{STATIC_FILTERS[column]['column_name']} <= ?")
                        params.append(max_val)
                elif filter_type == "selectbox" and column in ["B2B", "B2C"]:
                    # Handle B2B/B2C selectbox logic
                    if value == "Exclude B2B" or value == "Exclude B2C":
                        where_clauses.append(f"{STATIC_FILTERS[column]['column_name']} = ?")
                        params.append(0)
                    elif value == "Show only B2B" or value == "Show only B2C":
                        where_clauses.append(f"{STATIC_FILTERS[column]['column_name']} = ?")
                        params.append(1)
                    # If "Include ...", do not filter
                elif filter_type == "text" and value.strip():
                    terms = [term.strip().lower() for term in value.split() if term.strip()]
                    if terms:
                        for term in terms:
                            where_clauses.append(f"LOWER({STATIC_FILTERS[column]['column_name']}) LIKE ?")
                            params.append(f"%{term}%")
                elif filter_type == "location_radius" and value.get("address", "").strip():
                    # Handle location radius search
                    address = value.get("address", "").strip()
                    radius = value.get("radius", 25)
                    
                    # Geocode the address
                    st.write(f"Debug - Geocoding address: '{address}'")
                    geocoded = geocode_address(address)
                    st.write(f"Debug - Geocoded result: {geocoded}")
                    
                    if geocoded:
                        lat, lng = geocoded
                        st.write(f"Debug - Parsed coordinates: lat={lat}, lng={lng}")
                        
                        # Validate coordinates are reasonable for US
                        if not (20 <= lat <= 70 and -180 <= lng <= -60):
                            st.error(f"⚠️ Invalid coordinates for US location: {lat}, {lng}")
                            return pd.DataFrame(), 0
                        
                        # Build radius filter SQL using Snowflake geospatial functions
                        # Use main.LATITUDE and main.LONGITUDE since we're using table alias
                        radius_sql = build_radius_filter_sql(lat, lng, radius, "main.LATITUDE", "main.LONGITUDE")
                        st.write(f"Debug - Generated radius SQL: {radius_sql}")
                        where_clauses.append(radius_sql)
                        # Debug: Show what radius filter is being applied
                        st.success(f"🌍 Applying radius filter: {address} → {lat:.4f}, {lng:.4f} ({radius} miles)")
                        # No parameters needed since coordinates are embedded in SQL
                    else:
                        # If geocoding fails, show a warning but continue with other filters
                        st.warning(f"⚠️ Could not geocode address '{address}'. Radius filter will be ignored.")
            elif column == "show_all_customers" and value:
                # Special filter for showing only customers with valid identifier
                where_clauses.append("identifier IS NOT NULL AND TRIM(identifier) != '' AND UPPER(TRIM(identifier)) != 'NAN'")
        
        if where_clauses:
            condition = " WHERE " + " AND ".join(where_clauses)
            count_query += condition
            query += condition
            # Debug: Show the final SQL query with all filters including HAS_CONTACT_INFO
            if st.session_state.get("filter_debug", False):
                st.write("**Debug SQL Generation:**")
                st.write(f"- WHERE clauses: {where_clauses}")
                st.write(f"- Parameters: {params}")
                st.write(f"- Full query: {query[:800]}...")
        
        if st.session_state.get("filter_debug", False):
            st.write(f"**Debug - Total filters processed:** {len(filters)}")
            st.write(f"**Debug - WHERE clauses generated:** {len(where_clauses)}")
        total_records = execute_sql_query(count_query, params=params, operation_name="fetch_filtered_data_count", return_single_value=True)
        if "limit_warning" in st.session_state:
            del st.session_state["limit_warning"]
        query += f" ORDER BY DBA_NAME"
        if total_records > MAX_RESULTS:
            st.session_state["limit_warning"] = f"Result set contains {total_records} records, which exceeds the limit of {MAX_RESULTS}. Displaying the first {MAX_RESULTS} records."
            query += f" LIMIT {MAX_RESULTS}"
            total_records = min(total_records, MAX_RESULTS)
        elif not fetch_all:
            offset = calculate_sql_offset(current_page, page_size)
            query += f" LIMIT {page_size} OFFSET {offset}"
        df = execute_sql_query(query, params=params, operation_name="fetch_filtered_data")
        return df, total_records
    except Exception as e:
        show_error_message("Error fetching filtered data", f"{str(e)}\nQuery: {query}\nParams: {params}")
        return pd.DataFrame(), 0

def display_filter_summary(filters):
    active_filters = []
    for column, value in filters.items():
        config = STATIC_FILTERS.get(column, {})
        label = config.get("label", column)
        if config.get("type") == "dropdown" and value:
            active_filters.append(f"{label}: {', '.join(map(str, value))}")
        elif config.get("type") == "range" and value != [None, None]:
            min_val, max_val = value
            if min_val is not None and max_val is not None:
                active_filters.append(f"{label}: {min_val} to {max_val}")
            elif min_val is not None:
                active_filters.append(f"{label}: ≥ {min_val}")
            elif max_val is not None:
                active_filters.append(f"{label}: ≤ {max_val}")
        elif config.get("type") == "checkbox" and value:
            active_filters.append(f"{label}: Excluded")
        elif config.get("type") == "text" and value.strip():
            terms = [term.strip() for term in value.split() if term.strip()]
            if terms:
                active_filters.append(f"{label}: Contains {', '.join(f'{term!r}' for term in terms)}")
        elif column == "show_all_customers" and value:
            active_filters.append("Customer Filter: Existing Customers Only")

    filtered_df = st.session_state.get("filtered_df", None)
    has_results = filtered_df is not None and not filtered_df.empty
    if active_filters or has_results:
        with st.expander(f"Active Search Filters ({len(active_filters)})", expanded=False):

            chips_per_row = CHIPS_PER_ROW  # More chips per row for compactness
            for i in range(0, len(active_filters), chips_per_row):
                cols = st.columns(chips_per_row)
                for j, filter_str in enumerate(active_filters[i:i+chips_per_row]):
                    with cols[j]:
                        if ":" in filter_str:
                            parts = filter_str.split(":", 1)  # Split on first colon only
                            filter_label = parts[0].strip()
                            filter_value = parts[1].strip()
                            
                            if filter_label.lower() == "zip code" and "contains" in filter_value.lower():
                                import re
                                zip_match = re.search(r"'([^']+)'", filter_value)
                                if zip_match:
                                    filter_value = zip_match.group(1)
                            styled_filter = f"{filter_label}: <strong>{filter_value}</strong>"
                        else:
                            styled_filter = filter_str
                        
                        st.markdown(f"""
                        <div style="
                            background: linear-gradient(135deg, #f8f8f8 0%, #ffffff 100%);
                            border: 1px solid #c4c4c4;
                            border-radius: 12px;
                            padding: 0.25rem 0.5rem;
                            font-family: 'DM Sans', sans-serif;
                            font-size: 0.75rem;
                            color: #0c0c0c;
                            text-align: center;
                            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
                            border-left: 2px solid #262AFF;
                            margin-bottom: 0.25rem;
                            line-height: 1.2;
                        ">
                            {styled_filter}
                        </div>
                        """, unsafe_allow_html=True)
    else:
        filtered_df = st.session_state.get("filtered_df", None)
        if filtered_df is None or filtered_df.empty:
            st.markdown("""
            <style>
            .no-filters-container {
                background: linear-gradient(135deg, var(--gp-mist) 0%, var(--gp-white) 100%);
                border: 2px dashed var(--gp-smoke);
                border-radius: 12px;
                padding: 1.5rem;
                text-align: center;
                margin: 0.5rem 0;
                transition: all 0.3s ease;
            }
            
            .no-filters-container:hover {
                border-color: var(--gp-primary);
                transform: translateY(-1px);
                box-shadow: 0 4px 12px rgba(38, 42, 255, 0.08);
            }
            
            .no-filters-icon {
                font-size: 2rem;
                margin-bottom: 0.5rem;
                opacity: 0.6;
            }
            
            .no-filters-text {
                font-family: var(--font-family-primary);
                font-size: 0.95rem;
                color: var(--gp-charcoal);
                font-weight: 500;
                margin: 0;
            }
            
            .no-filters-subtext {
                font-family: var(--font-family-primary);
                font-size: 0.8rem;
                color: var(--gp-charcoal);
                opacity: 0.8;
                margin-top: 0.25rem;
            }
            </style>
            
            <div class="no-filters-container">
                <p class="no-filters-text">No search filters currently applied</p>
                <p class="no-filters-subtext">Use the sidebar to narrow down your business prospects</p>
            </div>
            """, unsafe_allow_html=True)
            
def apply_b2b_b2c_filters(df, filters):
    # B2B logic
    b2b_choice = filters.get("B2B", "Include B2B & B2C")
    if b2b_choice == "Exclude B2B":
        df = df[df["IS_B2B"] == 0]
    elif b2b_choice == "Only B2B":
        df = df[df["IS_B2B"] == 1]
    # B2C logic
    b2c_choice = filters.get("B2C", "Include B2B & B2C")
    if b2c_choice == "Exclude B2C":
        df = df[df["IS_B2C"] == 0]
    elif b2c_choice == "Only B2C":
        df = df[df["IS_B2C"] == 1]
    return df

def add_businesses_to_salesforce(business_df):

    if business_df.empty:
        return 0
    business_ids = business_df.index.tolist()
    newly_added = 0
    
    for business_id in business_ids:
        if add_business_to_salesforce(business_id):
            newly_added += 1
    
    return newly_added

def create_sidebar_filters():
    def generate_text_filter(column, config, placeholder=None):
        label = config["label"]
        default_placeholder = f"Search {config['label'].lower()}"
        if placeholder is None:
            placeholder = "Enter ZIP code (e.g., 12345)" if column == "ZIP" else default_placeholder
        
        search_value = st.text_input(
            label,
            value=st.session_state["filters"].get(column, ""),
            key=f"{column}_filter",
            placeholder=placeholder,
            help=f"Enter terms to search for in {config['label'].lower()} (case-insensitive).",
            label_visibility="visible"
        )
        
        prev_value = st.session_state["filters"].get(column, "")
        if search_value != prev_value:
            st.session_state["filters"][column] = search_value.strip()
            # Don't auto-update - wait for apply button
            reset_to_first_page()
            update_filter_triggers(get_filters_by_type("dropdown"))
        
        return search_value.strip()
    
    def generate_dropdown_filter(column, config):
        # Get current filter values to create dependencies
        dependent_filters = {}
        
        # Always include contact info filter in dependencies since it's always active
        contact_info_filter = st.session_state.get("has_contact_info", "Show Only Prospects with Contact Info")
        if contact_info_filter == "Show Only Prospects with Contact Info":
            dependent_filters["HAS_CONTACT_INFO"] = True
        
        # Include location-based filters if they exist and are active
        if st.session_state.get("active_filters", {}).get("location_radius"):
            location_filter = st.session_state["active_filters"]["location_radius"]
            if location_filter.get("address") and location_filter.get("radius"):
                # Include location filter in dependencies
                dependent_filters["location_filter"] = f"{location_filter['address']}:{location_filter['radius']}"
        
        # Include other dropdown and text filters as dependencies (excluding the current column)
        for k in filter_columns:
            if k != column and k not in ["B2B", "B2C", "HAS_CONTACT_INFO"]:
                filter_value = st.session_state["filters"].get(k, "")
                if is_filter_active(k, filter_value):
                    dependent_filters[k] = filter_value
        
        # Debug output for dependencies
        if st.session_state.get("filter_debug", False):
            st.write(f"**Debug - {column} filter dependencies:**")
            st.write(f"- Contact info: {contact_info_filter}")
            st.write(f"- Dependencies: {dependent_filters}")
        
        cache_key = create_cache_key(column, dependent_filters)
        values = with_loading_spinner(
            f"Loading {config['label'].lower()} options...",
            lambda: fetch_unique_values(
                column,
                tuple(dependent_filters.items()),
                cache_key,
                st.session_state["filter_update_trigger"].get(column, 0)
            )
        )
        
        valid_values = [
            v for v in values
            if isinstance(v, (str, int, float))
            and str(v).strip()
            and str(v).lower() not in ['d', 'i', 'ii', 'u', 'none', 'null', '[', ']', '', 'invalid']
        ]
        
        current_value = st.session_state["filters"].get(column, [])
        if not valid_values:
            st.warning(f"No {config['label'].lower()} options available. Try adjusting other filters.")
        
        selected = st.multiselect(
            config["label"],
            options=sorted(set(valid_values + current_value)),
            default=current_value,
            key=f"{column}_filter",
            placeholder=f"Select {config['label'].lower()}" if valid_values else f"No {config['label'].lower()} available",
            disabled=not valid_values,
            help=f"Select one or more {config['label'].lower()} to filter results. Options adjust based on other active filters including contact info."
        )
        
        if selected != st.session_state["filters"][column]:
            st.session_state["filters"][column] = selected
            # Don't auto-update - wait for apply button
            reset_to_first_page()
            # Update filter triggers for ALL dropdown filters when any dropdown changes
            # This ensures that dependent filters (like SUB_INDUSTRY) update when parent filters (like PRIMARY_INDUSTRY) change
            all_dropdown_filters = get_filters_by_type("dropdown")
            update_filter_triggers(all_dropdown_filters, exclude_column=column)
        
        return selected

    filters = {}
    filter_columns = list(STATIC_FILTERS.keys())
    dropdown_columns = [k for k, v in STATIC_FILTERS.items() if v["type"] == "dropdown"]
    
    if not st.session_state["sidebar_collapsed"]:
        with st.sidebar:
            # Contact Info Filter dropdown (before Location expander)
            contact_info_options = ["Show Only Prospects with Contact Info", "Show All Prospects"]
            current_contact_info = st.session_state.get("has_contact_info", contact_info_options[0])
            if current_contact_info not in contact_info_options:
                current_contact_info = contact_info_options[0]
            
            has_contact_info = st.selectbox(
                "Contact Info Filter",
                options=contact_info_options,
                index=contact_info_options.index(current_contact_info),
                key="has_contact_info_selectbox",
                help="Choose whether to show all prospects or only those with contact information."
            )
            
            # Check if the value changed and update dependent filters
            if has_contact_info != st.session_state.get("has_contact_info", contact_info_options[0]):
                st.session_state["has_contact_info"] = has_contact_info
                # Trigger update for ALL dropdown filters since they ALL depend on contact info
                all_dropdown_filters = [k for k, v in STATIC_FILTERS.items() if v["type"] == "dropdown"]
                for col in all_dropdown_filters:
                    if col in st.session_state.get("filter_update_trigger", {}):
                        st.session_state["filter_update_trigger"][col] += 1
            else:
                st.session_state["has_contact_info"] = has_contact_info
                
            # Convert dropdown selection to boolean for filter logic
            filters['HAS_CONTACT_INFO'] = (has_contact_info == "Show Only Prospects with Contact Info")
            
            # Debug information for testing (remove in production)
            if st.checkbox("Show Filter Debug Info", key="filter_debug"):
                st.write("**Contact Info Filter State:**")
                st.write(f"- Current Selection: {has_contact_info}")
                st.write(f"- Filter Value: {filters['HAS_CONTACT_INFO']}")
                st.write("**All Active Filters:**")
                for k, v in filters.items():
                    if k != 'HAS_CONTACT_INFO' and v and v != [] and v != [None, None]:
                        st.write(f"- {k}: {v}")
                st.write("**Filter Update Triggers:**")
                for k, v in st.session_state.get('filter_update_trigger', {}).items():
                    st.write(f"- {k}: {v}")
                st.write("**Session State Filters:**")
                for k, v in st.session_state.get('filters', {}).items():
                    if v and v != [] and v != [None, None]:
                        st.write(f"- {k}: {v}")
            # Enhanced CSS for the expandable sections design
            st.markdown("""
                <style>
                /* Button styling improvements - keep it simple and targeted */
                .stButton > button {
                    width: 100% !important;
                    padding: 0.5rem !important;
                    border-radius: 6px !important;
                    font-size: 0.9rem !important;
                    min-height: 40px !important;
                    font-weight: 500 !important;
                    transition: all 0.2s ease !important;
                }
                
                /* Primary button styling */
                .stButton > button[kind="primary"] {
                    background-color: #262aff !important;
                    color: white !important;
                    border: none !important;
                }
                
                /* Secondary button styling */
                .stButton > button[kind="secondary"] {
                    background-color: #ffffff !important;
                    color: #1a1b23 !important;
                    border: 2px solid #e6e9f3 !important;
                }
                
                /* Hover effects */
                .stButton > button[kind="primary"]:hover {
                    background-color: #1b1c6e !important;
                    transform: translateY(-1px) !important;
                }
                
                .stButton > button[kind="secondary"]:hover {
                    background-color: #f0f2f7 !important;
                    border-color: #2e3748 !important;
                }
                
                /* Disabled state */
                .stButton > button:disabled {
                    opacity: 0.5 !important;
                    cursor: not-allowed !important;
                    transform: none !important;
                }
                
                /* Input field styling */
                div[data-testid="stSidebar"] .stTextInput > div > input,
                div[data-testid="stSidebar"] .stNumberInput > div > input {
                    border-radius: 6px !important;
                    font-size: 0.85rem !important;
                }
                
                /* Expander styling */
                .streamlit-expanderHeader {
                    background-color: var(--gp-haze) !important;
                    border-radius: 6px !important;
                    font-weight: 500 !important;
                    padding: 8px 12px !important;
                    margin-bottom: 0.5rem !important;
                }
                
                /* Add spacing between expanders */
                .streamlit-expander {
                    margin-bottom: 1rem !important;
                }
                </style>
            """, unsafe_allow_html=True)
            
            # Location Filters Expander
            with st.expander("Location", expanded=True):
                st.info("🎯 **Smart Location Search**: Enter any address, city, or ZIP code + radius to find businesses. This replaces ZIP/City/State filters with precise location targeting.")
                
                # Location Radius Search
                st.markdown("**🌍 Address & Radius Search**")
                current_location_filter = st.session_state["filters"].get("LOCATION_RADIUS", {"address": "", "radius": 25})
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    address_input = st.text_input(
                        "Address or ZIP Code",
                        value=current_location_filter["address"],
                        placeholder="Enter address, city, or ZIP code",
                        help="Enter any address, city, or ZIP code to search within a radius",
                        key="location_radius_address"
                    )
                with col2:
                    radius_input = st.number_input(
                        "Radius (miles)",
                        min_value=1,
                        max_value=500,
                        value=current_location_filter["radius"],
                        step=1,
                        help="Search radius in miles",
                        key="location_radius_miles"
                    )
                
                # Update session state if values changed
                new_location_filter = {"address": address_input, "radius": radius_input}
                if new_location_filter != current_location_filter:
                    st.session_state["filters"]["LOCATION_RADIUS"] = new_location_filter
                    # Trigger dropdown filter updates since location affects available options
                    all_dropdown_filters = [k for k, v in STATIC_FILTERS.items() if v["type"] == "dropdown"]
                    for col in all_dropdown_filters:
                        if col in st.session_state.get("filter_update_trigger", {}):
                            st.session_state["filter_update_trigger"][col] += 1
                    # Don't auto-update - wait for apply button
                
                filters["LOCATION_RADIUS"] = new_location_filter
                
                # Show geocoding status if address is provided
                if address_input.strip():
                    with st.spinner("🔍 Geocoding address..."):
                        geocoded = geocode_address(address_input.strip())
                        if geocoded:
                            lat, lng = geocoded
                            st.success(f"✅ Found location: {lat:.4f}, {lng:.4f}")
                            st.caption("💡 Use other filters below (Industry, Revenue, etc.) to narrow your results within this radius.")
                        else:
                            st.error("❌ Could not find this address. Please try a different search term or be more specific (e.g., include city and state).")
                            st.info("💡 Try formats like: '123 Main St, New York, NY' or '10001'")
                
                st.divider()
                
                st.markdown("**ℹ️ Address search replaces ZIP/City/State filters**")
                st.caption("Use the address search above for location-based filtering. Other business filters are available below.")
            
            # Business Filters Expander
            with st.expander("Business Details", expanded=False):
                # Business name text filter
                for column in ["DBA_NAME"]:
                    if column in STATIC_FILTERS and STATIC_FILTERS[column]["type"] == "text":
                        placeholder = f"Search {STATIC_FILTERS[column]['label'].lower()} (e.g., Taco Bell)"
                        filters[column] = generate_text_filter(column, STATIC_FILTERS[column], placeholder)
                
                # Business dropdown filters
                for column in ["PRIMARY_INDUSTRY", "SUB_INDUSTRY", "SIC_CODE"]:
                    if column in STATIC_FILTERS and STATIC_FILTERS[column]["type"] == "dropdown":
                        filters[column] = generate_dropdown_filter(column, STATIC_FILTERS[column])
                
                # Business Type Filters (B2B/B2C) as selectboxes
                for column in ["B2B", "B2C"]:
                    config = STATIC_FILTERS[column]
                    current_value = st.session_state["filters"].get(column, config["options"][0])
                    if current_value not in config["options"]:
                        current_value = config["options"][0]
                    selected = st.selectbox(
                        config["label"],
                        config["options"],
                        index=config["options"].index(current_value),
                        key=f"{column}_filter_selectbox",
                        help="Filter for Business to Business or Business to Customer accounts."
                    )
                    filters[column] = selected
                    if selected != st.session_state["filters"][column]:
                        st.session_state["filters"][column] = selected
                        # Don't auto-update - wait for apply button
                        reset_to_first_page()
                
                # Current customers section
                st.markdown("**Customer Status**")
                customer_status_options = [
                    "Current and Non-Customers",
                    "Current Customers Only",
                    "Non-Customers Only"
                ]
                current_customer_status = st.session_state["filters"].get("customer_status", customer_status_options[0])
                if current_customer_status not in customer_status_options:
                    current_customer_status = customer_status_options[0]
                customer_status = st.selectbox(
                    "Current Customer Status",
                    customer_status_options,
                    index=customer_status_options.index(current_customer_status),
                    key="customer_status_filter_selectbox",
                    help="Filter by current customer status."
                )
                filters["customer_status"] = customer_status
                if customer_status != st.session_state["filters"].get("customer_status", customer_status_options[0]):
                    st.session_state["filters"]["customer_status"] = customer_status
            
            # Metrics Filters Expander
            with st.expander("Metrics", expanded=False):
                # Range filters
                for column, config in STATIC_FILTERS.items():
                    if config["type"] == "range":
                        st.markdown(f"**{config['label']}**")
                        col1, col2 = create_two_column_layout([1, 1])
                        current_min = st.session_state["filters"][column][0]
                        current_max = st.session_state["filters"][column][1]
                        with col1:
                            min_val = st.number_input(
                                "Minimum",
                                value=float(current_min) if current_min is not None else None,
                                key=f"min_{column.lower()}_{get_load_search_counter()}",
                                step=DEFAULT_STEP_SIZE,
                                format="%.0f",
                                placeholder="Min",
                                help=f"Set the minimum {config['label'].lower()}."
                            )
                        with col2:
                            max_val = st.number_input(
                                "Maximum",
                                value=float(current_max) if current_max is not None else None,
                                key=f"max_{column.lower()}_{get_load_search_counter()}",
                                step=DEFAULT_STEP_SIZE,
                                format="%.0f",
                                placeholder="Max",
                                help=f"Set the maximum {config['label'].lower()}."
                            )
                        if min_val is not None and max_val is not None and min_val > max_val:
                            show_error_message(f"Error: Minimum {config['label']} cannot be greater than Maximum.")
                        new_value = [min_val, max_val]
                        if new_value != st.session_state["filters"][column]:
                            st.session_state["filters"][column] = new_value
                            # Don't auto-update - wait for apply button
                            reset_to_first_page()
                            update_filter_triggers(get_filters_by_type("dropdown"))
                        filters[column] = new_value
            
            # Check for filters and errors
            has_filters = has_active_filters(filters)
            has_errors = False
            for column, value in filters.items():
                if STATIC_FILTERS.get(column, {}).get("type") == "range" and value != [None, None]:
                    min_val, max_val = value
                    if min_val is not None and max_val is not None and min_val > max_val:
                        has_errors = True
                        break
            
            # Action buttons - using full width layout instead of columns
            
            # Reset button
            if st.button("Reset All", key="reset_filters", use_container_width=True, type="secondary"):
                st.session_state["filters"] = {
                    col: (
                        [] if STATIC_FILTERS[col]["type"] == "dropdown" else
                        [None, None] if STATIC_FILTERS[col]["type"] == "range" else
                        {"address": "", "radius": 25} if STATIC_FILTERS[col]["type"] == "location_radius" else
                        STATIC_FILTERS[col]["options"][0] if STATIC_FILTERS[col]["type"] == "selectbox" else
                        ""
                    )
                    for col in STATIC_FILTERS
                }
                # Reset Current Customer Status filter to default
                st.session_state["filters"]["customer_status"] = "Current and Non-Customers"
                # Reset contact info filter to default (show only those with contact info)
                st.session_state["has_contact_info"] = "Show Only Prospects with Contact Info"
                st.session_state["last_update_time"] = 0
                reset_to_first_page()
                st.session_state["filtered_df"] = pd.DataFrame()
                st.session_state["active_filters"] = {}
                st.session_state["page_size"] = DEFAULT_PAGE_SIZE
                st.session_state["total_records"] = 0
                st.session_state["confirm_delete_search"] = False
                st.session_state["search_to_delete"] = None
                st.session_state["filter_update_trigger"] = {col: 0 for col in STATIC_FILTERS if STATIC_FILTERS[col]["type"] == "dropdown"}
                st.session_state["search_name"] = ""
                st.session_state["selected_search"] = ""
                st.session_state["reset_sidebar_cortex_prompt"] = True
                st.session_state["last_sidebar_cortex_prompt"] = ""
                st.session_state["cortex_messages"] = []
                increment_session_state_counter('reset_counter')
                if "map_view_state" in st.session_state:
                    del st.session_state.map_view_state
                if "previous_point_selector" in st.session_state:
                    del st.session_state.previous_point_selector
                if "business_multiselect" in st.session_state:
                    del st.session_state.business_multiselect
                st.session_state["business_search_term"] = ""
                st.session_state["selected_business_indices"] = []
                st.session_state["radius_scale"] = DEFAULT_RADIUS_SCALE
                st.rerun()

            # Apply button
            apply_filters = st.button(
                "🔍 Apply Filters & Search",
                key="apply_filters",
                use_container_width=True,
                type="primary",
                disabled=not has_filters or has_errors,
                help="Click to execute search with your selected criteria"
            )
            if apply_filters and has_filters and not has_errors:
                st.session_state["active_filters"] = filters
                st.session_state["radius_scale"] = 1.0
                # Standardize output columns for sidebar filters
                display_columns = [
                    "DBA_NAME", "STATE", "CITY", "ZIP", "PRIMARY_INDUSTRY", "SUB_INDUSTRY", "SIC_CODE", "B2B", "B2C"
                ]
                st.session_state["filtered_df"] = get_filtered_dataframe(
                    st.session_state["full_df"] if "full_df" in st.session_state else pd.DataFrame(),
                    filters,
                    display_columns
                )
            
            # Saved searches section
            with st.expander("Saved Searches"):
                # Use current Snowflake user as default for User ID
                current_user = get_current_user(get_active_session())
                user_id = st.text_input("User ID", value=current_user, help="Enter your user ID or email", key="user_id_input")
                search_name_key = f"search_name_input_{get_load_search_counter()}_{st.session_state['reset_counter']}"
                st.session_state["search_name"] = st.text_input(
                    "Search Name",
                    value=st.session_state["search_name"],
                    placeholder="Enter a name for this search",
                    key=search_name_key
                )
                if st.button("Save Search", key="save_search", use_container_width=True, type="primary"):
                    if st.session_state["search_name"] and has_active_filters(filters):
                        save_search(user_id, st.session_state["search_name"], filters)
                        st.session_state["search_name"] = ""
                        st.session_state["selected_search"] = ""
                        st.rerun()
                    else:
                        st.warning("Please provide a search name and apply at least one filter.")
                
                saved_searches = load_saved_searches(user_id)
                load_search_key = f"load_search_{get_load_search_counter()}_{st.session_state['reset_counter']}"
                st.session_state["selected_search"] = st.selectbox(
                    "Load Saved Search",
                    options=[""] + [s["SEARCH_NAME"] for s in saved_searches],
                    index=0 if not st.session_state["selected_search"] else [s["SEARCH_NAME"] for s in saved_searches].index(st.session_state["selected_search"]) + 1 if st.session_state["selected_search"] in [s["SEARCH_NAME"] for s in saved_searches] else 0,
                    placeholder="Select a saved search",
                    key=load_search_key
                )
                
                # Load Search button (full width)
                if st.button("Load Search", key="load_search_button", use_container_width=True, type="primary", disabled=not st.session_state["selected_search"]):
                    if st.session_state["selected_search"]:
                        load_search(user_id, st.session_state["selected_search"])
                
                # Delete Search button (full width)
                if st.button("Delete Search", key="delete_search", use_container_width=True, type="secondary", disabled=not st.session_state["selected_search"]):
                    st.session_state["confirm_delete_search"] = True
                    st.session_state["search_to_delete"] = st.session_state["selected_search"]
                    time.sleep(0.1)
                    st.rerun()
                
                if st.session_state["confirm_delete_search"] and st.session_state["search_to_delete"]:
                    st.warning(f"Are you sure you want to delete '{st.session_state['search_to_delete']}'?")
                    col_confirm, col_cancel = create_two_column_layout([1, 1])
                    with col_confirm:
                        if st.button("Confirm", key="confirm_delete", use_container_width=True, type="primary"):
                            try:
                                execute_sql_command(
                                    """
                                    DELETE FROM SANDBOX.CONKLIN.SAVED_SEARCHES
                                    WHERE USER_ID = ? AND SEARCH_NAME = ?
                                    """,
                                    params=[user_id, st.session_state["search_to_delete"]],
                                    operation_name="delete_saved_search"
                                )
                                show_success_message(f"Deleted search '{st.session_state['search_to_delete']}' successfully!")
                                st.session_state["confirm_delete_search"] = False
                                st.session_state["search_to_delete"] = None
                                st.session_state["search_name"] = ""
                                st.session_state["selected_search"] = ""
                                st.rerun()
                            except Exception as e:
                                show_error_message("Error deleting search", str(e))
                                st.session_state["confirm_delete_search"] = False
                                st.session_state["search_to_delete"] = None
                                st.session_state["search_name"] = ""
                                st.session_state["selected_search"] = ""
                                st.rerun()
                    with col_cancel:
                        if st.button("Cancel", key="cancel_delete", use_container_width=True, type="secondary"):
                            st.session_state["confirm_delete_search"] = False
                            st.session_state["search_to_delete"] = None
                            st.session_state["search_name"] = ""
                            st.session_state["selected_search"] = ""
                            st.rerun()
    
    return filters, apply_filters

def format_url(value):

    if pd.isna(value) or not value or str(value).strip() in ['-', '', 'nan', 'None']:
        return None
    
    url = str(value).strip()

    # Remove leading slash if present
    if url.startswith('/'):
        url = url[1:]

    # Only add https:// if not already present
    if not url.lower().startswith(('http://', 'https://')):
        url = f"https://{url}"

    return url

def format_phone_for_display(value):

    if pd.isna(value) or not value or str(value).strip() in ['-', '', 'nan', 'None']:
        return None
    
    phone = re.sub(r'\D', '', str(value).strip())
    if len(phone) == PHONE_LENGTH_STANDARD:
        return f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
    elif len(phone) == PHONE_LENGTH_WITH_COUNTRY and phone.startswith('1'):
        return f"({phone[1:4]}) {phone[4:7]}-{phone[7:]}"
    else:
        return str(value).strip()

def format_phone_for_link(value):

    if pd.isna(value) or not value or str(value).strip() in ['-', '', 'nan', 'None']:
        return None
    
    phone = re.sub(r'\D', '', str(value).strip())
    if len(phone) == PHONE_LENGTH_STANDARD:
        formatted_display = f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
        return f"tel:+1{phone}"
    elif len(phone) == PHONE_LENGTH_WITH_COUNTRY and phone.startswith('1'):
        formatted_display = f"({phone[1:4]}) {phone[4:7]}-{phone[7:]}"
        return f"tel:+{phone}"
    else:
        # For international or non-standard formats, just prepend tel:
        return f"tel:{phone}" if phone else None

def format_address_for_link(address_parts):

    if not address_parts:
        return None
    
    # Join address parts with commas and encode for URL
    address_str = ', '.join(address_parts)
    encoded_address = urllib.parse.quote_plus(address_str)
    
    # Create map app URL (works with Apple Maps, Google Maps, and other map apps)
    return f"maps:q={encoded_address}"

def format_currency(value):
    """Format currency values for display"""
    if pd.isna(value):
        return "-"
    return f"${value:,.2f}"

def format_number(value):
    """Format numeric values for display"""
    if pd.isna(value):
        return "-"
    return f"{value:,.0f}"

def format_zip(value):
    """Format ZIP codes for display"""
    if pd.isna(value):
        return "-"
    return str(value).strip()

def format_phone(value):
    """Format phone numbers for display in data tables"""
    if pd.isna(value):
        return "-"
    phone = re.sub(r'\D', '', str(value).strip())
    if len(phone) == 10:
        return f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
    elif len(phone) == 11 and phone.startswith('1'):
        return f"({phone[1:4]}) {phone[4:7]}-{phone[7:]}"
    else:
        return str(value).strip()

def format_email_for_link(email):
    """Format email addresses for clickable mailto: links"""
    if pd.isna(email) or email in [None, '', 'nan']:
        return None
    email_str = str(email).strip()
    if email_str and '@' in email_str:
        return f"mailto:{email_str}"
    return None

def extract_address_parts(row):
    """Extract address components from a dataframe row"""
    address_parts = []
    if is_valid_value(row.get('ADDRESS')):
        address_parts.append(row['ADDRESS'])
    if is_valid_value(row.get('CITY')):
        address_parts.append(row['CITY'])
    if is_valid_value(row.get('STATE')):
        address_parts.append(row['STATE'])
    if is_valid_value(row.get('ZIP')):
        address_parts.append(str(row['ZIP']))
    return address_parts

def create_address_link(row):
    """Create a clickable map link for an address"""
    address_parts = extract_address_parts(row)
    return format_address_for_link(address_parts) if address_parts else None

def create_full_address(row):
    """Create a full address string from address components"""
    address_parts = extract_address_parts(row)
    return ', '.join(address_parts) if address_parts else "-"

def format_contact_name(value):
    """Format contact names for display"""
    return str(value).strip() if pd.notna(value) else "-"

def is_valid_value(value):
    """Check if a value is not null, empty, or 'nan' string"""
    return pd.notna(value) and str(value).strip() not in ['None', '', 'nan']

def get_current_map_style():
    """Get the current map style from session state with default fallback"""
    return st.session_state.get("map_style_selector", ":material/dark_mode:")

def get_sf_business_ids():
    """Get the list of Salesforce business IDs from session state"""
    return st.session_state.get("sf_business_ids", [])

def get_load_search_counter():
    """Get the load search counter from session state"""
    return st.session_state.get('load_search_counter', 0)

def get_sf_pushed_count():
    """Get the Salesforce pushed count from session state"""
    return st.session_state.get("sf_pushed_count", 0)

def is_dark_map_style(map_style=None):
    """Determine if the current or provided map style is dark (needs light tooltip)"""
    style = map_style if map_style is not None else get_current_map_style()
    return style in [":material/dark_mode:", ":material/satellite_alt:"]

def build_tooltip_sections(business_data):
    sections = []

    # Add DNC warning if INTERNAL_DNC is True
    dnc_val = business_data.get("INTERNAL_DNC")
    if dnc_val is True or dnc_val == 1 or (isinstance(dnc_val, str) and dnc_val.strip().lower() in ["true", "1"]):
        dnc_warning = "<span style='color:red; font-weight:bold; font-size:1.1em;'>🚫 INTERNAL DNC</span>"
        sections.append(("", [dnc_warning]))

    # ...existing code...

    location_items = []
    parent_name_col = "PARENT_NAME"
    business_name_col = "DBA_NAME"
    parent_name = str(business_data.get(parent_name_col, '') or '').strip().lower()
    business_name = str(business_data.get(business_name_col, '') or '').strip().lower()
    if parent_name_col and is_valid_value(business_data.get(parent_name_col)) and parent_name and parent_name != business_name:
        location_items.append(
            f'<span style="font-weight:bold; font-size:1.25em;">🏢 {business_data[parent_name_col]}</span>'
        )

    address_parts = extract_address_parts(business_data)
    if address_parts:
        address_str = ', '.join(address_parts)
        location_items.append(f'📍 {address_str}')

    phone_col = "PHONE"
    if phone_col and is_valid_value(business_data.get(phone_col)):
        formatted_phone = format_phone_for_display(business_data[phone_col])
        if formatted_phone:
            location_items.append(f'📞 {formatted_phone}')

    url_col = "WEBSITE"
    if url_col and is_valid_value(business_data.get(url_col)):
        formatted_url = format_url(business_data[url_col])
        if formatted_url:
            location_items.append(f'🌐 {formatted_url}')

    if location_items:
        sections.append(('Location Details', location_items))

    # Contact Information Section
    contact_items = []
    # Add NATIONAL DNC warning for contact if present, above CONTACT_NAME
    contact_natl_dnc = business_data.get("CONTACT_NATIONAL_DNC")
    if contact_natl_dnc is True or contact_natl_dnc == 1 or (isinstance(contact_natl_dnc, str) and contact_natl_dnc.strip().lower() in ["true", "1"]):
        contact_items.append("<span style='color:red; font-size:.75em;'>🚫 NATIONAL DNC</span>")

    contact_name_col = "CONTACT_NAME"
    if contact_name_col and is_valid_value(business_data.get(contact_name_col)):
        contact_items.append(f'👤 {business_data[contact_name_col]}')

    contact_phone_col = "CONTACT_PHONE"
    if contact_phone_col and is_valid_value(business_data.get(contact_phone_col)):
        formatted_phone = format_phone_for_display(business_data[contact_phone_col])
        if formatted_phone:
            contact_items.append(f'📞 {formatted_phone}')

    contact_mobile_col = "CONTACT_MOBILE"
    if contact_mobile_col and is_valid_value(business_data.get(contact_mobile_col)):
        formatted_mobile = format_phone_for_display(business_data[contact_mobile_col])
        if formatted_mobile:
            contact_items.append(f'📱 {formatted_mobile}')

    contact_job_title_col = "CONTACT_JOB_TITLE"
    if contact_job_title_col and is_valid_value(business_data.get(contact_job_title_col)):
        contact_items.append(f'💼 {business_data[contact_job_title_col]}')

    contact_email_col = "CONTACT_EMAIL"
    if contact_email_col and is_valid_value(business_data.get(contact_email_col)):
        contact_items.append(f'📧 {business_data[contact_email_col]}')

    if contact_items:
        sections.append(('Contact Information', contact_items))

    # Business Metrics Section
    metrics_items = []
    revenue_col = "REVENUE"
    if revenue_col and is_valid_value(business_data.get(revenue_col)):
        try:
            revenue_value = float(business_data[revenue_col])
            metrics_items.append(f'💵 Revenue: ${revenue_value:,.0f}')
        except (ValueError, TypeError):
            pass

    employees_col = "NUMBER_OF_EMPLOYEES"
    if employees_col and is_valid_value(business_data.get(employees_col)):
        metrics_items.append(f'👥 Employees: {business_data[employees_col]}')

    locations_col = "NUMBER_OF_LOCATIONS"
    if locations_col and is_valid_value(business_data.get(locations_col)):
        metrics_items.append(f'🏢 Locations: {business_data[locations_col]}')

    industry_col = "PRIMARY_INDUSTRY"
    if industry_col and is_valid_value(business_data.get(industry_col)):
        metrics_items.append(f'🏭 Industry: {business_data[industry_col]}')

    sic_col = "SIC"
    if sic_col and is_valid_value(business_data.get(sic_col)):
        metrics_items.append(f'📊 SIC Code: {business_data[sic_col]}')

    if metrics_items:
        sections.append(('Business Metrics', metrics_items))

    return sections

def build_business_card_sections(business_data):
    """Build standardized business data sections for HTML business cards"""
    # Helper function to create metric HTML
    def create_metric(icon, label, value, link=None):
        metric_value = f'<a href="{link}" target="_blank">{value}</a>' if link else value
        return f'<div class="data-metric"><div class="metric-accent"></div><div class="metric-icon">{icon}</div><div class="metric-label">{label}</div><div class="metric-value">{metric_value}</div></div>'

    def create_section(title, metrics):
        if not metrics:
            return ''
        return f'<div class="data-viz-section"><div class="section-header">{title}</div><div class="data-viz-grid">{"".join(metrics)}</div></div>'

    parent_metrics = []
    parent_name_col = "PARENT_NAME"
    parent_phone_col = "PARENT_PHONE"
    parent_website_col = "PARENT_WEBSITE"
    business_name_col = "DBA_NAME"
    business_phone_col = "PHONE"
    business_website_col = "WEBSITE"

    parent_name = str(business_data.get(parent_name_col, '') or '').strip().lower()
    business_name = str(business_data.get(business_name_col, '') or '').strip().lower()
    parent_phone = str(business_data.get(parent_phone_col, '') or '').strip().lower()
    business_phone = str(business_data.get(business_phone_col, '') or '').strip().lower()
    parent_website = str(business_data.get(parent_website_col, '') or '').strip().lower()
    business_website = str(business_data.get(business_website_col, '') or '').strip().lower()

    parent_info_present = any([
        is_valid_value(business_data.get(parent_name_col)),
        is_valid_value(business_data.get(parent_phone_col)),
        is_valid_value(business_data.get(parent_website_col))
    ])
    parent_info_differs = (
        (parent_name and parent_name != business_name) or
        (parent_phone and parent_phone != business_phone) or
        (parent_website and parent_website != business_website)
    )

    if parent_info_present and parent_info_differs:
        if parent_name:
            parent_metrics.append(create_metric('🏢', 'Parent Company', business_data[parent_name_col]))
        if parent_phone:
            formatted_parent_phone = format_phone_for_display(business_data[parent_phone_col])
            parent_phone_link = format_phone_for_link(business_data[parent_phone_col])
            if formatted_parent_phone:
                parent_metrics.append(create_metric('📞', 'Parent Phone', formatted_parent_phone, parent_phone_link))
        if parent_website:
            formatted_parent_website = format_url(business_data[parent_website_col])
            if formatted_parent_website:
                parent_metrics.append(create_metric('🌐', 'Parent Website', formatted_parent_website, formatted_parent_website))

    location_metrics = []
    address_parts = extract_address_parts(business_data)
    if address_parts:
        address_str = ', '.join(address_parts)
        address_link = format_address_for_link(address_parts)
        location_metrics.append(create_metric('📍', 'Address', address_str, address_link))

    phone_col = "PHONE"
    if phone_col and is_valid_value(business_data.get(phone_col)):
        formatted_phone = format_phone_for_display(business_data[phone_col])
        phone_link = format_phone_for_link(business_data[phone_col])
        if formatted_phone:
            location_metrics.append(create_metric('📞', 'Phone', formatted_phone, phone_link))

    url_col = "WEBSITE"
    if url_col and is_valid_value(business_data.get(url_col)):
        formatted_url = format_url(business_data[url_col])
        if formatted_url:
            location_metrics.append(create_metric('🌐', 'Website', formatted_url, formatted_url))

    # Business metrics
    business_metrics = []
    revenue_col = "REVENUE"
    if revenue_col and is_valid_value(business_data.get(revenue_col)):
        try:
            revenue_value = float(business_data[revenue_col])
            business_metrics.append(create_metric('💵', 'Annual Revenue', f'${revenue_value:,.0f}'))
        except (ValueError, TypeError):
            pass

    metric_fields = [
        ("NUMBER_OF_EMPLOYEES", "👥", "Employees"),
        ("NUMBER_OF_LOCATIONS", "🏢", "Locations")
    ]
    for logical_field, icon, label in metric_fields:
        # If you need to map logical to actual, do it here, else use logical_field directly
        logical_to_actual = {"SIC_CODE": "SIC", "B2B": "IS_B2B", "B2C": "IS_B2C"}
        actual_col = logical_to_actual.get(logical_field, logical_field)
        if actual_col and is_valid_value(business_data.get(actual_col)):
            business_metrics.append(create_metric(icon, label, business_data[actual_col]))

    # Return HTML sections
    sections = []
    if parent_metrics:
        sections.append(create_section("Parent Company", parent_metrics))
    sections.extend([
        create_section("Location Details", location_metrics),
        create_section("Business Metrics", business_metrics)
    ])

    # Add Contact Hierarchy section only if there are contacts in TOP10_CONTACTS
    import json
    top_contacts = business_data.get("TOP10_CONTACTS")
    has_contacts = False
    contacts_iter = []
    if top_contacts:
        contacts_obj = top_contacts
        if isinstance(top_contacts, str):
            try:
                contacts_obj = json.loads(top_contacts)
            except Exception:
                contacts_obj = []
        if (isinstance(contacts_obj, dict) and bool(contacts_obj)):
            contacts_iter = contacts_obj.values()
            has_contacts = True
        elif isinstance(contacts_obj, list) and len(contacts_obj) > 0:
            contacts_iter = contacts_obj
            has_contacts = True
    if has_contacts:
        def create_contact_header():
            labels = ["Name", "Job Title", "Direct Phone", "Mobile Phone", "Email", ""]
            boxes = [
                f'<div style="flex:1 1 0; min-width:0; padding:4px 8px; display:flex; align-items:center; justify-content:flex-start; font-family: DM Sans, -apple-system, BlinkMacSystemFont, sans-serif; background:#f8faff; font-size:11px; color:#6c7280; font-weight:600; letter-spacing:0.5px; text-transform:uppercase; border-bottom:1px solid #e6e9f3;">{label}</div>'
                for label in labels
            ]
            return f'<div style="display:flex; flex-direction:row; gap:0; width:100%;">' + ''.join(boxes) + '</div>'

        def create_contact_row(contact):
            name = contact.get('name', '')
            job_title = contact.get('job_title', '')
            direct_phone = contact.get('direct_phone_number', '')
            mobile_phone = contact.get('mobile_phone', '')
            email = contact.get('email_address', '')
            # Format phone numbers as clickable links
            formatted_direct_phone = format_phone_for_display(direct_phone)
            direct_phone_link = format_phone_for_link(direct_phone)

            link_style = "color:#262AFF; text-decoration:none;"
            if formatted_direct_phone and direct_phone_link:
                direct_phone_html = f'<a href="{direct_phone_link}" style="{link_style}">{formatted_direct_phone}</a>'
            else:
                direct_phone_html = direct_phone if direct_phone else "&nbsp;"

            formatted_mobile_phone = format_phone_for_display(mobile_phone)
            mobile_phone_link = format_phone_for_link(mobile_phone)
            if formatted_mobile_phone and mobile_phone_link:
                mobile_phone_html = f'<a href="{mobile_phone_link}" style="{link_style}">{formatted_mobile_phone}</a>'
            else:
                mobile_phone_html = mobile_phone if mobile_phone else "&nbsp;"

            # Format email as clickable mailto link
            email_link = format_email_for_link(email)
            if email_link:
                email_html = f'<a href="{email_link}" style="{link_style}">{email}</a>'
            else:
                email_html = email if email else "&nbsp;"

            # Check for DNC flag
            is_dnc = contact.get('is_dnc')
            show_dnc = is_dnc is True or is_dnc == 1 or (isinstance(is_dnc, str) and is_dnc.strip().lower() in ['true', '1'])
            if show_dnc:
                dnc_col = '<div style="flex:1 1 0; min-width:0; padding:4px 8px; display:flex; align-items:center; justify-content:center; color:red; font-weight:bold; font-size:1em;">🚫 NATIONAL DNC</div>'
            else:
                dnc_col = '<div style="flex:1 1 0; min-width:0; padding:4px 8px; display:flex; align-items:center;">&nbsp;</div>'
            values = [name, job_title, direct_phone_html, mobile_phone_html, email_html]
            boxes = [
                f'<div style="flex:1 1 0; min-width:0; padding:4px 8px; display:flex; align-items:center; font-family: DM Sans, -apple-system, BlinkMacSystemFont, sans-serif; font-size:15px; color:#222; font-weight:500;">{value if value else "&nbsp;"}</div>'
                for value in values
            ]
            row_html = f'<div style="display:flex; flex-direction:row; gap:0; width:100%; border-bottom:1px solid #e6e9f3; background:#fff;">' + ''.join(boxes) + dnc_col + '</div>'
            return row_html

        contact_rows = [create_contact_header()]
        for contact in contacts_iter:
            contact_rows.append(create_contact_row(contact))
        contact_hierarchy_html = f'<div class="data-viz-section"><div class="section-header">Contact Hierarchy</div><div style="display:flex; flex-direction:column; gap:8px;">{"".join(contact_rows)}</div></div>'
        sections.append(contact_hierarchy_html)

    return sections

def create_data_editor_column_config():
    """Create standardized column configuration for st.data_editor"""
    return {
        # Checkbox columns
        "Map": st.column_config.CheckboxColumn("Map", help="Check to include on map", default=True, width="small"),
        "SF": st.column_config.CheckboxColumn("SF", help="Check to push to Salesforce", default=False, width="small"),
        
        # Text columns  
        "Current Customer": st.column_config.TextColumn("Current Customer", help="🔵 = Existing Customer, ⚪ = Prospect"),
        "FULL_ADDRESS": st.column_config.TextColumn("Full Address", help="Complete address"),
        "IDENTIFIER": st.column_config.TextColumn("Identifier", help="Identifier"),
        "CONTACT_NAME": st.column_config.TextColumn("Contact Name", help="Contact Name"),
        "PRIMARY_INDUSTRY": st.column_config.TextColumn("Primary Industry", help="Primary Industry"),
        "SUB_INDUSTRY": st.column_config.TextColumn("Sub Industry", help="Identifier"),
        "SIC": st.column_config.TextColumn("SIC Code", help="SIC"),
        "DBA_NAME": st.column_config.TextColumn("DBA Name", help="DBA Name"),
        "PARENT_NAME": st.column_config.TextColumn("Parent Name", help="Parent Company Name"),
        
        # Link columns
        "WEBSITE": st.column_config.LinkColumn("Website", help="Click to visit website", display_text=BUTTON_LABEL_VISIT_SITE),
        "PHONE": st.column_config.LinkColumn("Phone", help="Click to call", display_text=BUTTON_LABEL_CALL),
        "CONTACT_PHONE": st.column_config.LinkColumn("Contact Phone", help="Click to call", display_text=BUTTON_LABEL_CALL),
        "CONTACT_MOBILE": st.column_config.LinkColumn("Contact Mobile", help="Click to call", display_text=BUTTON_LABEL_CALL),
        "CONTACT_JOB_TITLE": st.column_config.TextColumn("Contact Job Title", help="Job title of the contact"),
        "CONTACT_EMAIL": st.column_config.LinkColumn("Contact Email", help="Click to send email", display_text=BUTTON_LABEL_EMAIL),
        "ADDRESS_LINK": st.column_config.LinkColumn("Directions", help="Click to open in Maps app", display_text=BUTTON_LABEL_GET_DIRECTIONS),
        "PARENT_PHONE": st.column_config.LinkColumn("Parent Phone", help="Click to call parent company", display_text=BUTTON_LABEL_CALL),
        "PARENT_WEBSITE" : st.column_config.LinkColumn("Parent Website", help="Click to visit parent company website", display_text=BUTTON_LABEL_VISIT_SITE),
        
        # Number columns
        "REVENUE": st.column_config.NumberColumn("Revenue", help="Annual revenue in USD", format="$%.0f"),
        "NUMBER_OF_EMPLOYEES": st.column_config.NumberColumn("Employees", help="Number of employees", format="%.0f"),
        "NUMBER_OF_LOCATIONS": st.column_config.NumberColumn("Locations", help="Number of locations", format="%.0f")
    }

def get_disabled_columns():
    """Get list of columns that should be disabled in data editor"""
    return [
        "DBA_NAME", "FULL_ADDRESS", "PHONE", "WEBSITE", "IDENTIFIER", "Current Customer", 
        "CONTACT_NAME", "CONTACT_EMAIL", "CONTACT_PHONE", "PRIMARY_INDUSTRY", 
        "SUB_INDUSTRY", "SIC_CODE", "REVENUE", "NUMBER_OF_EMPLOYEES", 
        "NUMBER_OF_LOCATIONS", "PARENT_NAME", "PARENT_PHONE", "PARENT_WEBSITE", "IS_B2B", "IS_B2C", 
        "TOP10_CONTACTS", "CONTACT_NATIONAL_DNC", "INTERNAL_DNC"
    ]

def get_dataframe_format_config():
    """Get standardized formatting configuration for styled dataframes"""
    return {
        "REVENUE": format_currency,
        "NUMBER_OF_EMPLOYEES": format_number,
        "NUMBER_OF_LOCATIONS": format_number,
        "ZIP": format_zip,
        "PHONE": format_phone,
        "CONTACT_PHONE": format_phone,
        "CONTACT_NAME": format_contact_name,
        "CONTACT_MOBILE": format_phone,
        "PARENT_PHONE": format_phone,
        
    }

def calculate_pagination_values(total_records, page_size, current_page):
    """Calculate pagination values including total pages, start/end indices, and validated current page"""
    total_pages = (total_records + page_size - 1) // page_size if total_records > 0 else 1
    validated_current_page = min(current_page, max(total_pages, 1))
    start_idx = (validated_current_page - 1) * page_size
    end_idx = min(start_idx + page_size, total_records)
    
    return {
        'total_pages': total_pages,
        'validated_current_page': validated_current_page,
        'start_idx': start_idx,
        'end_idx': end_idx
    }

def reset_to_first_page():
    """Reset pagination to first page"""
    st.session_state["current_page"] = 1

def calculate_sql_offset(current_page, page_size):
    """Calculate SQL OFFSET value for pagination"""
    return (current_page - 1) * page_size

def init_session_state_key(key, default_value):
    """Initialize session state key if it doesn't exist"""
    if key not in st.session_state:
        st.session_state[key] = default_value

def increment_session_state_counter(counter_name, increment=1):
    """Increment a session state counter"""
    st.session_state[counter_name] = st.session_state.get(counter_name, 0) + increment

def update_filter_triggers(columns_to_update, exclude_column=None):
    """Update filter triggers for specified columns"""
    for col in columns_to_update:
        if exclude_column is None or col != exclude_column:
            st.session_state["filter_update_trigger"][col] += 1

def refresh_data_editor_and_rerun():
    """Refresh data editor and trigger rerun"""
    st.session_state["data_editor_refresh_counter"] += 1
    st.rerun()

def create_html_wrapper(tag, css_class, content="", close_tag=True):
    """Create HTML wrapper with optional content"""
    opening_tag = f'<{tag} class="{css_class}">{content}'
    if close_tag:
        closing_tag = f'</{tag}>'
        return opening_tag, closing_tag
    else:
        return opening_tag

def display_html_wrapper(tag, css_class, content="", close_immediately=False):
    """Display HTML wrapper using st.markdown"""
    if close_immediately:
        st.markdown(f'<{tag} class="{css_class}">{content}</{tag}>', unsafe_allow_html=True)
    else:
        st.markdown(f'<{tag} class="{css_class}">{content}', unsafe_allow_html=True)

def close_html_wrapper(tag, comment=""):
    """Close HTML wrapper with optional comment"""
    comment_text = f"  # {comment}" if comment else ""
    st.markdown(f'</{tag}>', unsafe_allow_html=True)

def create_page_size_selector():
    """Create and handle page size selector widget"""
    new_page_size = st.selectbox(
        "Show",
        options=PAGE_SIZE_OPTIONS,
        index=PAGE_SIZE_OPTIONS.index(st.session_state["page_size"]),
        key="page_size_selector",
        label_visibility="visible"
    )
    if new_page_size != st.session_state["page_size"]:
        st.session_state["page_size"] = new_page_size
        reset_to_first_page()
        # Force data editor refresh but don't fetch new data - it's already loaded
        refresh_data_editor_and_rerun()

def create_page_selector(total_pages):
    """Create and handle page selector widget"""
    page_options = list(range(1, total_pages + 1))
    new_page = st.selectbox(
        "Page",
        options=page_options,
        index=st.session_state["current_page"] - 1,
        key="page_selector"
    )
    if new_page != st.session_state["current_page"]:
        st.session_state["current_page"] = new_page
        # Force data editor refresh but don't fetch new data - it's already loaded
        refresh_data_editor_and_rerun()

def create_pagination_navigation_buttons(total_pages):
    """Create and handle pagination navigation buttons (Previous/Next)"""
    btn_col1, btn_col2 = create_equal_columns()
    with btn_col1:
        if st.button(":material/skip_previous:", disabled=st.session_state["current_page"] == 1, key="prev_page", use_container_width=True, help="Previous page"):
            st.session_state["current_page"] -= 1
            # Force data editor refresh but don't fetch new data
            refresh_data_editor_and_rerun()
    with btn_col2:
        if st.button(":material/skip_next:", disabled=bool(st.session_state["current_page"] == total_pages), key="next_page", use_container_width=True, help="Next page"):
            st.session_state["current_page"] += 1
            # Force data editor refresh but don't fetch new data
            refresh_data_editor_and_rerun()

def display_pagination_status(start_idx, end_idx, total_records, total_pages):
    """Display pagination status information"""
    display_html_wrapper("div", "pagination-status")
    st.caption(f"Viewing {start_idx + 1}–{end_idx} of {total_records} records (Page {st.session_state['current_page']} of {total_pages})")
    close_html_wrapper("div")

def create_complete_pagination_ui(total_records, total_pages, start_idx, end_idx):
    """Create complete pagination UI with all components"""
    display_html_wrapper("div", "page-navigation-container")
    
    # Top row - Page size and page selector
    col_left, col_right = create_two_column_layout([3, 2])
    with col_left:
        display_html_wrapper("div", "page-size-controls")
        sub_col1, sub_col2 = create_equal_columns()
        with sub_col1:
            create_page_size_selector()
        with sub_col2:
            create_page_selector(total_pages)
        close_html_wrapper("div") 
    with col_right:
        display_html_wrapper("div", "pagination-nav-buttons")
        create_pagination_navigation_buttons(total_pages)
        close_html_wrapper("div")
    
    display_pagination_status(start_idx, end_idx, total_records, total_pages)
    close_html_wrapper("div")  # Close page-navigation-container

def create_tooltip_style(is_dark_map=False):
    """Generate tooltip styling based on map theme - light tooltip for dark maps, dark tooltip for light maps"""
    if is_dark_map:
        # Light tooltip for dark map backgrounds (better contrast)
        return """
            background: linear-gradient(135deg, #ffffff 0%, #f8f9ff 100%);
            color: #2e3748;
            font-family: "DM Sans", -apple-system, BlinkMacSystemFont, sans-serif;
            padding: 8px 10px;
            border-radius: 10px;
            border: 1px solid #e2e8f0;
            box-shadow: 0 4px 20px rgba(38, 42, 255, 0.12);
            max-width: 320px;
            line-height: 1.3;
            font-size: 10px;
            position: relative;
            overflow: hidden;
        """
    else:
        # Dark tooltip for light map backgrounds (better contrast)
        return """
            background: linear-gradient(135deg, #1a1b23 0%, #2e3748 100%);
            color: #ffffff;
            font-family: "DM Sans", -apple-system, BlinkMacSystemFont, sans-serif;
            padding: 8px 10px;
            border-radius: 10px;
            border: 1px solid #4a5568;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
            max-width: 320px;
            line-height: 1.3;
            font-size: 10px;
            position: relative;
            overflow: hidden;
        """

def create_tooltip_header_style(is_dark_map=False):
    """Generate tooltip header styling - light header for dark maps, dark header for light maps"""
    if is_dark_map:
        # Bright header for light tooltip on dark map
        return """
            background: linear-gradient(135deg, #262aff 0%, #4da8da 100%);
            color: white;
            margin: -8px -10px 6px -10px;
            padding: 6px 10px;
            border-radius: 10px 10px 0 0;
            display: flex;
            align-items: center;
            gap: 6px;
        """
    else:
        # Darker header for dark tooltip on light map
        return """
            background: linear-gradient(135deg, #1b1c6e 0%, #2d5a87 100%);
            color: white;
            margin: -8px -10px 6px -10px;
            padding: 6px 10px;
            border-radius: 10px 10px 0 0;
            display: flex;
            align-items: center;
            gap: 6px;
        """
def create_two_column_layout(ratio=[1, 1]):
    """Create a two-column layout with specified ratio"""
    return st.columns(ratio)

def create_three_column_layout(ratio=[1, 1, 1]):
    """Create a three-column layout with specified ratio"""  
    return st.columns(ratio)

def create_button_columns(left_content=None, right_content=None, ratio=[1, 1]):
    """Create button layout columns with optional content"""
    col1, col2 = st.columns(ratio)
    if left_content:
        with col1:
            left_content()
    if right_content:
        with col2:
            right_content()
    return col1, col2

def create_button_layout():
    """Create standard [1, 2] column layout for buttons (common pattern)"""
    return st.columns([1, 2])

def create_map_controls_layout():
    """Create standard map controls layout [0.3, 0.4, 0.3] (left controls, spacer, right controls)"""
    return st.columns([0.3, 0.4, 0.3])

def create_map_style_buttons_layout():
    """Create 4-column layout for map style buttons"""
    return st.columns(4)

def create_radius_controls_layout():
    """Create 3-column layout for map radius controls (larger, reset, smaller)"""
    return st.columns(3)

def create_reset_button_layout():
    """Create [5, 1] layout for reset button (content, reset button)"""
    return st.columns([5, 1])

def create_wide_button_layout():
    """Create [1, 3] layout for button with more space (button, wider content)"""
    return st.columns([1, 3])

def create_equal_columns():
    """Create [1, 1] layout for equal-width columns"""
    return st.columns([1, 1])

def with_loading_spinner(message, func):
    """Execute function with loading spinner"""
    with st.spinner(message):
        return func()

def validate_query_params(query, params, operation_name="query"):
    """Validate that query parameters match placeholders"""
    if params:
        placeholder_count = query.count('?')
        if placeholder_count != len(params):
            error_msg = f"Parameter mismatch for {operation_name}: {placeholder_count} placeholders, {len(params)} parameters"
            st.error(error_msg)
            return False
    return True

def execute_sql_query(query, params=None, operation_name="query", return_single_value=False):
    """Execute SQL query and return pandas DataFrame or single value"""
    try:
        session = get_active_session()
        if params:
            result = session.sql(query, params=params)
        else:
            result = session.sql(query)
        
        df = result.to_pandas()
        if return_single_value:
            return df.iloc[0, 0]
        return df
    except Exception as e:
        error_msg = f"Error in {operation_name}: {str(e)}"
        if params:
            error_msg += f"\nQuery: {query}\nParams: {params}"
        else:
            error_msg += f"\nQuery: {query}"
        st.error(error_msg)
        return pd.DataFrame() if not return_single_value else 0

def execute_sql_command(query, params=None, operation_name="command"):
    """Execute SQL command (INSERT/UPDATE/DELETE) and return result"""
    try:
        session = get_active_session()
        if params:
            result = session.sql(query, params=params).collect()
        else:
            result = session.sql(query).collect()
        return result
    except Exception as e:
        error_msg = f"Error in {operation_name}: {str(e)}"
        if params:
            error_msg += f"\nQuery: {query}\nParams: {params}"
        else:
            error_msg += f"\nQuery: {query}"
        st.error(error_msg)
        return []

def show_error_message(message, details=None, log_error=True):
    """Display error message with optional details and logging"""
    if details:
        full_message = f"{message}\n{details}"
    else:
        full_message = message
    
    st.error(full_message)
    

    if log_error:
        pass

def show_success_message(message, log_success=True):
    """Display success message with optional logging"""
    st.success(message)
    

    if log_success:
        pass

def has_active_filters(filters):
    """Check if any filters are active/non-empty"""
    # Always consider HAS_CONTACT_INFO as an active filter if it's restricting results
    if filters.get("HAS_CONTACT_INFO") is True:
        return True
    
    # Check for location radius filter (both from active_filters and regular filters)
    if "LOCATION_RADIUS" in filters:
        location_filter = filters["LOCATION_RADIUS"]
        if isinstance(location_filter, dict) and location_filter.get("address", "").strip():
            return True
    
    # Also check for location_radius in the filters dict (alternative key)
    if "location_radius" in filters:
        location_filter = filters["location_radius"]
        if isinstance(location_filter, dict) and location_filter.get("address", "").strip():
            return True
    
    # Check for customer status filter (if not default)
    if filters.get("customer_status") and filters["customer_status"] != "Current and Non-Customers":
        return True
    
    # Check other filters
    return any(
        (STATIC_FILTERS[k]["type"] == "dropdown" and v != []) or
        (STATIC_FILTERS[k]["type"] == "range" and v != [None, None]) or
        (STATIC_FILTERS[k]["type"] == "selectbox" and k == "HAS_CONTACT_INFO" and v is True) or
        (STATIC_FILTERS[k]["type"] == "selectbox" and k in ["B2B", "B2C"] and v and "Include" not in v) or
        (STATIC_FILTERS[k]["type"] == "text" and v and str(v).strip())
        for k, v in filters.items()
        if k in STATIC_FILTERS
    )

def is_filter_active(filter_key, filter_value):
    """Check if a single filter is active/non-empty"""
    if filter_key not in STATIC_FILTERS:
        return False
    
    filter_type = STATIC_FILTERS[filter_key]["type"]
    if filter_type == "dropdown":
        return bool(filter_value)
    elif filter_type == "range":
        return filter_value != [None, None]
    elif filter_type == "checkbox":
        return bool(filter_value)
    elif filter_type == "text":
        return bool(filter_value.strip() if isinstance(filter_value, str) else filter_value)
    elif filter_type == "location_radius":
        return isinstance(filter_value, dict) and bool(filter_value.get("address", "").strip())
    elif filter_type == "selectbox":
        # For selectbox, check if it's not the default "Include" option
        return filter_value and not filter_value.startswith("Include")
    return False

def get_filters_by_type(filter_type):
    """Get all filter keys of a specific type"""
    return [k for k in STATIC_FILTERS if STATIC_FILTERS[k]["type"] == filter_type]

def get_map_styles():
    """Get the map styles configuration"""
    return {
        ":material/light_mode:": "mapbox://styles/mapbox/light-v10",
        ":material/dark_mode:": "mapbox://styles/mapbox/dark-v10",
        ":material/satellite_alt:": "mapbox://styles/mapbox/satellite-v9",
        ":material/terrain:": "mapbox://styles/mapbox/streets-v11"
    }

def create_map_style_button(icon, key, help_text, current_style, column):
    """Create a map style button with consistent styling"""
    with column:
        if st.button(
            icon,
            key=key,
            use_container_width=True,
            help=help_text,
            type="primary" if current_style == icon else "secondary"
        ):
            st.session_state["map_style_selector"] = icon
            st.rerun()

def adjust_radius_scale(scale_factor, min_value=0.0001, max_value=10.0):
    """Adjust radius scale for selected or initial radius"""
    if st.session_state["selected_business_indices"]:
        current_scale = st.session_state["selected_radius_scale"]
        new_scale = max(min_value, min(max_value, current_scale * scale_factor))
        st.session_state["selected_radius_scale"] = new_scale
    else:
        current_scale = st.session_state["initial_radius_scale"]
        new_scale = max(min_value, min(max_value, current_scale * scale_factor))
        st.session_state["initial_radius_scale"] = new_scale

def reset_radius_scale():
    """Reset radius scale to default values"""
    if st.session_state["selected_business_indices"]:
        st.session_state["selected_radius_scale"] = st.session_state["default_selected_radius_scale"]
    else:
        st.session_state["initial_radius_scale"] = 1.0

def apply_gradient_class(element_class, gradient_type="primary"):
    """Apply gradient class to elements via CSS injection"""
    gradient_classes = {
        "primary": "gp-gradient-primary",
        "surface": "gp-gradient-surface", 
        "light": "gp-gradient-light",
        "dark": "gp-gradient-dark"
    }
    
    class_name = gradient_classes.get(gradient_type, "gp-gradient-primary")
    
    st.markdown(f"""
        <style>
        .{element_class} {{
            background: var(--gp-gradient-{gradient_type}) !important;
        }}
        </style>
    """, unsafe_allow_html=True)

def main():
    if "cortex_warnings" not in st.session_state:
        st.session_state.cortex_warnings = []
    if "cortex_messages" not in st.session_state:
        st.session_state.cortex_messages = []

    # Initialize filter triggers to force refresh of dropdowns when contact info filter is set to default
    if "app_initialized" not in st.session_state:
        st.session_state["app_initialized"] = True
        # Force all dropdown filters to refresh since we start with contact info filter active
        dropdown_filters = [k for k, v in STATIC_FILTERS.items() if v["type"] == "dropdown"]
        
        # Initialize filter_update_trigger if it doesn't exist
        if "filter_update_trigger" not in st.session_state:
            st.session_state["filter_update_trigger"] = {col: 0 for col in dropdown_filters}
        
        # Increment all dropdown filter triggers to force refresh with contact info dependency
        for col in dropdown_filters:
            st.session_state["filter_update_trigger"][col] += 1
        
        # Also ensure we mark this as needing a filter refresh for the UI
        st.session_state["force_filter_refresh"] = True

    filters, apply_filters = create_sidebar_filters()

    CORTEX_MODEL_PATH = "SANDBOX.CONKLIN.CORTEX_ANALYST_PROSPECTOR/lead_portal.yaml"
    API_ENDPOINT = "/api/v2/cortex/analyst/message"
    API_TIMEOUT = 50000
    session = get_active_session()

    sidebar_prompt = st.session_state.get("sidebar_cortex_prompt", "")
    if run_analyst and sidebar_prompt:
        st.session_state["analyst_running"] = True
        st.rerun()
    if st.session_state.get("analyst_running", False):
        with st.spinner("Running Analyst..."):
            run_cortex_analyst(
                sidebar_prompt,
                session,
                _snowflake,
                CORTEX_MODEL_PATH,
                API_ENDPOINT,
                API_TIMEOUT,
                rerun_on_success=True
            )

    display_filter_summary(st.session_state["active_filters"])
    
    # Ensure sf_business_ids is initialized
    init_session_state_key("sf_business_ids", [])
    
    # Ensure sf_pushed_count is initialized
    init_session_state_key("sf_pushed_count", 0)
    
    # Ensure sf_last_update is initialized
    init_session_state_key("sf_last_update", datetime.now().isoformat())
    
    tab1, tab2, tab3 = st.tabs(["List View", "Map View", "Salesforce"])

    cortex_df = None
    cortex_total_records = 0
    sidebar_prompt = st.session_state.get("sidebar_cortex_prompt", "")
    if sidebar_prompt and "cortex_messages" in st.session_state:
        for msg in reversed(st.session_state["cortex_messages"]):
            if msg.get("role") == "analyst":
                for item in msg.get("content", []):
                    if item.get("type") == "sql" and "statement" in item:
                        original_sql = item["statement"]
                        import re
                        simple_select_pattern = r"^\s*SELECT\s+([\w,\s]+)\s+FROM\s+([\w\.]+)"  # e.g. SELECT col1, col2 FROM table
                        if re.match(simple_select_pattern, original_sql, flags=re.IGNORECASE):
                            rewritten_sql = re.sub(r"SELECT\s+.+?\s+FROM", "SELECT * FROM", original_sql, flags=re.IGNORECASE|re.DOTALL)
                        else:
                            rewritten_sql = original_sql
                        try:
                            cortex_df = session.sql(rewritten_sql).to_pandas()
                            cortex_total_records = len(cortex_df)
                        except Exception as e:
                            st.error(f"Error executing Cortex SQL for List/Map View: {e}")
                        break
                if cortex_df is not None:
                    break
    if cortex_df is not None:
        st.session_state["filtered_df"] = cortex_df
        st.session_state["total_records"] = cortex_total_records
    elif apply_filters and has_active_filters(filters):
        def fetch_data():
            cache_key = create_cache_key("filtered_data", filters)
            reset_to_first_page()
            return fetch_filtered_data(
                filters, cache_key, st.session_state["page_size"], st.session_state["current_page"], fetch_all=True
            )
        st.session_state["filtered_df"], st.session_state["total_records"] = with_loading_spinner(
            "Fetching data...", fetch_data
        )
        if "map_view_state" in st.session_state:
            del st.session_state.map_view_state
        if "point_selector" in st.session_state:
            del st.session_state.point_selector
        st.session_state["radius_scale"] = 1.0
        st.session_state["search_name"] = ""
        st.session_state["selected_search"] = ""
        if len(st.session_state["filtered_df"]) == st.session_state["page_size"] and st.session_state["total_records"] >= MAX_RESULTS:
            st.warning(f"Result set limited to {MAX_RESULTS} rows. Refine filters to see more specific results.")
        st.rerun()
    with tab1:
        # Check if no data is available and show helpful message
        show_df = st.session_state.get('filtered_df', None)
        show_total = st.session_state.get('total_records', 0)
        
        if show_df is None or show_df.empty:
            if not has_active_filters(st.session_state.get("active_filters", {})):
                st.info("👆 **Set your search criteria in the sidebar and click 'Apply Filters' to see results.**")
                st.markdown("""
                **Quick Start:**
                1. 🌍 Enter an address/ZIP + radius in the Location section
                2. 🏢 Select industry, revenue range, or other business criteria  
                3. 🔍 Click **Apply Filters** to search
                """)
            else:
                st.warning("No results found matching your current filters. Try adjusting your search criteria.")
            return  # Exit early if no data to display
        
        # Unified List View: always use filtered_df and total_records
        # --- Normalize columns and format for analyst and filter results ---
        if show_df is not None and not show_df.empty:
            # Drop unnecessary columns but keep address fields for now
            display_df = show_df.copy()
            # Add missing columns if not present
            for col in [
                "Map", "SF", "Current Customer", "DBA_NAME", "ADDRESS_LINK", "FULL_ADDRESS",
                "PHONE", "WEBSITE", "CONTACT_NAME", "CONTACT_EMAIL", "CONTACT_PHONE",
                "PRIMARY_INDUSTRY", "SUB_INDUSTRY", "SIC_CODE", "REVENUE",
                "NUMBER_OF_EMPLOYEES", "NUMBER_OF_LOCATIONS", "IS_B2B", "IS_B2C", "PARENT_NAME", "PARENT_PHONE", "PARENT_WEBSITE"
            ]:
                if col not in display_df.columns:
                    display_df[col] = ""  # Add empty column
            display_df['Map'] = True
            display_df['SF'] = False
            # Create a combined address column for Google Maps links
            address_cols = ['ADDRESS', 'CITY', 'STATE', 'ZIP']
            if all(col in display_df.columns for col in address_cols):
                display_df['ADDRESS_LINK'] = display_df.apply(create_address_link, axis=1)
                display_df['FULL_ADDRESS'] = display_df.apply(create_full_address, axis=1)
            # Format phone numbers for clickable tel: links
            if 'PHONE' in display_df.columns:
                display_df['PHONE'] = display_df['PHONE'].apply(format_phone_for_link)
            if 'CONTACT_PHONE' in display_df.columns:
                display_df['CONTACT_PHONE'] = display_df['CONTACT_PHONE'].apply(format_phone_for_link)
            if 'CONTACT_MOBILE' in display_df.columns:
                display_df['CONTACT_MOBILE'] = display_df['CONTACT_MOBILE'].apply(format_phone_for_link)
            # Format email addresses for clickable mailto: links
            if 'CONTACT_EMAIL' in display_df.columns:
                display_df['CONTACT_EMAIL'] = display_df['CONTACT_EMAIL'].apply(format_email_for_link)
            if 'PARENT_PHONE' in display_df.columns:
                display_df['PARENT_PHONE'] = display_df['PARENT_PHONE'].apply(format_phone_for_link)
            # Reorder columns to match sidebar filter output
            column_order = [
                "Map", "SF", "Current Customer", "DBA_NAME", "ADDRESS_LINK", "FULL_ADDRESS",
                "PHONE", "WEBSITE", "CONTACT_NAME", "CONTACT_EMAIL", "CONTACT_PHONE", "CONTACT_MOBILE", "CONTACT_JOB_TITLE",
                "PRIMARY_INDUSTRY", "SUB_INDUSTRY", "SIC_CODE", "REVENUE",
                "NUMBER_OF_EMPLOYEES", "NUMBER_OF_LOCATIONS", "PARENT_NAME", "PARENT_PHONE", "PARENT_WEBSITE", "IS_B2B", "IS_B2C"
            ]
            display_df = display_df[column_order]

            if "limit_warning" in st.session_state:
                st.warning(st.session_state.limit_warning)
            total_records = show_total
            pagination_values = calculate_pagination_values(total_records, st.session_state.page_size, st.session_state.current_page)
            total_pages = pagination_values['total_pages']

            if not isinstance(st.session_state.current_page, int):
                show_error_message(f"Invalid current_page type: {type(st.session_state.current_page)} - {st.session_state.current_page}")
                reset_to_first_page()
            st.session_state.current_page = pagination_values['validated_current_page']
            start_idx = pagination_values['start_idx']
            end_idx = pagination_values['end_idx']
            rows_to_display = min(st.session_state.page_size, total_records - start_idx)
            display_df = display_df.iloc[start_idx:end_idx]
            if rows_to_display < st.session_state.page_size:

                height_for_rows = rows_to_display
                min_rows = MIN_DISPLAY_ROWS  # Minimum height for at least 2 rows even if fewer results
                effective_rows = max(height_for_rows, min_rows)
            else:

                effective_rows = st.session_state.page_size
            
            # Calculate dataframe height with minimal buffer
            base_height = effective_rows * ROW_HEIGHT
            header_buffer = HEADER_BUFFER_HEIGHT  # Reduced buffer for header row and minimal padding
            total_height = base_height + header_buffer
            
            # Set tighter min and max heights to reduce empty space
            min_height = (MIN_DISPLAY_ROWS * ROW_HEIGHT) + header_buffer  # Minimum for 2 rows
            max_height = MAX_DATAFRAME_HEIGHT  # Reduced maximum height
            dataframe_height = max(min_height, min(total_height, max_height))
            display_df = show_df.iloc[start_idx:end_idx]
            
            # Update column order to use FULL_ADDRESS and exclude individual address fields
            column_order = [
                "Map", "SF", "Current Customer", "DBA_NAME", "ADDRESS_LINK", "FULL_ADDRESS",
                "PHONE", "WEBSITE", "CONTACT_NAME", "CONTACT_EMAIL", "CONTACT_PHONE", "CONTACT_MOBILE", "CONTACT_JOB_TITLE",
                "PRIMARY_INDUSTRY", "SUB_INDUSTRY", "SIC_CODE", "REVENUE",
                "NUMBER_OF_EMPLOYEES", "NUMBER_OF_LOCATIONS", "PARENT_NAME", "PARENT_PHONE", "PARENT_WEBSITE", "IS_B2B", "IS_B2C"
            ]
            
            # Initialize Map and SF columns with their default states
            page_key = f"page_{st.session_state.current_page}_size_{st.session_state.page_size}"
            display_df['Map'] = True  # Default value for Map column
            display_df['SF'] = False  # Default value for SF column
            
            # Create Current Customer column based on IS_CURRENT_CUSTOMER field
            if 'IS_CURRENT_CUSTOMER' in display_df.columns:
                display_df['Current Customer'] = display_df['IS_CURRENT_CUSTOMER'].apply(
                    lambda x: "🔵" if x is True or x == True else "⚪"
                )
            else:
                display_df['Current Customer'] = "⚪"
            display_df = display_df.drop(columns=['LONGITUDE', 'LATITUDE', 'IS_CURRENT_CUSTOMER'], errors='ignore')            
            # Format URLs to ensure they are absolute URLs
            if 'WEBSITE' in display_df.columns:
                display_df['WEBSITE'] = display_df['WEBSITE'].apply(format_url)
            if 'PARENT_WEBSITE' in display_df.columns:
                display_df['PARENT_WEBSITE'] = display_df['PARENT_WEBSITE'].apply(format_url)
            
            # Create a combined address column for Google Maps links
            address_cols = ['ADDRESS', 'CITY', 'STATE', 'ZIP']
            if all(col in display_df.columns for col in address_cols):
                # Create combined address parts list and add Address Link column
                display_df['ADDRESS_LINK'] = display_df.apply(create_address_link, axis=1)
                
                # Add the FULL_ADDRESS column
                display_df['FULL_ADDRESS'] = display_df.apply(create_full_address, axis=1)
                
                # Reorder columns to put ADDRESS_LINK right after DBA_NAME and before FULL_ADDRESS
                if 'DBA_NAME' in display_df.columns:
                    # Get all columns
                    cols = display_df.columns.tolist()
                    
                    # Start with specified columns in order
                    ordered_cols = []
                    for col in ["Map", "SF", "Current Customer", "DBA_NAME", "ADDRESS_LINK", "FULL_ADDRESS", "PHONE"]:
                        if col in cols:
                            ordered_cols.append(col)
                            cols.remove(col)
                    
                    # Add WEBSITE directly (not after identifier)
                    if "WEBSITE" in cols:
                        ordered_cols.append("WEBSITE")
                        cols.remove("WEBSITE")
                    
                    # Remove identifier from the columns list to add it at the end
                    if "IDENTIFIER" in cols:
                        cols.remove("IDENTIFIER")
                    
                    # Remove individual address columns
                    for addr_col in ['ADDRESS', 'CITY', 'STATE', 'ZIP']:
                        if addr_col in cols:
                            cols.remove(addr_col)
                    
                    # Add remaining columns
                    ordered_cols.extend(cols)
                    
                    # Add identifier at the very end
                    if "IDENTIFIER" in display_df.columns:
                        ordered_cols.append("IDENTIFIER")
                    
                    # Apply the new order
                    display_df = display_df[ordered_cols]
            
            # Format phone numbers for clickable tel: links  
            if 'PHONE' in display_df.columns:
                display_df['PHONE'] = display_df['PHONE'].apply(format_phone_for_link)
            if 'CONTACT_PHONE' in display_df.columns:
                display_df['CONTACT_PHONE'] = display_df['CONTACT_PHONE'].apply(format_phone_for_link)
            if 'CONTACT_MOBILE' in display_df.columns:
                display_df['CONTACT_MOBILE'] = display_df['CONTACT_MOBILE'].apply(format_phone_for_link)
            # Format email addresses for clickable mailto: links
            if 'CONTACT_EMAIL' in display_df.columns:
                display_df['CONTACT_EMAIL'] = display_df['CONTACT_EMAIL'].apply(format_email_for_link)
            if 'PARENT_PHONE' in display_df.columns:
                display_df['PARENT_PHONE'] = display_df['PARENT_PHONE'].apply(format_phone_for_link)
            
            styled_df = display_df.style.format(get_dataframe_format_config())
            def apply_gp_branding(row):
                """Apply Global Payments bento-style soft UI design with rounded corners and brand colors"""
                styles = []
                
                # Base styling with soft shapes and rounded corners
                base_style = (
                    'background: linear-gradient(135deg, #ffffff 0%, #f8f9ff 100%); '
                    'border: 1px solid #e6e9f3; '
                    'border-radius: 8px; '
                    'padding: 8px 12px; '
                    'margin: 2px; '
                    'box-shadow: 0 2px 8px rgba(38, 42, 255, 0.08); '
                    'font-family: "DM Sans", -apple-system, BlinkMacSystemFont, sans-serif; '
                    'transition: all 0.2s ease;'
                )

                if row.name % 2 == 0:
                    # Even rows - lighter Global Blue tint
                    bg_gradient = 'background: linear-gradient(135deg, #f6f8ff 0%, #ffffff 100%);'
                else:
                    # Odd rows - pure white with subtle shadow
                    bg_gradient = 'background: linear-gradient(135deg, #ffffff 0%, #fafbff 100%);'
                
                # Apply to all columns
                for col in row.index:
                    if col == 'DBA_NAME':
                        # Company name gets primary brand treatment
                        styles.append(
                            f'{bg_gradient} '
                            f'border-left: 4px solid #262aff; '
                            f'font-weight: 600; '
                            f'color: #1a1b23; '
                            f'border-radius: 8px; '
                            f'padding: 10px 12px; '
                            f'box-shadow: 0 3px 12px rgba(38, 42, 255, 0.12);'
                        )
                    elif col in ['NUMBER_OF_EMPLOYEES', 'NUMBER_OF_LOCATIONS']:
                        # Business metrics get tertiary color accent
                        styles.append(
                            f'{bg_gradient} '
                            f'border-left: 3px solid #4da8da; '
                            f'color: #2e3748; '
                            f'border-radius: 8px; '
                            f'padding: 8px 12px;'
                        )
                    elif col in ['CONTACT_NAME', 'CONTACT_EMAIL', 'CONTACT_PHONE']:
                        # Contact info gets subtle accent
                        styles.append(
                            f'{bg_gradient} '
                            f'border-left: 2px solid #1ccbff; '
                            f'color: #2e3748; '
                            f'border-radius: 8px; '
                            f'padding: 8px 12px;'
                        )
                    else:
                        # Standard columns with soft UI treatment
                        styles.append(
                            f'{bg_gradient} '
                            f'border: 1px solid #e6e9f3; '
                            f'color: #2e3748; '
                            f'border-radius: 8px; '
                            f'padding: 8px 12px;'
                        )
                
                return styles
            
            styled_df = styled_df.apply(apply_gp_branding, axis=1)

            st.markdown("""
                <style>
                /* Global Payments Data Visualization Styling - Consolidated */
                .stDataEditor, .stDataFrame {
                    font-family: "DM Sans", -apple-system, BlinkMacSystemFont, sans-serif !important;
                }
                
                .stDataEditor > div, .stDataFrame > div {
                    border-radius: 12px !important;
                    overflow: hidden !important;
                    box-shadow: 0 4px 20px rgba(38, 42, 255, 0.08) !important;
                    border: 1px solid #e6e9f3 !important;
                }
                
                /* Header styling with Global Blue gradient */
                .stDataEditor thead th, .stDataFrame thead th {
                    background: linear-gradient(135deg, #262aff 0%, #4da8da 100%) !important;
                    color: white !important;
                    font-weight: 600 !important;
                    padding: 12px 16px !important;
                    border: none !important;
                    font-size: 12px !important;
                    text-transform: uppercase !important;
                    letter-spacing: 0.5px !important;
                }
                
                /* Row and cell styling */
                .stDataEditor tbody tr:hover, .stDataFrame tbody tr:hover {
                    transform: translateY(-1px) !important;
                    box-shadow: 0 6px 24px rgba(38, 42, 255, 0.15) !important;
                    transition: all 0.2s ease !important;
                }
                
                .stDataEditor td, .stDataFrame td {
                    border: none !important;
                    padding: 2px !important;
                }
                
                /* Accent lines for visual grouping */
                .stDataEditor tbody tr:nth-child(5n+1) td:first-child::before, 
                .stDataFrame tbody tr:nth-child(5n+1) td:first-child::before {
                    content: '';
                    position: absolute;
                    left: 0;
                    top: 0;
                    height: 100%;
                    width: 3px;
                    background: linear-gradient(45deg, #262aff 0%, #4da8da 100%);
                    border-radius: 0 2px 2px 0;
                }
                
                /* Scrollbar styling */
                .stDataEditor ::-webkit-scrollbar, .stDataFrame ::-webkit-scrollbar {
                    width: 8px;
                    height: 8px;
                }
                
                .stDataEditor ::-webkit-scrollbar-track, .stDataFrame ::-webkit-scrollbar-track {
                    background: #f1f5f9;
                    border-radius: 4px;
                }
                
                .stDataEditor ::-webkit-scrollbar-thumb, .stDataFrame ::-webkit-scrollbar-thumb {
                    background: linear-gradient(135deg, #262aff 0%, #4da8da 100%);
                    border-radius: 4px;
                }
                
                .stDataEditor ::-webkit-scrollbar-thumb:hover, .stDataFrame ::-webkit-scrollbar-thumb:hover {
                    background: linear-gradient(135deg, #1b1c6e 0%, #2d5a87 100%);
                }
                
                /* Link styling - consolidated for all link types */
                .stDataEditor a, .stDataFrame a {
                    color: #262aff !important;
                    text-decoration: none !important;
                    font-weight: 500 !important;
                }
                
                .stDataEditor a:hover, .stDataFrame a:hover {
                    color: #1b1c6e !important;
                    text-decoration: underline !important;
                }
                
                /* Link icons */
                .stDataEditor a[href^="tel:"]::before, .stDataFrame a[href^="tel:"]::before {
                    content: "📞 ";
                    font-size: 0.9em;
                    margin-right: 4px;
                }
                
                .stDataEditor a[href^="mailto:"]::before, .stDataFrame a[href^="mailto:"]::before {
                    content: "📧 ";
                    font-size: 0.9em;
                    margin-right: 4px;
                }
                
                /* Success message styling - minimal and compact */
                .element-container:has(.stSuccess) {
                    margin: -20px 0 !important;
                    height: auto !important;
                    min-height: 0 !important;
                }
                
                .stSuccess {
                    padding: 0 !important;
                    min-height: 0 !important;
                    height: auto !important;
                    background: transparent !important;
                    border: none !important;
                    box-shadow: none !important;
                }
                
                .stSuccess > div {
                    padding: 0 !important;
                    min-height: 0 !important;
                    height: auto !important;
                }
                
                .stSuccess p {
                    font-size: 8px !important;
                    padding: 0 !important;
                    white-space: nowrap !important;
                    margin: 0 !important;
                    color: #0c8a15 !important;
                    font-weight: 500 !important;
                }
                
                .stSuccess svg {
                    display: none !important;
                }
                
                .salesforce-section {
                    margin: 15px 0 20px 0;
                    border-top: 1px solid #f0f2f7;
                    padding-top: 10px;
                }
                </style>
            """, unsafe_allow_html=True)
            
            def load_data_editor():
                # Configure columns for st.data_editor to make URLs and phone numbers clickable
                column_config = create_data_editor_column_config()
                
                # Create a unique key that includes a counter to force fresh instances
                # This helps prevent state conflicts that cause the double-click issue
                if 'data_editor_refresh_counter' not in st.session_state:
                    init_session_state_key('data_editor_refresh_counter', 0)
                
                # Add a simple reset button to restore hidden columns
                reset_col2, reset_col1 = create_reset_button_layout()
                with reset_col1:
                    if st.button("Reset Columns", 
                                key="reset_columns", 
                                type="secondary", 
                                use_container_width=True,
                                help="Restore any hidden columns"):
                        # Increment the refresh counter to force a re-render of the data editor
                        st.session_state.data_editor_refresh_counter += 1
                        st.rerun()
                
                editor_key = f"business_selector_{page_key}_{st.session_state.data_editor_refresh_counter}"
                
                # Display the data editor without any callbacks
                # Let Streamlit handle the state naturally
                edited_df = st.data_editor(
                    display_df,
                    use_container_width=True,
                    height=dataframe_height,
                    hide_index=True,
                    disabled=get_disabled_columns(),
                    column_config=column_config,
                    key=editor_key
                )
                
                # Process the current selections without complex state management
                if 'Map' in edited_df.columns and 'SF' in edited_df.columns:
                    # Process selected businesses for map
                    selected_for_map = edited_df[edited_df['Map'] == True].copy()
                    st.session_state.selected_map_businesses = selected_for_map
                    
                    if len(selected_for_map) > 0:
                        st.caption(f"📍 {len(selected_for_map)} businesses selected for mapping")
                    else:
                        st.caption("📍 No businesses selected - map will be empty")
                    
                    # Process selected businesses for Salesforce
                    selected_for_sf = edited_df[edited_df['SF'] == True].copy()
                    
                    # Don't automatically add to Salesforce - just show what's selected
                    # The actual push will happen when the button is clicked
                    
                    if len(selected_for_sf) > 0:
                        st.caption(f"🚀 {len(selected_for_sf)} businesses selected for Salesforce")
                        
                        # Check if all selected businesses are already pushed
                        all_pushed = True
                        for _, business in selected_for_sf.iterrows():
                            business_idx = business.name if hasattr(business, 'name') else None
                            business_idx_str = str(business_idx)
                            if business_idx_str not in get_sf_business_ids():
                                all_pushed = False
                                break
                        
                        # Create columns for left-justified button layout
                        button_col1, button_col2 = create_wide_button_layout()
                        
                        with button_col1:
                            if all_pushed:
                                # Show success message instead of button when all are pushed
                                success_msg = f'<p style="color:#0c8a15; font-size:11px; margin:0; padding:0; font-weight:500;">✓ All pushed</p>'
                                st.markdown(success_msg, unsafe_allow_html=True)
                            else:
                                # Salesforce Push Button - left-justified, not full width
                                button_label = "Send Selected to Salesforce"
                                
                                # Add CSS for button styling but remove full-width
                                st.markdown("""
                                <style>
                                button[kind="primary"] span {
                                    white-space: nowrap !important;
                                    overflow: visible !important;
                                }
                                </style>
                                """, unsafe_allow_html=True)
                                
                                if st.button(button_label, type="primary", key="sf_push_button"):
                                    # Only add to Salesforce when button is actually clicked
                                    add_businesses_to_salesforce(selected_for_sf)
                                    
                                    # The success message will show on next rerun when all_pushed becomes True
                                    st.rerun()
            
            with_loading_spinner("Loading data...", load_data_editor)
            
            # Clean responsive pagination layout with CSS classes for styling
            create_complete_pagination_ui(total_records, total_pages, start_idx, end_idx)    

        elif apply_filters and has_active_filters(filters):
            st.warning("No data matches the filters.")

    
    with tab2:
        map_styles = get_map_styles()
        if hasattr(st.session_state, 'active_filters') and st.session_state.active_filters and has_active_filters(st.session_state.active_filters):
            lon_col, lat_col = "LONGITUDE", "LATITUDE"
            if lon_col in st.session_state.filtered_df.columns and lat_col in st.session_state.filtered_df.columns:
                def fetch_map_data():
                    cache_key = create_cache_key("map_data", st.session_state.active_filters)
                    map_df, total_records = fetch_filtered_data(
                        st.session_state.active_filters, cache_key, st.session_state.page_size, 1, fetch_all=True
                    )
                    return map_df, total_records

                map_df, total_records = with_loading_spinner("Fetching all data for map...", fetch_map_data)
                # Logical columns to display on the map
                logical_cols = [
                    "DBA_NAME", "NUMBER_OF_EMPLOYEES", "NUMBER_OF_LOCATIONS", "REVENUE",
                    "ADDRESS", "CITY", "STATE", "ZIP", "PHONE", "WEBSITE", "PARENT_NAME", "PARENT_PHONE", "PARENT_WEBSITE",
                    "CONTACT_NAME", "CONTACT_EMAIL", "CONTACT_PHONE", "CONTACT_MOBILE", "CONTACT_JOB_TITLE", "DATA_AGG_UID", 
                    "IS_CURRENT_CUSTOMER", "TOP10_CONTACTS", "INTERNAL_DNC", "CONTACT_NATIONAL_DNC"
                ]
                # Always include lat/lon columns
                all_logical_cols = [lat_col, lon_col] + logical_cols
                # Map logical to actual column names, skipping any that are None
                logical_to_actual = {"SIC_CODE": "SIC", "B2B": "IS_B2B", "B2C": "IS_B2C"}
                actual_cols = [logical_to_actual.get(col, col) for col in all_logical_cols if col is not None]
                # Only keep columns that exist in the DataFrame
                existing_cols = [col for col in actual_cols if col in map_df.columns]
                map_data = map_df[existing_cols].dropna(subset=[lat_col, lon_col])
                map_data = map_data.rename(columns={lat_col: "lat", lon_col: "lon"})
                map_data = map_data[
                    (map_data["lat"].between(-90, 90)) &
                    (map_data["lon"].between(-180, 180))
                ]

                # Filter map data based on selected businesses from list view
                if hasattr(st.session_state, 'selected_map_businesses'):
                    # User has interacted with the list view checkboxes
                    if len(st.session_state.selected_map_businesses) > 0:
                        # Some businesses are selected - show only those
                        selected_business_names = st.session_state.selected_map_businesses['DBA_NAME'].tolist()
                        map_data = map_data[map_data['DBA_NAME'].isin(selected_business_names)]
                    else:
                        # No businesses selected (user unchecked all) - show empty map
                        map_data = map_data.iloc[0:0]  # Empty dataframe with same structure
                else:
                    # User hasn't interacted with list view yet - show all businesses by default
                    pass
                
                if len(map_data) > MAP_POINTS_LIMIT:
                    map_data = map_data.sample(n=MAP_POINTS_LIMIT, random_state=42)
                    st.warning(f"Map limited to {MAP_POINTS_LIMIT} points for performance. Total records: {total_records}.")
                if not map_data.empty:
                    # Function to get tooltip style based on map style with proper data validation
                    def get_tooltip_style(row):
                        is_dark_map = is_dark_map_style()
                        
                        # Use helper functions for styling
                        tooltip_style = create_tooltip_style(is_dark_map)
                        header_style = create_tooltip_header_style(is_dark_map)

                        # Build sections with proper data validation (same logic as selected business card)
                        sections = build_tooltip_sections(row)

                        # Generate tooltip content with consolidated styling
                        content_html = ""
                        for section_title, items in sections:
                            if items:
                                items_html = "".join(f"<div style='display: flex; align-items: center; gap: 10px; margin-bottom: 6px;'><span style='font-size: 16px;'>{item}</span></div>" for item in items)
                                section_color = "#81c5f4" if is_dark_map else "#4da8da"
                                content_html += f"""
                                    <div style='margin-bottom: 16px;'>
                                        <div style='color: {section_color}; font-weight: 700; font-size: 15px; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 1px;'>{section_title}</div>
                                        {items_html}
                                    </div>
                                """

                        # Larger tooltip container and header
                        return f"""
                            <div style='{tooltip_style}; min-width: 340px; max-width: 480px; padding: 18px 22px; font-size: 16px;'>
                                <div style='{header_style}; padding-bottom: 10px;'>
                                    <span style='background: rgba(255, 255, 255, 0.2); width: 32px; height: 32px; display: inline-flex; align-items: center; justify-content: center; border-radius: 8px; font-size: 22px;'>🏢</span>
                                    <span style='font-size: 22px; font-weight: 700; line-height: 1.2; margin-left: 10px;'>{row['DBA_NAME']}</span>
                                </div>
                                <div style='padding: 6px 0;'>
                                    {content_html}
                                </div>
                            </div>
                        """
                    
                    map_data["tooltip"] = map_data.apply(get_tooltip_style, axis=1)
                    map_data["index"] = map_data.index
                    min_lat, max_lat = map_data["lat"].min(), map_data["lat"].max()
                    min_lon, max_lon = map_data["lon"].min(), map_data["lon"].max()
                    center_lat = (min_lat + max_lat) / 2
                    center_lon = (min_lon + max_lon) / 2
                    lat_diff = max_lat - min_lat
                    lon_diff = max_lon - min_lon
                    
                    # Add padding buffer for better visibility, especially with few points
                    padding_factor = 0.3  # 30% padding around the points
                    lat_diff = max(lat_diff, 0.01) + (lat_diff * padding_factor)  # Minimum diff for very close points
                    lon_diff = max(lon_diff, 0.01) + (lon_diff * padding_factor)
                    
                    if lat_diff == 0 or lon_diff == 0:
                        default_zoom = DEFAULT_MAP_ZOOM  # Reduced from 11 for better initial view
                        initial_radius = 200
                    else:
                        viewport_width = 800
                        viewport_height = 600
                        lat_zoom = math.log2(360 * viewport_height / (lat_diff * 256))
                        lon_zoom = math.log2(360 * viewport_width / (lon_diff * 256 * math.cos(math.radians(center_lat))))
                        # Reduced zoom calculation for better visibility with few points
                        default_zoom = min(lat_zoom, lon_zoom) - 2  # Changed from -1 to -2 for more zoom out
                        
                        # Special handling for small number of points
                        if len(map_data) <= 3:
                            default_zoom = min(default_zoom, 10)  # Cap zoom for few points
                        
                        default_zoom = max(2, min(15, round(default_zoom)))
                        initial_radius = max(50, 500000 / (2 ** default_zoom))
                    
                    # Initialize map-related session state
                    init_session_state_key("initial_radius_scale", 1.0)
                    init_session_state_key("selected_radius_scale", 1.0)
                    init_session_state_key("default_selected_radius_scale", 1.0)
                    init_session_state_key("map_view_state", {
                            "latitude": center_lat,
                            "longitude": center_lon,
                            "zoom": default_zoom
                        })
                    init_session_state_key("selected_business_indices", [])
                    
                    # Migrate from old single selection to new multiple selection (if it exists)
                    if hasattr(st.session_state, 'selected_business_index') and st.session_state.selected_business_index is not None:
                        st.session_state.selected_business_indices = [st.session_state.selected_business_index]
                        delattr(st.session_state, 'selected_business_index')
                    
                    # Clean up any indices that are no longer in the data
                    st.session_state.selected_business_indices = [
                        idx for idx in st.session_state.selected_business_indices 
                        if idx in map_data.index
                    ]
                    # Sort businesses alphabetically by name for better user experience
                    sorted_map_data = map_data.sort_values("DBA_NAME")
                    
                    # Create business options as list of DATA_AGG_UIDs (unique IDs)
                    business_options = [row["DATA_AGG_UID"] for _, row in sorted_map_data.iterrows()]
                    # Map UID to index and UID to display name (optionally with address for clarity)
                    business_uid_to_index = {row["DATA_AGG_UID"]: row["index"] for _, row in sorted_map_data.iterrows()}
                    # Assign a display number for each duplicate DBA_NAME
                    dba_name_counts = {}
                    business_uid_to_label = {}
                    for _, row in sorted_map_data.iterrows():
                        dba = row["DBA_NAME"]
                        dba_name_counts[dba] = dba_name_counts.get(dba, 0) + 1
                        display_number = dba_name_counts[dba]
                        # Only add [n] if there are duplicates
                        if list(sorted_map_data["DBA_NAME"]).count(dba) > 1:
                            label = f"{dba} [{display_number}]"
                        else:
                            label = dba
                        business_uid_to_label[row["DATA_AGG_UID"]] = label

                    # Get current selection for multiselect (as DATA_AGG_UIDs)
                    current_selection = []
                    if st.session_state.selected_business_indices:
                        for idx in st.session_state.selected_business_indices:
                            match = sorted_map_data.loc[sorted_map_data["index"] == idx]
                            if not match.empty:
                                current_selection.append(match.iloc[0]["DATA_AGG_UID"])

                    # Use multiselect with DATA_AGG_UID as value, DBA_NAME (and address) as display
                    selected_businesses = st.multiselect(
                        "🔍 Search and select up to 5 businesses to view details",
                        options=business_options,
                        default=current_selection,
                        key="business_multiselect",
                        help="Type to search and select up to 5 businesses - alphabetically sorted",
                        max_selections=5,
                        placeholder="Type to search business names...",
                        format_func=lambda uid: business_uid_to_label.get(uid, str(uid))
                    )

                    # Handle selection logic - allow multiple selections
                    selected_indices = []
                    if selected_businesses:
                        selected_indices = [business_uid_to_index[uid] for uid in selected_businesses]
                    
                    # Show total count
                    if hasattr(st.session_state, 'selected_map_businesses'):
                        if len(st.session_state.selected_map_businesses) > 0:
                            st.caption(f"Showing {len(map_data)} selected businesses on map • {len(selected_businesses)}/5 selected")
                        else:
                            st.caption(f"No businesses selected for mapping - map is empty • {len(selected_businesses)}/5 selected")
                    else:
                        st.caption(f"Showing all {len(map_data)} businesses on map (default) • {len(selected_businesses)}/5 selected")
                    
                    # Update session state with new selections
                    if set(selected_indices) != set(st.session_state.selected_business_indices):
                        st.session_state.selected_business_indices = selected_indices
                        
                        # Calculate new map view state to encompass all selected businesses
                        if selected_indices:
                            selected_data = map_data.loc[map_data.index.isin(selected_indices)]
                            if len(selected_indices) == 1:
                                # Single selection - zoom in close
                                single_business = selected_data.iloc[0]
                                st.session_state.map_view_state = {
                                    "latitude": float(single_business["lat"]),
                                    "longitude": float(single_business["lon"]),
                                    "zoom": SELECTED_BUSINESS_ZOOM
                                }
                            else:
                                # Multiple selections - fit all businesses in view with padding
                                selected_lat_min, selected_lat_max = selected_data["lat"].min(), selected_data["lat"].max()
                                selected_lon_min, selected_lon_max = selected_data["lon"].min(), selected_data["lon"].max()
                                selected_center_lat = (selected_lat_min + selected_lat_max) / 2
                                selected_center_lon = (selected_lon_min + selected_lon_max) / 2
                                
                                # Calculate base differences
                                selected_lat_diff = selected_lat_max - selected_lat_min
                                selected_lon_diff = selected_lon_max - selected_lon_min
                                
                                # Add padding buffer for better visibility (SAME AS INITIAL MAP VIEW)
                                padding_factor = 0.3  # 30% padding around the points (matching initial view)
                                selected_lat_diff = max(selected_lat_diff, 0.01) + (selected_lat_diff * padding_factor)
                                selected_lon_diff = max(selected_lon_diff, 0.01) + (selected_lon_diff * padding_factor)
                                
                                if selected_lat_diff == 0 or selected_lon_diff == 0:
                                    selected_zoom = DEFAULT_MAP_ZOOM  # Same as initial view for zero diff
                                else:
                                    # Calculate zoom level to fit the padded area (SAME AS INITIAL MAP VIEW)
                                    viewport_width = 800
                                    viewport_height = 600
                                    lat_zoom = math.log2(360 * viewport_height / (selected_lat_diff * 256))
                                    lon_zoom = math.log2(360 * viewport_width / (selected_lon_diff * 256 * math.cos(math.radians(selected_center_lat))))
                                    
                                    # Reduced zoom calculation for better visibility (SAME AS INITIAL MAP VIEW)
                                    selected_zoom = min(lat_zoom, lon_zoom) - 2  # Same -2 reduction as initial view
                                    
                                    # Special handling for 2-3 business selections (more aggressive zoom-in)
                                    num_selected = len(st.session_state.selected_business_indices)
                                    if num_selected == 2 or num_selected == 3:
                                        # For 2-3 businesses, use a much higher minimum zoom for closer view
                                        selected_zoom = max(selected_zoom, 11)  # Increased from 8 to 11 for much closer view
                                        selected_zoom = min(selected_zoom, 14)  # Allow up to 14 for very close viewing
                                    elif num_selected <= 3:
                                        # For other small numbers, use the original logic
                                        selected_zoom = max(selected_zoom, 8)
                                        selected_zoom = min(selected_zoom, 12)
                                    
                                    # Ensure reasonable zoom bounds (SAME AS INITIAL MAP VIEW)
                                    selected_zoom = max(4, min(15, round(selected_zoom)))  # Raised minimum from 2 to 4
                                
                                st.session_state.map_view_state = {
                                    "latitude": selected_center_lat,
                                    "longitude": selected_center_lon,
                                    "zoom": selected_zoom
                                }
                            
                            base_selected_scale = 30 / initial_radius if initial_radius != 0 else 1.0
                            st.session_state.selected_radius_scale = base_selected_scale
                            st.session_state.default_selected_radius_scale = base_selected_scale
                        else:
                            # No selection - reset to default view
                            st.session_state.map_view_state = {
                                "latitude": center_lat,
                                "longitude": center_lon,
                                "zoom": default_zoom
                            }
                            st.session_state.initial_radius_scale = 1.0
                            st.session_state.selected_radius_scale = 1.0
                            st.session_state.default_selected_radius_scale = 1.0
                        st.rerun()
                    
                    # Function to get non-selected point color based on map style
                    def get_non_selected_color():
                        current_map_style = get_current_map_style()
                        # For light and streets maps: use dark blue (Deep Blue)
                        if current_map_style in [":material/light_mode:", ":material/terrain:"]:
                            return [27, 30, 198, 100]  # Global Payments Deep Blue
                        # For dark and satellite maps: use a much lighter blue (lighter than Pulse Blue)  
                        else:  # dark_mode or satellite_alt
                            return [173, 216, 255, 100]  # Light Sky Blue (lighter derivative of Global Blue palette)

                    # Function to get color for each map point, overriding for current customers
                    def get_map_point_color(row, selected=False):
                        if row.get('IS_CURRENT_CUSTOMER', False) is True:
                            return [244, 54, 76, 200]  # Global Raspberry
                        if selected:
                            # Use selected business color logic (existing)
                            idx = st.session_state.selected_business_indices.index(row['index']) if row['index'] in st.session_state.selected_business_indices else 0
                            selected_colors = [
                                [38, 42, 255, 200],     # Global Blue
                                [255, 204, 0, 200],     # Sunshine
                                [253, 160, 82, 200],    # Creamsicle
                                [135, 23, 157, 200],    # Grape
                                [28, 171, 255, 200],    # Pulse Blue
                            ]
                            return selected_colors[idx % len(selected_colors)]
                        return get_non_selected_color()
                    
                    # Display selected business details
                    if st.session_state.selected_business_indices:
                        # Define colors for selected businesses using Global Payments tertiary palette
                        selected_colors = [
                            [38, 42, 255, 200],     # Global Blue
                            [255, 204, 0, 200],    # Sunshine
                            [253, 160, 82, 200],   # Creamsicle
                            [135, 23, 157, 200],   # Grape
                            [28, 171, 255, 200],   # Pulse Blue
                        ]
                        
                        selected_business_data = map_data.loc[map_data.index.isin(st.session_state.selected_business_indices)]
                        
                        
                        def format_business_data_html(business_data):
                            """Generate business card HTML with simplified structure"""
                            business_idx_str = str(business_data.name if hasattr(business_data, 'name') else business_data.get('BUSINESS_ID', ''))
                            already_pushed = business_idx_str in get_sf_business_ids()
                            

                            # Add INTERNAL_DNC flag if needed
                            dnc_flag = ''
                            dnc_val = business_data.get("INTERNAL_DNC")
                            if dnc_val == 1:
                                dnc_flag = '<span style="color:red; font-weight:bold; font-size:1.1em; margin-left:12px;">🚫 INTERNAL DNC</span>'

                            # Build header
                            sf_status = '<span class="sf-push-status">✓ Pushed to Salesforce</span>' if already_pushed else ''
                            header = f'<h3><div class="business-name-container">{business_data["DBA_NAME"]}{dnc_flag}</div>{sf_status}</h3>'
                            
                            # Build sections using consolidated helper
                            sections = build_business_card_sections(business_data)
                            
                            return f'''<div class="business-details-card">{header}<div class="business-data-dashboard">{"".join(sections)}</div></div>'''
                        
                        if len(st.session_state.selected_business_indices) == 1:
                            # Single business - show full details
                            business_data = selected_business_data.iloc[0]
                            st.markdown(format_business_data_html(business_data), unsafe_allow_html=True)
                            
                            # Add native Streamlit button for Salesforce action
                            business_idx = business_data.name if hasattr(business_data, 'name') else None
                            sf_key = f"sf_push_{business_idx}"
                            business_name = business_data.get("DBA_NAME", "")
                            
                            # Check if this business was already pushed to Salesforce
                            business_idx_str = str(business_idx)
                            already_pushed = business_idx_str in get_sf_business_ids()
                            
                            if not already_pushed:
                                # Create columns for left-justified button
                                sf_cols = create_button_layout()
                                with sf_cols[0]:  # Button in left column
                                    # Updated button label with business name
                                    button_label = f"Send {business_name} to Salesforce"
                                    
                                    push_button = st.button(button_label, type="primary", key=sf_key)
                                    
                                    if push_button:
                                        # Make sure we have the complete business data
                                        business_idx = business_data.name if hasattr(business_data, 'name') else None
                                        
                                        # Add this business to Salesforce
                                        add_business_to_salesforce(business_idx)
                                        
                                        # Show success message with compact styling
                                        success_msg = f'<p style="color:#0c8a15; font-size:11px; margin:0; padding:0; font-weight:500;">✓ Added to Salesforce</p>'
                                        st.markdown(success_msg, unsafe_allow_html=True)
                                        
                                        # Log for debugging
                                        print(f"Pushed business ID {business_idx} to Salesforce")
                                        print(f"Total businesses in Salesforce: {st.session_state.sf_pushed_count}")
                                        
                                        # Rerun to update UI
                                        st.rerun()
                        else:
                            # Multiple businesses - show in tabs and add bulk actions
                            
                            # Check if all selected businesses are already pushed
                            all_pushed = True
                            for idx in st.session_state.selected_business_indices:
                                if str(idx) not in get_sf_business_ids():
                                    all_pushed = False
                                    break
                            
                            # Add compact bulk push button - left justified
                            # Create columns for left-justified button layout
                            btn_col1, btn_col2 = create_button_layout()
                            
                            with btn_col1:
                                if all_pushed:
                                    # Use markdown with custom styling - left-justified
                                    st.markdown('<p style="color:#0c8a15; font-size:11px; margin:0; padding:0; font-weight:500;">✓ All pushed</p>', unsafe_allow_html=True)
                                else:
                                    # Updated button label
                                    button_label = "Send Selected to Salesforce"
                                    
                                    # Add CSS to prevent button text wrapping
                                    st.markdown("""
                                    <style>
                                    button[kind="primary"] span {
                                        white-space: nowrap !important;
                                        overflow: visible !important;
                                    }
                                    </style>
                                    """, unsafe_allow_html=True)
                                    
                                    bulk_push = st.button(button_label, 
                                                type="primary", key="sf_bulk_push_button")
                                    if bulk_push:
                                        # Get the subset of businesses that are selected
                                        selected_businesses = selected_business_data.loc[selected_business_data.index.isin(st.session_state.selected_business_indices)].copy()
                                        
                                        # Add each business to Salesforce
                                        count = add_businesses_to_salesforce(selected_businesses)
                                        
                                        # Rerun to update UI
                                        st.rerun()
                            
                            # Multiple businesses - show in tabs
                            business_names = []
                            for idx in st.session_state.selected_business_indices:
                                name = selected_business_data.loc[selected_business_data.index == idx, "DBA_NAME"].iloc[0]
                                # Add indicator if already pushed
                                idx_str = str(idx)
                                already_pushed = idx_str in get_sf_business_ids()
                                if already_pushed:
                                    name = f"{name} ✓"
                                business_names.append(name)
                            
                            tab_labels = [f"📍 {name[:25]}..." if len(name) > 25 else f"📍 {name}" for name in business_names]
                            selected_tabs = st.tabs(tab_labels)
                            
                            for i, (tab, idx) in enumerate(zip(selected_tabs, st.session_state.selected_business_indices)):
                                with tab:
                                    business_data = selected_business_data.loc[selected_business_data.index == idx].iloc[0]
                                    st.markdown(format_business_data_html(business_data), unsafe_allow_html=True)
                                    
                                    # Add native Streamlit button for Salesforce action
                                    business_idx = business_data.name if hasattr(business_data, 'name') else None
                                    sf_key = f"sf_push_tab_{i}_{business_idx}"
                                    business_name = business_data.get("DBA_NAME", "")
                                    
                                    # Check if this business was already pushed to Salesforce
                                    business_idx_str = str(business_idx)
                                    already_pushed = business_idx_str in get_sf_business_ids()
                                    
                                    # Create a smaller, more compact layout with columns
                                    if already_pushed:
                                        # No button needed, so no columns needed
                                        pass
                                    else:
                                        # Determine column ratio based on business name length - more space for longer names
                                        # This helps ensure the button text stays on one line
                                        left_col_size = 1
                                        right_col_size = 1
                                        
                                        if len(business_name) <= 10:
                                            # Very short names
                                            left_col_size, right_col_size = 2, 1  # 2:1 ratio
                                        elif len(business_name) <= 20:
                                            # Medium length names
                                            left_col_size, right_col_size = 1, 1  # 1:1 ratio
                                        else:
                                            # Long names
                                            left_col_size, right_col_size = 1, 2  # 1:2 ratio
                                        
                                        # Create a dynamic layout with columns for left-justified button
                                        sf_cols = create_button_layout()
                                        with sf_cols[0]:  # Button in left column
                                            # Updated button label with business name
                                            button_label = f"Send {business_name} to Salesforce"
                                            
                                            push_button = st.button(button_label, type="primary", key=sf_key)
                                            
                                            if push_button:
                                                # Get the business ID
                                                business_idx = business_data.name if hasattr(business_data, 'name') else None
                                                
                                                # Add to Salesforce
                                                add_business_to_salesforce(business_idx)
                                                
                                                # Rerun to update UI
                                                st.rerun()
                                                st.write("Updating Salesforce tab data...")
                                                
                                                # Show current state
                                                st.write(f"Total businesses in Salesforce: {st.session_state.sf_pushed_count}")
                                                st.write(f"Business IDs in Salesforce: {st.session_state.sf_business_ids}")
                                                
                                                # Continue only if the user clicks to confirm
                                                if st.button("Continue", key=f"tab_continue_{i}"):
                                                    st.rerun()
                        

                    
                    # Create map layers with multiple selection support
                    layers = []
                    
                    if st.session_state.selected_business_indices:
                        # Calculate dynamic radius based on zoom level and selection count
                        current_zoom = st.session_state.map_view_state["zoom"]
                        selection_count = len(st.session_state.selected_business_indices)
                        
                        # Dynamic radius calculation with better zoom scaling (reduced by ~50% for better visual clarity)
                        if selection_count == 1:
                            # Single selection - scale down radius for high zoom levels (reduced by ~75% total)
                            if current_zoom >= 15:
                                dynamic_radius_multiplier = st.session_state.selected_radius_scale * 0.025  # Much smaller for very close zoom
                            elif current_zoom >= 13:
                                dynamic_radius_multiplier = st.session_state.selected_radius_scale * 0.075  # Smaller for close zoom
                            elif current_zoom >= 11:
                                dynamic_radius_multiplier = st.session_state.selected_radius_scale * 0.15   # Medium size
                            else:
                                dynamic_radius_multiplier = st.session_state.selected_radius_scale * 0.25   # Reduced for far zoom
                        else:
                            # Multiple selections - scale based on zoom level (reduced by ~75% total)
                            base_multiplier = st.session_state.selected_radius_scale
                            
                            # Zoom-based scaling: higher zoom = much smaller points
                            if current_zoom >= 15:
                                zoom_scale = 0.025  # Very small for very close zoom
                            elif current_zoom >= 13:
                                zoom_scale = 0.125  # Small for close zoom
                            elif current_zoom >= 11:
                                zoom_scale = 0.5   # Medium for medium zoom
                            elif current_zoom <= 8:
                                zoom_scale = 4.0  # Larger for far zoom
                            else:
                                zoom_scale = 2.0  # Default for other zoom levels
                            
                            dynamic_radius_multiplier = base_multiplier * zoom_scale
                        
                        # Separate selected and non-selected businesses
                        non_selected_data = map_data[~map_data.index.isin(st.session_state.selected_business_indices)]
                        
                        # Create separate radius calculation for map view selected businesses (reduced by ~50% for better visual clarity)
                        map_view_radius_multiplier = st.session_state.selected_radius_scale
                        if current_zoom >= 15:
                            map_view_radius_multiplier *= 1.0  # Smaller for very close zoom
                        elif current_zoom >= 13:
                            map_view_radius_multiplier *= 1.5  # Smaller for close zoom
                        elif current_zoom >= 11:
                            map_view_radius_multiplier *= 2.0  # Medium-small size
                        else:
                            map_view_radius_multiplier *= 2.5  # Reduced for far zoom
                        
                        # Add non-selected businesses layer (precompute fill_color)
                        if not non_selected_data.empty:
                            non_selected_data = non_selected_data.copy()
                            non_selected_data["fill_color"] = non_selected_data.apply(lambda row: get_map_point_color(row, selected=False), axis=1)
                            layers.append(
                                pdk.Layer(
                                    "ScatterplotLayer",
                                    data=non_selected_data,
                                    get_position=["lon", "lat"],
                                    get_fill_color="fill_color",
                                    get_radius=initial_radius * map_view_radius_multiplier * 0.9 * 0.9,
                                    pickable=True,
                                    auto_highlight=True
                                )
                            )
                        
                        # Add each selected business as a separate layer with 3D columns/pillars
                        for i, business_idx in enumerate(st.session_state.selected_business_indices):
                            if business_idx in map_data.index:
                                selected_data = map_data.loc[[business_idx]]
                                # Use ColumnLayer for selected businesses to make them stand out as 3D pillars
                                selected_data = selected_data.copy()
                                selected_data["fill_color"] = selected_data.apply(lambda row: get_map_point_color(row, selected=True), axis=1)
                                layers.append(
                                    pdk.Layer(
                                        "ColumnLayer",
                                        data=selected_data,
                                        get_position=["lon", "lat"],
                                        get_fill_color="fill_color",
                                        get_elevation=20,
                                        elevation_scale=initial_radius * map_view_radius_multiplier * 0.05,
                                        radius=initial_radius * map_view_radius_multiplier * 0.9,
                                        pickable=True,
                                        auto_highlight=True
                                    )
                                )
                    else:
                        # No selection - show all businesses, precompute fill_color
                        map_data = map_data.copy()
                        map_data["fill_color"] = map_data.apply(lambda row: get_map_point_color(row, selected=False), axis=1)
                        layers.append(
                            pdk.Layer(
                                "ScatterplotLayer",
                                data=map_data,
                                get_position=["lon", "lat"],
                                get_fill_color="fill_color",
                                get_radius=initial_radius * st.session_state.initial_radius_scale,
                                pickable=True,
                                auto_highlight=True
                            )
                        )
                    
                    # Create map view state
                    view_state = pdk.ViewState(
                        latitude=float(st.session_state.map_view_state["latitude"]),
                        longitude=float(st.session_state.map_view_state["longitude"]),
                        zoom=int(st.session_state.map_view_state["zoom"]),
                        pitch=0
                    )
                    
                    # Create tooltip
                    tooltip = {
                        "html": "{tooltip}",
                        "style": {
                            "background-color": "transparent",
                            "color": "transparent",
                            "padding": "0",
                            "box-shadow": "none",
                            "border-radius": "0"
                        }
                    }
                    
                    # Create and display the map
                    deck = pdk.Deck(
                        layers=layers,
                        initial_view_state=view_state,
                        map_style=map_styles.get(get_current_map_style()),
                        tooltip=tooltip
                    )
                    
                    # No longer need JavaScript for Salesforce buttons - using native Streamlit buttons
                    
                    #st.markdown(f"**Total Businesses Displayed:** {len(map_data)}")
                    st.pydeck_chart(deck)
                    
                    # Map controls styling
                    st.markdown(
                        """
                        <style>
                        div[data-testid="stHorizontalBlock"] > div:first-child {
                            display: flex;
                            justify-content: flex-start;
                            align-items: center;
                            padding-left: 0;
                            margin-left: 0;
                        }
                        div[data-testid="stHorizontalBlock"] > div:first-child > div[data-testid="stHorizontalBlock"] {
                            display: flex;
                            justify-content: flex-start;
                            gap: 4px;
                            margin: 0;
                            padding: 0;
                        }
                        div[data-testid="stHorizontalBlock"] > div:first-child button[kind="secondary"] {
                            padding: 6px;
                            font-size: 12px;
                            width: 36px;
                            height: 36px;
                            min-width: unset;
                            border: 1px solid #e6e6e6;
                            background-color: #f0f2f6;
                            color: #333333;
                            border-radius: 4px;
                        }
                        div[data-testid="stHorizontalBlock"] > div:last-child {
                            display: flex;
                            justify-content: flex-end;
                            align-items: center;
                            padding-right: 0;
                            margin-right: 0;
                        }
                        </style>
                        """,
                        unsafe_allow_html=True
                    )
                    
                    # Map controls
                    col_left, col_spacer, col_right = create_map_controls_layout()
                    with col_left:
                        col_larger, col_reset, col_smaller = create_radius_controls_layout()
                        with col_smaller:
                            if st.button(":material/remove:", key="radius_smaller", use_container_width=True, help="Shrink map points"):
                                adjust_radius_scale(0.5)
                                st.rerun()
                        with col_larger:
                            if st.button(":material/add:", key="radius_larger", use_container_width=True, help="Enlarge map points"):
                                adjust_radius_scale(2.0)
                                st.rerun()
                        with col_reset:
                            if st.button(":material/refresh:", key="radius_refresh", use_container_width=True, help="Reset map points radius"):
                                reset_radius_scale()
                                st.rerun()
                    with col_right:
                        # Map style buttons arranged in single row
                        style_col1, style_col2, style_col3, style_col4 = create_map_style_buttons_layout()
                        
                        current_style = get_current_map_style()
                        
                        create_map_style_button(
                            ":material/light_mode:",
                            "map_style_light", 
                            "Light map style",
                            current_style,
                            style_col1
                        )
                        
                        create_map_style_button(
                            ":material/dark_mode:",
                            "map_style_dark",
                            "Dark map style", 
                            current_style,
                            style_col2
                        )
                        
                        create_map_style_button(
                            ":material/satellite_alt:",
                            "map_style_satellite",
                            "Satellite map style",
                            current_style,
                            style_col3
                        )
                        
                        create_map_style_button(
                            ":material/terrain:",
                            "map_style_terrain",
                            "Street map style",
                            current_style,
                            style_col4
                        )
                else:
                    init_session_state_key("map_view_state", {
                        "latitude": 39.8283,
                        "longitude": -98.5795,
                        "zoom": 4
                    })
                    st.warning("No valid longitude/latitude data available after filtering.")
                    view_state = pdk.ViewState(
                        latitude=float(st.session_state.map_view_state["latitude"]),
                        longitude=float(st.session_state.map_view_state["longitude"]),
                        zoom=int(st.session_state.map_view_state["zoom"]),
                        pitch=0
                    )
                    deck = pdk.Deck(
                        layers=[],
                        initial_view_state=view_state,
                        map_style=map_styles.get(get_current_map_style())
                    )
                    st.pydeck_chart(deck)
            else:
                st.error(f"Map requires '{lon_col}' and '{lat_col}' columns in the table.")
                init_session_state_key("map_view_state", {
                    "latitude": 39.8283,
                    "longitude": -98.5795,
                    "zoom": 4
                })
                view_state = pdk.ViewState(
                    latitude=float(st.session_state.map_view_state["latitude"]),
                    longitude=float(st.session_state.map_view_state["longitude"]),
                    zoom=int(st.session_state.map_view_state["zoom"]),
                    pitch=0
                )
                deck = pdk.Deck(
                    layers=[],
                    initial_view_state=view_state,
                    map_style=map_styles.get(get_current_map_style())
                )
                st.pydeck_chart(deck)
        # else:
            # Removed redundant message - already shown in active filters section
    
    with tab3:
        st.markdown("""
        <div style="padding: 20px; border-radius: 12px; background: linear-gradient(135deg, #f8faff 0%, #ffffff 100%); 
                    border: 1px solid #e6e9f3; box-shadow: 0 4px 20px rgba(38, 42, 255, 0.08);">
            <h2 style="color: #262aff; margin-bottom: 20px; display: flex; align-items: center;">
                <span style="font-size: 28px; margin-right: 10px;">🚀</span> Salesforce Integration
            </h2>
            <p style="font-size: 16px; color: #333; margin-bottom: 20px;">
                This tab will display Salesforce integration status, push history, and configuration options.
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Debug version info - helps track which approach is running
        st.caption(f"Version: Simple ID List (v3.0) | Last updated: {st.session_state.sf_last_update}")
        
        # Status Dashboard
        st.subheader("Lead Push Status")
        
        # Debug information
        if st.checkbox("Show Debug Info", key="sf_debug"):
            st.write("sf_business_ids:", get_sf_business_ids())
            st.write("sf_pushed_count:", get_sf_pushed_count())
            
            # Build business info from filtered_df if available
            business_details = []
            if "filtered_df" in st.session_state and not st.session_state.filtered_df.empty:
                filtered_df = st.session_state.filtered_df
                for business_id in get_sf_business_ids():
                    # Try both string and numeric comparisons
                    try:
                        numeric_id = int(business_id)
                        matching_rows = filtered_df[filtered_df.index == numeric_id]
                    except ValueError:
                        matching_rows = filtered_df[filtered_df.index.astype(str) == business_id]
                    
                    if not matching_rows.empty:
                        row = matching_rows.iloc[0]
                        business_details.append({
                            "ID": business_id,
                            "Name": row.get("DBA_NAME", "Unknown"),
                            "City": row.get("CITY", ""),
                            "State": row.get("STATE", "")
                        })
            
            if business_details:
                st.write("Business details:")
                st.json(business_details)
            
            # Also check for session state keys
            st.write("All session state keys:", list(st.session_state.keys()))
        
        # Status dashboard
        num_selected = get_sf_pushed_count()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Selected for Push", num_selected, help="Number of businesses marked for Salesforce push")
            
            if num_selected > 0 and st.checkbox("Show Selected Businesses", key="show_sf_selected"):
                # Try to get business names from filtered_df
                if "filtered_df" in st.session_state and not st.session_state.filtered_df.empty:
                    filtered_df = st.session_state.filtered_df
                    
                    # Get the business IDs from session state
                    business_ids = get_sf_business_ids()
                    
                    # Try to find each business in the filtered_df
                    found_businesses = []
                    for business_id in business_ids:
                        # Try different formats of the ID
                        business_found = False
                        
                        # Try as string
                        matching_str = filtered_df[filtered_df.index.astype(str) == business_id]
                        if not matching_str.empty:
                            found_businesses.append({
                                "ID": business_id,
                                "Name": matching_str.iloc[0].get("DBA_NAME", "Unknown")
                            })
                            business_found = True
                            continue
                        
                        # Try as int
                        try:
                            numeric_id = int(business_id)
                            matching_int = filtered_df[filtered_df.index == numeric_id]
                            if not matching_int.empty:
                                found_businesses.append({
                                    "ID": business_id,
                                    "Name": matching_int.iloc[0].get("DBA_NAME", "Unknown")
                                })
                                business_found = True
                                continue
                        except ValueError:
                            pass
                        
                        # If not found, add a placeholder
                        if not business_found:
                            found_businesses.append({
                                "ID": business_id,
                                "Name": f"Business {business_id}"
                            })
                    
                    # Show the businesses
                    if found_businesses:
                        st.dataframe(pd.DataFrame(found_businesses))
                    else:
                        st.info("No business details available")
                else:
                    # Simple display without details
                    st.write(f"Business IDs: {st.session_state.get('sf_business_ids', [])}")
            
            # Add a manual reset option
            if st.button("Reset Salesforce Data"):
                # Store current state for debugging
                old_ids = list(get_sf_business_ids())
                old_count = get_sf_pushed_count()
                
                # Reset all Salesforce-related variables
                st.session_state.sf_business_ids = []
                st.session_state.sf_pushed_count = 0
                st.session_state.sf_last_update = datetime.now().isoformat()
                
                # Also clear any other Salesforce-related variables that might exist
                for key in list(st.session_state.keys()):
                    if key.startswith("sf_") and key not in ["sf_business_ids", "sf_pushed_count", "sf_last_update"]:
                        del st.session_state[key]
                
                # Debug output
                st.write(f"Reset {old_count} businesses from Salesforce")
                st.write(f"Cleared IDs: {old_ids}")
                
                show_success_message("Salesforce data reset successfully")
                st.rerun()
                
        with col2:
            st.metric("Successfully Pushed", 0, help="Number of leads successfully pushed to Salesforce")
        with col3:
            st.metric("Failed", 0, help="Number of leads that failed to push")
            
        # Salesforce Configuration
        st.subheader("Configuration")
        
        # Tabs for different configuration sections
        config_tab1, config_tab2, config_tab3 = st.tabs(["Connection", "Field Mapping", "Workflow"])
        
        with config_tab1:
            st.markdown("### Salesforce Connection Settings")
            st.text_input("Salesforce Instance URL", placeholder="https://yourinstance.salesforce.com", disabled=True)
            st.text_input("API Username", placeholder="api.user@example.com", disabled=True)
            st.text_input("API Key", placeholder="••••••••••••••••", type="password", disabled=True)
            st.checkbox("Use Sandbox Environment", value=True, disabled=True)
            
            st.info("Salesforce API credentials will be configured when the integration is ready.")
            
        with config_tab2:
            st.markdown("### Field Mapping")
            st.markdown("""
            The following fields will be mapped to Salesforce Lead object:
            
            | Prospect Tool Field | Salesforce Field |
            | ------------------- | ---------------- |
            | DBA_NAME | Company |
            | CONTACT_NAME | Name |
            | CONTACT_EMAIL | Email |
            | CONTACT_PHONE | Phone |
            | ADDRESS | Street |
            | CITY | City |
            | STATE | State |
            | ZIP | PostalCode |
            | REVENUE | AnnualRevenue |
            | NUMBER_OF_EMPLOYEES | NumberOfEmployees |
            | PRIMARY_INDUSTRY | Industry |
            """)
            
            st.info("Custom field mappings will be available when the integration is ready.")
            
        with config_tab3:
            st.markdown("### Workflow Configuration")
            st.selectbox(
                "Lead Owner Assignment",
                ["Round Robin", "By Territory", "By Industry", "Manual Assignment"],
                disabled=True
            )
            st.selectbox(
                "Lead Status",
                ["New", "Working", "Qualified", "Unqualified"],
                index=0,
                disabled=True
            )
            st.multiselect(
                "Trigger Workflow Rules",
                ["Lead Assignment", "Lead Scoring", "Email Notification", "Task Creation"],
                disabled=True
            )
            
            st.info("Workflow configuration will be available when the integration is ready.")
        
        # API Test Section
        st.subheader("API Testing")
        st.write("This section will allow testing the Salesforce API connection and lead push functionality.")
        
        test_col1, test_col2 = st.columns(2)
        with test_col1:
            st.button("Test Connection", disabled=True)
        with test_col2:
            st.button("Push Test Lead", disabled=True)
            
        st.info("API testing will be available when the integration is ready.")

if __name__ == "__main__":
    main()
