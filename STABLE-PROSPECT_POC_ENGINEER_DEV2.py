
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

import pydeck as pdk   # For interactive maps with prospect locations
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
MAP_POINTS_LIMIT = 100  # Maximum prospectes to display on map for performance
PAGE_SIZE_OPTIONS = [10, 25, 50, 100]  # Available pagination options for different screen sizes
DEFAULT_PAGE_SIZE = 100  # Default records per page (balanced for responsive design)
ROW_HEIGHT = 45  # Height of each row in data display (optimized for compact view)

CACHE_TTL = 600  # Cache time-to-live in seconds (10 minutes)

DEFAULT_MAP_ZOOM = 9  # Default zoom level for map view
SELECTED_prospect_ZOOM = 15  # Zoom level when a single prospect is selected
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
    "DBA_NAME": {"type": "text", "label": "Prospect Name", "column_name": "DBA_NAME"},
    # Location radius search filters
    "LOCATION_ADDRESS": {"type": "text", "label": "Address or ZIP Code", "column_name": "LOCATION_ADDRESS"},
    "RADIUS_MILES": {"type": "number", "label": "Radius (Miles)", "column_name": "RADIUS_MILES", "min_value": 1, "max_value": 500, "default": 25},
    # Dropdown filters (populated dynamically from database)
    "PRIMARY_INDUSTRY": {"type": "dropdown", "label": "Primary Industry", "column_name": "PRIMARY_INDUSTRY"},
    "SUB_INDUSTRY": {"type": "dropdown", "label": "Sub Industry", "column_name": "SUB_INDUSTRY"},
    "MCC_CODE": {"type": "dropdown", "label": "MCC Code", "column_name": "MCC_CODE"},
    # Range filters for numeric values (min/max sliders)
    "REVENUE": {"type": "range", "label": "Revenue", "column_name": "REVENUE"},
    "NUMBER_OF_EMPLOYEES": {"type": "range", "label": "Number of Employees", "column_name": "NUMBER_OF_EMPLOYEES"},
    "NUMBER_OF_LOCATIONS": {"type": "range", "label": "Number of Locations", "column_name": "NUMBER_OF_LOCATIONS"},
    "CONTACT_INFO_FILTER": {
        "type": "checkbox", 
        "label": "Show only prospects with", 
        "column_name": "CONTACT_INFO_FILTER",
        "options": [
            "Address",
            "Phone Contact",
            "Email Contact"
        ]
    },
    "PROSPECT_TYPE": {
        "type": "checkbox",
        "label": "Prospect type",
        "column_name": "PROSPECT_TYPE",
        "options": [
            "B2B",
            "B2C"
        ]
    },
    "customer_status": {
        "type": "checkbox",
        "label": "Customer Status",
        "column_name": "IS_CURRENT_CUSTOMER",
        "options": [
            "Current Customers",
            "Not Current Customers"
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
                [] if STATIC_FILTERS[col]["type"] == "multiselect" else
                [None, None] if STATIC_FILTERS[col]["type"] == "range" else
                STATIC_FILTERS[col]["options"][0] if STATIC_FILTERS[col]["type"] == "selectbox" else
                STATIC_FILTERS[col]["default"] if STATIC_FILTERS[col]["type"] == "number" else
                {option: False for option in STATIC_FILTERS[col]["options"]} if STATIC_FILTERS[col]["type"] == "checkbox" else
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
        "filter_update_trigger": {          # Tracks when dropdown and multiselect filters need refreshing
            col: 0 for col in STATIC_FILTERS if STATIC_FILTERS[col]["type"] in ["dropdown", "multiselect"]
        },
        "reset_counter": 0,                 # Triggers filter reset when incremented
        "sidebar_collapsed": False,         # Controls sidebar visibility
        "data_editor_refresh_counter": 0,   # Forces data editor refresh
        # Map Interaction
        "map_style_selector": ":material/dark_mode:",  # Current map style
        "selected_prospect_indices": [],    # prospectes selected on map
        "prospect_search_term": "",         # Search term for prospect filtering
        # Salesforce Integration (Simple ID tracking approach)
        "sf_pushed_count": 0,              # Count of prospectes written to sf_leads table
        "sf_prospect_ids": [],             # List of prospect IDs written to sf_leads table
        "sf_last_update": datetime.now().isoformat(),  # Timestamp of last sf_leads table update
        # Staging for Salesforce submission
        "staged_prospects": [],             # Prospects staged for Salesforce submission
        # Geocoding cache
        "geocode_cache": {}                 # Cache for geocoded addresses to avoid repeated API calls
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
    
    # Clean up legacy B2B/B2C filters and ensure PROSPECT_TYPE is properly initialized
    if "B2B" in st.session_state.get("filters", {}):
        del st.session_state["filters"]["B2B"]
    if "B2C" in st.session_state.get("filters", {}):
        del st.session_state["filters"]["B2C"]
    
    # Ensure PROSPECT_TYPE is properly initialized
    if "PROSPECT_TYPE" not in st.session_state.get("filters", {}):
        st.session_state["filters"]["PROSPECT_TYPE"] = {"B2B": False, "B2C": False}
    
    # Clean up legacy customer_status and ensure it's properly initialized as checkbox
    current_customer_status = st.session_state.get("filters", {}).get("customer_status")
    if not isinstance(current_customer_status, dict):
        # Convert from old selectbox format to new checkbox format
        st.session_state["filters"]["customer_status"] = {"Current Customers": False, "Not Current Customers": False}


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


def escape_sql_string(value):
    """
    Escape a string value for SQL by replacing single quotes with double quotes.
    Returns 'NULL' for None values.
    """
    if value is None:
        return 'NULL'
    
    # Handle pandas Series - extract the scalar value
    if hasattr(value, 'iloc'):
        try:
            value = value.iloc[0] if len(value) > 0 else None
            if value is None:
                return 'NULL'
        except:
            return 'NULL'
    
    # Handle empty strings and convert to string
    str_value = str(value) if value is not None else ''
    
    # Check for pandas NaN or other problematic values
    if str_value.lower() in ['nan', 'none', '<na>']:
        return 'NULL'
    
    # Escape single quotes
    escaped_value = str_value.replace("'", "''")
    return f"'{escaped_value}'"

def escape_sql_number(value):
    """
    Escape a numeric value for SQL.
    Returns 'NULL' for None values.
    """
    if value is None:
        return 'NULL'
    
    # Handle pandas Series - extract the scalar value
    if hasattr(value, 'iloc'):
        try:
            value = value.iloc[0] if len(value) > 0 else None
            if value is None:
                return 'NULL'
        except:
            return 'NULL'
    
    # Convert to string and check for problematic values
    str_value = str(value) if value is not None else ''
    
    # Check for pandas NaN or other problematic values
    if str_value.lower() in ['nan', 'none', '<na>', '']:
        return 'NULL'
    
    # Try to convert to a number to validate
    try:
        # If it's a valid number, return it as string
        float(str_value)  # This will raise ValueError if not a number
        return str_value
    except ValueError:
        # If not a valid number, return NULL
        return 'NULL'


def get_streamlit_user_email():
    """
    Get the current Streamlit user's email.
    TODO: Replace with actual user authentication when available.
    
    Returns:
        str: The user's email address
    """
    # TODO: Replace with actual Streamlit user session email
    return 'jacob.mcferran@e-hps.com'


def get_salesforce_owner_id(user_email, session):
    """
    Get the Salesforce owner ID for a given email address.
    
    Args:
        user_email (str): Email address to lookup
        session: Active Snowflake session
        
    Returns:
        dict: {'success': bool, 'owner_id': str, 'message': str}
    """
    try:
        escaped_email = escape_sql_string(user_email)
        owner_query = f"""
        SELECT id 
        FROM salesforce.sfdc.user 
        WHERE email = {escaped_email}
        """
        
        owner_result = session.sql(owner_query).collect()
        
        if not owner_result:
            return {
                'success': False,
                'owner_id': None,
                'message': f'No Salesforce user found with email {user_email}'
            }
        
        return {
            'success': True,
            'owner_id': owner_result[0]['ID'],
            'message': f'Found Salesforce user for {user_email}'
        }
        
    except Exception as e:
        return {
            'success': False,
            'owner_id': None,
            'message': f'Error looking up Salesforce user: {str(e)}'
        }


def insert_prospect_to_sf_table(prospect_id, session, contact_data_override=None):

    try:
        # Always fetch the prospect data from the main table first
        prospect_query = f"""
        SELECT 
            prospect_id,
            address,
            revenue,
            city,
            state,
            zip,
            dba_name,
            primary_industry,
            contact_email,
            taxid,
            contact_name,
            mcc_code,
            identifier,
            number_of_locations,
            number_of_employees,
            phone,
            contact_mobile,
            zi_c_company_id,
            zi_contact_id,
            website,
            data_agg_uid,
            contact_job_title,
            ZI_C_LOCATION_ID
        FROM {get_table_name()}
        WHERE prospect_id = '{prospect_id}'
        """
        
        prospect_result = session.sql(prospect_query).collect()
        
        if not prospect_result:
            return {
                'success': False, 
                'message': f'Prospect ID {prospect_id} not found in source table',
                'is_duplicate': False
            }
        
        prospect_row = prospect_result[0]
        prospect_data = {
            'PROSPECT_ID': prospect_row['PROSPECT_ID'],
            'ADDRESS': prospect_row['ADDRESS'],
            'REVENUE': prospect_row['REVENUE'],
            'CITY': prospect_row['CITY'],
            'STATE': prospect_row['STATE'],
            'ZIP': prospect_row['ZIP'],
            'DBA_NAME': prospect_row['DBA_NAME'],
            'PRIMARY_INDUSTRY': prospect_row['PRIMARY_INDUSTRY'],
            'CONTACT_EMAIL': prospect_row['CONTACT_EMAIL'],
            'TAXID': prospect_row['TAXID'],
            'CONTACT_NAME': prospect_row['CONTACT_NAME'],
            'MCC_CODE': prospect_row['MCC_CODE'],
            'IDENTIFIER': prospect_row['IDENTIFIER'],
            'NUMBER_OF_LOCATIONS': prospect_row['NUMBER_OF_LOCATIONS'],
            'NUMBER_OF_EMPLOYEES': prospect_row['NUMBER_OF_EMPLOYEES'],
            'PHONE': prospect_row['PHONE'],
            'CONTACT_MOBILE': prospect_row['CONTACT_MOBILE'],
            'ZI_C_COMPANY_ID': prospect_row['ZI_C_COMPANY_ID'],
            'ZI_CONTACT_ID': prospect_row['ZI_CONTACT_ID'],
            'WEBSITE': prospect_row['WEBSITE'],
            'DATA_AGG_UID': prospect_row['DATA_AGG_UID'],
            'CONTACT_JOB_TITLE': prospect_row['CONTACT_JOB_TITLE'],
            'ZI_C_LOCATION_ID': prospect_row['ZI_C_LOCATION_ID']
        }
        
        # Override contact fields if provided
        if contact_data_override:
            for key, value in contact_data_override.items():
                if key in prospect_data and value:  # Only override if value is not empty
                    prospect_data[key] = value
        
        # Parse contact name into first and last name
        contact_name = prospect_data['CONTACT_NAME'] or ''
        name_parts = contact_name.split(' ') if contact_name else ['', '']
        first_name = name_parts[0] if len(name_parts) > 0 else None
        last_name = name_parts[-1] if len(name_parts) > 1 else None
        
        # Get owner_id from Salesforce user table based on email
        streamlit_user_email = get_streamlit_user_email()
        
        owner_lookup = get_salesforce_owner_id(streamlit_user_email, session)
        
        if not owner_lookup['success']:
            return {
                'success': False,
                'message': owner_lookup['message'],
                'is_duplicate': False
            }
        
        owner_id = owner_lookup['owner_id']
        
        # Prepare the data for insertion based on your mapping
        insert_data = {
            'id': f"{prospect_id}_{owner_id}",  # Concatenate prospect_id and owner_id for the ID field
            'address': prospect_data['ADDRESS'],
            'historical_affiliate_id': None,  # Always null per your mapping
            'annual_revenue': prospect_data['REVENUE'],
            'city': prospect_data['CITY'],
            'state': prospect_data['STATE'],
            'zip': prospect_data['ZIP'],
            'company': prospect_data['DBA_NAME'],
            'company_industry': prospect_data['PRIMARY_INDUSTRY'],
            'email': prospect_data['CONTACT_EMAIL'],
            'federal_tax_id': prospect_data['TAXID'],
            'first_name': first_name,
            'last_name': last_name,
            'lead_affiliate_ids': None,  # Always null per your mapping
            'mcc_code_lookup': prospect_data['MCC_CODE'],
            'lead_mid': prospect_data['IDENTIFIER'],
            'no_of_locations': prospect_data['NUMBER_OF_LOCATIONS'],
            'no_of_employees': prospect_data['NUMBER_OF_EMPLOYEES'],
            'phone': prospect_data['PHONE'],
            'phone_mobile': prospect_data['CONTACT_MOBILE'],
            'phone_extension': None,  # Always null per your mapping
            'sic_code': None,  # Always null per your mapping
            'sic_description': None,  # Always null per your mapping
            'vendor_company_id': prospect_data['ZI_C_COMPANY_ID'],
            'vendor_contact_id': prospect_data['ZI_CONTACT_ID'],
            'website': prospect_data['WEBSITE'],
            'zi_location_id': prospect_data['DATA_AGG_UID'],
            'data_vendor_names': 'ZoomInfo' if str(prospect_data['DATA_AGG_UID'] or '').startswith('Wkkx') else None,
            'lead_source_url': 'www.globalpayments.com',
            'owner_id': owner_id,
            'record_type_id': None,  # Always null per your mapping
            'lead_source': 'Self Sourced',
            'lead_source_desc': 'GP Prospecting Portal',
            'status': '1a. Cold Prospecting',
            'title': prospect_data['CONTACT_JOB_TITLE'],
            'zi_location_id_org': prospect_data['ZI_C_LOCATION_ID']
        }
        
        # Check for duplicates using the composite ID field (prospect_id + owner_id)
        # This is the proper way to check for duplicates since the ID is unique per prospect per owner
        duplicate_check_query = f"""
        SELECT COUNT(*) as count_existing
        FROM sandbox.conklin.sf_leads
        WHERE id = {escape_sql_string(insert_data["id"])}
        """
        
        duplicate_result = session.sql(duplicate_check_query).collect()
        
        if duplicate_result[0]['COUNT_EXISTING'] > 0:
            return {
                'success': False,
                'message': f'Prospect {insert_data["company"] or "Unknown"} already exists',
                'is_duplicate': True
            }
        
        # Build the INSERT statement with proper escaping
        insert_query = f"""
        INSERT INTO sandbox.conklin.sf_leads (
            id,
            address,
            historical_affiliate_id,
            annual_revenue,
            city,
            state,
            zip,
            company,
            company_industry,
            email,
            federal_tax_id,
            first_name,
            last_name,
            lead_affiliate_ids,
            mcc_code_lookup,
            lead_mid,
            no_of_locations,
            no_of_employees,
            phone,
            phone_mobile,
            phone_extension,
            sic_code,
            sic_description,
            vendor_company_id,
            vendor_contact_id,
            website,
            zi_location_id,
            data_vendor_names,
            lead_source_url,
            owner_id,
            record_type_id,
            lead_source,
            lead_source_desc,
            status,
            title,
            zi_location_id_org
        )
        VALUES (
            {escape_sql_string(insert_data["id"])},
            {escape_sql_string(insert_data["address"])},
            {escape_sql_string(insert_data["historical_affiliate_id"])},
            {escape_sql_number(insert_data["annual_revenue"])},
            {escape_sql_string(insert_data["city"])},
            {escape_sql_string(insert_data["state"])},
            {escape_sql_string(insert_data["zip"])},
            {escape_sql_string(insert_data["company"])},
            {escape_sql_string(insert_data["company_industry"])},
            {escape_sql_string(insert_data["email"])},
            {escape_sql_string(insert_data["federal_tax_id"])},
            {escape_sql_string(insert_data["first_name"])},
            {escape_sql_string(insert_data["last_name"])},
            {escape_sql_string(insert_data["lead_affiliate_ids"])},
            {escape_sql_string(insert_data["mcc_code_lookup"])},
            {escape_sql_string(insert_data["lead_mid"])},
            {escape_sql_number(insert_data["no_of_locations"])},
            {escape_sql_number(insert_data["no_of_employees"])},
            {escape_sql_string(insert_data["phone"])},
            {escape_sql_string(insert_data["phone_mobile"])},
            {escape_sql_string(insert_data["phone_extension"])},
            {escape_sql_string(insert_data["sic_code"])},
            {escape_sql_string(insert_data["sic_description"])},
            {escape_sql_string(insert_data["vendor_company_id"])},
            {escape_sql_string(insert_data["vendor_contact_id"])},
            {escape_sql_string(insert_data["website"])},
            {escape_sql_string(insert_data["zi_location_id"])},
            {escape_sql_string(insert_data["data_vendor_names"])},
            {escape_sql_string(insert_data["lead_source_url"])},
            {escape_sql_string(insert_data["owner_id"])},
            {escape_sql_string(insert_data["record_type_id"])},
            {escape_sql_string(insert_data["lead_source"])},
            {escape_sql_string(insert_data["lead_source_desc"])},
            {escape_sql_string(insert_data["status"])},
            {escape_sql_string(insert_data["title"])},
            {escape_sql_number(insert_data["zi_location_id_org"])}
        )
        """
        
        # Execute the insert
        try:
            # Debug: Log the query for inspection
            print(f"DEBUG: SQL Query about to execute:\n{insert_query}")
            session.sql(insert_query).collect()
        except Exception as sql_error:
            print(f"DEBUG: SQL Error: {str(sql_error)}")
            print(f"DEBUG: SQL Query that failed:\n{insert_query}")
            raise sql_error
        
        return {
            'success': True,
            'message': f'Successfully added {insert_data["company"] or "prospect"} to Salesforce',
            'is_duplicate': False
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': f'Error inserting prospect to Salesforce: {str(e)}',
            'is_duplicate': False
        }


def add_prospect_to_salesforce(prospect_id):

    prospect_id_str = str(prospect_id)
    
    # Check if already tracked in session state to avoid duplicates in UI
    if prospect_id_str in st.session_state["sf_prospect_ids"]:
        return {
            'success': False,
            'message': f'Prospect {prospect_id} already tracked in this session',
            'is_duplicate': False,
            'already_tracked': True
        }
    
    try:
        insert_result = insert_prospect_to_sf_table(prospect_id, session)
        
        if insert_result['success']:
            # Only add to session state tracking if the database insert succeeded
            st.session_state["sf_prospect_ids"].append(prospect_id_str)
            st.session_state["sf_pushed_count"] = len(st.session_state["sf_prospect_ids"])
            st.session_state["sf_last_update"] = datetime.now().isoformat()
            
            return {
                'success': True,
                'message': insert_result['message'],
                'is_duplicate': False,
                'already_tracked': False
            }
        else:
            # Insert failed - return the error details but don't update session state
            return {
                'success': False,
                'message': insert_result['message'],
                'is_duplicate': insert_result['is_duplicate'],
                'already_tracked': False
            }
            
    except Exception as e:
        return {
            'success': False,
            'message': f'Error processing prospect {prospect_id}: {str(e)}',
            'is_duplicate': False,
            'already_tracked': False
        }




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
    .prospect-details-card h3,
    .step-icon,
    .gp-progress-bar {
        background: var(--gp-gradient-primary) !important;
    }
    
    .prospect-data-timeline {
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

    /* prospect details card - Enhanced with new component system */
    .prospect-details-card {
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
    .prospect-details-card:hover {
        box-shadow: var(--gp-shadow-xl);
        transform: translateY(-2px);
    }
    .prospect-details-card h3 {
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
    .prospect-details-card h3::before {
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
    .prospect-details-card h3::after {
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
    .prospect-data-dashboard {
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
    
    /* sf_leads table write status in header */
    .prospect-name-container {
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
    .prospect-data-timeline {
        position: relative;
        padding: 30px 20px;
        border-radius: var(--gp-radius-2xl);
        border: 1px solid var(--gp-border);
        box-shadow: var(--gp-shadow-lg);
        overflow: hidden;
    }
    
    .prospect-data-timeline::before {
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
            # Legacy handling for non-checkbox selectbox filters
            if "Exclude" in value:
                filtered_df = filtered_df[filtered_df[col_name] == 0]
            elif "Show only" in value:
                filtered_df = filtered_df[filtered_df[col_name] == 1]
            # "Include" means no filter
        elif filter_cfg["type"] == "checkbox":
            # Handle checkbox filters (PROSPECT_TYPE, CONTACT_INFO_FILTER, customer_status)
            if isinstance(value, dict):
                checked_options = [option for option, checked in value.items() if checked]
                if checked_options:
                    if key == "customer_status":
                        # Customer status logic
                        mask = pd.Series(False, index=filtered_df.index)
                        for customer_type in checked_options:
                            if customer_type == "Current Customers":
                                mask |= (filtered_df[col_name] == 1)
                            elif customer_type == "Not Current Customers":
                                mask |= (filtered_df[col_name] == 0)
                        filtered_df = filtered_df[mask]
                    elif key == "PROSPECT_TYPE":
                        # Prospect type logic
                        mask = pd.Series(False, index=filtered_df.index)
                        for prospect_type in checked_options:
                            if prospect_type == "B2B" and "IS_B2B" in filtered_df.columns:
                                mask |= (filtered_df["IS_B2B"] == 1)
                            elif prospect_type == "B2C" and "IS_B2C" in filtered_df.columns:
                                mask |= (filtered_df["IS_B2C"] == 1)
                        filtered_df = filtered_df[mask]
        elif filter_cfg["type"] == "checkbox":
            # Handle checkbox filters (like CONTACT_INFO_FILTER)
            if key == "CONTACT_INFO_FILTER" and isinstance(value, dict):
                checked_options = [option for option, checked in value.items() if checked]
                if checked_options:
                    # Create conditions for each checked option
                    mask = pd.Series([False] * len(filtered_df), index=filtered_df.index)
                    for contact_type in checked_options:
                        if contact_type == "Phone Contact" and "HAS_PHONE_CONTACT" in filtered_df.columns:
                            mask |= (filtered_df["HAS_PHONE_CONTACT"] == True)
                        elif contact_type == "Email Contact" and "HAS_EMAIL_CONTACT" in filtered_df.columns:
                            mask |= (filtered_df["HAS_EMAIL_CONTACT"] == True)
                        elif contact_type == "Address" and "HAS_ADDRESS_INFO" in filtered_df.columns:
                            mask |= (filtered_df["HAS_ADDRESS_INFO"] == True)
                    # Apply the combined mask
                    filtered_df = filtered_df[mask]
            elif key == "PROSPECT_TYPE" and isinstance(value, dict):
                checked_options = [option for option, checked in value.items() if checked]
                if checked_options:
                    # Create conditions for each checked option
                    mask = pd.Series([False] * len(filtered_df), index=filtered_df.index)
                    for prospect_type in checked_options:
                        if prospect_type == "B2B" and "IS_B2B" in filtered_df.columns:
                            mask |= (filtered_df["IS_B2B"] == 1)
                        elif prospect_type == "B2C" and "IS_B2C" in filtered_df.columns:
                            mask |= (filtered_df["IS_B2C"] == 1)
                    # Apply the combined mask
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
        st.image("https://cdn.bfldr.com/SXSS6YA/at/99mh6vs475xtvxqvgqmw6ss/gpguide_logo_3", use_container_width=True)
        st.markdown("<h1 class='gp-center-text gp-font-sm gp-color-primary' style='margin: 0rem 0; font-family: var(--font-family-primary);'>Prospecting Portal</h1>", unsafe_allow_html=True)







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
            elif config.get("type") == "multiselect":
                if isinstance(value, list) and all(isinstance(v, str) and v in config.get("options", []) for v in value):
                    cleaned_filters[key] = value
                else:
                    cleaned_filters[key] = []
            elif config.get("type") == "checkbox":
                if isinstance(value, dict) and all(isinstance(k, str) and isinstance(v, bool) for k, v in value.items()):
                    # Validate that all keys are valid options
                    valid_options = set(config.get("options", []))
                    if all(k in valid_options for k in value.keys()):
                        cleaned_filters[key] = value
                    else:
                        show_error_message("Invalid checkbox options", f"for {key}: {list(value.keys())} (expected options from {list(valid_options)})")
                        return
                elif isinstance(value, bool):
                    # Legacy single boolean support
                    cleaned_filters[key] = value
                else:
                    show_error_message("Invalid checkbox filter", f"for {key}: {value} (expected dictionary of option:boolean pairs)")
                    return
            elif config.get("type") == "text":
                if isinstance(value, str):
                    cleaned_filters[key] = value.strip()
                else:
                    show_error_message("Invalid text filter", f"for {key}: {value} (expected string)")
                    return
            elif config.get("type") == "selectbox":
                if isinstance(value, str) and value in config.get("options", []):
                    cleaned_filters[key] = value
                else:
                    show_error_message("Invalid selectbox filter", f"for {key}: {value} (expected one of {config.get('options', [])})")
                    return
            elif config.get("type") == "number":
                if isinstance(value, (int, float)):
                    cleaned_filters[key] = value
                else:
                    show_error_message("Invalid number filter", f"for {key}: {value} (expected number)")
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
                    elif STATIC_FILTERS[key]["type"] == "multiselect":
                        st.session_state["filters"][key] = []
                    elif STATIC_FILTERS[key]["type"] == "checkbox":
                        st.session_state["filters"][key] = {option: False for option in STATIC_FILTERS[key]["options"]}
                    elif STATIC_FILTERS[key]["type"] == "selectbox":
                        st.session_state["filters"][key] = STATIC_FILTERS[key]["options"][0]
                    elif STATIC_FILTERS[key]["type"] == "number":
                        st.session_state["filters"][key] = STATIC_FILTERS[key].get("default", 0)
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
                    elif STATIC_FILTERS[key]["type"] == "number":
                        try:
                            # Check if the filter expects integer values
                            if all(isinstance(STATIC_FILTERS[key].get(param), int) for param in ["min_value", "max_value", "default"]):
                                # Convert to int if all parameter types are integers
                                st.session_state["filters"][key] = int(float(value)) if value is not None else STATIC_FILTERS[key].get("default", 0)
                            else:
                                # Convert to float for other numeric filters
                                st.session_state["filters"][key] = float(value) if value is not None else STATIC_FILTERS[key].get("default", 0)
                        except (ValueError, TypeError):
                            st.session_state["filters"][key] = STATIC_FILTERS[key].get("default", 0)
                    elif STATIC_FILTERS[key]["type"] == "checkbox":
                        if isinstance(value, dict):
                            # Ensure all options exist and have boolean values
                            checkbox_dict = {option: False for option in STATIC_FILTERS[key]["options"]}
                            for option, checked in value.items():
                                if option in checkbox_dict and isinstance(checked, bool):
                                    checkbox_dict[option] = checked
                            st.session_state["filters"][key] = checkbox_dict
                        elif isinstance(value, bool):
                            # Legacy single boolean - convert to new format
                            st.session_state["filters"][key] = {option: False for option in STATIC_FILTERS[key]["options"]}
                        else:
                            # Invalid format - use default
                            st.session_state["filters"][key] = {option: False for option in STATIC_FILTERS[key]["options"]}
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
            elif STATIC_FILTERS[dep_col]["type"] == "checkbox":
                # Handle checkbox filters properly
                if isinstance(dep_val, dict):
                    # Create a consistent string representation of checked options
                    checked_options = [option for option, checked in sorted(dep_val.items()) if checked]
                    val_str = ",".join(checked_options) if checked_options else "none"
                else:
                    val_str = str(dep_val)
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

    try:
        start_time = time.time()
        column_name = STATIC_FILTERS.get(column, {}).get("column_name", column)
        query = f"SELECT DISTINCT {column_name} FROM {get_filter_table_name()} WHERE {column_name} IS NOT NULL"
        params = []
        dropdown_columns = [k for k, v in STATIC_FILTERS.items() if v["type"] == "dropdown"]
        for dep_col, dep_values in dependent_filters:
            if dep_col not in dropdown_columns and STATIC_FILTERS[dep_col]["type"] != "text":
                continue
            dep_column_name = STATIC_FILTERS.get(dep_col, {}).get("column_name", dep_col)
            if isinstance(dep_values, (list, tuple)) and len(dep_values) == 2 and all(v is None for v in dep_values):
                continue
            if dep_values:
                if isinstance(dep_values, list) and dep_values and STATIC_FILTERS[dep_col]["type"] == "dropdown":
                    valid_values = [v for v in dep_values if v is not None and str(v).strip()]
                    if valid_values:
                        query += f" AND {dep_column_name} IN ({','.join(['?' for _ in valid_values])})"
                        params.extend(valid_values)
                elif isinstance(dep_values, (list, tuple)) and len(dep_values) == 2 and STATIC_FILTERS[dep_col]["type"] == "range":
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
                elif STATIC_FILTERS[dep_col]["type"] == "text" and dep_values.strip():
                    terms = [term.strip().lower() for term in dep_values.split() if term.strip()]
                    if terms:
                        for term in terms:
                            query += f" AND LOWER({dep_column_name}) LIKE ?"
                            params.append(f"%{term}%")
        if not validate_query_params(query, params, f"fetch_unique_values for {column}"):
            return []
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
            "PROSPECT_ID", "IDENTIFIER", "DBA_NAME", "ADDRESS", "CITY", "STATE", "ZIP", "PHONE", "CONTACT_NAME", "CONTACT_EMAIL", "CONTACT_PHONE", "CONTACT_MOBILE", "CONTACT_JOB_TITLE", "PRIMARY_INDUSTRY", "SUB_INDUSTRY", "MCC_CODE",
            "REVENUE", "NUMBER_OF_EMPLOYEES", "NUMBER_OF_LOCATIONS", "IS_B2B", "IS_B2C", "LONGITUDE", "LATITUDE", "FULL_ADDRESS", "WEBSITE", "IS_CURRENT_CUSTOMER", "DATA_AGG_UID", "PARENT_NAME", "PARENT_PHONE", "PARENT_WEBSITE",
            "TOP10_CONTACTS", "CONTACT_NATIONAL_DNC", "INTERNAL_DNC", "HAS_CONTACT_INFO", "HAS_PHONE_CONTACT", "HAS_EMAIL_CONTACT",
            "HAS_ADDRESS_INFO", "ACTIVE_PRODUCTS", "PRODUCT_FAMILY"
            ]
        logical_to_actual = {"MCC_CODE": "MCC_CODE", "B2B": "IS_B2B", "B2C": "IS_B2C"}
        # All columns except TOP10_CONTACTS come from main table, TOP10_CONTACTS comes from tc
        columns = [
            f"main.{logical_to_actual.get(col, col)}" if col != "TOP10_CONTACTS" else "tc.TOP10_CONTACTS" for col in logical_columns
        ]
        count_query = f"SELECT COUNT(*) AS total FROM {get_table_name()} main"
        query = f"SELECT {', '.join(columns)} FROM {get_table_name()} main LEFT JOIN sandbox.conklin.top_contacts tc ON main.PROSPECT_ID = tc.PROSPECT_ID"
        where_clauses = []
        params = []
        
        # Handle radius-based location search
        location_address = filters.get("LOCATION_ADDRESS", "")
        radius_miles = filters.get("RADIUS_MILES", 25)
        
        if location_address and location_address.strip():
            # Geocode the address
            geocode_result = geocode_address(location_address)
            if geocode_result:
                center_lat = geocode_result['latitude']
                center_lon = geocode_result['longitude']
                
                # Store the search center location in session state for map display
                st.session_state["search_center_location"] = {
                    'address': location_address.strip(),
                    'latitude': center_lat,
                    'longitude': center_lon,
                    'radius_miles': radius_miles
                }
                
                # Use Haversine formula for radius search
                # Distance calculation in miles using Snowflake's built-in functions
                distance_formula = f"""
                    3959 * ACOS(
                        COS(RADIANS(?)) * COS(RADIANS(main.LATITUDE)) * 
                        COS(RADIANS(main.LONGITUDE) - RADIANS(?)) + 
                        SIN(RADIANS(?)) * SIN(RADIANS(main.LATITUDE))
                    )
                """
                where_clauses.append(f"({distance_formula}) <= ?")
                params.extend([center_lat, center_lon, center_lat, radius_miles])
                
                # Also add a bounding box filter for performance optimization
                lat_delta = radius_miles / 69.0  # Approximate miles per degree latitude
                lon_delta = radius_miles / (69.0 * math.cos(math.radians(center_lat)))  # Adjust for longitude
                
                where_clauses.append("main.LATITUDE BETWEEN ? AND ?")
                where_clauses.append("main.LONGITUDE BETWEEN ? AND ?")
                params.extend([
                    center_lat - lat_delta, center_lat + lat_delta,
                    center_lon - lon_delta, center_lon + lon_delta
                ])
            else:
                # If geocoding fails, show error and return empty results
                st.error(f"Could not geocode address: '{location_address}'. Please try a different address or ZIP code.")
                return pd.DataFrame(), 0
        else:
            # Clear search center location if no location filter is active
            if "search_center_location" in st.session_state:
                del st.session_state["search_center_location"]
        
        for column, value in filters.items():
            # Skip the new location filters as they're handled above
            if column in ["LOCATION_ADDRESS", "RADIUS_MILES"]:
                continue
                
            if column == "customer_status" and isinstance(value, dict):
                # Handle new checkbox customer status filter logic
                # Only add filter if at least one checkbox is checked
                checked_options = [option for option, checked in value.items() if checked]
                if checked_options:
                    customer_conditions = []
                    for customer_type in checked_options:
                        if customer_type == "Current Customers":
                            customer_conditions.append("IS_CURRENT_CUSTOMER = ?")
                            params.append(True)
                        elif customer_type == "Not Current Customers":
                            customer_conditions.append("IS_CURRENT_CUSTOMER = ?")
                            params.append(False)
                    
                    # If any customer conditions were added, combine them with OR
                    if customer_conditions:
                        where_clauses.append(f"({' OR '.join(customer_conditions)})")
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
                elif filter_type == "checkbox" and column == "PROSPECT_TYPE" and isinstance(value, dict):
                    # Handle new checkbox prospect type filter logic
                    # Only add filter if at least one checkbox is checked
                    checked_options = [option for option, checked in value.items() if checked]
                    if checked_options:
                        prospect_type_conditions = []
                        for prospect_type in checked_options:
                            if prospect_type == "B2B":
                                prospect_type_conditions.append("IS_B2B = ?")
                                params.append(1)
                            elif prospect_type == "B2C":
                                prospect_type_conditions.append("IS_B2C = ?")
                                params.append(1)
                        
                        # If any prospect type conditions were added, combine them with OR
                        if prospect_type_conditions:
                            where_clauses.append(f"({' OR '.join(prospect_type_conditions)})")
                elif filter_type == "selectbox" and column in ["B2B", "B2C"]:
                    # Legacy B2B/B2C selectbox handling for backward compatibility
                    if value == "Exclude B2B" or value == "Exclude B2C":
                        where_clauses.append(f"{STATIC_FILTERS[column]['column_name']} = ?")
                        params.append(0)
                    elif value == "Show only B2B" or value == "Show only B2C":
                        where_clauses.append(f"{STATIC_FILTERS[column]['column_name']} = ?")
                        params.append(1)
                    # If "Include ...", do not filter
                elif filter_type == "selectbox" and column == "HAS_CONTACT_INFO":
                    # Legacy handling - should be replaced by CONTACT_INFO_FILTER
                    if value == "Only Prospects with Contact Info":
                        where_clauses.append(f"{STATIC_FILTERS[column]['column_name']} = ?")
                        params.append(True)
                    elif value == "Only Prospects without Contact Info":
                        where_clauses.append(f"{STATIC_FILTERS[column]['column_name']} = ?")
                        params.append(False)
                    # If "Show All Prospects", do not filter
                elif filter_type == "checkbox" and column == "CONTACT_INFO_FILTER" and isinstance(value, dict):
                    # Handle new checkbox contact info filter logic
                    # Only add filter if at least one checkbox is checked
                    checked_options = [option for option, checked in value.items() if checked]
                    if checked_options:
                        contact_conditions = []
                        for contact_type in checked_options:
                            if contact_type == "Phone Contact":
                                contact_conditions.append("HAS_PHONE_CONTACT = ?")
                                params.append(True)
                            elif contact_type == "Email Contact":
                                contact_conditions.append("HAS_EMAIL_CONTACT = ?")
                                params.append(True)
                            elif contact_type == "Address":
                                contact_conditions.append("HAS_ADDRESS_INFO = ?")
                                params.append(True)
                        
                        # If any contact conditions were added, combine them with OR
                        if contact_conditions:
                            where_clauses.append(f"({' OR '.join(contact_conditions)})")
                elif filter_type == "multiselect" and column == "CONTACT_INFO_FILTER" and value:
                    # Legacy multiselect handling for backward compatibility
                    # If "All Prospects" is selected, don't add any contact filters
                    if "All Prospects" not in value:
                        contact_conditions = []
                        for contact_type in value:
                            if contact_type == "Prospects with Any Contact Info":
                                contact_conditions.append("HAS_CONTACT_INFO = ?")
                                params.append(True)
                            elif contact_type == "Prospects with Phone Contact":
                                contact_conditions.append("HAS_PHONE_CONTACT = ?")
                                params.append(True)
                            elif contact_type == "Prospects with Email Contact":
                                contact_conditions.append("HAS_EMAIL_CONTACT = ?")
                                params.append(True)
                            elif contact_type == "Prospects with Address Info":
                                contact_conditions.append("HAS_ADDRESS_INFO = ?")
                                params.append(True)
                        
                        # If any contact conditions were added, combine them with OR
                        if contact_conditions:
                            where_clauses.append(f"({' OR '.join(contact_conditions)})")
                elif filter_type == "checkbox" and column != "CONTACT_INFO_FILTER" and column != "PROSPECT_TYPE" and value:
                    # Legacy checkbox handling (for simple boolean checkboxes)
                    where_clauses.append(f"{STATIC_FILTERS[column]['column_name']} = ?")
                    params.append(True)
                elif filter_type == "text" and value.strip():
                    terms = [term.strip().lower() for term in value.split() if term.strip()]
                    if terms:
                        for term in terms:
                            where_clauses.append(f"LOWER({STATIC_FILTERS[column]['column_name']}) LIKE ?")
                            params.append(f"%{term}%")
            elif column == "show_all_customers" and value:
                # Special filter for showing only customers with valid identifier
                where_clauses.append("identifier IS NOT NULL AND TRIM(identifier) != '' AND UPPER(TRIM(identifier)) != 'NAN'")
        
        if where_clauses:
            condition = " WHERE " + " AND ".join(where_clauses)
            count_query += condition
            query += condition
        
        total_records = execute_sql_query(count_query, params=params, operation_name="fetch_filtered_data_count", return_single_value=True)
        query += f" ORDER BY DBA_NAME"
        
        # Don't set warning here - let the calling function handle it
        original_total = total_records
        if total_records > MAX_RESULTS:
            query += f" LIMIT {MAX_RESULTS}"
            total_records = min(total_records, MAX_RESULTS)
            
        if not fetch_all and original_total <= MAX_RESULTS:
            offset = calculate_sql_offset(current_page, page_size)
            query += f" LIMIT {page_size} OFFSET {offset}"
        df = execute_sql_query(query, params=params, operation_name="fetch_filtered_data")
        return df, total_records, original_total
    except Exception as e:
        show_error_message("Error fetching filtered data", f"{str(e)}\nQuery: {query}\nParams: {params}")
        return pd.DataFrame(), 0, 0

def display_filter_summary(filters):
    active_filters = []
    for column, value in filters.items():
        # Skip location filters as they are handled separately below
        if column in ["LOCATION_ADDRESS", "RADIUS_MILES"]:
            continue
            
        config = STATIC_FILTERS.get(column, {})
        label = config.get("label", column)
        if config.get("type") == "dropdown" and value:
            active_filters.append(f"{label}: {', '.join(map(str, value))}")
        elif config.get("type") == "multiselect" and value:
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
        elif config.get("type") == "selectbox" and value and value != config.get("options", [""])[0]:
            # Only show selectbox filter if it's not the default "show all" option
            active_filters.append(f"{label}: {value}")
        elif config.get("type") == "text" and value.strip():
            terms = [term.strip() for term in value.split() if term.strip()]
            if terms:
                active_filters.append(f"{label}: Contains {', '.join(f'{term!r}' for term in terms)}")
        elif config.get("type") == "number" and value and value != config.get("default"):
            active_filters.append(f"{label}: {value}")
        elif column == "show_all_customers" and value:
            active_filters.append("Customer Filter: Existing Customers Only")
    
    # Special handling for location radius search
    location_address = filters.get("LOCATION_ADDRESS", "")
    radius_miles = filters.get("RADIUS_MILES", 25)
    if location_address and location_address.strip():
        # Add search center indicator emoji
        active_filters.append(f"🎯 Location: Within {radius_miles} miles of '{location_address}'")

    filtered_df = st.session_state.get("filtered_df", None)
    has_results = filtered_df is not None and not filtered_df.empty
    
    # Show search center status even when no other filters are active
    show_search_center_info = "search_center_location" in st.session_state
    
    # Only show the "no filters" message when there are no active filters
    if not active_filters and not has_results and not show_search_center_info:
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
                <p class="no-filters-subtext">Use the sidebar to narrow down your prospect prospects</p>
            </div>
            """, unsafe_allow_html=True)

def add_prospectes_to_salesforce(prospect_df):
    """
    Add multiple prospects to Salesforce by inserting into sf_leads table.
    
    Args:
        prospect_df: DataFrame containing prospects to add (uses index as prospect ID)
        
    Returns:
        dict: {
            'newly_added': int, 
            'duplicates': int, 
            'errors': int, 
            'already_tracked': int,
            'messages': list
        }
    """
    if prospect_df.empty:
        return {
            'newly_added': 0,
            'duplicates': 0,
            'errors': 0,
            'already_tracked': 0,
            'messages': ['No prospects provided']
        }
    
    # Get list of actual PROSPECT_IDs from the DataFrame
    prospect_ids = []
    for _, row in prospect_df.iterrows():
        prospect_id = row.get("PROSPECT_ID") or row.get("IDENTIFIER")
        if prospect_id:
            prospect_ids.append(prospect_id)
    
    newly_added = 0
    duplicates = 0
    errors = 0
    already_tracked = 0
    messages = []
    
    for prospect_id in prospect_ids:
        result = add_prospect_to_salesforce(prospect_id)
        
        if result['success']:
            newly_added += 1
            messages.append(f"✅ {result['message']}")
        elif result['is_duplicate']:
            duplicates += 1
            messages.append(f"⚠️ {result['message']}")
        elif result['already_tracked']:
            already_tracked += 1
            messages.append(f"ℹ️ {result['message']}")
        else:
            errors += 1
            messages.append(f"❌ {result['message']}")
    
    return {
        'newly_added': newly_added,
        'duplicates': duplicates,
        'errors': errors,
        'already_tracked': already_tracked,
        'messages': messages
    }

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
            st.session_state["last_update_time"] = time.time()
            reset_to_first_page()
            update_filter_triggers(get_filters_by_type("dropdown"))
            if time.time() - st.session_state["last_update_time"] < 0.3:
                st.rerun()
        
        return search_value.strip()
    
    def generate_dropdown_filter(column, config):
        dependent_filters = {
            k: st.session_state["filters"].get(k, "")
            for k in filter_columns
            if k != column and k not in ["PROSPECT_TYPE", "LOCATION_ADDRESS", "RADIUS_MILES"] and is_filter_active(k, st.session_state["filters"].get(k, ""))
        }
        
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
            help=f"Select one or more {config['label'].lower()} to filter results."
        )
        
        if selected != st.session_state["filters"][column]:
            st.session_state["filters"][column] = selected
            st.session_state["last_update_time"] = time.time()
            reset_to_first_page()
            update_filter_triggers(get_filters_by_type("dropdown"), exclude_column=column)
            if time.time() - st.session_state["last_update_time"] < 0.3:
                st.rerun()
        
        return selected

    filters = {}
    filter_columns = list(STATIC_FILTERS.keys())
    dropdown_columns = [k for k, v in STATIC_FILTERS.items() if v["type"] == "dropdown"]
    
    if not st.session_state["sidebar_collapsed"]:
        with st.sidebar:
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
            
            # Location Filters Expander - Radius Search
            with st.expander("Search Location", expanded=True):
                # Address/ZIP input for radius search
                location_address = st.text_input(
                    "Address or ZIP Code",
                    value=st.session_state["filters"].get("LOCATION_ADDRESS", ""),
                    key="location_address_filter",
                    placeholder="Enter address or ZIP code (e.g., '1234 Main St, Anytown, ST' or '12345')",
                    help="Enter a full address or ZIP code to search within a radius of that location."
                )
                
                # Radius selection
                current_radius = st.session_state["filters"].get("RADIUS_MILES", STATIC_FILTERS["RADIUS_MILES"]["default"])
                # Ensure the value is an integer to match min_value, max_value, and step types
                if isinstance(current_radius, float):
                    current_radius = int(current_radius)
                
                radius_miles = st.number_input(
                    "Search Radius (Miles)",
                    min_value=STATIC_FILTERS["RADIUS_MILES"]["min_value"],
                    max_value=STATIC_FILTERS["RADIUS_MILES"]["max_value"],
                    value=current_radius,
                    step=1,
                    key="radius_miles_filter",
                    help="Set the search radius in miles from the specified location."
                )
                
                # Update filters if values changed
                if location_address != st.session_state["filters"].get("LOCATION_ADDRESS", ""):
                    st.session_state["filters"]["LOCATION_ADDRESS"] = location_address.strip()
                    st.session_state["last_update_time"] = time.time()
                    reset_to_first_page()
                    update_filter_triggers(get_filters_by_type("dropdown"))
                
                if radius_miles != st.session_state["filters"].get("RADIUS_MILES", STATIC_FILTERS["RADIUS_MILES"]["default"]):
                    st.session_state["filters"]["RADIUS_MILES"] = radius_miles
                    st.session_state["last_update_time"] = time.time()
                    reset_to_first_page()
                
                filters["LOCATION_ADDRESS"] = location_address.strip()
                filters["RADIUS_MILES"] = radius_miles
            
            # prospect Filters Expander
            with st.expander("Prospect Details", expanded=False):

                # prospect name text filter
                for column in ["DBA_NAME"]:
                    if column in STATIC_FILTERS and STATIC_FILTERS[column]["type"] == "text":
                        placeholder = f"Search {STATIC_FILTERS[column]['label'].lower()} (e.g., Taco Bell)"
                        filters[column] = generate_text_filter(column, STATIC_FILTERS[column], placeholder)                
                
                # Contact Info Filter as checkboxes
                contact_config = STATIC_FILTERS["CONTACT_INFO_FILTER"]
                current_contact_value = st.session_state["filters"].get("CONTACT_INFO_FILTER", {})
                
                # Ensure current value is a dictionary
                if not isinstance(current_contact_value, dict):
                    current_contact_value = {option: False for option in contact_config["options"]}
                
                # Ensure all options exist in the dictionary
                for option in contact_config["options"]:
                    if option not in current_contact_value:
                        current_contact_value[option] = False
                
                st.markdown(f"**{contact_config['label']}**")
                
                # Create checkboxes for each option
                contact_selected = {}
                for option in contact_config["options"]:
                    contact_selected[option] = st.checkbox(
                        option,
                        value=current_contact_value.get(option, False),
                        key=f"contact_info_filter_checkbox_{option.replace(' ', '_').lower()}",
                        help=f"Filter to show only prospects with {option.lower()}."
                    )
                
                filters["CONTACT_INFO_FILTER"] = contact_selected
                if contact_selected != st.session_state["filters"]["CONTACT_INFO_FILTER"]:
                    st.session_state["filters"]["CONTACT_INFO_FILTER"] = contact_selected
                    st.session_state["last_update_time"] = time.time()
                    reset_to_first_page()
                
                # Customer Status Filter as checkboxes
                customer_config = STATIC_FILTERS["customer_status"]
                current_customer_value = st.session_state["filters"].get("customer_status", {})
                
                # Ensure current value is a dictionary
                if not isinstance(current_customer_value, dict):
                    current_customer_value = {option: False for option in customer_config["options"]}
                
                # Ensure all options exist in the dictionary
                for option in customer_config["options"]:
                    if option not in current_customer_value:
                        current_customer_value[option] = False
                
                st.markdown(f"**{customer_config['label']}**")
                
                # Create checkboxes for each option
                customer_selected = {}
                for option in customer_config["options"]:
                    customer_selected[option] = st.checkbox(
                        option,
                        value=current_customer_value.get(option, False),
                        key=f"customer_status_filter_checkbox_{option.replace(' ', '_').lower()}",
                        help=f"Filter to show only {option.lower()}."
                    )
                
                filters["customer_status"] = customer_selected
                if customer_selected != st.session_state["filters"]["customer_status"]:
                    st.session_state["filters"]["customer_status"] = customer_selected
                    st.session_state["last_update_time"] = time.time()
                    reset_to_first_page()
                
                # prospect dropdown filters
                for column in ["PRIMARY_INDUSTRY", "SUB_INDUSTRY", "MCC_CODE"]:
                    if column in STATIC_FILTERS and STATIC_FILTERS[column]["type"] == "dropdown":
                        filters[column] = generate_dropdown_filter(column, STATIC_FILTERS[column])
                
                # Prospect Type Filter as checkboxes
                prospect_type_config = STATIC_FILTERS["PROSPECT_TYPE"]
                current_prospect_type_value = st.session_state["filters"].get("PROSPECT_TYPE", {})
                
                # Ensure current value is a dictionary
                if not isinstance(current_prospect_type_value, dict):
                    current_prospect_type_value = {option: False for option in prospect_type_config["options"]}
                
                # Ensure all options exist in the dictionary
                for option in prospect_type_config["options"]:
                    if option not in current_prospect_type_value:
                        current_prospect_type_value[option] = False
                
                st.markdown(f"**{prospect_type_config['label']}**")
                
                # Create checkboxes for each option
                prospect_type_selected = {}
                for option in prospect_type_config["options"]:
                    prospect_type_selected[option] = st.checkbox(
                        option,
                        value=current_prospect_type_value.get(option, False),
                        key=f"prospect_type_filter_checkbox_{option.replace(' ', '_').lower()}",
                        help=f"Filter to show only {option} prospects."
                    )
                
                filters["PROSPECT_TYPE"] = prospect_type_selected
                if prospect_type_selected != st.session_state["filters"]["PROSPECT_TYPE"]:
                    st.session_state["filters"]["PROSPECT_TYPE"] = prospect_type_selected
                    st.session_state["last_update_time"] = time.time()
                    reset_to_first_page()
            
            # Metrics Filters Expander
            with st.expander("Prospect Metrics", expanded=False):
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
                            st.session_state["last_update_time"] = time.time()
                            reset_to_first_page()
                            update_filter_triggers(get_filters_by_type("dropdown"))
                            if time.time() - st.session_state["last_update_time"] < 0.3:
                                st.rerun()
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
                        STATIC_FILTERS[col]["options"][0] if STATIC_FILTERS[col]["type"] == "selectbox" else
                        STATIC_FILTERS[col]["default"] if STATIC_FILTERS[col]["type"] == "number" else
                        {option: False for option in STATIC_FILTERS[col]["options"]} if STATIC_FILTERS[col]["type"] == "checkbox" else
                        ""
                    )
                    for col in STATIC_FILTERS
                }
                st.session_state["last_update_time"] = 0
                reset_to_first_page()
                # Clear filtered_df completely to ensure "no filters" message appears
                if "filtered_df" in st.session_state:
                    del st.session_state["filtered_df"]
                st.session_state["active_filters"] = {}
                st.session_state["page_size"] = DEFAULT_PAGE_SIZE
                st.session_state["total_records"] = 0
                st.session_state["confirm_delete_search"] = False
                st.session_state["search_to_delete"] = None
                st.session_state["filter_update_trigger"] = {col: 0 for col in STATIC_FILTERS if STATIC_FILTERS[col]["type"] in ["dropdown", "multiselect"]}
                st.session_state["search_name"] = ""
                st.session_state["selected_search"] = ""
                # Clear any cached data
                if "full_df" in st.session_state:
                    del st.session_state["full_df"]
                # Clear search center location to reset location-based searches
                if "search_center_location" in st.session_state:
                    del st.session_state["search_center_location"]
                # Force data refresh
                st.session_state["last_update_time"] = time.time()
                increment_session_state_counter('reset_counter')
                if "map_view_state" in st.session_state:
                    del st.session_state.map_view_state
                if "previous_point_selector" in st.session_state:
                    del st.session_state.previous_point_selector
                if "prospect_multiselect" in st.session_state:
                    del st.session_state.prospect_multiselect
                st.session_state["prospect_search_term"] = ""
                st.session_state["selected_prospect_indices"] = []
                st.session_state["radius_scale"] = DEFAULT_RADIUS_SCALE
                st.session_state["geocode_cache"] = {}  # Clear geocoding cache
                st.rerun()

            # Apply button
            apply_filters = st.button(
                "Apply Filters",
                key="apply_filters",
                use_container_width=True,
                type="primary",
                disabled=not has_filters or has_errors
            )
            if apply_filters and has_filters and not has_errors:
                st.session_state["active_filters"] = filters
                st.session_state["radius_scale"] = 1.0
                # Standardize output columns for sidebar filters
                display_columns = [
                    "DBA_NAME", "ADDRESS", "CITY", "STATE", "ZIP", "PRIMARY_INDUSTRY", "SUB_INDUSTRY", "MCC_CODE", "PROSPECT_TYPE"
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
                
                # Build the options list
                search_options = [""] + [s["SEARCH_NAME"] for s in saved_searches]
                
                # Calculate safe index
                current_selection = st.session_state.get("selected_search", "")
                try:
                    if current_selection and current_selection in search_options:
                        selected_index = search_options.index(current_selection)
                    else:
                        selected_index = 0
                except (ValueError, TypeError):
                    selected_index = 0
                
                st.session_state["selected_search"] = st.selectbox(
                    "Load Saved Search",
                    options=search_options,
                    index=selected_index,
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

def get_sf_prospect_ids():
    """Get the list of Salesforce prospect IDs from session state"""
    return st.session_state.get("sf_prospect_ids", [])

def get_load_search_counter():
    """Get the load search counter from session state"""
    return st.session_state.get('load_search_counter', 0)

def get_sf_pushed_count():
    """Get the Salesforce pushed count from session state"""
    return st.session_state.get("sf_pushed_count", 0)


def get_staged_prospects():
    """Get the staged prospects from session state"""
    return st.session_state.get("staged_prospects", [])


def clean_staged_prospects():
    """Clean up corrupted or invalid staged prospects - only remove truly corrupted entries"""
    if "staged_prospects" not in st.session_state:
        return 0
    
    original_count = len(st.session_state.staged_prospects)
    valid_prospects = []
    
    for prospect in st.session_state.staged_prospects:
        # Only check for basic data integrity, not submission status
        # Staged prospects are meant to be queued for review, not necessarily submitted yet
        prospect_id = prospect.get("prospect_id")
        company = prospect.get("company")
        
        # Only remove if the prospect data is actually corrupted (missing required fields)
        if prospect_id and company:  # Must have at least ID and company name
            valid_prospects.append(prospect)
        # If missing basic required data, this is truly corrupted - remove it
    
    st.session_state.staged_prospects = valid_prospects
    cleaned_count = original_count - len(valid_prospects)
    
    return cleaned_count


def add_prospect_to_staging(prospect_data, selected_contact=None):
    """Add a prospect to the staging area for Salesforce submission"""
    if "staged_prospects" not in st.session_state:
        st.session_state.staged_prospects = []
    
    # Extract prospect ID
    prospect_id = prospect_data.get("PROSPECT_ID") or prospect_data.get("IDENTIFIER")
    
    # Debug: Check if prospect_id is None
    if prospect_id is None:
        st.write(f"DEBUG STAGING: Failed - prospect_id is None for {prospect_data.get('DBA_NAME', 'Unknown')}")
        return False  # Don't stage if we can't get a valid ID

    # Check if already staged (avoid duplicates) - improved logic
    existing_ids = [str(p.get("prospect_id")) for p in st.session_state.staged_prospects if p.get("prospect_id") is not None]
    prospect_id_str = str(prospect_id) if prospect_id is not None else None
    
    st.write(f"DEBUG STAGING: Checking '{prospect_data.get('DBA_NAME', 'Unknown')}' (ID: {prospect_id_str})")
    st.write(f"  - Existing staged IDs: {existing_ids}")
    
    if prospect_id_str and prospect_id_str in existing_ids:
        st.write(f"  - BLOCKED: Already staged")
        return False  # Already staged
    
    # Use selected contact info if provided, otherwise default to main table contact info
    if selected_contact:
        # Use the selected contact from TOP10_CONTACTS
        contact_name = selected_contact.get("name", "")
        contact_email = selected_contact.get("email_address", "")
        contact_phone = selected_contact.get("direct_phone_number", "")
        contact_mobile = selected_contact.get("mobile_phone", "")
        contact_title = selected_contact.get("job_title", "")
    else:
        # Use default contact info from the main table
        contact_name = prospect_data.get("CONTACT_NAME", "")
        contact_email = prospect_data.get("CONTACT_EMAIL", "")
        contact_phone = prospect_data.get("CONTACT_PHONE", "")
        contact_mobile = prospect_data.get("CONTACT_MOBILE", "")
        contact_title = prospect_data.get("CONTACT_JOB_TITLE", "")
    
    # Parse contact name for first/last
    name_parts = contact_name.split(' ') if contact_name else ['', '']
    first_name = name_parts[0] if len(name_parts) > 0 else ""
    last_name = name_parts[-1] if len(name_parts) > 1 else ""
    
    # Debug: Let's see what data we're getting from prospect_data (simplified)
    if prospect_data.get('DBA_NAME') == '1st Tribal Lending':  # Only debug this specific problematic prospect
        print(f"DEBUG: Staging prospect {prospect_data.get('DBA_NAME', 'Unknown')}")
        print(f"  Prospect ID: '{prospect_id}'")
        print(f"  TYPE of prospect_data: {type(prospect_data)}")
        print(f"  TOP10_CONTACTS: '{prospect_data.get('TOP10_CONTACTS', 'MISSING')}'")
        print(f"  HAS_CONTACT_INFO: '{prospect_data.get('HAS_CONTACT_INFO', 'MISSING')}'")
    
    # Create staging record
    staging_record = {
        "prospect_id": prospect_id,
        "company": prospect_data.get("DBA_NAME", ""),
        "first_name": first_name,
        "last_name": last_name,
        "email": contact_email,
        "phone": prospect_data.get("PHONE", ""),  # Keep main company phone
        "city": prospect_data.get("CITY", ""),
        "state": prospect_data.get("STATE", ""),
        "zip": prospect_data.get("ZIP", ""),
        "industry": prospect_data.get("PRIMARY_INDUSTRY", ""),
        "revenue": prospect_data.get("REVENUE", ""),
        "employees": prospect_data.get("NUMBER_OF_EMPLOYEES", ""),
        "contact_name": contact_name,
        "contact_mobile": contact_mobile,
        "contact_phone": contact_phone,  # Contact's direct phone
        "address": prospect_data.get("FULL_ADDRESS") or prospect_data.get("ADDRESS", ""),
        "website": prospect_data.get("WEBSITE", ""),
        "job_title": contact_title,
        # Additional mapped fields for full record
        "mcc_code": prospect_data.get("MCC_CODE", ""),
        "identifier": prospect_data.get("IDENTIFIER", ""),
        "taxid": prospect_data.get("TAXID", ""),
        "number_of_locations": prospect_data.get("NUMBER_OF_LOCATIONS", ""),
        "zi_c_company_id": prospect_data.get("ZI_C_COMPANY_ID", ""),
        "zi_contact_id": prospect_data.get("ZI_CONTACT_ID", ""),
        "data_agg_uid": prospect_data.get("DATA_AGG_UID", ""),
        "zi_c_location_id": prospect_data.get("ZI_C_LOCATION_ID", ""),
        # Store original address components separately for proper field mapping
        "original_address": prospect_data.get("ADDRESS", ""),  # Street address only
        "original_city": prospect_data.get("CITY", ""),
        "original_state": prospect_data.get("STATE", ""),
        "original_zip": prospect_data.get("ZIP", ""),
        # Store TOP10_CONTACTS data for persistent contact selection across app reloads
        "top10_contacts": prospect_data.get("TOP10_CONTACTS", ""),
        "added_timestamp": datetime.now().isoformat()
    }
    
    # Debug: Let's see what's actually in our staging record (simplified)
    if prospect_data.get('DBA_NAME') == '1st Tribal Lending':  # Only debug this specific problematic prospect
        print(f"DEBUG: Staged record created for {staging_record.get('company')}")
        print(f"  prospect_id: '{staging_record.get('prospect_id')}'")
        st.write(f"  zi_c_location_id: '{staging_record.get('zi_c_location_id', 'MISSING')}'")
    
    st.session_state.staged_prospects.append(staging_record)
    st.write(f"  - SUCCESS: Staged '{prospect_data.get('DBA_NAME', 'Unknown')}' successfully")
    return True  # Successfully staged


def remove_prospect_from_staging(prospect_id):
    """Remove a prospect from the staging area"""
    if "staged_prospects" not in st.session_state:
        return False
    
    original_count = len(st.session_state.staged_prospects)
    prospect_id_str = str(prospect_id) if prospect_id is not None else None
    
    st.session_state.staged_prospects = [
        p for p in st.session_state.staged_prospects 
        if str(p.get("prospect_id")) != prospect_id_str
    ]
    
    removed_count = original_count - len(st.session_state.staged_prospects)
    
    return removed_count > 0


def clear_staging():
    """Clear all staged prospects"""
    st.session_state.staged_prospects = []


def validate_staged_prospect_enhanced(prospect):
    """Enhanced validation that includes contact selection check"""
    # Basic validation
    validation_result = validate_staged_prospect(prospect)
    
    # Check if this prospect needs contact selection using stored TOP10_CONTACTS
    prospect_id = prospect.get('prospect_id')
    needs_contact_selection = False
    contact_selection_valid = True
    
    # Parse TOP10_CONTACTS from the staged prospect data
    top_contacts = prospect.get("top10_contacts")
    
    # Parse TOP10_CONTACTS to check for multiple contacts
    contacts_available = []
    has_contacts = False
    if hasattr(top_contacts, 'empty'):  # pandas Series
        has_contacts = not top_contacts.empty and not top_contacts.isna().all()
        if has_contacts:
            top_contacts = top_contacts.iloc[0] if len(top_contacts) > 0 else {}
    else:
        has_contacts = bool(top_contacts)
    if has_contacts:
        if isinstance(top_contacts, str):
            try:
                import json
                contacts_obj = json.loads(top_contacts)
            except:
                contacts_obj = {}
        else:
            contacts_obj = top_contacts
        
        if isinstance(contacts_obj, dict) and contacts_obj:
            contacts_available = list(contacts_obj.values())
        elif isinstance(contacts_obj, list) and contacts_obj:
            contacts_available = contacts_obj
    
    # Add main table contact if it has meaningful data
    main_table_contact = {
        "name": prospect.get("contact_name", ""),
        "email_address": prospect.get("email", ""),
        "direct_phone_number": prospect.get("contact_phone", ""),
        "mobile_phone": prospect.get("contact_mobile", ""),
        "job_title": prospect.get("job_title", ""),
        "source": "main_table"
    }
    
    main_contact_has_data = any(
        bool(main_table_contact.get(field, "").strip()) if main_table_contact.get(field) else False
        for field in ["name", "email_address", "direct_phone_number", "mobile_phone", "job_title"]
    )
    
    if main_contact_has_data:
        contacts_available.insert(0, main_table_contact)
    
    # Check if contact selection is needed and valid
    if len(contacts_available) > 1:
                needs_contact_selection = True
                current_selection_key = f"contact_selection_{prospect_id}"
                current_selection = st.session_state.get(current_selection_key, 0)
                
                # Check if "Please select" option is still selected (index 0)
                contact_selection_valid = current_selection != 0
    
    # Overall validation combines basic validation and contact selection
    is_valid = validation_result['is_valid'] and contact_selection_valid
    missing_fields = validation_result.get('missing_fields', []).copy()
    if needs_contact_selection and not contact_selection_valid:
        missing_fields.append("Contact Selection")
    
    return {
        "is_valid": is_valid,
        "missing_fields": missing_fields,
        "errors": missing_fields,
        "needs_contact_selection": needs_contact_selection,
        "contact_selection_valid": contact_selection_valid
    }


def validate_staged_prospect(prospect):
    """Validate a staged prospect for required fields"""
    required_fields = {
        "company": "Company Name",
        "first_name": "First Name", 
        "last_name": "Last Name"
    }
    
    missing_fields = []
    for field, label in required_fields.items():
        if not prospect.get(field) or prospect.get(field).strip() == "":
            missing_fields.append(label)
    
    return {
        "is_valid": len(missing_fields) == 0,
        "missing_fields": missing_fields,
        "errors": missing_fields  # Add errors alias for consistency
    }


def submit_staged_prospects_to_sf():
    """Submit all staged prospects to the sf_leads table"""
    if "staged_prospects" not in st.session_state or not st.session_state.staged_prospects:
        return {
            "success": False,
            "message": "No prospects in staging area",
            "results": []
        }
    
    results = []
    success_count = 0
    error_count = 0
    duplicate_count = 0
    
    session = get_active_session()
    
    for prospect in st.session_state.staged_prospects:
        # Validate before submission
        validation = validate_staged_prospect(prospect)
        if not validation["is_valid"]:
            results.append({
                "prospect": prospect.get("company", "Unknown"),
                "status": "error",
                "message": f"Missing required fields: {', '.join(validation['missing_fields'])}"
            })
            error_count += 1
            continue
        
        # Check if there's a selected contact for this prospect
        prospect_id = prospect["prospect_id"]
        selected_contact_key = f"selected_contact_for_{prospect_id}"
        selected_contact = st.session_state.get(selected_contact_key)
        
        # Create a modified prospect data that includes the selected contact info
        if selected_contact and selected_contact.get("source") != "main_table":
            # Use the selected contact from TOP10_CONTACTS
            modified_prospect = prospect.copy()
            
            # Update contact fields with selected contact data
            if selected_contact.get("name"):
                modified_prospect["contact_name"] = selected_contact["name"]
                # Update first_name and last_name as well
                name_parts = selected_contact["name"].split(' ') if selected_contact["name"] else ['', '']
                modified_prospect["first_name"] = name_parts[0] if len(name_parts) > 0 else ""
                modified_prospect["last_name"] = name_parts[-1] if len(name_parts) > 1 else ""
            
            if selected_contact.get("email_address"):
                modified_prospect["email"] = selected_contact["email_address"]
            
            if selected_contact.get("direct_phone_number"):
                modified_prospect["contact_phone"] = selected_contact["direct_phone_number"]
            
            if selected_contact.get("mobile_phone"):
                modified_prospect["contact_mobile"] = selected_contact["mobile_phone"]
            
            if selected_contact.get("job_title"):
                modified_prospect["job_title"] = selected_contact["job_title"]
            
            # Create a prospect data dict compatible with insert_prospect_to_sf_table
            # This must match the exact structure that insert_prospect_to_sf_table expects
            prospect_data_for_insert = {
                "PROSPECT_ID": modified_prospect["prospect_id"],
                "ADDRESS": modified_prospect.get("original_address", ""),
                "REVENUE": modified_prospect.get("revenue", ""),
                "CITY": modified_prospect.get("original_city", ""),
                "STATE": modified_prospect.get("original_state", ""),
                "ZIP": modified_prospect.get("original_zip", ""),
                "DBA_NAME": modified_prospect.get("company", ""),
                "PRIMARY_INDUSTRY": modified_prospect.get("industry", ""),
                "CONTACT_EMAIL": modified_prospect.get("email", ""),
                "TAXID": modified_prospect.get("taxid", ""),
                "CONTACT_NAME": modified_prospect.get("contact_name", ""),
                "MCC_CODE": modified_prospect.get("mcc_code", ""),
                "IDENTIFIER": modified_prospect.get("identifier", ""),
                "NUMBER_OF_LOCATIONS": modified_prospect.get("number_of_locations", ""),
                "NUMBER_OF_EMPLOYEES": modified_prospect.get("employees", ""),
                "PHONE": modified_prospect.get("phone", ""),
                "CONTACT_MOBILE": modified_prospect.get("contact_mobile", ""),
                "ZI_C_COMPANY_ID": modified_prospect.get("zi_c_company_id", ""),
                "ZI_CONTACT_ID": modified_prospect.get("zi_contact_id", ""),
                "WEBSITE": modified_prospect.get("website", ""),
                "DATA_AGG_UID": modified_prospect.get("data_agg_uid", ""),
                "CONTACT_JOB_TITLE": modified_prospect.get("job_title", ""),
                "ZI_C_LOCATION_ID": modified_prospect.get("zi_c_location_id", ""),
                "CONTACT_PHONE": modified_prospect.get("contact_phone", "")
            }
        else:
            # Use the original prospect data (either main table contact or no contact selection)
            prospect_data_for_insert = {
                "PROSPECT_ID": prospect["prospect_id"],
                "ADDRESS": prospect.get("original_address", ""),  # Use stored original ADDRESS
                "REVENUE": prospect.get("revenue", ""),
                "CITY": prospect.get("original_city", ""),  # Use stored original CITY
                "STATE": prospect.get("original_state", ""),  # Use stored original STATE
                "ZIP": prospect.get("original_zip", ""),  # Use stored original ZIP
                "DBA_NAME": prospect.get("company", ""),
                "PRIMARY_INDUSTRY": prospect.get("industry", ""),
                "CONTACT_EMAIL": prospect.get("email", ""),
                "TAXID": prospect.get("taxid", ""),
                "CONTACT_NAME": prospect.get("contact_name", ""),
                "MCC_CODE": prospect.get("mcc_code", ""),
                "IDENTIFIER": prospect.get("identifier", ""),
                "NUMBER_OF_LOCATIONS": prospect.get("number_of_locations", ""),
                "NUMBER_OF_EMPLOYEES": prospect.get("employees", ""),
                "PHONE": prospect.get("phone", ""),
                "CONTACT_MOBILE": prospect.get("contact_mobile", ""),
                "ZI_C_COMPANY_ID": prospect.get("zi_c_company_id", ""),
                "ZI_CONTACT_ID": prospect.get("zi_contact_id", ""),
                "WEBSITE": prospect.get("website", ""),
                "DATA_AGG_UID": prospect.get("data_agg_uid", ""),
                "CONTACT_JOB_TITLE": prospect.get("job_title", ""),
                "ZI_C_LOCATION_ID": prospect.get("zi_c_location_id", ""),
                "CONTACT_PHONE": prospect.get("contact_phone", "")
            }
        
        # Submit to sf_leads table - let the function fetch data from DB and only override contact fields
        try:
            # Check if we need to override contact fields
            contact_override = None
            if selected_contact and selected_contact.get("source") != "main_table":
                # Use the selected contact from TOP10_CONTACTS
                contact_override = {}
                if selected_contact.get("name"):
                    contact_override["CONTACT_NAME"] = selected_contact["name"]
                if selected_contact.get("email_address"):
                    contact_override["CONTACT_EMAIL"] = selected_contact["email_address"]
                if selected_contact.get("direct_phone_number"):
                    contact_override["CONTACT_PHONE"] = selected_contact["direct_phone_number"]
                if selected_contact.get("mobile_phone"):
                    contact_override["CONTACT_MOBILE"] = selected_contact["mobile_phone"]
                if selected_contact.get("job_title"):
                    contact_override["CONTACT_JOB_TITLE"] = selected_contact["job_title"]
            
            # Call the function - it will fetch all data from DB and override only contact fields if provided
            result = insert_prospect_to_sf_table(prospect["prospect_id"], session, contact_override)
                
        except Exception as insert_error:
            error_count += 1
            results.append({
                "prospect": prospect.get("company", "Unknown"),
                "status": "error",
                "message": f"Insertion error: {str(insert_error)}"
            })
            continue
        
        if result["success"]:
            success_count += 1
            results.append({
                "prospect": prospect.get("company", "Unknown"),
                "status": "success",
                "message": result["message"]
            })
            # Add to session tracking
            prospect_id_str = str(prospect["prospect_id"])
            if prospect_id_str not in st.session_state.get("sf_prospect_ids", []):
                if "sf_prospect_ids" not in st.session_state:
                    st.session_state.sf_prospect_ids = []
                st.session_state.sf_prospect_ids.append(prospect_id_str)
                st.session_state.sf_pushed_count = len(st.session_state.sf_prospect_ids)
                st.session_state.sf_last_update = datetime.now().isoformat()
        
        elif result["is_duplicate"]:
            duplicate_count += 1
            results.append({
                "prospect": prospect.get("company", "Unknown"),
                "status": "duplicate",
                "message": result["message"]
            })
        else:
            error_count += 1
            results.append({
                "prospect": prospect.get("company", "Unknown"),
                "status": "error", 
                "message": result["message"]
            })
    
    # Clear staging after submission attempt
    clear_staging()
    
    return {
        "success": success_count > 0,
        "success_count": success_count,
        "error_count": error_count,
        "duplicate_count": duplicate_count,
        "results": results,
        "total_processed": len(results),
        "message": f"Processed {len(results)} prospects: {success_count} successful, {error_count} errors, {duplicate_count} duplicates" if len(results) > 0 else "No prospects to process"
    }



def is_dark_map_style(map_style=None):
    """Determine if the current or provided map style is dark (needs light tooltip)"""
    style = map_style if map_style is not None else get_current_map_style()
    return style in [":material/dark_mode:", ":material/satellite_alt:"]

def build_tooltip_sections(prospect_data):
    sections = []

    # Add DNC warning if INTERNAL_DNC is True
    dnc_val = prospect_data.get("INTERNAL_DNC")
    if dnc_val is True or dnc_val == 1 or (isinstance(dnc_val, str) and dnc_val.strip().lower() in ["true", "1"]):
        dnc_warning = "<span style='color:red; font-weight:bold; font-size:1.1em;'>🚫 INTERNAL DNC</span>"
        sections.append(("", [dnc_warning]))

    location_items = []
    parent_name_col = "PARENT_NAME"
    prospect_name_col = "DBA_NAME"
    parent_name = str(prospect_data.get(parent_name_col, '') or '').strip().lower()
    prospect_name = str(prospect_data.get(prospect_name_col, '') or '').strip().lower()
    if parent_name_col and is_valid_value(prospect_data.get(parent_name_col)) and parent_name and parent_name != prospect_name:
        location_items.append(
            f'<span style="font-weight:bold; font-size:1.25em;">🏢 {prospect_data[parent_name_col]}</span>'
        )

    address_parts = extract_address_parts(prospect_data)
    if address_parts:
        address_str = ', '.join(address_parts)
        location_items.append(f'📍 {address_str}')

    phone_col = "PHONE"
    if phone_col and is_valid_value(prospect_data.get(phone_col)):
        formatted_phone = format_phone_for_display(prospect_data[phone_col])
        if formatted_phone:
            location_items.append(f'📞 {formatted_phone}')

    url_col = "WEBSITE"
    if url_col and is_valid_value(prospect_data.get(url_col)):
        formatted_url = format_url(prospect_data[url_col])
        if formatted_url:
            location_items.append(f'🌐 {formatted_url}')

    if location_items:
        sections.append(('Location Details', location_items))

    # Contact Information Section
    contact_items = []
    # Add NATIONAL DNC warning for contact if present, above CONTACT_NAME
    contact_natl_dnc = prospect_data.get("CONTACT_NATIONAL_DNC")
    if contact_natl_dnc is True or contact_natl_dnc == 1 or (isinstance(contact_natl_dnc, str) and contact_natl_dnc.strip().lower() in ["true", "1"]):
        contact_items.append("<span style='color:red; font-size:.75em;'>🚫 NATIONAL DNC</span>")

    contact_name_col = "CONTACT_NAME"
    if contact_name_col and is_valid_value(prospect_data.get(contact_name_col)):
        contact_items.append(f'👤 {prospect_data[contact_name_col]}')

    contact_phone_col = "CONTACT_PHONE"
    if contact_phone_col and is_valid_value(prospect_data.get(contact_phone_col)):
        formatted_phone = format_phone_for_display(prospect_data[contact_phone_col])
        if formatted_phone:
            contact_items.append(f'📞 {formatted_phone}')

    contact_mobile_col = "CONTACT_MOBILE"
    if contact_mobile_col and is_valid_value(prospect_data.get(contact_mobile_col)):
        formatted_mobile = format_phone_for_display(prospect_data[contact_mobile_col])
        if formatted_mobile:
            contact_items.append(f'📱 {formatted_mobile}')

    contact_job_title_col = "CONTACT_JOB_TITLE"
    if contact_job_title_col and is_valid_value(prospect_data.get(contact_job_title_col)):
        contact_items.append(f'💼 {prospect_data[contact_job_title_col]}')

    contact_email_col = "CONTACT_EMAIL"
    if contact_email_col and is_valid_value(prospect_data.get(contact_email_col)):
        contact_items.append(f'📧 {prospect_data[contact_email_col]}')

    if contact_items:
        sections.append(('Contact Information', contact_items))

    # prospect Metrics Section
    metrics_items = []
    revenue_col = "REVENUE"
    if revenue_col and is_valid_value(prospect_data.get(revenue_col)):
        try:
            revenue_value = float(prospect_data[revenue_col])
            metrics_items.append(f'💵 Revenue: ${revenue_value:,.0f}')
        except (ValueError, TypeError):
            pass

    employees_col = "NUMBER_OF_EMPLOYEES"
    if employees_col and is_valid_value(prospect_data.get(employees_col)):
        metrics_items.append(f'👥 Employees: {prospect_data[employees_col]}')

    locations_col = "NUMBER_OF_LOCATIONS"
    if locations_col and is_valid_value(prospect_data.get(locations_col)):
            try:
                loc_val = int(float(prospect_data[locations_col]))
                metrics_items.append(f'🏢 Locations: {loc_val:,}')
            except (ValueError, TypeError):
                metrics_items.append(f'🏢 Locations: {prospect_data[locations_col]}')

    industry_col = "PRIMARY_INDUSTRY"
    if industry_col and is_valid_value(prospect_data.get(industry_col)):
        metrics_items.append(f'🏭 Industry: {prospect_data[industry_col]}')

    mcc_col = "MCC_CODE"
    if mcc_col and is_valid_value(prospect_data.get(mcc_col)):
        metrics_items.append(f'📊 MCC Code: {prospect_data[mcc_col]}')

    if metrics_items:
        sections.append(('prospect Metrics', metrics_items))

    return sections

def build_prospect_card_sections(prospect_data):
    """Build standardized prospect data sections for HTML prospect cards"""
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
    prospect_name_col = "DBA_NAME"
    prospect_phone_col = "PHONE"
    prospect_website_col = "WEBSITE"

    parent_name = str(prospect_data.get(parent_name_col, '') or '').strip().lower()
    prospect_name = str(prospect_data.get(prospect_name_col, '') or '').strip().lower()
    parent_phone = str(prospect_data.get(parent_phone_col, '') or '').strip().lower()
    prospect_phone = str(prospect_data.get(prospect_phone_col, '') or '').strip().lower()
    parent_website = str(prospect_data.get(parent_website_col, '') or '').strip().lower()
    prospect_website = str(prospect_data.get(prospect_website_col, '') or '').strip().lower()

    parent_info_present = any([
        is_valid_value(prospect_data.get(parent_name_col)),
        is_valid_value(prospect_data.get(parent_phone_col)),
        is_valid_value(prospect_data.get(parent_website_col))
    ])
    parent_info_differs = (
        (parent_name and parent_name != prospect_name) or
        (parent_phone and parent_phone != prospect_phone) or
        (parent_website and parent_website != prospect_website)
    )

    if parent_info_present and parent_info_differs:
        if parent_name:
            parent_metrics.append(create_metric('🏢', 'Parent Company', prospect_data[parent_name_col]))
        if parent_phone:
            formatted_parent_phone = format_phone_for_display(prospect_data[parent_phone_col])
            parent_phone_link = format_phone_for_link(prospect_data[parent_phone_col])
            if formatted_parent_phone:
                parent_metrics.append(create_metric('📞', 'Parent Phone', formatted_parent_phone, parent_phone_link))
        if parent_website:
            formatted_parent_website = format_url(prospect_data[parent_website_col])
            if formatted_parent_website:
                parent_metrics.append(create_metric('🌐', 'Parent Website', formatted_parent_website, formatted_parent_website))

    location_metrics = []
    address_parts = extract_address_parts(prospect_data)
    if address_parts:
        address_str = ', '.join(address_parts)
        address_link = format_address_for_link(address_parts)
        location_metrics.append(create_metric('📍', 'Address', address_str, address_link))

    phone_col = "PHONE"
    if phone_col and is_valid_value(prospect_data.get(phone_col)):
        formatted_phone = format_phone_for_display(prospect_data[phone_col])
        phone_link = format_phone_for_link(prospect_data[phone_col])
        if formatted_phone:
            location_metrics.append(create_metric('📞', 'Phone', formatted_phone, phone_link))

    url_col = "WEBSITE"
    if url_col and is_valid_value(prospect_data.get(url_col)):
        formatted_url = format_url(prospect_data[url_col])
        if formatted_url:
            location_metrics.append(create_metric('🌐', 'Website', formatted_url, formatted_url))

    # prospect metrics
    prospect_metrics = []
    revenue_col = "REVENUE"
    if revenue_col and is_valid_value(prospect_data.get(revenue_col)):
        try:
            revenue_value = float(prospect_data[revenue_col])
            prospect_metrics.append(create_metric('💵', 'Annual Revenue', f'${revenue_value:,.0f}'))
        except (ValueError, TypeError):
            pass

    metric_fields = [
        ("NUMBER_OF_EMPLOYEES", "👥", "Employees"),
        ("NUMBER_OF_LOCATIONS", "🏢", "Locations")
    ]
    for logical_field, icon, label in metric_fields:
        # If you need to map logical to actual, do it here, else use logical_field directly
        logical_to_actual = {"MCC_CODE": "MCC_CODE", "B2B": "IS_B2B", "B2C": "IS_B2C"}
        actual_col = logical_to_actual.get(logical_field, logical_field)
        if actual_col and is_valid_value(prospect_data.get(actual_col)):
                if actual_col == "NUMBER_OF_LOCATIONS":
                    try:
                        loc_val = int(float(prospect_data[actual_col]))
                        prospect_metrics.append(create_metric(icon, label, f"{loc_val:,}"))
                    except (ValueError, TypeError):
                        prospect_metrics.append(create_metric(icon, label, prospect_data[actual_col]))
                else:
                    prospect_metrics.append(create_metric(icon, label, prospect_data[actual_col]))

    # Return HTML sections
    sections = []
    if parent_metrics:
        sections.append(create_section("Parent Company", parent_metrics))
    sections.extend([
        create_section("Location Details", location_metrics),
        create_section("Prospect Metrics", prospect_metrics)
    ])

    # Add Contact Hierarchy section if there are contacts in TOP10_CONTACTS, or fall back to main table contact info
    import json
    top_contacts = prospect_data.get("TOP10_CONTACTS")
    has_contacts = False
    contacts_iter = []
    
    # Handle pandas Series for top_contacts
    if hasattr(top_contacts, 'empty'):  # pandas Series
        has_contacts = not top_contacts.empty and not top_contacts.isna().all()
        if has_contacts:
            top_contacts = top_contacts.iloc[0] if len(top_contacts) > 0 else {}
    else:
        has_contacts = bool(top_contacts)
    
    if has_contacts:
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
    
    # If no TOP10_CONTACTS, check for fallback contact info from main table
    has_fallback_contact = False
    if not has_contacts:
        contact_fields = ["CONTACT_NAME", "CONTACT_PHONE", "CONTACT_MOBILE", "CONTACT_JOB_TITLE", "CONTACT_EMAIL"]
        has_fallback_contact = any(is_valid_value(prospect_data.get(field)) for field in contact_fields)
    
    if has_contacts or has_fallback_contact:
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
        
        def create_fallback_contact_row():
            # Create a contact row using main table contact fields
            name = prospect_data.get('CONTACT_NAME', '') or ''
            job_title = prospect_data.get('CONTACT_JOB_TITLE', '') or ''
            direct_phone = prospect_data.get('CONTACT_PHONE', '') or ''
            mobile_phone = prospect_data.get('CONTACT_MOBILE', '') or ''
            email = prospect_data.get('CONTACT_EMAIL', '') or ''
            
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

            # Check for NATIONAL DNC flag from main table
            contact_natl_dnc = prospect_data.get("CONTACT_NATIONAL_DNC")
            show_dnc = contact_natl_dnc is True or contact_natl_dnc == 1 or (isinstance(contact_natl_dnc, str) and contact_natl_dnc.strip().lower() in ['true', '1'])
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
        
        if has_contacts:
            # Use TOP10_CONTACTS data
            for contact in contacts_iter:
                contact_rows.append(create_contact_row(contact))
            contact_section_title = "Contact Hierarchy"
        else:
            # Use fallback contact info from main table
            contact_rows.append(create_fallback_contact_row())
            contact_section_title = "Contact Information"
        
        contact_hierarchy_html = f'<div class="data-viz-section"><div class="section-header">{contact_section_title}</div><div style="display:flex; flex-direction:column; gap:8px;">{"".join(contact_rows)}</div></div>'
        sections.append(contact_hierarchy_html)

    return sections

def create_data_editor_column_config():
    """Create standardized column configuration for st.data_editor"""
    config = {
        # Checkbox columns
        "Map": st.column_config.CheckboxColumn("Map", help="Check to include on map", default=True, width="small"),
        "SF": st.column_config.CheckboxColumn("SF", help="Check to write to salesforce", default=False, width="small"),
        
        # Text columns  
        "Current Customer": st.column_config.TextColumn("Current Customer", help="🔵 = Existing Customer, ⚪ = Prospect"),
        "FULL_ADDRESS": st.column_config.TextColumn("Full Address", help="Complete address"),
        "IDENTIFIER": st.column_config.TextColumn("Identifier", help="Identifier"),
        "CONTACT_NAME": st.column_config.TextColumn("Contact Name", help="Contact Name"),
        "PRIMARY_INDUSTRY": st.column_config.TextColumn("Primary Industry", help="Primary Industry"),
        "SUB_INDUSTRY": st.column_config.TextColumn("Sub Industry", help="Identifier"),
        "MCC_CODE": st.column_config.TextColumn("MCC Code", help="MCC"),
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
        "REVENUE": st.column_config.TextColumn("Revenue", help="Annual revenue in USD"),
        "NUMBER_OF_EMPLOYEES": st.column_config.NumberColumn("Employees", help="Number of employees", format="%.0f"),
        "NUMBER_OF_LOCATIONS": st.column_config.NumberColumn("Locations", help="Number of locations", format="%.0f")
    }
    
    # Hide columns by setting them to None
    hidden_columns = get_hidden_columns()
    for col in hidden_columns:
        config[col] = None
    
    return config

def get_disabled_columns():
    """Get list of columns that should be disabled in data editor"""
    return [
        "PROSPECT_ID", "DBA_NAME", "FULL_ADDRESS", "PHONE", "WEBSITE", "IDENTIFIER", "Current Customer", 
        "CONTACT_NAME", "CONTACT_EMAIL", "CONTACT_PHONE", "PRIMARY_INDUSTRY", 
        "SUB_INDUSTRY", "MCC_CODE", "REVENUE", "NUMBER_OF_EMPLOYEES", 
        "NUMBER_OF_LOCATIONS", "PARENT_NAME", "PARENT_PHONE", "PARENT_WEBSITE", "IS_B2B", "IS_B2C", 
        "TOP10_CONTACTS", "CONTACT_NATIONAL_DNC", "INTERNAL_DNC"
    ]

def get_hidden_columns():

    # Add columns to hide in data editor/tab1
    return [
        "HAS_CONTACT_INFO" ,
        "PROSPECT_ID"
    ]

# def get_dataframe_format_config():
#     """Get standardized formatting configuration for styled dataframes"""
#     return {
#         "REVENUE": format_currency,
#         "NUMBER_OF_EMPLOYEES": format_number,
#         "NUMBER_OF_LOCATIONS": format_number,
#         "ZIP": format_zip,
#         "PHONE": format_phone,
#         "CONTACT_PHONE": format_phone,
#         "CONTACT_NAME": format_contact_name,
#         "CONTACT_MOBILE": format_phone,
#         "PARENT_PHONE": format_phone,
#        
#    }

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

def geocode_address(address_or_zip):
    """
    Convert an address or ZIP code to latitude/longitude coordinates using Snowflake UDF.
    Returns dict with 'latitude' and 'longitude' keys, or None if geocoding fails.
    Uses session state cache to avoid repeated geocoding of the same address.
    """
    try:
        if not address_or_zip or not address_or_zip.strip():
            return None
        
        # Normalize the address for cache lookup
        normalized_address = address_or_zip.strip().lower()
        
        # Check cache first
        if "geocode_cache" not in st.session_state:
            st.session_state["geocode_cache"] = {}
        
        if normalized_address in st.session_state["geocode_cache"]:
            return st.session_state["geocode_cache"][normalized_address]
            
        # Call your Snowflake UDF - REPLACE 'YOUR_GEOCODING_UDF' with the actual UDF name
        # Example: query = "SELECT GEOCODE_ADDRESS(?) AS geocode_result"
        query = "SELECT python_workloads.data_engineering.geocode_address(?) AS geocode_result"
        result = execute_sql_query(query, params=[address_or_zip.strip()], operation_name="geocode_address", return_single_value=True)
        
        geocode_result = None
        if result:
            # Parse the JSON result
            geocode_data = json.loads(result) if isinstance(result, str) else result
            if isinstance(geocode_data, dict) and 'latitude' in geocode_data and 'longitude' in geocode_data:
                geocode_result = {
                    'latitude': float(geocode_data['latitude']),
                    'longitude': float(geocode_data['longitude'])
                }
        
        # Cache the result (even if None to avoid repeated failed attempts)
        st.session_state["geocode_cache"][normalized_address] = geocode_result
        return geocode_result
        
    except Exception as e:
        st.warning(f"Geocoding failed for '{address_or_zip}': {str(e)}")
        return None

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
    # Check for location address which is the main location filter
    if filters.get("LOCATION_ADDRESS", "").strip():
        return True
    
    return any(
        (STATIC_FILTERS[k]["type"] == "dropdown" and v != []) or
        (STATIC_FILTERS[k]["type"] == "multiselect" and v != []) or
        (STATIC_FILTERS[k]["type"] == "range" and v != [None, None]) or
        (STATIC_FILTERS[k]["type"] == "checkbox" and (
            any(checked for checked in v.values()) if isinstance(v, dict) else bool(v)
        )) or
        (STATIC_FILTERS[k]["type"] == "selectbox" and v != STATIC_FILTERS[k].get("options", [""])[0]) or
        (STATIC_FILTERS[k]["type"] == "text" and v.strip()) or
        (STATIC_FILTERS[k]["type"] == "number" and v != STATIC_FILTERS[k].get("default"))
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
    elif filter_type == "multiselect":
        return bool(filter_value)
    elif filter_type == "range":
        return filter_value != [None, None]
    elif filter_type == "checkbox":
        # Handle both dictionary format (new) and boolean format (legacy)
        if isinstance(filter_value, dict):
            return any(checked for checked in filter_value.values())
        else:
            return bool(filter_value)
    elif filter_type == "selectbox":
        return filter_value != STATIC_FILTERS[filter_key].get("options", [""])[0]
    elif filter_type == "text":
        return bool(filter_value.strip() if isinstance(filter_value, str) else filter_value)
    elif filter_type == "number":
        return filter_value != STATIC_FILTERS[filter_key].get("default")
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
    if st.session_state["selected_prospect_indices"]:
        current_scale = st.session_state["selected_radius_scale"]
        new_scale = max(min_value, min(max_value, current_scale * scale_factor))
        st.session_state["selected_radius_scale"] = new_scale
    else:
        current_scale = st.session_state["initial_radius_scale"]
        new_scale = max(min_value, min(max_value, current_scale * scale_factor))
        st.session_state["initial_radius_scale"] = new_scale

def reset_radius_scale():
    """Reset radius scale to default values"""
    if st.session_state["selected_prospect_indices"]:
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
    # Display Salesforce operation results if they exist
    if 'sf_bulk_result' in st.session_state:
        result = st.session_state.sf_bulk_result
        
        if result['newly_added'] > 0 or result['duplicates'] > 0 or result['errors'] > 0:
            if result['newly_added'] > 0:
                st.success(f"✅ Successfully added {result['newly_added']} prospects to Salesforce")
            if result['duplicates'] > 0:
                st.warning(f"⚠️ {result['duplicates']} prospects were duplicates and not added")
            if result['errors'] > 0:
                st.error(f"❌ {result['errors']} prospects failed to be added")
            if result['already_tracked'] > 0:
                st.info(f"ℹ️ {result['already_tracked']} prospects were already tracked in this session")
                
            # Show detailed messages in an expander
            with st.expander("View detailed results"):
                for message in result['messages']:
                    st.write(message)
        
        # Clear the result after displaying
        del st.session_state.sf_bulk_result
    
    if 'sf_single_result' in st.session_state:
        result = st.session_state.sf_single_result
        
        if result['success']:
            st.success(f"✅ {result['message']}")
        elif result['is_duplicate']:
            st.warning(f"⚠️ {result['message']}")
        elif result['already_tracked']:
            st.info(f"ℹ️ {result['message']}")
        else:
            st.error(f"❌ {result['message']}")
        
        # Clear the result after displaying
        del st.session_state.sf_single_result

    filters, apply_filters = create_sidebar_filters()

    display_filter_summary(st.session_state["active_filters"])
    
    # Ensure sf_prospect_ids is initialized
    init_session_state_key("sf_prospect_ids", [])
    
    # Ensure sf_pushed_count is initialized
    init_session_state_key("sf_pushed_count", 0)
    
    # Ensure sf_last_update is initialized
    init_session_state_key("sf_last_update", datetime.now().isoformat())
    
    tab1, tab2, tab3 = st.tabs(["List View", "Map View", "Salesforce Queue"])

    if apply_filters and has_active_filters(filters):
        def fetch_data():
            cache_key = create_cache_key("filtered_data", filters)
            reset_to_first_page()
            return fetch_filtered_data(
                filters, cache_key, st.session_state["page_size"], st.session_state["current_page"], fetch_all=True
            )
        result = with_loading_spinner("Fetching data...", fetch_data)
        st.session_state["filtered_df"], st.session_state["total_records"], original_total = result
        # Store the full DataFrame for internal logic (staging, etc.)
        st.session_state["full_filtered_df"] = st.session_state["filtered_df"].copy()
        
        # Handle the warning outside of the cached function
        if "limit_warning" in st.session_state:
            del st.session_state["limit_warning"]
        if original_total > MAX_RESULTS:
            st.session_state["limit_warning"] = f"Result set contains {original_total} records, which exceeds the limit of {MAX_RESULTS}. Displaying the first {MAX_RESULTS} records."
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
        # Unified List View: always use filtered_df and total_records
        show_df = st.session_state.get('filtered_df', None)
        show_total = st.session_state.get('total_records', 0)
        # --- Normalize columns and format for filter results ---
        if show_df is not None and not show_df.empty:
            # Create display_df that includes ALL columns from the full data for internal logic
            display_df = show_df.copy()
            
            # Add UI-only columns if not present
            ui_columns = ["Map", "SF", "Current Customer", "ADDRESS_LINK", "FULL_ADDRESS"]
            for col in ui_columns:
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
            
            # Create preferred column order (UI columns first, then all data columns)
            preferred_order = [
                "Map", "SF", "Current Customer", "DBA_NAME", "ADDRESS_LINK", "FULL_ADDRESS",
                "PHONE", "WEBSITE", "CONTACT_NAME", "CONTACT_EMAIL", "CONTACT_PHONE", "CONTACT_MOBILE", "CONTACT_JOB_TITLE",
                "PRIMARY_INDUSTRY", "SUB_INDUSTRY", "MCC_CODE", "REVENUE",
                "NUMBER_OF_EMPLOYEES", "NUMBER_OF_LOCATIONS", "PARENT_NAME", "PARENT_PHONE", "PARENT_WEBSITE", "IS_B2B", "IS_B2C"
            ]
            
            # Get all columns in display_df
            all_cols = display_df.columns.tolist()
            
            # Create final column order: preferred first, then any remaining columns
            final_order = []
            for col in preferred_order:
                if col in all_cols:
                    final_order.append(col)
                    all_cols.remove(col)
            
            # Add any remaining columns that weren't in preferred_order
            final_order.extend(all_cols)
            
            # Apply the column order
            display_df = display_df[final_order]

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
            
            # Apply pagination to display_df
            display_df = display_df.iloc[start_idx:end_idx]
            
            # Initialize Map and SF columns with their default states for the paginated data
            page_key = f"page_{st.session_state.current_page}_size_{st.session_state.page_size}"
            display_df = display_df.copy()  # Make a copy to avoid SettingWithCopyWarning
            display_df['Map'] = True  # Default value for Map column
            display_df['SF'] = False  # Default value for SF column
            
            # Create Current Customer column based on IS_CURRENT_CUSTOMER field
            if 'IS_CURRENT_CUSTOMER' in display_df.columns:
                display_df['Current Customer'] = display_df['IS_CURRENT_CUSTOMER'].apply(
                    lambda x: "🔵" if x is True or x == True else "⚪"
                )
            else:
                display_df['Current Customer'] = "⚪"
            
            # Drop columns that are not needed for display (but keep them in full_filtered_df)
            columns_to_drop = ['LONGITUDE', 'LATITUDE', 'IS_CURRENT_CUSTOMER'] + get_hidden_columns()
            display_df = display_df.drop(columns=columns_to_drop, errors='ignore')
            
            # Format URLs to ensure they are absolute URLs
            if 'WEBSITE' in display_df.columns:
                display_df['WEBSITE'] = display_df['WEBSITE'].apply(format_url)
            if 'PARENT_WEBSITE' in display_df.columns:
                display_df['PARENT_WEBSITE'] = display_df['PARENT_WEBSITE'].apply(format_url)
            
            styled_df = display_df#.style.format(get_dataframe_format_config())
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
                        # prospect metrics get tertiary color accent
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
                
                editor_key = f"prospect_selector_{page_key}_{st.session_state.data_editor_refresh_counter}"
                
                # Display the data editor without any callbacks
                # Let Streamlit handle the state naturally
                # Pre-format REVENUE column for display with commas and dollar sign
                if "REVENUE" in display_df.columns:
                    display_df["REVENUE"] = display_df["REVENUE"].apply(lambda x: f"${int(x):,}" if pd.notnull(x) else "-")
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
                    # Process selected prospectes for map
                    selected_for_map = edited_df[edited_df['Map'] == True].copy()
                    st.session_state.selected_map_prospectes = selected_for_map
                    
                    if len(selected_for_map) > 0:
                        st.caption(f"📍 {len(selected_for_map)} prospectes selected for mapping")
                    else:
                        st.caption("📍 No prospectes selected - map will be empty")
                    
                    # Process selected prospectes for Salesforce
                    selected_for_sf = edited_df[edited_df['SF'] == True].copy()
                    
                    # Don't automatically add to Salesforce - just show what's selected
                    # The actual push will happen when the button is clicked
                    
                    if len(selected_for_sf) > 0:
                        st.caption(f"{len(selected_for_sf)} prospectes selected for salesforce")
                        
                        # Check if all selected prospects are already staged (in queue), not submitted
                        all_staged = True
                        staged_prospect_ids = [str(p.get("prospect_id")) for p in st.session_state.get("staged_prospects", []) if p.get("prospect_id")]
                        
                        for _, prospect in selected_for_sf.iterrows():
                            prospect_id = prospect.get("PROSPECT_ID") or prospect.get("IDENTIFIER")
                            prospect_id_str = str(prospect_id)
                            if prospect_id_str not in staged_prospect_ids:
                                all_staged = False
                                break
                        
                        # Create columns for left-justified button layout
                        button_col1, button_col2 = create_wide_button_layout()
                        
                        with button_col1:
                            if all_staged:
                                # Show message when all are already staged in queue
                                success_msg = f'<p style="color:#ff8c00; font-size:11px; margin:0; padding:0; font-weight:500;">⚠ All already in queue</p>'
                                st.markdown(success_msg, unsafe_allow_html=True)
                            else:
                                # Salesforce Push Button - left-justified, not full width
                                button_label = "Add Selected to Salesforce Queue"
                                
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
                                    # Check if there are multiple contacts available for this prospect
                                    top_contacts = selected_for_sf.get("TOP10_CONTACTS", {})
                                    contacts_available = []
                                    
                                    # Parse TOP10_CONTACTS - handle pandas Series
                                    has_contacts = False
                                    if hasattr(top_contacts, 'empty'):  # pandas Series
                                        has_contacts = not top_contacts.empty and not top_contacts.isna().all()
                                        if has_contacts:
                                            top_contacts = top_contacts.iloc[0] if len(top_contacts) > 0 else {}
                                    else:
                                        has_contacts = bool(top_contacts)
                                    
                                    if has_contacts:
                                        if isinstance(top_contacts, str):
                                            try:
                                                import json
                                                contacts_obj = json.loads(top_contacts)
                                            except:
                                                contacts_obj = {}
                                        else:
                                            contacts_obj = top_contacts
                                        
                                        if isinstance(contacts_obj, dict) and contacts_obj:
                                            contacts_available = list(contacts_obj.values())
                                        elif isinstance(contacts_obj, list) and contacts_obj:
                                            contacts_available = contacts_obj
                                    
                                    # Stage prospects with appropriate contact handling
                                    staged_count = 0
                                    skipped_count = 0
                                    
                                    # Use the original show_df which has all columns (before display filtering)
                                    show_df = st.session_state.get('filtered_df', None)
                                    
                                    for _, prospect_display in selected_for_sf.iterrows():
                                        # The prospect_display doesn't have PROSPECT_ID because it's hidden from display
                                        # So we need to get it from the original data using the row index
                                        display_row_index = prospect_display.name  # This is the DataFrame row index
                                        
                                        # Get the original row from show_df using the same index
                                        if show_df is not None and display_row_index in show_df.index:
                                            # Get the full row from original data which has all columns
                                            prospect = show_df.loc[display_row_index]
                                        else:
                                            # Fallback: try to find by DBA_NAME if index lookup fails
                                            company_name = prospect_display.get("DBA_NAME")
                                            if company_name and show_df is not None:
                                                matching_rows = show_df[show_df["DBA_NAME"] == company_name]
                                                if not matching_rows.empty:
                                                    prospect = matching_rows.iloc[0]
                                                else:
                                                    prospect = prospect_display
                                            else:
                                                prospect = prospect_display
                                        
                                        # For single prospect, if multiple contacts available, use the first one from TOP10_CONTACTS
                                        # Otherwise, use default main table contact
                                        selected_contact = None
                                        if len(contacts_available) > 0:
                                            # Use the first contact from TOP10_CONTACTS (highest priority)
                                            selected_contact = contacts_available[0]
                                        
                                        # Debug: Add some logging to understand what's happening
                                        prospect_id = prospect.get("PROSPECT_ID") or prospect.get("IDENTIFIER")
                                        company_name = prospect.get("DBA_NAME", "Unknown")
                                        st.write(f"DEBUG LIST VIEW: Attempting to stage '{company_name}' (ID: {prospect_id})")
                                        st.write(f"  - Prospect type: {type(prospect)}")
                                        st.write(f"  - Has TOP10_CONTACTS: {'TOP10_CONTACTS' in prospect}")
                                        
                                        staging_result = add_prospect_to_staging(prospect, selected_contact)
                                        st.write(f"  - Staging result: {staging_result}")
                                        
                                        if staging_result:
                                            staged_count += 1
                                        else:
                                            skipped_count += 1
                                    
                                    # Show confirmation message
                                    if staged_count > 0:
                                        if skipped_count > 0:
                                            st.success(f"✅ Added {staged_count} prospects to Salesforce Queue({skipped_count} already added)")
                                        else:
                                            st.success(f"✅ Added {staged_count} prospects to Salesforce Queue")
                                        if len(contacts_available) > 1:
                                            st.info("💡 Go to the Salesforce tab to review contacts and submit")
                                        else:
                                            st.info("💡 Go to the Salesforce tab to review and submit")
                                    else:
                                        st.warning("⚠️ All selected prospects are already staged")
                                    
                                    # Brief pause to show message before rerun
                                    time.sleep(1.5)
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
                    map_df, total_records, original_total = fetch_filtered_data(
                        st.session_state.active_filters, cache_key, st.session_state.page_size, 1, fetch_all=True
                    )
                    return map_df, total_records  # Only return 2 values for map

                map_df, total_records = with_loading_spinner("Fetching all data for map...", fetch_map_data)
                # Logical columns to display on the map
                logical_cols = [
                    "PROSPECT_ID", "DBA_NAME", "NUMBER_OF_EMPLOYEES", "NUMBER_OF_LOCATIONS", "REVENUE",
                    "ADDRESS", "CITY", "STATE", "ZIP", "PHONE", "WEBSITE", "PARENT_NAME", "PARENT_PHONE", "PARENT_WEBSITE",
                    "CONTACT_NAME", "CONTACT_EMAIL", "CONTACT_PHONE", "CONTACT_MOBILE", "CONTACT_JOB_TITLE", "DATA_AGG_UID", 
                    "IS_CURRENT_CUSTOMER", "TOP10_CONTACTS", "INTERNAL_DNC", "CONTACT_NATIONAL_DNC"
                ]
                # Always include lat/lon columns
                all_logical_cols = [lat_col, lon_col] + logical_cols
                # Map logical to actual column names, skipping any that are None
                logical_to_actual = {"MCC_CODE": "MCC_CODE", "B2B": "IS_B2B", "B2C": "IS_B2C"}
                actual_cols = [logical_to_actual.get(col, col) for col in all_logical_cols if col is not None]
                # Only keep columns that exist in the DataFrame
                existing_cols = [col for col in actual_cols if col in map_df.columns]
                map_data = map_df[existing_cols].dropna(subset=[lat_col, lon_col])
                map_data = map_data.rename(columns={lat_col: "lat", lon_col: "lon"})
                map_data = map_data[
                    (map_data["lat"].between(-90, 90)) &
                    (map_data["lon"].between(-180, 180))
                ]

                # Filter map data based on selected prospectes from list view
                if hasattr(st.session_state, 'selected_map_prospectes'):
                    # User has interacted with the list view checkboxes
                    if len(st.session_state.selected_map_prospectes) > 0:
                        # Some prospectes are selected - show only those
                        selected_prospect_names = st.session_state.selected_map_prospectes['DBA_NAME'].tolist()
                        map_data = map_data[map_data['DBA_NAME'].isin(selected_prospect_names)]
                    else:
                        # No prospectes selected (user unchecked all) - show empty map
                        map_data = map_data.iloc[0:0]  # Empty dataframe with same structure
                else:
                    # User hasn't interacted with list view yet - show all prospectes by default
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

                        # Build sections with proper data validation (same logic as selected prospect card)
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
                    
                    # Calculate bounds including search center location if present
                    all_lats = list(map_data["lat"])
                    all_lons = list(map_data["lon"])
                    
                    # Add search center location to bounds calculation if active
                    if "search_center_location" in st.session_state:
                        search_center = st.session_state["search_center_location"]
                        all_lats.append(search_center['latitude'])
                        all_lons.append(search_center['longitude'])
                    
                    min_lat, max_lat = min(all_lats), max(all_lats)
                    min_lon, max_lon = min(all_lons), max(all_lons)
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
                    init_session_state_key("selected_prospect_indices", [])
                    
                    # Migrate from old single selection to new multiple selection (if it exists)
                    if hasattr(st.session_state, 'selected_prospect_index') and st.session_state.selected_prospect_index is not None:
                        st.session_state.selected_prospect_indices = [st.session_state.selected_prospect_index]
                        delattr(st.session_state, 'selected_prospect_index')
                    
                    # Clean up any indices that are no longer in the data
                    st.session_state.selected_prospect_indices = [
                        idx for idx in st.session_state.selected_prospect_indices 
                        if idx in map_data.index
                    ]
                    # Sort prospectes alphabetically by name for better user experience
                    sorted_map_data = map_data.sort_values("DBA_NAME")
                    
                    # Create prospect options as list of DATA_AGG_UIDs (unique IDs)
                    prospect_options = [row["DATA_AGG_UID"] for _, row in sorted_map_data.iterrows()]
                    # Map UID to index and UID to display name (optionally with address for clarity)
                    prospect_uid_to_index = {row["DATA_AGG_UID"]: row["index"] for _, row in sorted_map_data.iterrows()}
                    # Assign a display number for each duplicate DBA_NAME
                    dba_name_counts = {}
                    prospect_uid_to_label = {}
                    for _, row in sorted_map_data.iterrows():
                        dba = row["DBA_NAME"]
                        dba_name_counts[dba] = dba_name_counts.get(dba, 0) + 1
                        display_number = dba_name_counts[dba]
                        # Only add [n] if there are duplicates
                        if list(sorted_map_data["DBA_NAME"]).count(dba) > 1:
                            label = f"{dba} [{display_number}]"
                        else:
                            label = dba
                        prospect_uid_to_label[row["DATA_AGG_UID"]] = label

                    # Get current selection for multiselect (as DATA_AGG_UIDs)
                    current_selection = []
                    if st.session_state.selected_prospect_indices:
                        for idx in st.session_state.selected_prospect_indices:
                            match = sorted_map_data.loc[sorted_map_data["index"] == idx]
                            if not match.empty:
                                current_selection.append(match.iloc[0]["DATA_AGG_UID"])

                    # Use multiselect with DATA_AGG_UID as value, DBA_NAME (and address) as display
                    selected_prospectes = st.multiselect(
                        "🔍 Search and select up to 5 prospectes to view details",
                        options=prospect_options,
                        default=current_selection,
                        key="prospect_multiselect",
                        help="Type to search and select up to 5 prospectes - alphabetically sorted",
                        max_selections=5,
                        placeholder="Type to search prospect names...",
                        format_func=lambda uid: prospect_uid_to_label.get(uid, str(uid))
                    )

                    # Handle selection logic - allow multiple selections
                    selected_indices = []
                    if selected_prospectes:
                        selected_indices = [prospect_uid_to_index[uid] for uid in selected_prospectes]
                    
                    # Show total count
                    if hasattr(st.session_state, 'selected_map_prospectes'):
                        if len(st.session_state.selected_map_prospectes) > 0:
                            st.caption(f"Showing {len(map_data)} selected prospectes on map • {len(selected_prospectes)}/5 selected")
                        else:
                            st.caption(f"No prospectes selected for mapping - map is empty • {len(selected_prospectes)}/5 selected")
                    else:
                        st.caption(f"Showing all {len(map_data)} prospectes on map (default) • {len(selected_prospectes)}/5 selected")
                    
                    # Update session state with new selections
                    if set(selected_indices) != set(st.session_state.selected_prospect_indices):
                        st.session_state.selected_prospect_indices = selected_indices
                        
                        # Calculate new map view state to encompass all selected prospectes
                        if selected_indices:
                            selected_data = map_data.loc[map_data.index.isin(selected_indices)]
                            if len(selected_indices) == 1:
                                # Single selection - zoom in close
                                single_prospect = selected_data.iloc[0]
                                st.session_state.map_view_state = {
                                    "latitude": float(single_prospect["lat"]),
                                    "longitude": float(single_prospect["lon"]),
                                    "zoom": SELECTED_prospect_ZOOM
                                }
                            else:
                                # Multiple selections - fit all prospectes in view with padding
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
                                    
                                    # Special handling for 2-3 prospect selections (more aggressive zoom-in)
                                    num_selected = len(st.session_state.selected_prospect_indices)
                                    if num_selected == 2 or num_selected == 3:
                                        # For 2-3 prospectes, use a much higher minimum zoom for closer view
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
                            # Use selected prospect color logic (existing)
                            idx = st.session_state.selected_prospect_indices.index(row['index']) if row['index'] in st.session_state.selected_prospect_indices else 0
                            selected_colors = [
                                [255, 204, 0, 200],     # Sunshine
 
                            ]
                            return selected_colors[idx % len(selected_colors)]
                        return get_non_selected_color()
                    
                    # Display selected prospect details
                    if st.session_state.selected_prospect_indices:
                        # Define colors for selected prospectes using Global Payments tertiary palette
                        selected_colors = [
                            [255, 204, 0, 200],     # Sunshine

                        ]
                        
                        selected_prospect_data = map_data.loc[map_data.index.isin(st.session_state.selected_prospect_indices)]
                        
                        
                        def format_prospect_data_html(prospect_data):
                            """Generate prospect card HTML with simplified structure"""
                            prospect_id = prospect_data.get("PROSPECT_ID") or prospect_data.get("IDENTIFIER")
                            prospect_id_str = str(prospect_id)
                            already_pushed = prospect_id_str in get_sf_prospect_ids()
                            

                            # Add INTERNAL_DNC flag if needed
                            dnc_flag = ''
                            dnc_val = prospect_data.get("INTERNAL_DNC")
                            if dnc_val == 1:
                                dnc_flag = '<span style="color:red; font-weight:bold; font-size:1.1em; margin-left:12px;">🚫 INTERNAL DNC</span>'

                            # Build header
                            sf_status = '<span class="sf-push-status">✓ Pushed to Salesforce Queue</span>' if already_pushed else ''
                            header = f'<h3><div class="prospect-name-container">{prospect_data["DBA_NAME"]}{dnc_flag}</div>{sf_status}</h3>'
                            
                            # Build sections using consolidated helper
                            sections = build_prospect_card_sections(prospect_data)
                            
                            return f'''<div class="prospect-details-card">{header}<div class="prospect-data-dashboard">{"".join(sections)}</div></div>'''
                        
                        if len(st.session_state.selected_prospect_indices) == 1:
                            # Single prospect - show full details
                            prospect_data = selected_prospect_data.iloc[0]
                            st.markdown(format_prospect_data_html(prospect_data), unsafe_allow_html=True)
                            
                            # Add native Streamlit button for Salesforce action
                            prospect_id = prospect_data.get("PROSPECT_ID") or prospect_data.get("IDENTIFIER")
                            sf_key = f"sf_push_{prospect_id}"
                            prospect_name = prospect_data.get("DBA_NAME", "")
                            
                            # Check if this prospect was already pushed to Salesforce
                            prospect_id_str = str(prospect_id)
                            already_pushed = prospect_id_str in get_sf_prospect_ids()
                            
                            if not already_pushed:
                                # Create columns for left-justified button
                                sf_cols = create_button_layout()
                                with sf_cols[0]:  # Button in left column
                                    # Updated button label with prospect name
                                    button_label = f"Add {prospect_name} to Salesforce Queue"

                                    push_button = st.button(button_label, type="primary", key=sf_key)
                                    
                                    if push_button:
                                        # Check if there are multiple contacts available for this prospect
                                        top_contacts = prospect_data.get("TOP10_CONTACTS", {})
                                        contacts_available = []
                                        
                                        # Parse TOP10_CONTACTS - handle pandas Series
                                        has_contacts = False
                                        if hasattr(top_contacts, 'empty'):  # pandas Series
                                            has_contacts = not top_contacts.empty and not top_contacts.isna().all()
                                            if has_contacts:
                                                top_contacts = top_contacts.iloc[0] if len(top_contacts) > 0 else {}
                                        else:
                                            has_contacts = bool(top_contacts)
                                        
                                        if has_contacts:
                                            if isinstance(top_contacts, str):
                                                try:
                                                    import json
                                                    contacts_obj = json.loads(top_contacts)
                                                except:
                                                    contacts_obj = {}
                                            else:
                                                contacts_obj = top_contacts
                                            
                                            if isinstance(contacts_obj, dict) and contacts_obj:
                                                contacts_available = list(contacts_obj.values())
                                            elif isinstance(contacts_obj, list) and contacts_obj:
                                                contacts_available = contacts_obj
                                        
                                        # Use the first contact from TOP10_CONTACTS if available
                                        selected_contact = None
                                        if len(contacts_available) > 0:
                                            selected_contact = contacts_available[0]
                                        
                                        # Add this prospect to staging instead of direct submission
                                        staging_result = add_prospect_to_staging(prospect_data, selected_contact)
                                        if staging_result:
                                            success_msg = f'<p style="color:#0c8a15; font-size:11px; margin:0; padding:0; font-weight:500;">✅ {prospect_name} sent to Salesforce Queue</p>'
                                            st.markdown(success_msg, unsafe_allow_html=True)
                                            if len(contacts_available) > 1:
                                                st.info("💡 Go to the Salesforce tab to review contacts and submit")
                                            else:
                                                st.info("💡 Go to the Salesforce tab to review and submit")
                                        else:
                                            warning_msg = f'<p style="color:#ff8c00; font-size:11px; margin:0; padding:0; font-weight:500;">⚠️ {prospect_name} already sent to Salesforce Queue</p>'
                                            st.markdown(warning_msg, unsafe_allow_html=True)
                                        
                                        # Log for debugging
                                        print(f"Processed prospect ID {prospect_id}: {'Successfully staged' if staging_result else 'Already staged'}")
                                        print(f"Total prospectes in Salesforce: {st.session_state.sf_pushed_count}")
                                        
                                        # Rerun to update UI
                                        st.rerun()
                        else:
                            # Multiple prospectes - show in tabs and add bulk actions
                            
                            # Check if all selected prospectes are already pushed
                            all_pushed = True
                            for idx in st.session_state.selected_prospect_indices:
                                # Get the prospect data for this index
                                prospect_row = selected_prospect_data.loc[selected_prospect_data.index == idx]
                                if not prospect_row.empty:
                                    prospect_id = prospect_row.iloc[0].get("PROSPECT_ID") or prospect_row.iloc[0].get("IDENTIFIER")
                                    if str(prospect_id) not in get_sf_prospect_ids():
                                        all_pushed = False
                                        break
                            
                            # Add compact bulk push button - left justified
                            # Create columns for left-justified button layout
                            btn_col1, btn_col2 = create_button_layout()
                            
                            with btn_col1:
                                if all_pushed:
                                    # Use markdown with custom styling - left-justified
                                    st.markdown('<p style="color:#0c8a15; font-size:11px; margin:0; padding:0; font-weight:500;">✓ Already Sent to Salesforce Queue</p>', unsafe_allow_html=True)
                                else:
                                    # Updated button label
                                    button_label = "Add Selected to Salesforce Queue"
                                    
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
                                        # Get the subset of prospectes that are selected
                                        selected_prospectes = selected_prospect_data.loc[selected_prospect_data.index.isin(st.session_state.selected_prospect_indices)].copy()
                                        
                                        # Stage prospects with appropriate contact handling
                                        staged_count = 0
                                        skipped_count = 0
                                        
                                        for _, prospect in selected_prospectes.iterrows():
                                            # Check if there are multiple contacts available for this prospect
                                            top_contacts = prospect.get("TOP10_CONTACTS", {})
                                            contacts_available = []
                                            
                                            # Parse TOP10_CONTACTS - handle pandas Series
                                            has_contacts = False
                                            if hasattr(top_contacts, 'empty'):  # pandas Series
                                                has_contacts = not top_contacts.empty and not top_contacts.isna().all()
                                                if has_contacts:
                                                    top_contacts = top_contacts.iloc[0] if len(top_contacts) > 0 else {}
                                            else:
                                                has_contacts = bool(top_contacts)
                                            
                                            if has_contacts:
                                                if isinstance(top_contacts, str):
                                                    try:
                                                        import json
                                                        contacts_obj = json.loads(top_contacts)
                                                    except:
                                                        contacts_obj = {}
                                                else:
                                                    contacts_obj = top_contacts
                                                
                                                if isinstance(contacts_obj, dict) and contacts_obj:
                                                    contacts_available = list(contacts_obj.values())
                                                elif isinstance(contacts_obj, list) and contacts_obj:
                                                    contacts_available = contacts_obj
                                            
                                            # Use the first contact from TOP10_CONTACTS if available
                                            selected_contact = None
                                            if len(contacts_available) > 0:
                                                selected_contact = contacts_available[0]
                                            
                                            if add_prospect_to_staging(prospect, selected_contact):
                                                staged_count += 1
                                            else:
                                                skipped_count += 1
                                        
                                        # Show confirmation message
                                        if staged_count > 0:
                                            if skipped_count > 0:
                                                st.success(f"✅ Added {staged_count} prospects to Salesforce Queue ({skipped_count} already added)")
                                            else:
                                                st.success(f"✅ Added {staged_count} prospects to Salesforce Queue")
                                            st.info("💡 Go to the Salesforce tab to review contacts and submit")
                                        else:
                                            st.warning("⚠️ All selected prospects are already added to Salesforce queue")
                                        
                                        # Brief pause to show message before rerun
                                        time.sleep(1.5)
                                        st.rerun()
                            
                            # Multiple prospectes - show in tabs
                            prospect_names = []
                            for idx in st.session_state.selected_prospect_indices:
                                prospect_row = selected_prospect_data.loc[selected_prospect_data.index == idx]
                                name = prospect_row["DBA_NAME"].iloc[0]
                                # Add indicator if already pushed
                                prospect_id = prospect_row.iloc[0].get("PROSPECT_ID") or prospect_row.iloc[0].get("IDENTIFIER")
                                prospect_id_str = str(prospect_id)
                                already_pushed = prospect_id_str in get_sf_prospect_ids()
                                if already_pushed:
                                    name = f"{name} ✓"
                                prospect_names.append(name)
                            
                            tab_labels = [f"📍 {name[:25]}..." if len(name) > 25 else f"📍 {name}" for name in prospect_names]
                            selected_tabs = st.tabs(tab_labels)
                            
                            for i, (tab, idx) in enumerate(zip(selected_tabs, st.session_state.selected_prospect_indices)):
                                with tab:
                                    prospect_data = selected_prospect_data.loc[selected_prospect_data.index == idx].iloc[0]
                                    st.markdown(format_prospect_data_html(prospect_data), unsafe_allow_html=True)
                                    
                                    # Add native Streamlit button for Salesforce action
                                    prospect_id = prospect_data.get("PROSPECT_ID") or prospect_data.get("IDENTIFIER")
                                    sf_key = f"sf_push_tab_{i}_{prospect_id}"
                                    prospect_name = prospect_data.get("DBA_NAME", "")
                                    
                                    # Check if this prospect was already pushed to Salesforce
                                    prospect_id_str = str(prospect_id)
                                    already_pushed = prospect_id_str in get_sf_prospect_ids()
                                    
                                    # Create a smaller, more compact layout with columns
                                    if already_pushed:
                                        # No button needed, so no columns needed
                                        pass
                                    else:
                                        # Determine column ratio based on prospect name length - more space for longer names
                                        # This helps ensure the button text stays on one line
                                        left_col_size = 1
                                        right_col_size = 1
                                        
                                        if len(prospect_name) <= 10:
                                            # Very short names
                                            left_col_size, right_col_size = 2, 1  # 2:1 ratio
                                        elif len(prospect_name) <= 20:
                                            # Medium length names
                                            left_col_size, right_col_size = 1, 1  # 1:1 ratio
                                        else:
                                            # Long names
                                            left_col_size, right_col_size = 1, 2  # 1:2 ratio
                                        
                                        # Create a dynamic layout with columns for left-justified button
                                        sf_cols = create_button_layout()
                                        with sf_cols[0]:  # Button in left column
                                            # Updated button label with prospect name
                                            button_label = f"Add {prospect_name} to Salesforce Queue"

                                            push_button = st.button(button_label, type="primary", key=sf_key)
                                            
                                            if push_button:
                                                # Check if there are multiple contacts available for this prospect
                                                top_contacts = prospect_data.get("TOP10_CONTACTS", {})
                                                contacts_available = []
                                                
                                                # Parse TOP10_CONTACTS - handle pandas Series
                                                has_contacts = False
                                                if hasattr(top_contacts, 'empty'):  # pandas Series
                                                    has_contacts = not top_contacts.empty and not top_contacts.isna().all()
                                                    if has_contacts:
                                                        top_contacts = top_contacts.iloc[0] if len(top_contacts) > 0 else {}
                                                else:
                                                    has_contacts = bool(top_contacts)
                                                
                                                if has_contacts:
                                                    if isinstance(top_contacts, str):
                                                        try:
                                                            import json
                                                            contacts_obj = json.loads(top_contacts)
                                                        except:
                                                            contacts_obj = {}
                                                    else:
                                                        contacts_obj = top_contacts
                                                    
                                                    if isinstance(contacts_obj, dict) and contacts_obj:
                                                        contacts_available = list(contacts_obj.values())
                                                    elif isinstance(contacts_obj, list) and contacts_obj:
                                                        contacts_available = contacts_obj
                                                
                                                # Use the first contact from TOP10_CONTACTS if available
                                                selected_contact = None
                                                if len(contacts_available) > 0:
                                                    selected_contact = contacts_available[0]
                                                
                                                # Add this prospect to staging instead of direct submission
                                                if add_prospect_to_staging(prospect_data, selected_contact):
                                                    success_msg = f'<p style="color:#0c8a15; font-size:11px; margin:0; padding:0; font-weight:500;">✅ {prospect_name} sent to Salesforce Queue</p>'
                                                    st.markdown(success_msg, unsafe_allow_html=True)
                                                    if len(contacts_available) > 1:
                                                        st.info("💡 Go to the Salesforce tab to review contacts and submit")
                                                    else:
                                                        st.info("💡 Go to the Salesforce tab to review and submit")
                                                else:
                                                    warning_msg = f'<p style="color:#ff8c00; font-size:11px; margin:0; padding:0; font-weight:500;">⚠️ {prospect_name} already sent to Salesforce Queue</p>'
                                                    st.markdown(warning_msg, unsafe_allow_html=True)
                        

                    
                    # Create map layers with multiple selection support
                    layers = []
                    
                    # Add search center location point if active
                    if "search_center_location" in st.session_state:
                        search_center = st.session_state["search_center_location"]
                        center_data = pd.DataFrame([{
                            'lat': search_center['latitude'],
                            'lon': search_center['longitude'],
                            'address': search_center['address'],
                            'radius_miles': search_center['radius_miles'],
                            'tooltip': f"""
                                <div style='background: linear-gradient(135deg, #262AFF 0%, #1CABFF 100%); 
                                            color: white; padding: 16px 20px; border-radius: 16px; 
                                            box-shadow: 0 8px 32px rgba(38, 42, 255, 0.25); 
                                            font-family: "DM Sans", sans-serif; min-width: 280px; max-width: 350px;'>
                                    <div style='display: flex; align-items: center; gap: 12px; margin-bottom: 12px;'>
                                        <span style='background: rgba(255, 255, 255, 0.25); width: 36px; height: 36px; 
                                                     display: inline-flex; align-items: center; justify-content: center; 
                                                     border-radius: 12px; font-size: 18px;'>🎯</span>
                                        <span style='font-weight: 700; font-size: 18px;'>Search Center</span>
                                    </div>
                                    <div style='font-size: 14px; opacity: 0.95; margin-bottom: 8px;'>
                                        <strong>Location:</strong> {search_center['address']}
                                    </div>
                                    <div style='font-size: 14px; opacity: 0.95;'>
                                        <strong>Search Radius:</strong> {search_center['radius_miles']} miles
                                    </div>
                                </div>
                            """
                        }])
                        
                        # Add search center as a distinctive layer (star/target icon style)
                        # Use same radius scaling as prospect points, with a multiplier to make it slightly larger
                        # Use appropriate scale based on whether prospectes are selected and include zoom scaling
                        if st.session_state.selected_prospect_indices:
                            # Match the same zoom-based scaling logic as selected prospectes
                            current_zoom = st.session_state.map_view_state["zoom"]
                            map_view_radius_multiplier = st.session_state.selected_radius_scale
                            if current_zoom >= 15:
                                map_view_radius_multiplier *= 1.0  # Smaller for very close zoom
                            elif current_zoom >= 13:
                                map_view_radius_multiplier *= 1.5  # Smaller for close zoom
                            elif current_zoom >= 11:
                                map_view_radius_multiplier *= 2.0  # Medium-small size
                            else:
                                map_view_radius_multiplier *= 2.5  # Reduced for far zoom
                            search_center_radius = initial_radius * map_view_radius_multiplier * 1.2  # Slightly larger than prospect points
                        else:
                            # Use same scaling as unselected prospect points
                            search_center_radius = initial_radius * st.session_state.initial_radius_scale * 1.5
                        
                        layers.append(
                            pdk.Layer(
                                "ScatterplotLayer",
                                data=center_data,
                                get_position=["lon", "lat"],
                                get_fill_color=[38, 42, 255, 220],  # Global Blue with high opacity
                                get_line_color=[255, 255, 255, 255],  # White border
                                line_width_min_pixels=4,
                                get_radius=search_center_radius,  # Scaled with other map points
                                pickable=True,
                                auto_highlight=True
                            )
                        )
                    
                    if st.session_state.selected_prospect_indices:
                        # Calculate dynamic radius based on zoom level and selection count
                        current_zoom = st.session_state.map_view_state["zoom"]
                        selection_count = len(st.session_state.selected_prospect_indices)
                        
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
                        
                        # Separate selected and non-selected prospectes
                        non_selected_data = map_data[~map_data.index.isin(st.session_state.selected_prospect_indices)]
                        
                        # Create separate radius calculation for map view selected prospectes (reduced by ~50% for better visual clarity)
                        map_view_radius_multiplier = st.session_state.selected_radius_scale
                        if current_zoom >= 15:
                            map_view_radius_multiplier *= 1.0  # Smaller for very close zoom
                        elif current_zoom >= 13:
                            map_view_radius_multiplier *= 1.5  # Smaller for close zoom
                        elif current_zoom >= 11:
                            map_view_radius_multiplier *= 2.0  # Medium-small size
                        else:
                            map_view_radius_multiplier *= 2.5  # Reduced for far zoom
                        
                        # Add non-selected prospectes layer (precompute fill_color)
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
                        
                        # Add each selected prospect as a separate layer with 3D columns/pillars
                        for i, prospect_idx in enumerate(st.session_state.selected_prospect_indices):
                            if prospect_idx in map_data.index:
                                selected_data = map_data.loc[[prospect_idx]]
                                # Use ColumnLayer for selected prospectes to make them stand out as 3D pillars
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
                        # No selection - show all prospectes, precompute fill_color
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
                    
                    #st.markdown(f"**Total prospectes Displayed:** {len(map_data)}")
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
                    # No prospect data, but check for search center location
                    if "search_center_location" in st.session_state:
                        # Show map with search center point only
                        search_center = st.session_state["search_center_location"]
                        
                        # Center map on search location
                        init_session_state_key("map_view_state", {
                            "latitude": search_center['latitude'],
                            "longitude": search_center['longitude'],
                            "zoom": 12  # Good zoom level to see the search area
                        })
                        
                        # Create data for search center point
                        center_data = pd.DataFrame([{
                            'lat': search_center['latitude'],
                            'lon': search_center['longitude'],
                            'address': search_center['address'],
                            'radius_miles': search_center['radius_miles'],
                            'tooltip': f"""
                                <div style='background: linear-gradient(135deg, #262AFF 0%, #1CABFF 100%); 
                                            color: white; padding: 16px 20px; border-radius: 16px; 
                                            box-shadow: 0 8px 32px rgba(38, 42, 255, 0.25); 
                                            font-family: "DM Sans", sans-serif; min-width: 280px; max-width: 350px;'>
                                    <div style='display: flex; align-items: center; gap: 12px; margin-bottom: 12px;'>
                                        <span style='background: rgba(255, 255, 255, 0.25); width: 36px; height: 36px; 
                                                     display: inline-flex; align-items: center; justify-content: center; 
                                                     border-radius: 12px; font-size: 18px;'>🎯</span>
                                        <span style='font-weight: 700; font-size: 18px;'>Search Center</span>
                                    </div>
                                    <div style='font-size: 14px; opacity: 0.95; margin-bottom: 8px;'>
                                        <strong>Location:</strong> {search_center['address']}
                                    </div>
                                    <div style='font-size: 14px; opacity: 0.95;'>
                                        <strong>Search Radius:</strong> {search_center['radius_miles']} miles
                                    </div>
                                </div>
                            """
                        }])
                        
                        # Create view state
                        view_state = pdk.ViewState(
                            latitude=float(search_center['latitude']),
                            longitude=float(search_center['longitude']),
                            zoom=12,
                            pitch=0
                        )
                        
                        # Create layers with search center point
                        # Use same radius scaling as prospect points for consistency
                        search_center_radius = 300 * st.session_state.initial_radius_scale
                        layers = [
                            pdk.Layer(
                                "ScatterplotLayer",
                                data=center_data,
                                get_position=["lon", "lat"],
                                get_fill_color=[38, 42, 255, 220],
                                get_line_color=[255, 255, 255, 255],
                                line_width_min_pixels=4,
                                get_radius=search_center_radius,  # Scaled with radius controls
                                pickable=True,
                                auto_highlight=True
                            )
                        ]
                        
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
                        
                        st.info(f"No prospectes found within {search_center['radius_miles']} miles of '{search_center['address']}', but showing your search center location.")
                        st.pydeck_chart(deck)
                        
                        # Add map controls for search center point size adjustment
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
                        
                        # Map controls for search center point
                        col_left, col_spacer, col_right = create_map_controls_layout()
                        with col_left:
                            col_larger, col_reset, col_smaller = create_radius_controls_layout()
                            with col_smaller:
                                if st.button(":material/remove:", key="radius_smaller_search", use_container_width=True, help="Shrink search center point"):
                                    adjust_radius_scale(0.5)
                                    st.rerun()
                            with col_larger:
                                if st.button(":material/add:", key="radius_larger_search", use_container_width=True, help="Enlarge search center point"):
                                    adjust_radius_scale(2.0)
                                    st.rerun()
                            with col_reset:
                                if st.button(":material/refresh:", key="radius_refresh_search", use_container_width=True, help="Reset search center point radius"):
                                    reset_radius_scale()
                                    st.rerun()
                        with col_right:
                            # Map style buttons arranged in single row
                            style_col1, style_col2, style_col3, style_col4 = create_map_style_buttons_layout()
                            
                            current_style = get_current_map_style()
                            
                            create_map_style_button(
                                ":material/light_mode:",
                                "map_style_light_search", 
                                "Light map style",
                                current_style,
                                style_col1
                            )
                            
                            create_map_style_button(
                                ":material/dark_mode:",
                                "map_style_dark_search",
                                "Dark map style", 
                                current_style,
                                style_col2
                            )
                            
                            create_map_style_button(
                                ":material/satellite_alt:",
                                "map_style_satellite_search",
                                "Satellite map style",
                                current_style,
                                style_col3
                            )
                            
                            create_map_style_button(
                                ":material/terrain:",
                                "map_style_terrain_search",
                                "Street map style",
                                current_style,
                                style_col4
                            )
                    else:
                        # No prospect data and no search center
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
        
        # Note: Automatic cleanup disabled to prevent removing legitimate staged prospects
        # Use manual cleanup buttons in Debug section if needed
        
        # Get staged prospects
        staged_prospects = get_staged_prospects()
        
        # Debug section - Add debugging controls
        with st.expander("🔧 Debug & Troubleshooting", expanded=False):
            st.markdown("**Session State Debug Info:**")
            if "staged_prospects" in st.session_state:
                st.write(f"Raw staged_prospects count: {len(st.session_state.staged_prospects)}")
                for i, p in enumerate(st.session_state.staged_prospects):
                    st.write(f"Prospect {i}: ID={p.get('prospect_id')}, Company={p.get('company')}")
            else:
                st.write("No staged_prospects in session state")
            
            # Show SF tracking info
            sf_prospect_ids = get_sf_prospect_ids()
            st.write(f"SF tracked prospect IDs: {len(sf_prospect_ids)} total")
            if sf_prospect_ids:
                st.write(f"IDs: {', '.join(sf_prospect_ids[:10])}{'...' if len(sf_prospect_ids) > 10 else ''}")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("🗑️ Clear All Staged Prospects", type="secondary"):
                    st.session_state.staged_prospects = []
                    st.success("Cleared all staged prospects")
                    st.rerun()
            
            with col2:
                if st.button("🧹 Clean Corrupted Data", type="secondary"):
                    cleaned_count = clean_staged_prospects()
                    if cleaned_count > 0:
                        st.success(f"Cleaned {cleaned_count} corrupted records")
                    else:
                        st.info("No corrupted records found")
                    st.rerun()
            
            with col3:
                if st.button("🔄 Reset All Session Data", type="secondary"):
                    # Clear both staged prospects and SF tracking
                    st.session_state.staged_prospects = []
                    st.session_state.sf_prospect_ids = []
                    st.session_state.sf_pushed_count = 0
                    st.success("Reset all Salesforce session data")
                    st.rerun()
        
        # Summary row
        if staged_prospects:
            st.info(f"{len(staged_prospects)} prospects selected for Salesforce lead")
        else:
            st.info("No prospects currently staged. Use the 'Add Selected to Salesforce Queue' button in List View or Map View to add prospects.")

        # Message display area (for success/error messages)
        message_container = st.container()
        
        if staged_prospects:
            # Create table data for display
            table_data = []
            for prospect in staged_prospects:
                table_data.append({
                    'Company': prospect.get('company', 'N/A'),
                    'Contact': f"{prospect.get('first_name', '')} {prospect.get('last_name', '')}".strip() or 'N/A',
                    'Email': prospect.get('email', 'N/A'),
                    'City': prospect.get('city', 'N/A'),
                    'State': prospect.get('state', 'N/A'),
                    'Phone': prospect.get('phone', 'N/A'),
                    'ID': prospect.get('prospect_id', 'N/A')
                })
                        
            # Individual prospect details sections - showing only non-null/non-N/A details
            st.subheader("Prospect Details")
            
            for i, prospect in enumerate(staged_prospects):
                prospect_id = prospect.get('prospect_id', f'prospect_{i}')
                company_name = prospect.get('company', 'Unknown Company')
                
                # Enhanced validation that includes contact selection check
                validation_result = validate_staged_prospect_enhanced(prospect)
                
                status_emoji = "✅" if validation_result['is_valid'] else "❌"
                status_text = "Valid" if validation_result['is_valid'] else "Invalid"
                status_color = "#2d7d32" if validation_result['is_valid'] else "#d32f2f"
                
                # Create expandable section for each prospect
                with st.expander(f"{status_emoji} {company_name} - {status_text}", expanded=False):
                    
                    # Helper function to check if a value is meaningful (not null, empty, or 'N/A')
                    def is_meaningful_value(value):
                        if value is None:
                            return False
                        if isinstance(value, str):
                            return value.strip() not in ['', 'N/A', 'NA', 'n/a', 'None']
                        return bool(value)
                    
                    # Contact Selection Section - NEW FEATURE
                    st.markdown("**👤 Contact Selection**")
                    
                    # Get contact data from the staged prospect (prioritize TOP10_CONTACTS only)
                    contacts_available = []
                    contact_options = {}
                    
                    # Parse TOP10_CONTACTS from the staged prospect data
                    top_contacts = prospect.get("top10_contacts")
                    
                    # Handle different data types for top_contacts
                    has_contacts = False
                    if hasattr(top_contacts, 'empty'):  # pandas Series
                        has_contacts = not top_contacts.empty and not top_contacts.isna().all()
                        if has_contacts:
                            top_contacts = top_contacts.iloc[0] if len(top_contacts) > 0 else {}
                    else:
                        has_contacts = bool(top_contacts)
                    
                    if has_contacts:
                        if isinstance(top_contacts, str):
                            try:
                                import json
                                contacts_obj = json.loads(top_contacts)
                            except:
                                contacts_obj = {}
                        else:
                            contacts_obj = top_contacts
                        
                        if isinstance(contacts_obj, dict) and contacts_obj:
                            contacts_available = list(contacts_obj.values())
                        elif isinstance(contacts_obj, list) and contacts_obj:
                            contacts_available = contacts_obj
                    
                    # If no TOP10_CONTACTS available, add default "Contact Unknown" option
                    if not contacts_available:
                        default_contact = {
                            "name": "Contact Unknown",
                            "email_address": "",
                            "direct_phone_number": "",
                            "mobile_phone": "",
                            "job_title": "Update Contact Info in Salesforce after Lead Creation",
                            "source": "default"
                        }
                        contacts_available = [default_contact]
                    
                    # Check if main table contact is already in TOP10_CONTACTS to avoid duplicates
                    def is_similar_contact(contact1, contact2):
                        """Check if two contacts are similar enough to be considered duplicates"""
                        # Safely get email addresses
                        email1 = (contact1.get("email_address") or "").strip().lower() if contact1.get("email_address") else ""
                        email2 = (contact2.get("email_address") or "").strip().lower() if contact2.get("email_address") else ""
                        if email1 and email2 and email1 == email2:
                            return True
                        
                    
                    # Create contact selection UI
                    if len(contacts_available) > 1:
                        # Multiple contacts available - show selection
                        st.info(f"📋 {len(contacts_available)} contacts available. Select which contact to use for Salesforce:")
                        
                        # Create readable options for each contact
                        contact_options = {"⚠️ Please select a contact...": -1}  # Default "no selection" option
                        for idx, contact in enumerate(contacts_available):
                            contact_name = contact.get("name", "No name")
                            contact_title = contact.get("job_title", "No title")
                            contact_email = contact.get("email_address", "No email")
                            contact_phone = contact.get("direct_phone_number", "") or contact.get("mobile_phone", "")
                            source_indicator = " (Default)" if contact.get("source") == "default" else ""
                            
                            # Create a display label
                            contact_info_parts = []
                            if contact_name and contact_name != "No name":
                                contact_info_parts.append(contact_name)
                            if contact_title and contact_title != "No title":
                                contact_info_parts.append(f"({contact_title})")
                            
                            display_label = " ".join(contact_info_parts) if contact_info_parts else f"Contact {idx + 1}"
                            display_label += source_indicator
                            contact_options[display_label] = idx
                        
                        # Get current selection (default to no selection for multiple contacts)
                        current_selection_key = f"contact_selection_{prospect_id}"
                        if current_selection_key not in st.session_state:
                            st.session_state[current_selection_key] = 0  # Default to "Please select" option
                        
                        # Ensure we have a valid integer index
                        try:
                            current_index = int(st.session_state[current_selection_key])
                            if current_index >= len(contact_options):
                                current_index = 0
                        except (ValueError, TypeError):
                            current_index = 0
                        
                        # Get the options as a list
                        contact_option_list = list(contact_options.keys())
                        
                        # Track previous selection to detect changes
                        previous_selection_key = f"prev_contact_selection_{prospect_id}"
                        previous_selection = st.session_state.get(previous_selection_key, None)
                        
                        selected_contact_idx = st.selectbox(
                            "Choose contact:",
                            options=contact_option_list,
                            index=current_index,
                            key=current_selection_key + "_selectbox",  # Different key to avoid conflicts
                            help="Select which contact information to use when submitting to Salesforce"
                        )
                        
                        # Update session state with the selected index
                        try:
                            selected_index = contact_option_list.index(selected_contact_idx)
                            
                            # Check if selection changed and force rerun for immediate validation update
                            if previous_selection != selected_index:
                                st.session_state[previous_selection_key] = selected_index
                                st.session_state[current_selection_key] = selected_index
                                st.rerun()
                            
                            st.session_state[current_selection_key] = selected_index
                            
                            # Check if a valid contact was selected
                            if contact_options[selected_contact_idx] == -1:
                                # No contact selected - record is invalid
                                selected_contact = None
                                st.warning("⚠️ Please select a contact to proceed with submission.")
                            else:
                                selected_contact = contacts_available[contact_options[selected_contact_idx]]
                        except (ValueError, IndexError):
                            # Fallback to no selection
                            st.session_state[current_selection_key] = 0
                            selected_contact = None
                        
                        # Show selected contact details only if a contact is selected
                        if selected_contact:
                            st.markdown("**Selected Contact Details:**")
                            contact_details_data = {}
                            if is_meaningful_value(selected_contact.get("name")):
                                contact_details_data["Name"] = [selected_contact["name"]]
                            if is_meaningful_value(selected_contact.get("job_title")):
                                contact_details_data["Job Title"] = [selected_contact["job_title"]]
                            if is_meaningful_value(selected_contact.get("email_address")):
                                contact_details_data["Email"] = [selected_contact["email_address"]]
                            if is_meaningful_value(selected_contact.get("direct_phone_number")):
                                contact_details_data["Direct Phone"] = [selected_contact["direct_phone_number"]]
                            if is_meaningful_value(selected_contact.get("mobile_phone")):
                                contact_details_data["Mobile Phone"] = [selected_contact["mobile_phone"]]
                            
                            if contact_details_data:
                                contact_details_df = pd.DataFrame(contact_details_data)
                                st.dataframe(contact_details_df, hide_index=True, use_container_width=True)
                            else:
                                st.warning("Selected contact has no meaningful contact information.")
                        else:
                            # No contact selected - this is expected for the "Please select" option
                            pass
                        
                        # Store the selected contact in session state for use during submission
                        st.session_state[f"selected_contact_for_{prospect_id}"] = selected_contact
                        
                    elif len(contacts_available) == 1:
                        # Only one contact available - show info and auto-select
                        contact = contacts_available[0]
                        source_text = "" if contact.get("source") == "main_table" else "from contact hierarchy"
                        st.info(f"📋 Using the only available contact {source_text}:")
                        
                        # Show contact details
                        contact_details_data = {}
                        if is_meaningful_value(contact.get("name")):
                            contact_details_data["Name"] = [contact["name"]]
                        if is_meaningful_value(contact.get("job_title")):
                            contact_details_data["Job Title"] = [contact["job_title"]]
                        if is_meaningful_value(contact.get("email_address")):
                            contact_details_data["Email"] = [contact["email_address"]]
                        if is_meaningful_value(contact.get("direct_phone_number")):
                            contact_details_data["Direct Phone"] = [contact["direct_phone_number"]]
                        if is_meaningful_value(contact.get("mobile_phone")):
                            contact_details_data["Mobile Phone"] = [contact["mobile_phone"]]
                        
                        if contact_details_data:
                            contact_details_df = pd.DataFrame(contact_details_data)
                            st.dataframe(contact_details_df, hide_index=True, use_container_width=True)
                        else:
                            st.warning("Available contact has no meaningful contact information.")
                        
                        # Store the contact for submission
                        st.session_state[f"selected_contact_for_{prospect_id}"] = contact
                        
                    else:
                        # No contacts available
                        st.warning("📋 No contact information available for this prospect.")
                        st.session_state[f"selected_contact_for_{prospect_id}"] = None
                    
                    
                    # Collect all meaningful details
                    meaningful_details = []
                    
                    # Company Information
                    company_details = []
                    if is_meaningful_value(prospect.get('company')):
                        company_details.append(f"**Company:** {prospect['company']}")
                    if is_meaningful_value(prospect.get('industry')):
                        company_details.append(f"**Industry:** {prospect['industry']}")
                    if is_meaningful_value(prospect.get('revenue')):
                        company_details.append(f"**Revenue:** {prospect['revenue']}")
                    if is_meaningful_value(prospect.get('employees')):
                        company_details.append(f"**Employees:** {prospect['employees']}")
                    if is_meaningful_value(prospect.get('website')):
                        company_details.append(f"**Website:** {prospect['website']}")
                    if is_meaningful_value(prospect.get('mcc_code')):
                        company_details.append(f"**MCC Code:** {prospect['mcc_code']}")
                    
                    # Location Information
                    location_details = []
                    if is_meaningful_value(prospect.get('address')):
                        location_details.append(f"**Address:** {prospect['address']}")
                    if is_meaningful_value(prospect.get('city')):
                        location_details.append(f"**City:** {prospect['city']}")
                    if is_meaningful_value(prospect.get('state')):
                        location_details.append(f"**State:** {prospect['state']}")
                    if is_meaningful_value(prospect.get('zip')):
                        location_details.append(f"**ZIP:** {prospect['zip']}")
                    
                    # Display details in a clean table format organized by sections
                    if any([company_details, location_details]):
                        # Company Information Table
                        if company_details:
                            st.markdown("**🏢 Company Information**")
                            company_data = {}
                            for detail in company_details:
                                parts = detail.split(":** ", 1)
                                if len(parts) == 2:
                                    field_name = parts[0].replace("**", "")
                                    field_value = parts[1]
                                    company_data[field_name] = [field_value]
                            company_df = pd.DataFrame(company_data)
                            st.dataframe(company_df, hide_index=True, use_container_width=True)
                            st.markdown("")
                        # Location Information Table
                        if location_details:
                            st.markdown("**📍 Location Information**")
                            location_data = {}
                            for detail in location_details:
                                parts = detail.split(":** ", 1)
                                if len(parts) == 2:
                                    field_name = parts[0].replace("**", "")
                                    field_value = parts[1]
                                    location_data[field_name] = [field_value]
                            location_df = pd.DataFrame(location_data)
                            st.dataframe(location_df, hide_index=True, use_container_width=True)
                    else:
                        st.info("No additional details available for this prospect.")
                    # Show validation errors if invalid
                    if not validation_result['is_valid']:
                        st.error(f"**Missing Required Fields:** {', '.join(validation_result['missing_fields'])}")
                    # Action buttons for this prospect
                    col_remove, col_spacer = st.columns([1, 3])
                    with col_remove:
                        if st.button(f"🗑️ Remove", key=f"remove_{prospect_id}", type="secondary"):
                            remove_prospect_from_staging(prospect_id)
                            with message_container:
                                st.success(f"✅ Removed {company_name}")
                            st.rerun()

    
            # Bulk action buttons with proper alignment
            st.markdown("---")
            st.subheader("Actions")
            
            # Create custom CSS for aligned buttons and indicator
            st.markdown("""
            <style>
            .bulk-actions-container {
                display: flex;
                align-items: flex-start;
                gap: 15px;
                margin: 20px 0;
            }
            .action-button-container {
                flex: 0 0 auto;
            }
            .validation-indicator-container {
                flex: 1;
                margin-left: auto;
                display: flex;
                justify-content: flex-end;
            }
            .validation-indicator {
                background: linear-gradient(135deg, #f8faff 0%, #ffffff 100%);
                border: 1px solid #e6e9f3;
                border-radius: 12px;
                padding: 20px;
                text-align: center;
                height: 48px;
                display: flex;
                align-items: center;
                justify-content: center;
                min-width: 160px;
                box-shadow: 0 2px 8px rgba(38, 42, 255, 0.08);
            }
            </style>
            """, unsafe_allow_html=True)
            
            # Create columns with proper spacing: 2 narrow for buttons, 1 wide for indicator
            col1, col2, col_spacer, col3 = st.columns([1, 1, 0.5, 1.5])
            
            with col1:
                if st.button("Submit All to Salesforce", type="primary", key="submit_all_staged", use_container_width=True):
                    # Validate all prospects before submission
                    validation_errors = []
                    for i, prospect in enumerate(staged_prospects):
                        validation_result = validate_staged_prospect(prospect)
                        if not validation_result['is_valid']:
                            validation_errors.append(f"Prospect {i+1}: {', '.join(validation_result['errors'])}")
                    
                    if validation_errors:
                        with message_container:
                            st.error(f"❌ Cannot submit - Validation errors found:\n" + "\n".join(validation_errors))
                    else:
                        # Submit all prospects
                        with st.spinner("Submitting prospects to Salesforce..."):
                            try:
                                result = submit_staged_prospects_to_sf()
                                if result['success']:
                                    # Clear staging after successful submission
                                    clear_staging()
                                    with message_container:
                                        st.success(f"✅ Successfully submitted {result['success_count']} prospects to Salesforce!")
                                        if result.get('duplicate_count', 0) > 0:
                                            st.warning(f"⚠️ {result['duplicate_count']} prospects were duplicates and skipped")
                                        if result.get('error_count', 0) > 0:
                                            st.error(f"❌ {result['error_count']} prospects failed to submit")
                                    st.rerun()
                                else:
                                    with message_container:
                                        # Show more specific error for duplicate
                                        if result.get('duplicate_count', 0) > 0:
                                            st.error("❌ Submission failed: One or more records already exist (duplicate).")
                                        else:
                                            error_message = result.get('message', 'Unknown error')
                                            st.error(f"❌ Submission failed: {error_message}")
                                            
                                            # Show detailed error information if available
                                            if result.get('results'):
                                                error_details = []
                                                for res in result['results']:
                                                    if res.get('status') == 'error':
                                                        error_details.append(f"• {res.get('prospect', 'Unknown')}: {res.get('message', 'No details')}")
                                                
                                                if error_details:
                                                    st.error("Error details:\n" + "\n".join(error_details[:5]))  # Show first 5 errors
                                                    if len(error_details) > 5:
                                                        st.error(f"... and {len(error_details) - 5} more errors")
                            except Exception as e:
                                with message_container:
                                    st.error(f"❌ Submission failed: {str(e)}")
            
            with col2:
                if st.button("🗑️ Clear All", type="secondary", key="clear_all_staging", use_container_width=True):
                    clear_staging()
                    with message_container:
                        st.success("✅ Cleared successfully")
                    st.rerun()
            
            with col3:
                # Field validation summary with button-matching height
                total_prospects = len(staged_prospects)
                valid_prospects = 0
                for prospect in staged_prospects:
                    if validate_staged_prospect_enhanced(prospect)['is_valid']:
                        valid_prospects += 1
                
                # Display validation summary with visual indicators matching button height
                st.markdown(f"""
                <div class="validation-indicator">
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <div style="font-size: 20px; font-weight: 700; color: #262aff;">
                            {valid_prospects}/{total_prospects}
                        </div>
                        <div style="font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; line-height: 1.2;">
                            Valid<br>Prospects
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
    
if __name__ == "__main__":
    main()
