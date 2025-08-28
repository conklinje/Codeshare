-- Updated Snowflake UDF that handles both addresses and ZIP codes
CREATE OR REPLACE FUNCTION PYTHON_WORKLOADS.DATA_ENGINEERING.GEOCODE_ADDRESS("ADDRESS" VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('requests')
HANDLER = 'get_location'
EXTERNAL_ACCESS_INTEGRATIONS = (AZ_MAPS_APIS_ACCESS_INTEGRATION)
SECRETS = ('SUBSCRIPTION_KEY'=SUBSCRIPTION_KEY)
AS '
import _snowflake
import requests
import json
import re

def get_location(address):
    # Retrieve the subscription key from the secret
    subscription_key = _snowflake.get_generic_secret_string(''SUBSCRIPTION_KEY'')
    
    # Clean the input
    address = address.strip()
    
    # Check if input is a ZIP code (5 digits or 5+4 format)
    zip_pattern = re.match(r''^(\d{5})(?:-\d{4})?$'', address)
    
    if zip_pattern:
        # For ZIP codes, add "USA" to improve Azure Maps results
        query = f"{address}, USA"
        # Use postalCode parameter for better ZIP code handling
        url = f"https://atlas.microsoft.com/search/address/json?api-version=1.0&subscription-key={subscription_key}&query={query}&countrySet=US&limit=1"
    else:
        # For full addresses, use as-is but add USA if not present
        if "USA" not in address.upper() and "US" not in address.upper():
            query = f"{address}, USA"
        else:
            query = address
        url = f"https://atlas.microsoft.com/search/address/json?api-version=1.0&subscription-key={subscription_key}&query={query}&countrySet=US&limit=1"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        result = response.json()
        
        if result.get(''results'') and len(result[''results'']) > 0:
            coordinates = result[''results''][0][''position'']
            return {"latitude": coordinates[''lat''], "longitude": coordinates[''lon'']}
        else:
            return {"error": "Could not find this address. Please try a different search term."}
    except Exception as e:
        return {"error": f"Geocoding service error: {str(e)}"}
';
