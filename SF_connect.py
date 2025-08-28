# Import python packages
import streamlit as st
import pandas as pd

# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session
session = get_active_session()

Client_secret_query = "select python_workloads.data_engineering.get_mulesoft_secrets('MULESOFT_DEV_CLIENT_SECRET') as Client_Secret"
Client_secret = session.sql(Client_secret_query).collect()[0][0]

Client_id_query = "select python_workloads.data_engineering.get_mulesoft_secrets('MULESOFT_DEV_CLIENT_ID') as Client_ID"
Client_id = session.sql(Client_secret_query).collect()[0][0]

API_Key_query = "select python_workloads.data_engineering.get_mulesoft_secrets('MULESOFT_DEV_API_KEY') as API_Key"
API_Key = session.sql(API_Key_query).collect()[0][0]

token_url = 'https://dev.services.globalpayments.com/token-issuer-api-dev/api/v1/crm/token' 

import requests

# Use your credentials from above
client_id = Client_id
client_secret = Client_secret
api_key = API_Key
token_url = 'https://dev.services.globalpayments.com/token-issuer-api-dev/api/v1/crm/token'

# Prepare the payload and headers
payload = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret
}
headers = {
    'Content-Type': 'application/json',
    'x-api-key': api_key
}

# Make the request
response = requests.get(token_url, params=payload, headers=headers)

# Print the result
print("Status Code:", response.status_code)
print("Response:", response.json())