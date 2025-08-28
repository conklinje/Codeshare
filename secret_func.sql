CREATE OR REPLACE FUNCTION sandbox.conklin.get_mulesoft_secret(secret_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'get_secret_function4'
EXTERNAL_ACCESS_INTEGRATIONS = (MULESOFT_DEV_APIS_ACCESS_INTEGRATION)
SECRETS = (
    'MULESOFT_DEV_CLIENT_ID' = PYTHON_WORKLOADS.MULESOFT_DEV.MULESOFT_DEV_CLIENT_ID,
    'MULESOFT_DEV_CLIENT_SECRET' = PYTHON_WORKLOADS.MULESOFT_DEV.MULESOFT_DEV_CLIENT_SECRET,
    'MULESOFT_DEV_API_KEY' = PYTHON_WORKLOADS.MULESOFT_DEV.MULESOFT_DEV_API_KEY
)
AS
$$
import _snowflake
def get_secret_function4(secret_name):
    """
    Retrieve MuleSoft credentials from Snowflake secrets

    Args:
        secret_name (str): Secret name to retrieve
            'MULESOFT_DEV_CLIENT_ID'
            'MULESOFT_DEV_CLIENT_SECRET'
            'MULESOFT_DEV_API_KEY'

    Returns:
        str: Secret value or error message
    """
    try:
        secret_name = secret_name.upper().strip()
        secret_mapping = {
            'MULESOFT_DEV_CLIENT_ID': 'mulesoft_dev_client_id',
            'MULESOFT_DEV_CLIENT_SECRET': 'mulesoft_dev_client_secret',
            'MULESOFT_DEV_API_KEY': 'mulesoft_dev_api_key'
        }
        if secret_name not in secret_mapping:
            available_secrets = ', '.join(secret_mapping.keys())
            return f"ERROR: Invalid secret name '{secret_name}'. Available: {available_secrets}"
        internal_secret_name = secret_mapping[secret_name]
        secret_value = _snowflake.get_generic_secret_string(internal_secret_name)
        if secret_value is None or secret_value.strip() == '':
            return f"ERROR: Secret '{secret_name}' is empty or not found"
        return secret_value.strip()
    except Exception as e:
        return f"ERROR: Failed to retrieve secret '{secret_name}': {str(e)}"
$$;


select sandbox.conklin.get_mulesoft_secret('MULESOFT_DEV_API_KEY') as mulesoft_dev_api_key;