from utils import SnowFlakeConnector
import os

# Vault address and token (adjust these as needed)
vault_addr = "http://127.0.0.1:8200"  # Your Vault server address
vault_token = os.getenv('VAULT_TOKEN')

# Path where the Snowflake credentials (user and password) are stored in Vault
secret_path = "snowflake_password"  # Modify to match your secret path in Vault

# Snowflake connection details
account = 'FGJNKRP-HQ27383'
database = 'SNOWFLAKE_SAMPLE_DATA'
schema = 'TPCH_SF1'
warehouse = 'COMPUTE_WH'
role = 'ACCOUNTADMIN'

# Create an instance of the SnowFlakeConnector
sf_connector = SnowFlakeConnector(
    account=account,
    secret_path=secret_path,  # Path to the secret in Vault containing both user and password
    database=database,
    schema=schema,
    warehouse=warehouse,
    role=role,
    vault_addr=vault_addr,
    vault_token=vault_token
)

# Connect to Snowflake and execute the query
sf_connector.connector()

# Query to execute
query = 'SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER LIMIT 10'

# Execute the query and print the result
result = sf_connector.execute(query)
print(result)

# Close the connection
sf_connector.close_connection()
