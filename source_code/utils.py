import snowflake.connector
import hvac

class SnowFlakeConnector:
    def __init__(self, account, secret_path, database, schema, warehouse, role, vault_addr, vault_token):
        self.account = account
        self.secret_path = secret_path  # Path where both user and password are stored in Vault
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self.vault_addr = vault_addr  # Vault address
        self.vault_token = vault_token  # Vault token

    def get_credentials_from_vault(self):
        # Initialize Vault client
        client = hvac.Client(url=self.vault_addr, token=self.vault_token)

        # Read the secret from the specified path
        secret = client.secrets.kv.read_secret_version(path=self.secret_path)

        # Print the secret structure for debugging (optional)
        # print(secret)

        # Return both user and password from the secret (adjust the keys based on your Vault structure)
        user = secret['data']['data']['user']  # Adjust key for user in Vault secret
        password = secret['data']['data']['password']  # Adjust key for password in Vault secret
        return user, password

    def connector(self):
        # Fetch user and password from Vault
        self.user, self.password = self.get_credentials_from_vault()

        # Create Snowflake connection using the user and password from Vault
        self.connection = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
            role=self.role        
        )
        self.cursor = self.connection.cursor()

    def execute(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
