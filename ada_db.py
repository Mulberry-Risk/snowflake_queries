import snowflake.connector as db
import logging 
logger = logging.getLogger(__name__)
from decouple import config

class SnowflakeSession():
    """Create and work in a Snowflake Session, on a single database.
    
    Params:
        client (str): Client name for the database that is to be worked on
        
    """
    def __init__(self, client, schema='PUBLIC', warehouse=None, role=None):
        self.client = client.replace('-', '_').upper()
        self.database = f'{self.client}_DATA'
        self.schema = schema
        self.warehouse = warehouse if warehouse else f'BORDEREAUX_ELT'
        self.role = role if role else f'{self.client}_ELT'
        self.user = config('SF_USER')
        self.password = config('SF_PASSWORD')
        self.account = 'ys27644.west-europe.azure'
        self.timeout=360

    def _get_transaction(self):
        """Return a Connection object for `client` Ada database 

        Returns:
            Connection object: A connection object that conforms to PEP 249 – Python Database API Specification v2.0
                https://peps.python.org/pep-0249/#connection
        """

        return db.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            role=self.role,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            autocommit=False
        )
