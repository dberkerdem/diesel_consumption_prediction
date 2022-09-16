# from cassandra.cluster import Cluster, ResultSet
# from cassandra.auth import PlainTextAuthProvider
from abc import ABC, abstractmethod
from src.utils.parse_yaml import parse_yaml
import psycopg2

class DBConnector(ABC):
    def __init__(self, config: dict, db_type: str):
        db_path = config["dataload"][db_type]['path']
        self.config = config
        self.credentials = parse_yaml(db_path)
        self._connection = self._connect()
        
        
    @abstractmethod
    def _connect(self):
        pass

    def __call__(self, *args, **kwargs):
        return self._connection
    @abstractmethod
    def cursor(self):
        pass
    
    @abstractmethod
    def _close(self):
        pass
    
    
class CassandraConnector(DBConnector):
    """CassandraDataLoader loads data from the cluster and returns a pd.DataFrame object with get_dataset(**args) method
    """
    def __init__(self, config: dict, db_type: str="cassandra"):
        """Default Constructor

        Args:
            config (dict): config contain credentials and required parameters to connect to the cluster

        Raises:
            e: Exception 
        """
        super().__init__(config=config,db_type=db_type)
        
    def __call__(self, *args, **kwargs):
        return self._connection()
    
    def _connect(self):
        username = self.credentials['dataload']['cassandra']['username']
        password = self.credentials['dataload']['cassandra']['password']
        contact_points = self.credentials['dataload']['cassandra']['contact_points']
        request_timeout = self.credentials['dataload']['cassandra']['request_timeout']
        keyspace = self.credentials['dataload']['cassandra']['keyspace']
        # Create and Initialize the Cluster
        auth_provider = PlainTextAuthProvider(username=username, password=password)
        cluster = Cluster(contact_points, auth_provider=auth_provider)
        cluster.request_timeout = request_timeout
        # Establish connection to the database
        session = cluster.connect()
        session.set_keyspace(keyspace)
        return session
    
    # def _execute(self, query: str)-> ResultSet:
    #     sensor_data = self._connection.execute(query)
        return sensor_data
    
    def cursor(self):
        pass
    
    def _close(self):
        self._connection.shutdown()
        pass    


class PosgreConnector(DBConnector):
    def __init__(self,config, db_type: str='daas'):
        super().__init__(config=config,db_type=db_type)        
        
    def __call__(self, *args, **kwargs):
        return self._connection()
    
    def _connect(self):
        contact_points = self.credentials['dataload']['daas']['contact_points']
        database = self.credentials['dataload']['daas']['database']
        username = self.credentials['dataload']['daas']['username']
        password = self.credentials['dataload']['daas']['password']
        # Establish connection to the database
        conn = psycopg2.connect(
            host=contact_points,
            database=database,
            user=username,
            password=password)
        return conn
        
    def cursor(self):
        return self._connection.cursor()    
    
    def _close(self):
        self._connection.close()
        pass