# from cassandra.cluster import Cluster, ResultSet
# from cassandra.auth import PlainTextAuthProvider
from abc import ABC, abstractmethod
from src.utils.parse_yaml import parse_yaml
import psycopg2

class DBConnector(ABC):
    """DBConnector is the abstract class for database connector classes in this module.
    Contains must have abstract methods and attributes
    """
    def __init__(self, config: dict, db_type: str):
        """Default Constructor

        Args:
            config (dict): config contain credentials and required parameters to connect to the remote database
            db_type (str): db_type indicates the database type to be connected
        """
        db_path = config["dataload"][db_type]['path']
        self.config = config
        self.credentials = parse_yaml(db_path)
        self._connection = self._connect()
        
        
    @abstractmethod
    def _connect(self):
        """Abstract method to connect databases
        """
        pass
    @abstractmethod
    def __call__(self, *args, **kwargs):
        """The __call__ method enables Python programmers to write classes where the instances 
        behave like functions and can be called like a function.
        Returns:
            connector_object: type of this object depends on the database type 
        """
        return self._connection
    
    @abstractmethod
    def cursor(self):
        """Abstract method to execute queries
        """
        pass
    
    @abstractmethod
    def _close(self):
        """Abstract method to close the connections
        """
        pass
    
    
class CassandraConnector(DBConnector):
    """CassandraConnector class ensures the connection between local source and remote Cassandra Clusters 
    and enables to perform db operation.
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
        """The __call__ method enables Python programmers to write classes 
        where the instances behave like functions and can be called like a function.

        Returns:
            dse.Cassandra.Cluster: returns the Cluster object when invoked,
            which ensures the connection between local source and remote database and enables to perform database operations. 
        """
        return super().__call__(*args,**kwargs)
    
    def _connect(self):
        """This method establishes the connection between local source and the remote Cassandra Cluster. 

        Returns: cassandra.cluster.Cluster object when called. This object represents the connection and db operations 
        can be performed through this object.
        """
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

    def _execute(self, query: str):
        """This method is utilized in order to execute queries or perform db operations on remote Cassandra clusters

        Args:
            query (str): CQL style query to be executed

        Returns:
            cassandra.Cluster.ResultSet: Returns response to the executed query as an ResultSet object
        """
        sensor_data = self._connection.execute(query)
        return sensor_data
    
    @DeprecationWarning
    def cursor(self):
        """Cassandra does support execute method instead generic cursor method. 
        Therefore, this method is deprecated for this case.
        """
        pass
    
    def _close(self):
        """Terminates the connection with the Cassandra Cluster.
        """
        self._connection.shutdown()
        pass    


class PosgreConnector(DBConnector):
    """PosgreConnector class ensures the connection between local source and remote NoSQL databases 
    and enables to perform db operation.

    Args:
        DBConnector (ABC): Abstract class for database connector classes in this module.
    """
    def __init__(self, config: dict, db_type: str='daas'):
        """Default Constructor

        Args:
            config (dict): config contain credentials and required parameters to connect DAAS 
            db_type (str): db_type indicates the database type to be connected. Defaults to 'daas'.
        """
        super().__init__(config=config,db_type=db_type)        
        
    def __call__(self, *args, **kwargs):
        """The built-in __call__ method enables Python programmers to write classes 
        where the instances behave like functions and can be called like a function.

        Returns: pyscopg2.connection object when class instance is called,
            which ensures the connection between local source and remote database and enables to perform database operations. 
        """
        return super().__call__(*args,**kwargs)
    
    def _connect(self):
        """This method establishes the connection between local source and the remote NoSQL database. 

        Returns: pyscopg2.connection object when invoked,
            which ensures the connection between local source and remote database and enables to perform database operations.
        """
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
        """This method is utilized in order to execute queries or perform db operations on remote NoSQL databases

        Returns: NoSQL cursor object 
        """
        return self._connection.cursor()    
    
    def _close(self):
        """Terminates the connection with the DAAS database
        """
        self._connection.close()
        pass