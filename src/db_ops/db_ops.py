from typing import Any
from .db_connector import DBConnector, PosgreConnector, CassandraConnector
from .db_query_gen import CassandraQueryGenerator, PosgreQueryGenerator
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
# from cassandra.cluster import ResultSet
from abc import ABC, abstractmethod


class DBOperator(ABC):
    """DBOperator is the abstract class for database operator classes in this module.
        Contains must have abstract methods and attributes
    """
    def __init__(self, config:dict, data_loader: DBConnector):
        """Default Constructor
        Args:
            config (dict): config contain credentials and required parameters to connect to the remote database.
            data_loader (DBConnector): DBConnector is the abstract class for database connector classes. 
        """
        self.config = config
        self.data_loader = data_loader(config=self.config)
    
    @abstractmethod
    def __call__(self):
        """The __call__ method enables Python programmers to write classes where the instances 
        behave like functions and can be called like a function.

        Returns:
            DBOperator: Returns a DBOperator object when a class instance is called.
        """
        return self.data_loader
    @abstractmethod
    def get_data(self):
        """Abstract method to fetch data from databases
        """
        pass

    
class CassOps(DBOperator):
    """CassOps class contains required methods and attributes to perform database operations and execute CQL queries.

    Args:
        DBOperator (ABC): DBOperator is the base abstract class for database operator classes in this module.
    """
    def __init__(self, config, data_loader: CassandraConnector=CassandraConnector):
        """Default Constructor

        Args:
            config (Any): config contain credentials and required parameters to connect to the cluster
            data_loader (CassandraConnector, optional): CassandraConnector class ensures the connection between local source and remote Cassandra Clusters 
            and enables to perform db operation.. Defaults to CassandraConnector.
        """
        super().__init__(config=config, data_loader=data_loader)
        
    def __call__(self):
        """The __call__ method enables Python programmers to write classes 
        where the instances behave like functions and can be called like a function.

        Returns:
            CassOps: Returns CassOps object when class instance is called.
        """
        return super().__call__()
    
    def get_data(self,start_date: date=None,end_date: date=None,
                    sensor_id: int=None,size: int=None, partition: bool=True)-> pd.DataFrame:  
        """Fetches the dataset from the cassandra cluster and returns pd.DataFrame Object

        Args:
            start_date (date, optional): datetime.Date object refers to the starting date of the time interval. Defaults to None.
            end_date (date, optional): datetime.Date object refers to the ending date of the time interval.. Defaults to None.
            sensor_id (int, optional): ID of the target sensor . Defaults to None.
            size (int, optional): Size of the Dataset to be fetched . Defaults to 5000.

        Returns:
            DataFrame: Returns dataframe object contains columns with sensor_id, filter_time, sensor_time, state_tag
        """
        query = CassandraQueryGenerator(start_date=start_date,
                                end_date=end_date,
                                sensor_id=sensor_id,
                                size=size,
                                partition=partition
                                ).query
        # Fetch the data for given query
        sensor_data = self.data_loader._execute(query)
        # Converts a dse.cluster.ResultsSet object into pd.DataFrame
        df = self._convert_to_df(sensor_data)
        return df
    
    
    def get_daily_data(self, sensor_id: int=None, size: int=None, today: datetime=datetime.now())-> pd.DataFrame:
        end_date = today
        start_date = end_date - timedelta(1)
    
        df = self.get_data(start_date=start_date, end_date=end_date, 
                        sensor_id=sensor_id, size=size, partition=False)
        return df
    
    def get_monthly_data(self, sensor_id: int=None, size: int=None, today: datetime=datetime.now())-> pd.DataFrame:
        end_date = today
        start_date = end_date - timedelta(1)
    
        df = self.get_data(start_date=start_date, end_date=end_date, 
                        sensor_id=sensor_id, size=size) 
        return df
    
    # def _convert_to_df(self,sensor_data: ResultSet)-> pd.DataFrame:
    #     """Converts a dse.cluster.ResultsSet object into pd.DataFrame object

    #     Args:
    #         sensor_data (ResultSet): Contains fetched data from the cluster

    #     Returns:
    #         DataFrame: pd.Dataframe object that contains fetched rows from the cluster
    #     """
    #     return DataFrame(list(sensor_data))
    
    def shutdown(self):
        # Shutdown the connetion to the cluster
        self.data_loader._close()
        pass
    
    
class PosgreOps(DBOperator):
    """PosgreOps class contains required methods and attributes to perform database operations and execute NoSQL queries. 

    Args:
        DBOperator (ABC): DBOperator is the base abstract class for database operator classes in this module.
    """
    def __init__(self, config, data_loader: PosgreConnector=PosgreConnector):
        """Default Constructor

        Args:
            config (Any): config contain credentials and required parameters to connect to the cluster
            data_loader (PosgreConnector, optional): _description_. Defaults to PosgreConnector.
        """
        super().__init__(config=config, data_loader=data_loader)
        
    def __call__(self):
        """__call__ method enables Python programmers to write classes where the instances 
        behave like functions and can be called like a function.
        Returns:
            PosgreOps: Returns a PosgreOps object when a class instance is called.
        """
        return super().__call__()
    
    def get_data(self, start_date:str, end_date: str,
                    table_name: str=None, size: int=None)-> pd.DataFrame:
        """get_data method obtaines a query for given inputs from PosGreQueryGenerator class 
        and executes the query in connected NoSQL database.

        Args:
            start_date (Any): Refers to the starting date of the time interval.
            end_date (str): Refers to the ending date of the time interval. Defaults to None.
            table_name (str): Name of the table of interest in the database. Defaults to None.
            size (int): Indicates the number of records to be fetched. Defaults to None.

        Returns:
            pd.DataFrame: Returns pd.DataFrame object contains the records of interest provided in auto generated query.
        """
        query = PosgreQueryGenerator(start_date=start_date,
                                end_date=end_date,
                                table_name=table_name,
                                size=size,
                                ).query
        cursor = self.data_loader.cursor()
        cursor.execute(query)
        # Get column names
        colnames = [desc[0] for desc in cursor.description]
        # Get data
        data = cursor.fetchall()
        df = self._convert_to_df(data=data, colnames=colnames)
        return df
    
    def get_monthly_data(self, table_name: str=None, size: int=None, today: date=date.today(), months: int=1)-> pd.DataFrame:
        """get_monthly_data method modifies input parameters to fetch data and invokes get_data method with that parameters.
        

        Args:
            table_name (str): Name of the table of interest in the database. Defaults to None.
            size (int): Indicates the number of records to be fetched. Defaults to None.
            today (date, optional): Corresponds to date of today, this parameter is utilized to set end_date. Defaults to date.today().
            months (int, optional): Corresponds to the number of months towards past, in order to determine the start date. Defaults to 1.

        Returns:
            pd.DataFrame: Returns pd.DataFrame object contains the records of interest provided in auto generated query.
        """
        end_date = today
        start_date = end_date - relativedelta(months=months)
        if table_name in ("daas.epdk_petrol_province"): 
            end_date = end_date.strftime("%Y-01-%m")
            start_date = start_date.strftime("%Y-01-%m")
        else: 
            end_date = end_date.strftime("%Y-%m-%d")
            start_date = start_date.strftime("%Y-%m-%d")
                
        df = self.get_data(start_date=start_date, end_date=end_date, 
                        table_name=table_name, size=size) 
        return df
        
    def _convert_to_df(self, data: list, colnames: list=None)-> pd.DataFrame:
        """Converts a list into pd.DataFrame object with given column names.

        Args:
            data (list): Contains fetched data from the remote database
            colnames (list, optional): Contains name of columns. Defaults to None.

        Returns:
            pd.DataFrame: pd.Dataframe object that contains fetched data from the remote database
        """
        return pd.DataFrame(data, columns=colnames)    
    
    def shutdown(self):
        """Shutdown the connetion to the remote database
        """
        self.data_loader._close()
        pass    