from typing import Any
from .db_connector import DBConnector, PosgreConnector, CassandraConnector
from .db_query_gen import CassandraQueryGenerator, PosgreQueryGenerator
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
# from cassandra.cluster import ResultSet
from abc import ABC, abstractmethod


class DBOperator(ABC):
    def __init__(self, config, data_loader: DBConnector):
        self.config = config
        self.data_loader = data_loader(config=self.config)
    
    @abstractmethod
    def __call__(self):
        return self.data_loader
    @abstractmethod
    def get_data(self):
        pass

    
class CassOps(DBOperator):
    def __init__(self, config, data_loader: CassandraConnector=CassandraConnector):
        super().__init__(config=config, data_loader=data_loader)
        
    def __call__(self):
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
        # Converts a dse.cluster.ResultsSet object into DataFrame
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
    
    # def _convert_to_df(self,sensor_data: ResultSet)-> DataFrame:
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
    def __init__(self, config, data_loader: PosgreConnector=PosgreConnector):
        super().__init__(config=config, data_loader=data_loader)
        
    def __call__(self):
        return super().__call__()
    
    def get_data(self, start_date, end_date: str=None,
                    table_name: str=None, size: int=None)-> pd.DataFrame:
        
        query = PosgreQueryGenerator(start_date=start_date,
                                end_date=end_date,
                                table_name=table_name,
                                # size=size,
                                ).query
        cursor = self.data_loader.cursor()
        cursor.execute(query)
        # Get column names
        colnames = [desc[0] for desc in cursor.description]
        # Get data
        data = cursor.fetchall()
        df = self._convert_to_df(data=data, colnames=colnames)
        return df

    def get_daily_data(self, table_name: str=None, size: int=None, today: date=date.today())-> pd.DataFrame:
        end_date = today
        start_date = end_date - timedelta(1)
        
        # TODO: dtype handler monthly gibi düzelt
        
        if table_name in ("daas.epdk_petrol_province"): 
            end_date = end_date.strftime("%Y-%d-%m")
            start_date = start_date.strftime("%Y-%d-%m")
            
        else: 
            end_date = end_date.strftime("%Y-%m-%d")
            start_date = start_date.strftime("%Y-%m-%d")
            
        df = self.get_data(start_date=start_date, end_date=end_date, 
                        table_name=table_name, size=size)
        return df
    
    def get_monthly_data(self, table_name: str=None, size: int=None, today: date=date.today(), months: int=1)-> pd.DataFrame:
        end_date = today
        start_date = end_date - relativedelta(months=months)
        # TODO: dtype handler ekle
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
        return pd.DataFrame(data, columns=colnames)    
    
    def shutdown(self):
        # Shutdown the connetion to the cluster
        self.data_loader._close()
        pass    