from datetime import timedelta
from abc import ABC, abstractmethod

class QueryGenerator(ABC):
    def __init__(self):
        self.query = self.get_query()
    
    @abstractmethod    
    def get_query(self):
        pass
    
    def __call__(self):
        return self.query
    
    
class PosgreQueryGenerator(QueryGenerator):    
    def __init__(self, start_date, end_date, table_name: str, size: int=None):
        self.start_date = start_date
        self.end_date = end_date
        self.table_name = table_name
        self.size = size
        super().__init__()
        
    def get_query(self)-> str:
        if self.size is not None:
            self.size = "limit %d" % (self.size)
        else:
            self.size = ""
        if self.table_name is not None and self.start_date is not None and self.end_date is not None:
            query = """select * from %s where date between '%s' and '%s' %s ;""" % (self.table_name, self.start_date, self.end_date, self.size)
        else:
            print("Invalid Input")
            return
        self.query = query
        return query        
            
            
class CassandraQueryGenerator(QueryGenerator):
    def __init__(self, start_date, end_date, sensor_id: int, 
                    size: int, partition: bool, table_name : str="ford_sensor_data"):
        self.start_date = start_date
        self.end_date = end_date
        self.sensor_id = sensor_id
        self.size = size
        self.table_name = table_name
        self.partition = partition
        super().__init__()
        
    def get_query(self)-> str:
        """This method generates the required query based on given input variables

        Args:
            start_date (date): datetime.Date object refers to the starting date of the time interval. Defaults to None.
            end_date (date): datetime.Date object refers to the ending date of the time interval. Defaults to None.
            sensor_id (int): ID of the target sensor . Defaults to None.
            size (int): Size of the Dataset to be fetched . Defaults to 5000.
            table_name(str): Name of the table to be fetched from the cluster. Defaults to ford_sensor_data

        Returns:
            DataFrame: Returns the query in String format
        """
        if self.size is not None:
            self.size = "LIMIT %d" % (self.size)
        else:
            self.size = ""
        # Building a query for given inputs
        if self.sensor_id is not None and self.start_date is not None and self.end_date is not None:
            if self.partition:
                self.start_date = self.start_date.date()
                self.end_date = self.end_date.date()
                filter_time = self._filter_time_generator(self.start_date,self.end_date)
                query = "SELECT * FROM %s WHERE sensor_id = %d AND filter_time IN %s %s" % (self.table_name, self.sensor_id, filter_time,self.size)
            else:
                query = """SELECT * FROM %s WHERE sensor_id = %d AND sensor_time  >= '%s' AND sensor_time < '%s' %s ALLOW FILTERING""" % (self.table_name, self.sensor_id, self.start_date, self.end_date, self.size)
        elif self.sensor_id is None and self.start_date is not None and self.end_date is not None:
            query = """SELECT * FROM %s WHERE sensor_time  >= '%s' AND sensor_time < '%s' %s ALLOW FILTERING""" % (self.table_name, self.start_date, self.end_date, self.size)
        elif self.sensor_id is not None and self.start_date is None and self.end_date is None:
                query = "SELECT * FROM %s WHERE sensor_id=%d %s ALLOW FILTERING" % (self.table_name, self.sensor_id, self.size)
        else:
                query = "SELECT * FROM %s %s ALLOW FILTERING" % (self.table_name, self.size)
        self.query = query
        return query
            
            
    def _filter_time_generator(self)-> tuple:
            """This method generates filter name between given 2 dates and stores them in a tuple

            Args:
                start_date (date): datetime.Date object refers to the starting date of the time interval.
                end_date (date): datetime.Date object refers to the ending date of the time interval.

            Returns:
                tuple: Returns a tuple object that contains dates in the given interval with String format
            """
            # Initialize an empty dictionary
            list_of_days = []
            difference = self.end_date-self.start_date
            for current_date in range(difference.days):
                current_date = self.start_date + timedelta(days=current_date)
                temp = current_date.strftime('%Y%m%d')
                for j in range(0,24):
                    j = "%02d" % j
                    temp_ = int(temp + j)
                    list_of_days.append(temp_)
                    del temp_
            return tuple(list_of_days)            