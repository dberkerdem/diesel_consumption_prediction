from abc import ABC
import pandas as pd
from dateutil.relativedelta import relativedelta

class BaseForPreperation(ABC):
    """BaseForPreperation is the abstract class for data preperation classes in this module.
    """
    def __init__(self, data: pd.DataFrame):
        """Default Constructor.

        Args:
            data (pd.DataFrame): Data to be prepared.
        """
        self.data = data.copy()
        pass

class DataPreperation(BaseForPreperation):
    """DataPreperation class contains required methods and attributes to perform data preperation operations. 

    Args:
        BaseForPreperation (ABC): BaseForPreperation is the abstract class for data preperation classes in this module.
    """
    def __init__(self, data:pd.DataFrame):
        """Default Constructor.

        Args:
            data (pd.DataFrame): Data to be prepared.
        """
        super().__init__(data=data)
    
    def tts_last_month(self, index_column2: str, target_column: str="current_month_consumption", 
                        index_column1: str="date",  lag: int=0):
        """This method splits data into train and test. Deciding the test sets' date as (last_month-lag) and previous dates becomes the train set.

        Args:
            index_column2 (str): Name of the second index column
            target_column (str, optional): Name of the target value. Defaults to "current_month_consumption".
            index_column1 (str, optional): Name of the first index column. Defaults to "date".
            lag (int, optional): Number of lags. Defaults to 0.

        Returns: X_train, y_train, X_test and y_test
        """
        # Set index of date column
        self.data = self.data.set_index([index_column1]).copy()
        # Format end_date 
        end_date = (self.data.index.max() -relativedelta(months=lag)).strftime("%Y-%m-%d")
        # Split data into train and test
        data_train = self.data.loc[self.data.index < end_date].copy()
        data_test = self.data.loc[self.data.index == end_date].copy()
        
        if index_column2:
            data_train = data_train.reset_index().set_index([index_column1, index_column2]).copy()
            data_test = data_test.reset_index().set_index([index_column1, index_column2]).copy()
        
        # Sort by index
        data_train.sort_index(inplace=True)
        data_test.sort_index(inplace=True)
        
        # Split dataset into train and test sets
        X_train = data_train.drop(columns=[target_column])
        y_train = data_train[[target_column]]
        X_test = data_test.drop(columns=[target_column])
        y_test = data_test[[target_column]]
        
        return X_train, y_train, X_test, y_test
    
    def ttvs_last_month(self, target_column: str="current_month_consumption", 
                        index_column1: str="date", index_column2: str=None, lag: int=0, split_size: float=0.03):
        from sklearn.model_selection import train_test_split
        X_train, y_train, X_test, y_test = self.tts_last_month(target_column=target_column, index_column1=index_column1, index_column2=index_column2, lag=lag)
        X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=split_size, random_state=12)
        return  X_train, y_train, X_test, y_test, X_val, y_val


    def ttvs_random(self, target_column: str="current_month_consumption", 
                        index_column1: str="date", index_column2: str=None,lag: int=0, split_size: float=0.03):
        from sklearn.model_selection import train_test_split
        self.data = self.data.set_index([index_column1, index_column2]).copy()
        X = self.data.drop(columns=[target_column]).copy()
        y = self.data[[target_column]].copy()
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=split_size, random_state=12)
        X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=split_size, random_state=2)
        return  X_train, y_train, X_test, y_test, X_val, y_val