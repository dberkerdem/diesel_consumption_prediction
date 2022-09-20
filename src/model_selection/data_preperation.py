from abc import ABC
import pandas as pd
from dateutil.relativedelta import relativedelta

class BaseForPreperation(ABC):
    """BaseForPreperation is the abstract class for data preperation classes in this module.
    """
    def __init__(self, ):
        """Default Constructor.
        """
        pass

class DataPreperation(BaseForPreperation):
    """DataPreperation class contains required methods and attributes to perform data preperation operations. 

    Args:
        BaseForPreperation (ABC): BaseForPreperation is the abstract class for data preperation classes in this module.
    """
    def __init__(self):
        """Default Constructor.
        """
        # super().__init__(data=data)
    @staticmethod
    def ts_train_test_split(data: pd.DataFrame, index_column2: str, target_column: str="current_month_consumption", 
                        index_column1: str="date",  lag: int=0, n_months: int=None, n_provinces: int=81, verbose: bool=True):
        """This method splits data into (train,test)*(X,y). 
        Deciding the test sets' date as (last_month-lag) and previous dates becomes the train set.

        Args:
            data (pd.DataFrame): Data to be prepared.
            index_column2 (str): Name of the second index column
            target_column (str, optional): Name of the target value. Defaults to "current_month_consumption".
            index_column1 (str, optional): Name of the first index column. Defaults to "date".
            lag (int, optional): Number of lags. Defaults to 0.

        Returns: X_train, y_train, X_test and y_test
        TODO: Update Docstring
        """
        data_train, data_test = DataPreperation.train_test_split(data=data, index_column2=index_column2, 
                                                                 index_column1=index_column1, lag=lag,
                                                                 n_months=n_months, n_provinces=n_provinces)
        
        # Split dataset into train and test sets
        X_train = data_train.drop(columns=[target_column])
        y_train = data_train[[target_column]]
        X_test = data_test.drop(columns=[target_column])
        y_test = data_test[[target_column]]
        if verbose:
            print("Maximum date at train is: ", X_train.index.max()," Shape is: ", X_train.shape)
            print("Minimum date at train is: ", X_train.index.min()," Shape is: ", X_train.shape)
            print("Maximum date at test is: ", X_test.index.max(), " Shape is: ", X_test.shape)
            print("Minimum date at test is: ", X_test.index.min(), " Shape is: ", X_test.shape)
        return X_train, y_train, X_test, y_test
    
    @staticmethod
    def train_test_split(data: pd.DataFrame, index_column2: str, 
                        index_column1: str="date",  lag: int=0,
                        n_months: int=None, n_provinces: int=81, verbose: bool=True):
        """This method splits data into train and test. 
        
        Args:
            data (pd.DataFrame): Data to be prepared.
            index_column2 (str): Name of the second index column.
            index_column1 (str, optional): Name of the first index column. Defaults to "date". Defaults to "date".
            lag (int, optional): Number of lags. Defaults to 0.
            n_months (int, optional): Number of previous months in the dataframe to be returned. Defaults to None.
            n_provinces (int, optional): Number of provinces at each month. Defaults to 81.
            verbose (bool, optional): Verbosity flag to display time-shape. Defaults to True.

        Returns:
            Any: data_train, data_test
        TODO: Update Docstring
        """
        # Set index of date column
        temp_df = data.set_index([index_column1]).copy()
        # Format end_date 
        end_date = (temp_df.index.max() -relativedelta(months=lag)).strftime("%Y-%m-%d")
        # Split data into train and test
        data_train = temp_df.loc[temp_df.index < end_date].copy()
        data_test = temp_df.loc[temp_df.index == end_date].copy()
        if verbose:
            print("Date range date at train is: ", data_train.index.min(), data_train.index.max(),"with shape of: ",data_train.shape)
            print("Date range date at test is: ", data_test.index.min(), data_test.index.max(),"with shape of: ",data_test.shape)

        if index_column2:
            data_train = data_train.reset_index().set_index([index_column1, index_column2]).copy()
            data_test = data_test.reset_index().set_index([index_column1, index_column2]).copy()
        
        # Sort by index
        data_train.sort_index(inplace=True)
        data_test.sort_index(inplace=True)
        if n_months is not None and n_provinces is not None:
            return data_train[-n_months*n_provinces:], data_test
        return data_train, data_test