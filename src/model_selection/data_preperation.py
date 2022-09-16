from abc import ABC
import pandas as pd
from dateutil.relativedelta import relativedelta

class DataHandler(ABC):
    def __init__(self, data: pd.DataFrame):
        self.data = data.copy()
        pass

class DataPreperation(DataHandler):
    def __init__(self, data:pd.DataFrame):
        super().__init__(data=data)
    
    def tts_last_month(self, target_column: str="current_month_consumption", 
                        index_column1: str="date", index_column2: str=None, lag: int=0):
        """train_test_split and indexing"""
        # Set index of date column
        self.data = self.data.set_index([index_column1]).copy()
        # Format end_date 
        end_date = (self.data.index.max() -relativedelta(months=lag)).strftime("%Y-%m-%d")
        # Split data into train and test
        data_train = self.data.loc[self.data.index < end_date].copy()
        data_test = self.data.loc[self.data.index == end_date].copy()
        
        if index_column2 is not None:
            data_train = data_train.reset_index().set_index([index_column1, index_column2]).copy()
            data_test = data_test.reset_index().set_index([index_column1, index_column2]).copy()

        # Prepare train and test sets
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