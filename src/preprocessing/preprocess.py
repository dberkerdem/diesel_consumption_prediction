from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta

class Preprocess(ABC):
    """Preprocess is the abstract class for data preprocessing operations.
    """
    def __init__(self, data: pd.DataFrame):
        """Default Constructor

        Args:
            data (pd.DataFrame): Data to be preprocessed.
        """
        self.data = data.copy()
        pass

class PreprocessData(Preprocess):
    """PreprocessData class contains required methods to perform data preprocessing operations.

    Args:
        Preprocess (ABC): Preprocess is the abstract class for data preprocessing operations.
    """
    def __init__(self, data:pd.DataFrame, date_col: str="date"):
        """Default Constructor.

        Args:
            data (pd.DataFrame): Data to be preprocessed.
            date_col (str, optional): Name of the column that contains date index. Defaults to "date".
        """
        super().__init__(data=data)
        self.date_col = date_col

    def preprocess_data(self, target_col_list: list=None, row_drop_dict: dict=None, format_date_flag: bool=True, col_rename_dict: dict=None, anomaly_col: str=None, col_list: list=None, months: int=3)-> pd.DataFrame:
        """Takes an oil consumption df of various oil types for provinces, performs required consecutive preprocessing operations 
        and returns a df of diesel consumption.

        Args:
            target_col_list (list, optional): Contains list of columns to be kept. Defaults to None.
            row_drop_dict (dict, optional): Contains names of rows to be dropped.. Defaults to None.
            format_date_flag (bool, optional): Logical flag to determine whether modify the date column format. Defaults to True.
            col_rename_dict (dict, optional): Contains key:value pairs of old_column_name:new_column_name. Defaults to None.
            anomaly_col (str, optional): Indicates the name of column that records with anomaly are stored. Defaults to None.
            col_list (list, optional): Name of columns to be kept, hence to be filled. Defaults to None.
            months (int, optional): Indicates number of forward and backward months to be utilized while taking average. Defaults to 3.

        Returns:
            pd.DataFrame: Returns preprocessed dataframe of diesel consumption.
        """
        
        # Remove unnecessary columns and reduce it to only motorin
        self.filter_columns(target_col_list=target_col_list)
        # Drop unnecessary rows
        self.drop_rows(row_drop_dict=row_drop_dict)
        # Fix Date Format
        self.fix_date_format(format_date_flag=format_date_flag)
        # Rename columns
        self.fix_col_names(col_rename_dict=col_rename_dict)
        # Fix Anomaly
        self.fix_anomaly(anomaly_col=anomaly_col)
        # Fill Missing Months
        self.fill_missing_values(col_list=col_list, months=months)

        return self.data.sort_values(by=["date","province"])
    
    def filter_columns(self, target_col_list: list):
        """This method removes unnecessary columns and reduce it to only motorin.

        Args:
            target_col_list (list): Contains list of columns to be kept.
        """
        if target_col_list is not None:
            self.data = self.data[target_col_list].sort_values(by=self.date_col)
    
    def drop_rows(self, row_drop_dict: dict):
        """This method drops unnecessary rows.

        Args:
            row_drop_dict (dict): Contains names of rows to be dropped.
        """
        if row_drop_dict is not None:
            # Iterate over keys of dictionary and locate rows with corr. values to drop
            for target_column in row_drop_dict.keys():
                target_record = row_drop_dict[target_column]
                index = self.data.query(f"{target_column}=='{target_record}'").index
                self.data.drop(labels=index, axis=0, inplace=True)
                    
    def fix_date_format(self, format_date_flag: bool):
        """This method fixes invalid format of date in dataset retrieved from the database.

        Args:
            format_date_flag (bool): Logical flag to determine whether modify the date column format.
        """
        if format_date_flag:
            # Convert object type values to datetime and arrange its format
            self.data[self.date_col] = pd.to_datetime(self.data[self.date_col], format='%Y-%d-%m')
            
    def fix_col_names(self, col_rename_dict: dict):
        """This method renames given columns.

        Args:
            col_rename_dict (dict): Contains key:value pairs of old_column_name:new_column_name.
        """
        # Rename columns
        if col_rename_dict is not None:
            self.data.rename(columns=col_rename_dict, inplace=True)
        # Update date column attribute as well
        if self.date_col in col_rename_dict.keys():
            self.date_col = col_rename_dict[self.date_col]
            
    def fix_anomaly(self, anomaly_col: str):
        """This method fixes anomaly caused while parsing.

        Args:
            anomaly_col (str): Indicates the name of column that records with anomaly are stored.
        """
        if anomaly_col is not None:
            temp_df = self.data.copy()
            temp_df[anomaly_col] = temp_df[anomaly_col].apply(lambda row:\
                row/1000 if row%1==0 and row > 100000 else row)
            self.data = temp_df.copy()
        
    def fill_missing_values(self, col_list: list, months: int):
        """This method generates a new dataframe within the time range, fills missing months by taking average 
        and returns filled dataframe

        Args:
            col_list (list): Name of columns to be kept, hence to be filled.
            months (int): Indicates number of forward and backward months to be utilized while taking average.
        """
        # Check if any month is missed
        if True: #TODO: Add condition here 
            # Get dataframe with missing values
            missing_values_df = self.get_missing_values_df(col_list=col_list).copy()
            # Fill missing months
            self.data = self.fill_missing_months(df=missing_values_df, months=months).copy()
            pass
        else:
            # Do nothing
            pass
        
    def create_date_province_frame(self)-> pd.DataFrame:
        """This method creates a date interval dataframe for each province and add them to each other.

        Returns:
            pd.DataFrame: Returns a complete date x province dataframe.
        """
        # Yıl-ay-il bazında çoklama 
        # Create date interval dataframe
        temp_df = pd.DataFrame()
        min_date = self.data.date.min()
        max_date = self.data.date.max()
        temp_df["date"] = pd.date_range(min_date, max_date, freq="MS")
        print("Missing Dates are: ", list(set(temp_df.date.unique())-set(self.data.date.unique())))
        # Create date X province dataframe
        date_province_df = pd.merge(temp_df,pd.DataFrame(self.data.province.unique()), how="cross")
        date_province_df.rename(columns={0:"province"}, inplace=True) 
        return date_province_df
    
    def get_missing_values_df(self, col_list: list)-> pd.DataFrame:
        """This method insert existing data into newly created complete date x province dataframe.
        Existing values are remain the same, while missing values are indicated as NaN.
        
        Args:
            col_list (list): Name of columns to be kept, hence to be filled.

        Returns:
            pd.DataFrame: Dataframe that contains all existing data along with empty rows with missing dates.
        """
        date_province_df = self.create_date_province_frame()
        if col_list is None:
            col_list = ["date","province"]
        missing_values_df = pd.merge(date_province_df, self.data, on=col_list, how="left")
        return missing_values_df
    
    def fill_missing_months(self, df: pd.DataFrame(), months: int)-> pd.DataFrame:
        """This method fills missing months by taking backward:forward average of existing records 
        and returns filled dataframe

        Args:
            df (pd.DataFrame): Dataframe that contains all existing data along with empty rows with missing dates.
            months (int): Indicates number of forward and backward months to be utilized while taking average.

        Returns:
            pd.DataFrame: fills missing months by taking average 
        and returns filled dataframe
        """
        new_df = df.copy()
        for missing_date in new_df[new_df.isna().any(axis=1)].date.unique():
            for prov in new_df.province.unique():
                # Set the format of missing date of interest
                missing_date = pd.Timestamp(missing_date)
                # Initialize the placeholders
                sum = 0
                counter = 0
                for i in range(1,months+1):
                    # Set the dates 
                    date_1 = missing_date + relativedelta(months=i)
                    date_2 = missing_date - relativedelta(months=i)
                    # Set the conditions
                    try: # Assign lagged features
                        val_1 = new_df.query(f"date=='{date_1}' & province=='{prov}'").current_month_consumption.item()
                        val_2 = new_df.query(f"date=='{date_2}' & province=='{prov}'").current_month_consumption.item()
                    except: # Assign none if not exist
                        val_1 = np.nan
                        val_2 = np.nan
                    # Store the results
                    if pd.notna(val_1) and pd.notna(val_2):
                        sum += val_1+val_2
                        counter += 2
                        del val_1,val_2              
                # Fill missing value
                if sum != 0:
                    # Locate  and Fill missing value 
                    new_df.loc[(new_df.date==missing_date) & (new_df.province==prov),["current_month_consumption"]] = sum/counter
                else:
                    # fill with last record (TODO: improve this later)
                    new_val = new_df[(new_df.date==missing_date-relativedelta(months=1)) & (new_df.province==prov)].current_month_consumption.item()
                    # TODO: Burayı sklearn veya linear regression ile doldurabiliriz
                    # from sklearn.linear_model import LinearRegression
                    # deneme = preprocessed_df.query(f"(province == 'İSTANBUL') & (date >= '{date(2021,1,1)}') & (date < '{date(2021,10,1)}')").sort_values(by="date",ascending=True).copy()
                    # deneme.set_index("date", inplace=True)
                    # deneme['Time'] = np.arange(len(deneme.index))
                    # X_train = np.array(deneme.loc[:,"Time"])
                    # y_train = np.array(deneme.loc[:,"current_month_consumption"])
                    # from sklearn.linear_model import LinearRegression
                    # reg = LinearRegression()
                    # reg.fit(X_train.reshape(-1,1),y_train)
                    # reg.predict([[deneme.Time.max()+1]])
                    # Set the previous record to the missign spot
                    new_df.loc[(new_df.date==missing_date) & (new_df.province==prov),["current_month_consumption"]] = new_val
        return new_df