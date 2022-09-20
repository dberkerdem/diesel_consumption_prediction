from abc import ABC
from typing import Any
import pandas as pd
from datetime import datetime,date
from dateutil.relativedelta import relativedelta
import holidays
from src.model_selection.statistics import stats_ARIMA, stats_PROPHET

class FeatEng(ABC):
    """FeatEng is the abstract class for feature engineering operations.
    """
    def __init__(self, data: pd.DataFrame):
        """Default Constructor

        Args:
            data (pd.DataFrame): Data to be Feature Engineered.
        """
        self.data = data.copy()
        pass
    
class FeatureEngineering(FeatEng):
    """FeatureEngineering class contains required methods to perform data feature engineering operations.

    Args:
        FeatEng (ABC): FeatEng is the abstract class for feature engineering operations.
    """
    def __init__(self, data:pd.DataFrame):
        """Default Contructor.

        Args:
            data (pd.DataFrame): Data to be Feature Engineered.
        """
        super().__init__(data=data)
        # self.date_col = date_col
    
    def feature_engineering(self, num_of_auto_reg_months: int=3)-> pd.DataFrame:
        """This method performs consecutive feature engineering operations to add new features to the preprocessed data 
        and return feature engineered dataframe.

        Args:
            num_of_auto_reg_months (int, optional): This argument indicates the lag. Defaults to 3.

        Returns:
            pd.DataFrame: Feature engineered dataframe.
        """
        # Add ARIMA predictions
        self.add_ARIMA_predictions_()
        # Add auto regressive features
        self.add_auto_reg_features(num_of_auto_reg_months=num_of_auto_reg_months)
        # Add pandemic interval
        self.add_covid(start_date=datetime(2020,4,1),end_date=datetime(2021,6,1))
        # Add school holidays
        self.add_school_holidays()
        # Add demographics
        self.demographics()
        # Add Prophet predictions
        self.add_PROPHET_predictions_()
        # Drop NaN values generated through creating auto regressive features
        self.data.dropna(inplace=True)
        return self.data
    
    def add_PROPHET_predictions_(self,):
        prophet_data = stats_PROPHET(data=self.data[["date","province","current_month_consumption"]]).predictions.copy()
        self.data = pd.merge(self.data, prophet_data, on=["date","province","current_month_consumption"], how="left")
        pass
    def add_ARIMA_predictions_(self,):
        """This method initializes an stats_ARIMA object and insert ARIMA predictions to self.data.
        """
        self.data= stats_ARIMA(data=self.data).predictions.copy()
    
    def add_auto_reg_features(self, num_of_auto_reg_months: int):
        """This method adds auto regressive features into self.data. 
        Auto Regressive Features are:
        - Last years same months consumption
        - Total consumption since last year
        - Current month share
        - Lag x previous months shares
        - Lag x previous month consumptions
        - Quarter of the year
        
        Args:
            num_of_auto_reg_months (int): This argument indicates the lag.
            
            TODO: Update here
        """
        empty_df = pd.DataFrame()
        for province in self.data.province.unique():
            # Filter data by province
            temp_df = self.data.query(f"province == '{province}'")
            # Reset index
            temp_df = temp_df.reset_index(drop=True)
            # Add last year total consumption
            temp_df["last_year_total_consumption"] = temp_df.current_month_consumption.rolling(window=12,closed="left").sum()
            # Add Rolling means
            for i in range(1, num_of_auto_reg_months):
                col_name = f"rolling_mean_{i+1}"
                window = i+1
                temp_df[col_name] = temp_df.current_month_consumption.shift(1).rolling(window=window).mean()
            # Add previous month shares
            for i in range(num_of_auto_reg_months):
                col_name = f"lag{i+1}_monthly_share"
                temp_df[col_name] = temp_df.current_month_consumption.shift(i)/temp_df.last_year_total_consumption.shift(i)
            # Add previous months consumptions, i.e. lags
            for i in range(num_of_auto_reg_months):
                col_name = f"lag{i+1}"
                temp_df[col_name] = temp_df.current_month_consumption.shift(i+1)
            # Add quarter of the year
            temp_df['quarter'] = temp_df['date'].dt.quarter
            # Add month of the year
            temp_df['month'] = temp_df['date'].dt.month
            
            empty_df = pd.concat([empty_df,temp_df], ignore_index=True)
        self.data = empty_df.copy()      
        
    def add_covid(self,start_date: datetime, end_date: datetime):
        """This method adds covid pandemic feature into the self.data.  
        1 indicates that covid pandemic situation, whereas 0 indicates non-pandemic situation.
        Args:
            start_date (datetime): Start date of covid pandemic period.
            end_date (datetime): End date of covid pandemic period.
        """
        temp_df = self.data.copy()
        # Initialize covid column
        temp_df["covid"] = 0
        # Insert covid
        temp_df["covid"] = temp_df.apply(lambda row: 1 if (row.date >=start_date) & (row.date <=end_date) else row.covid, axis=1)
        self.data = temp_df.copy()
    
    def add_school_holidays(self):
        """This method adds sum of weekend_days + school_holidays + n_goverment_holidays of that month into self.data.
        """
        # Intiialize temporary dataframe
        temp_df = self.data.copy()
        # Initialize school_holiday column
        temp_df["school_holiday"] = 0
        # Initialize Govermantal holidays
        gov_holidays = holidays.country_holidays('TR')
        # Iterate over rows
        temp_df["school_holiday"] = temp_df.apply(lambda row: self.get_holidays(start_date=row.date, gov_holidays=gov_holidays), axis=1)
        # Update the data
        self.data = temp_df.copy()     
        
    def get_holidays(self, start_date: date, gov_holidays: Any):
        """This method calculates total number of holidays starting from given start date to the end of that month.
        HOLIDAYS = https://github.com/dr-prodigy/python-holidays
        Args:
            start_date (date): First day of that month.
            gov_holidays (Any): Govermantal holidays object of specified country from holidays library.
        """
        # Arrange date start = date(2022,1,1)
        end = (start_date + relativedelta(months=1,days=-1))
        # Initialize Counters
        n_gov_holidays = school_holidays = weekend = 0
        # Initialize Flags
        semester, start_end = True,True
        for dt in pd.date_range(start=start_date,end=end):
            # add weekends
            if dt.day_of_week>4:
                weekend += 1
            # add government-designated holidays
            elif dt in gov_holidays and dt.day_of_week<5:
                n_gov_holidays += 1
            # add school holidays
            elif dt.month == 7 or dt. month == 8:
                school_holidays +=1
            # add semesters 
            elif (dt.month == 1 or dt.month == 2) and semester:
                # Subtract weekends
                school_holidays += (7-2)
                semester = False
            # add start and end of schools
            elif (dt.month == 6 or dt.month == 9) and start_end:
                # Subtract weekends
                school_holidays += (15-4)
                start_end = False
        return(weekend+school_holidays+n_gov_holidays)        
        
            
    def demographics(self):
        """This method adds demographic features into self.data
        """
        # Intiialize temporary dataframe
        temp_df = self.data.copy()
        # Load demographics dataframe
        demographics = pd.read_excel('data/demographics_final.xlsx')
        # Add years to the temporary dataframe
        temp_df["years"] = temp_df["date"].dt.year
        new_df = pd.merge(temp_df,demographics, on=["years","province"], how="left")
        new_df["population"].fillna(method="ffill",inplace=True)     
        self.data = new_df.drop(columns=["years"])