from abc import ABC
import pandas as pd
from datetime import datetime

class FeatEng(ABC):
    def __init__(self, data: pd.DataFrame):
        self.data = data.copy()
        pass
    
class FeatureEngineering(FeatEng):
    def __init__(self, data:pd.DataFrame):
        super().__init__(data=data)
        # self.date_col = date_col

    def feature_engineering(self, num_of_auto_reg_months: int=3)-> pd.DataFrame:
        # Add auto regressive features
        self.add_auto_reg_features(num_of_auto_reg_months=num_of_auto_reg_months)
        # Add pandemic interval
        self.add_covid(start_date=datetime(2020,4,1),end_date=datetime(2021,6,1))
        # Add school holidays
        self.add_school_holidays()
        # Add demographics
        self.demographics()
        # Drop NaN values generated through creating auto regressive features
        self.data.dropna(inplace=True)

        return self.data
    
    def add_auto_reg_features(self, num_of_auto_reg_months: int):
        empty_df = pd.DataFrame()
        for province in self.data.province.unique():
            # Filter data by province
            temp_df = self.data.query(f"province == '{province}'")
            # Reset index
            temp_df = temp_df.reset_index(drop=True)
            # Add last year consumption
            temp_df["last_year_same_month_consumption"] = temp_df.current_month_consumption.shift(12)
            # Add last year total consumption
            temp_df["last_year_total_consumption"] = temp_df.current_month_consumption.rolling(window=12,closed="left").sum()
            # Monthly Share for current consumption
            temp_df["current_month_share"] = temp_df["current_month_consumption"]/temp_df["last_year_total_consumption"]
            # Add previous month shares
            for i in range(num_of_auto_reg_months):
                col_name = f"previous_{i+1}_month_share"
                temp_df[col_name] = temp_df.current_month_share.shift(i+1)
            # Add previous months consumptions, i.e. lags
            for i in range(num_of_auto_reg_months):
                col_name = f"previous_{i+1}_month_consumption"
                temp_df[col_name] = temp_df.current_month_consumption.shift(i+1)
            # Add quarter of the year
            temp_df['quarter'] = temp_df['date'].dt.quarter
            
            empty_df = pd.concat([empty_df,temp_df], ignore_index=True)
        self.data = empty_df.copy()      
        
    def add_covid(self,start_date: datetime, end_date: datetime):
        temp_df = self.data.copy()
        # Initialize covid column
        temp_df["covid"] = 0
        # Insert covid
        temp_df["covid"] = temp_df.apply(lambda row: 1 if (row.date >=start_date) & (row.date <=end_date) else row.covid, axis=1)
        self.data = temp_df.copy()
    
    def add_school_holidays(self):
        temp_df = self.data.copy()
        # Initialize school_holiday column
        temp_df["school_holiday"] = 0
        # ocak
        temp_df["school_holiday"] = temp_df.apply(lambda row: 15 if row.date.month==1 else row.school_holiday, axis=1)
        # şubat
        temp_df["school_holiday"] = temp_df.apply(lambda row: 14 if row.date.month==2 else row.school_holiday, axis=1)
        # mart
        temp_df["school_holiday"] = temp_df.apply(lambda row: 8 if row.date.month==3 else row.school_holiday, axis=1)
        # nisan
        temp_df["school_holiday"] = temp_df.apply(lambda row: 13 if row.date.month==4 else row.school_holiday, axis=1)
        # mayıs
        temp_df["school_holiday"] = temp_df.apply(lambda row: 12 if row.date.month==5 else row.school_holiday, axis=1)
        # haziran
        temp_df["school_holiday"] = temp_df.apply(lambda row: 17 if row.date.month==6 else row.school_holiday, axis=1)
        # temmuz & ağustos
        temp_df["school_holiday"] = temp_df.apply(lambda row: 30 if row.date.month==7 or \
                                                row.date.month==8 else row.school_holiday, axis=1)
        # eylül
        temp_df["school_holiday"] = temp_df.apply(lambda row: 11 if row.date.month==9 else row.school_holiday, axis=1)
        # ekim 
        temp_df["school_holiday"] = temp_df.apply(lambda row: 10 if row.date.month==10 else row.school_holiday, axis=1)
        # kasım
        temp_df["school_holiday"] = temp_df.apply(lambda row: 13 if row.date.month==11 else row.school_holiday, axis=1)
        # aralık
        temp_df["school_holiday"] = temp_df.apply(lambda row: 7 if row.date.month==12 else row.school_holiday, axis=1)
        self.data = temp_df.copy()     
        
    def demographics(self):
        demographics = pd.read_excel("data/demographics.xls")     
        self.data = pd.merge(self.data,demographics, on=["province"], how="left")