import pandas as pd
import itertools
import statsmodels.api as sm
import warnings
from statsmodels.tools.sm_exceptions import ConvergenceWarning
from statsmodels.tsa.arima.model import ARIMA
from abc import ABC, abstractmethod
from prophet import Prophet

class BaseEstimator(ABC):
    """BaseEstimator is the abstract class for statistical inference operations.
    """
    def __init__(self, **kwargs):
        """Default Constructor
        """
        self.data: pd.DataFrame
        self.input_validator(**kwargs)
        
    def input_validator(self, **kwargs):
        """This method validates inputs of this class and raises error if missing
        """
        try:
            self.data = kwargs["data"].copy()
        except Exception as e:
            print("Data is missing. Exception",e)
            raise e
        
    @abstractmethod    
    def predict_(self, **kwargs):
        """Abstract method for forecasting.
        """
        pass
    
    @abstractmethod
    def train_(self,**kwargs):
        """Abstract method for training.
        """
        pass
     
    @property
    def predictions(self):
        """Built-in property to return predictions for given time series data as an attribute.

        Returns:
            pd.DataFrame: This DataFrame contains initial input data along with statistical predictions. 
        """
        return self.predict_()

class stats_PROPHET(BaseEstimator):
    """stats_PROPHET class contains required methods to utilize facebooks' Prophet module.
    PROPHET = https://facebook.github.io/prophet/docs/diagnostics.html#hyperparameter-tuning
    
    Args:
        BaseEstimator (ABC): BaseEstimator is the abstract class for statistical inference operations.
    """
    def __init__(self, **kwargs):
        """Default Constructor
        kwargs:
            data(pd.DataFrame): Contains data to be fitted into PROPHER model
        """
        super().__init__(**kwargs)
    
    def fix_col_names(self, data:pd.DataFrame, col_rename_dict: dict=None, reverse: bool=False)-> pd.DataFrame:
        """This method renames given columns. Prophet object only accepts naming of date column as ds and target value as y.

        Args:
            data (pd.DataFrame): Dataframe to be renamed
            col_rename_dict (dict): Contains key:value pairs of old_column_name:new_column_name.
            reverse (bool, optional): Reverses default col_rename_dict for specific purposes. Defaults to False.

        Returns:
            pd.DataFrame: Dataframe with renamed columns.
        """
        # Rename columns
        if col_rename_dict is not None:
            if reverse:
                col_rename_dict = {v: k for k, v in col_rename_dict.items()}
            data.rename(columns=col_rename_dict, inplace=True)
        else:
            col_rename_dict = {"date":"ds","current_month_consumption":"y"}
            if reverse:
                col_rename_dict = {v: k for k, v in col_rename_dict.items()}
            data.rename(columns=col_rename_dict, inplace=True)
        return data
    
    def train_(self, model_df: pd.DataFrame):
        m = Prophet()
        m.fit(model_df)
        return m
                
    def predict_(self, periods: int=0, gs_flag: bool=False, col_list: list=None, **kwargs)-> pd.DataFrame:
        # Initialize a temporary dataframe
        temp_df = self.data.copy()
        # Initialize empty dataframe
        empty_df = pd.DataFrame()
        # Set the columns names to a proper format
        temp_df = self.fix_col_names(data=temp_df)
        # Set the columns of interest
        if col_list is None:
            col_list = ['ds', 'province', 'trend', 'yhat_lower', 'yhat_upper', 'yhat']
        for prov in temp_df.province.unique():
            temp_df_ = temp_df.query(f"province == '{prov}'").copy()
            # Train the Prophet model
            m = self.train_(model_df=temp_df_)
            # Create Future Dataframe
            future = m.make_future_dataframe(periods=periods,freq="MS")
            # Predict Future
            forecast = m.predict(future)
            # Insert province into forecasts
            forecast["province"] = prov
            # Concat results
            empty_df = pd.concat([empty_df,forecast[col_list]], ignore_index=True)
        temp_df = temp_df.merge(empty_df, on=["ds","province"], how="left")
        temp_df = self.fix_col_names(data=temp_df, reverse=True)
        return temp_df
    
                   
class stats_ARIMA(BaseEstimator):
    """stats_ARIMA class contains required methods to perform statistical ARIMA operations.
    ARIMA = https://hands-on.cloud/using-the-arima-model-and-python-for-time-series-forecasting/
    Args:
        BaseEstimator (ABC): BaseEstimator is the abstract class for statistical inference operations.
    """
    def __init__(self, **kwargs):
        """Default Constructor
        kwargs:
            data(pd.DataFrame): Contains data to be fitted into ARIMA model
        """
        super().__init__(**kwargs)
        self.warning_handler()
        
    def warning_handler(self,):
        """This method prevents displayment of unwanted errors.
        """
        warnings.simplefilter('ignore', ConvergenceWarning)

    def get_p_d_q(self,)-> dict:
        """This method decides on optimal p-d-q parameters for each province by considering the minimum AIC value over each p-d-q pair.

        Returns:
            dict: Returns {province:(p,d,q)} paired dictionary.
        """
        # Initialize (p,d,q) parameters dictionary, temp_df_
        param_dict = dict()
        temp_df_ = self.data.copy()
        # Fill dictionary for each province
        for prov in temp_df_.province.unique():
            temp_df = temp_df_.query(f"province == '{prov}'").drop(columns=["province"]).set_index(["date"]).copy()
            temp_df.index = pd.DatetimeIndex(temp_df.index.values,
                                            freq=temp_df.index.inferred_freq)
            results = self.AIC_PDQS(data=temp_df)
            param_dict[prov] =results.loc[results['aic'].idxmin()].pdq        
        return param_dict
    
    def train_(self, **kwargs):
        """This method initalizes an ARIMA estimator and fits given data.

        Returns: Trained ARIMA model.
        """
        model_df = kwargs["model_df"]
        order = kwargs["order"]
        # Initialize the estimator
        arima_model = ARIMA(model_df.current_month_consumption, order=order)
        # Train the ARIMA model
        model = arima_model.fit()        
        return model
    
    def predict_(self, **kwargs):
        # Initialize temporary dictionary
        temp_df = self.data.copy()
        # Initialize ARIMA predictions column
        temp_df["ARIMA_prediction"] = 0
        # Get (p,d,q) parameters dictionary
        param_dict = self.get_p_d_q()
        for prov in temp_df.province.unique():
            # Filter by province
            temp_df_ = temp_df.query(f"province == '{prov}'").drop(columns=["province"]).set_index(["date"]).copy()
            temp_df_.index = pd.DatetimeIndex(temp_df_.index.values,
                                            freq=temp_df_.index.inferred_freq)
            # Train the ARIMA model
            model = self.train_(model_df=temp_df_, order=param_dict[prov])
            
            # Predict the province
            predictions = model.predict(0,temp_df_.shape[0],dynamic=False)
            # Insert predictions into original dataframe
            temp_df["ARIMA_prediction"] = temp_df.apply(lambda row: (predictions.loc[predictions.index==row.date,].item()) if row.province==prov else row.ARIMA_prediction, axis=1)        

        return temp_df
    
    def AIC_PDQS(self, data: pd.DataFrame)-> pd.DataFrame:   
        ''' 
            Runs grid search to return lowest AIC result for permutataions of pdq/s values in range 0,2  
                            
            df: Dataframe to anlyse for best pdq/s permutation
            REFERENCES: https://hands-on.cloud/using-the-arima-model-and-python-for-time-series-forecasting/    
        '''
        
        # Define the p, d and q parameters to take any value between 0 and 2
        p = d = q = range(0, 2)
        
    #     Auto-Regressive (p) -> Number of autoregressive terms.
    #     Integrated (d) -> Number of nonseasonal differences needed for stationarity.
    #     Moving Average (q) -> Number of lagged forecast errors in the prediction equation.

        # Generate all different combinations of p, q and q triplets
        pdq = list(itertools.product(p, d, q))

        # Generate all different combinations of seasonal p, q and q triplets
        pdqs = [(x[0], x[1], x[2], 12) for x in list(itertools.product(p, d, q))]


        # Run a grid with pdq and seasonal pdq parameters calculated above and get the best AIC value
        ans = []
        for comb in pdq:
            for combs in pdqs:
                try:
                    mod = sm.tsa.statespace.SARIMAX(data,
                                                order=comb,
                                                seasonal_order=combs,
                                                enforce_stationarity=False,
                                                enforce_invertibility=False)
                    output = mod.fit(maxiter=500,disp=False)
                    
                    ans.append([comb, combs, output.aic])
                except:
                    continue
                
        # Find the parameters with minimal AIC value
        ans_df = pd.DataFrame(ans, columns=['pdq', 'pdqs', 'aic'])
        return ans_df