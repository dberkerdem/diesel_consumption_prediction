from abc import ABC, abstractmethod
from typing import Any
from src.utils.model_utils import save_model
import pandas as pd
from xgboost import XGBRegressor
from sklearn.model_selection import GridSearchCV

class MLModelHandler(ABC):
    def __init__(self, **kwargs):
        self.train_params = kwargs
        self._model: Any
        self._best_params: Any
        
    @abstractmethod    
    def train_(self, **kwargs):
        pass
    
    @abstractmethod
    def __call__(self, **kwargs):
        self.train_(**kwargs)
        pass
    
    @property
    def best_model(self):
        return self._model
    
    @property
    def best_params(self,):
        return self._best_params


class xgb_simulator(MLModelHandler):
    """xgb = xgboost_simulator(...)
    # xgb(train,test,.....)
    # xgb.best_model
    # xgb.best_params"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def __call__(self, **kwargs):
        return super().__call__(**kwargs)
        
    def train_(self, X_train: pd.DataFrame, y_train: pd.DataFrame, 
                          parallel_jobs: int=-1, save: bool=True, 
                          model_name: str="overall_xgb_model.pkl"):
        # Set the parameters to train the xgb model
        init_params = self.train_params["init_params"]
        fit_params = self.train_params["fit_params"]
        grid_search_params = self.train_params["grid_search_params"]
        # Set Validator
        cv = self.tscv(validator = "time_series_split", test_size=81).split(X=X_train, y=y_train)
        # Initialize the regressor
        xgb_reg = XGBRegressor(**init_params)
        # Initialize the grid search
        xgb_grid = GridSearchCV(estimator=xgb_reg, 
                                param_grid=grid_search_params,
                                scoring="mean_absolut_percentage_error", 
                                cv=cv, 
                                n_jobs=parallel_jobs)
        # Train the model
        xgb_grid.fit(X=X_train, y=y_train, **fit_params)
        # Set the best parameters and estimator
        self._best_params = xgb_grid.best_params_
        self._model = xgb_grid.best_estimator_
        if save:
            save_model(model=self._model,model_name=model_name)
            
    def tscv(self, validator: str="time_series_split", k: int=5, test_size: int=81):
        if validator == "time_series_split":
            from sklearn.model_selection import TimeSeriesSplit
            tscv = TimeSeriesSplit(gap=0, max_train_size=None, n_splits=k, test_size=test_size)
            return tscv
        else:
            print("k-fold cross validation")
            return k