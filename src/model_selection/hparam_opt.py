from abc import ABC, abstractmethod
from typing import Any
import pandas as pd
import numpy as np
import lightgbm as lgb
import xgboost as xgb
import catboost as cbt
from sklearn.model_selection import TimeSeriesSplit, GridSearchCV, RandomizedSearchCV
from src.model_selection.data_preperation import DataPreperation as dp

class BaseEstimator(ABC):
    """BaseEstimator is the abstract class for Statistical/ML/DeepLearning models.
    """
    def __init__(self, **kwargs):
        """Default Constructor
        """
        
        pass

class GBTrees(BaseEstimator):
    def __init__(self, **kwargs):
        """Default Constructor
        """
        pass
    @staticmethod
    def make_estimator_pipeline_(estimator_list: list, **kwargs):
        estimator_pipeline = dict()
        print("Estimators are:",estimator_list)
        # print(kwargs)
        if "xgboost" in estimator_list:
            estimator_pipeline["xgboost"] = xgb.XGBRegressor(**kwargs["xgboost__init_params"])
            # print(kwargs["xgboost__init_params"])
        if "lightgbm" in estimator_list:
            estimator_pipeline["lightgbm"] = lgb.LGBMRegressor(**kwargs["lightgbm__init_params"])
            # print(kwargs["lightgbm__init_params"])
        if "catboost" in estimator_list:
            estimator_pipeline["catboost"] = cbt.CatBoostRegressor(**kwargs["catboost__init_params"])
            # print(kwargs["catboost__init_params"])
        return estimator_pipeline
    
    
    
    
class HyparamOptimizer(BaseEstimator):
    def __init__(self, **kwargs):
        """Default Constructor.
        """
        super().__init__(**kwargs)
    
    @staticmethod
    def optimize_hyparams(X_train: pd.DataFrame, y_train: pd.DataFrame, optimization_config: dict):        
        best_models_and_params = HyparamOptimizer().optimizer_pipeline_(X_train=X_train, y_train=y_train, 
                                                                      **optimization_config)
        return best_models_and_params

    def optimizer_pipeline_(self, X_train: pd.DataFrame, y_train: pd.DataFrame,
                            estimator_list: list, estimator_params: dict, **kwargs):
        # Establish Estimators pipeline
        estimator_pipeline = GBTrees().make_estimator_pipeline_(estimator_list=estimator_list, **estimator_params["init_params"])
        # Initialize the Cross Validator
        cv = self.get_cross_validator(**kwargs["cross_validation_params"])
        # Initialize an dictionary for optimized model:hyparameter pairs
        best_models_params = dict()
        # Perform Optimization and Store Results
        for est in estimator_list:
            print("Estimator is",est)
            # Alttaki satırı sil
            self.calc_grid_pairs(estimator_params["grid_search_params"][f"{est}__grid_search_params"])        
            grid = self.hp_optimizer(
                estimator=estimator_pipeline[est], 
                cv=cv, 
                search_params=estimator_params["grid_search_params"][f"{est}__grid_search_params"],
                **kwargs["hp_optimizer"]
                )
            grid.fit(X=X_train, y=y_train, **estimator_params["fit_params"][f"{est}__fit_params"])
            best_models_params[est] = [grid.best_estimator_,grid.best_params_]
            print(f"R-S for {est} is done.")
        return best_models_params
        
    def hp_optimizer(self, estimator: Any, search_params:dict, cv:Any, optimizer_type:str, scoring:str, n_jobs:int):
        if "GridSearchCV" in optimizer_type:
            return GridSearchCV(
                            estimator=estimator, # Target Estimator
                            param_distributions=search_params, # Hyperparameters set
                            scoring=scoring, # Scoring parameter
                            cv=cv, # Cross Validator
                            n_jobs=n_jobs, # Number of parallel jobs
                            Verbose=True
                            )
        elif "RandomSearchCV" in optimizer_type:       
            grid = RandomizedSearchCV(
                            estimator=estimator, # Target Estimator
                            param_distributions=search_params, # Hyperparameters set
                            scoring=scoring, # Scoring parameter
                            cv=cv, # Cross Validator
                            n_jobs=n_jobs, # Number of parallel jobs
                            )
            return grid    
                        
    def get_cross_validator(self, validator: str, k: int=9, test_size: int=81, gap: int=0):
        if validator == "time_series":
            from sklearn.model_selection import TimeSeriesSplit
            # print("time_series_cross_val")
            tscv = TimeSeriesSplit(gap=gap, max_train_size=None, n_splits=k, test_size=test_size)
            return tscv
        else:
            print("k-fold cross validation")
            return k
        
    def calc_grid_pairs(self, dict: dict()):
        prod = 1
        for key in dict.keys():
            prod *= len(dict[key])
        print("Number of candidates are: ",prod)
