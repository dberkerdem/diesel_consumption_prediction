from abc import ABC, abstractmethod
import lightgbm as lgb
import xgboost as xgb
import catboost as cbt
import pandas as pd
from typing import Any

class BaseEstimator(ABC):
    """BaseEstimator is the abstract class for Statistical/ML/DeepLearning models.
    """
    def __init__(self, estimator_list: list,**kwargs):
        """Default Constructor
        """
        self.estimator_list = estimator_list
        pass
        
class GBTrees(BaseEstimator):
    def __init__(self, estimator_list: list, **kwargs):
        """Default Constructor
        """
        super().__init__(estimator_list=estimator_list,**kwargs)
        self.score_: Any
        pass
    @property
    def prediction_scores(self)->pd.DataFrame:
        return self.score_
            
    def tree_init_pipeline_(self, **kwargs):
        estimator_init_pipeline = dict()
        if "xgboost" in self.estimator_list:
            estimator_init_pipeline["xgboost"] = xgb.XGBRegressor(**kwargs["init_params"]["xgboost__init_params"])
            # print(kwargs["xgboost__init_params"])
        if "lightgbm" in self.estimator_list:
            estimator_init_pipeline["lightgbm"] = lgb.LGBMRegressor(**kwargs["init_params"]["lightgbm__init_params"])
            # print(kwargs["lightgbm__init_params"])
        if "catboost" in self.estimator_list:
            estimator_init_pipeline["catboost"] = cbt.CatBoostRegressor(**kwargs["init_params"]["catboost__init_params"])
            # print(kwargs["catboost__init_params"])
        return estimator_init_pipeline
    
    def tree_fit_pipeline_(self, X_train: pd.DataFrame, y_train: pd.DataFrame, hyper_params:dict):
        estimator_fit_pipeline = dict()
        # Init Trees
        estimator_init_pipeline = self.tree_init_pipeline_(**hyper_params)
        # Fit Trees
        for est in self.estimator_list:
            estimator_fit_pipeline[est] = estimator_init_pipeline[est].fit(X_train, y_train,)
        return estimator_fit_pipeline
            
    def tree_predict_pipeline_(self, X_train: pd.DataFrame, y_train: pd.DataFrame, X_test: pd.DataFrame, 
                               y_test: pd.DataFrame, hyper_params:dict, run: int=0):
        estimator_predict_pipeline = y_test.reset_index(level=["date"],drop=True).copy()
        fitted_gbtrees = self.tree_fit_pipeline_(X_train=X_train, y_train=y_train, hyper_params=hyper_params)
        for est in self.estimator_list:
            estimator_predict_pipeline[f"{est}_pred"] = fitted_gbtrees[est].predict(X_test)
        self._prediction_scores_(predictions=estimator_predict_pipeline, run=run)
        return estimator_predict_pipeline
        
    def ape(self, actual_value: Any, prediction: Any)->float:
        ape_score = abs(actual_value-prediction)/actual_value*100
        return 1/ape_score

    def _prediction_scores_(self, predictions: pd.DataFrame, run: int, target_value: str="current_month_consumption")-> pd.DataFrame:
        temp_df = predictions.copy()
        col_list = []
        for est in self.estimator_list:
            # Add percentage difference
            temp_df[f"{est}_{run}_APE"] = temp_df.apply(lambda row: self.ape(row[target_value], row[f"{est}_pred"]), axis=1)
            col_list.append(f"{est}_{run}_APE")
        self.score_ = temp_df[col_list]