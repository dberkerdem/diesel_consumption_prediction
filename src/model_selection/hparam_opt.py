from abc import ABC, abstractmethod
from typing import Any
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit, GridSearchCV, RandomizedSearchCV
from src.model_selection.gbtrees  import GBTrees

class BaseEstimator(ABC):
    """BaseEstimator is the abstract class for Statistical/ML/DeepLearning models.
    """
    def __init__(self, **kwargs):
        """Default Constructor
        """
        pass    
    
class HyparamOptimizer(BaseEstimator):
    def __init__(self, **kwargs):
        """Default Constructor.
        """
        super().__init__(**kwargs)

    def optimize_hyparams(self, X_train: pd.DataFrame, y_train: pd.DataFrame, estimator_list: list, optimization_params: dict):        
        best_models_and_params = self.optimizer_pipeline_(X_train=X_train, y_train=y_train,
                                                                        estimator_list=estimator_list, **optimization_params)
        return best_models_and_params

    def optimizer_pipeline_(self, X_train: pd.DataFrame, y_train: pd.DataFrame,
                            estimator_list: list, estimator_params: dict, **kwargs):
        # Establish Estimators pipeline
        estimator_pipeline = GBTrees(estimator_list=estimator_list,).tree_init_pipeline_(**estimator_params)
        # Initialize the Cross Validator
        cv = self.get_cross_validator(**kwargs["cross_validation_params"])
        # Initialize an dictionary for optimized hyparameters pairs
        optimal_params = {"init_params":{}}
        # Perform Optimization and Store Results
        for est in estimator_list:
            # Alttaki satırı sil
            self.calc_grid_pairs(estimator_params["grid_search_params"][f"{est}__grid_search_params"])        
            grid = self.hp_optimizer(
                estimator=estimator_pipeline[est], 
                cv=cv, 
                search_params=estimator_params["grid_search_params"][f"{est}__grid_search_params"],
                **kwargs["hp_optimizer"]
                )
            grid.fit(X=X_train, y=y_train, **estimator_params["fit_params"][f"{est}__fit_params"])
            optimal_params["init_params"][f"{est}__init_params"] = grid.best_params_
            # best_models_params[est] = [grid.best_estimator_,grid.best_params_]
            print(f"R-S for {est} is done.")
        return optimal_params
        
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
