from abc import ABC, abstractmethod
from typing import Any
from src.utils.model_utils import save_model
import pandas as pd
from xgboost import XGBRegressor
from sklearn.model_selection import GridSearchCV

class BaseEstimator(ABC):
    """BaseEstimator is the abstract class for Statistical/ML/DeepLearning models.
    """
    def __init__(self, **kwargs):
        """Default Constructor
        """
        self.train_params = kwargs
        self._model: Any
        self._best_params: Any
        
    @abstractmethod    
    def train_(self, **kwargs):
        """Abstract method to train models.
        """
        pass
    
    @abstractmethod
    def __call__(self, **kwargs):
        """The __call__ method enables Python programmers to write classes where the instances 
        behave like functions and can be called like a function.
        Triggers train_ method when an instance is called.
        """
        self.train_(**kwargs)
        pass
    
    @property
    def best_model(self)-> Any:
        """This attribute correponds to the best scoring model object obtained from the grid search.

        Returns:
            Any: Returns best scoring model object.
        """
        return self._model
    
    @property
    def best_params(self,)-> dict:
        """This attribute correponds to the hyperparameters of the best scoring model in grid search.

        Returns:
            dict: Contains hyperparameters of the best scoring model.
        """
        return self._best_params


class xgb_simulator(BaseEstimator):
    """This class contains required methods to utilize XGBoost module.
    XGBOOST = https://xgboost.readthedocs.io/en/stable/parameter.html
    Args:
        BaseEstimator (_type_): _description_
    """
    def __init__(self, **kwargs):
        """Default Constructor.
        """
        super().__init__(**kwargs)
    
    def __call__(self, **kwargs):
        """The __call__ method enables Python programmers to write classes where the instances 
        behave like functions and can be called like a function.

        Returns: Parent classes' __call__ method.
        """
        return super().__call__(**kwargs)
        
    def train_(self, X_train: pd.DataFrame, y_train: pd.DataFrame, 
                          parallel_jobs: int=-1, save: bool=True, 
                          model_name: str="xgb_model.pkl"):
        """This method performs a grid search on xgboost model. Sets best_params and best model accordingly. 

        Args:
            X_train (pd.DataFrame): Features of the training set.
            y_train (pd.DataFrame): Target value of training set.
            parallel_jobs (int, optional): Number of parallel jobs. Defaults to -1.
            save (bool, optional): Logical flag for saving the model. Defaults to True.
            model_name (str, optional): Name of the model to be saved. Defaults to "xgb_model.pkl".
        """
        # Set Validator
        cv = self.tscv(validator = "time_series_split", test_size=81)
        # Initialize the regressor
        xgb_reg = XGBRegressor(**self.train_params["init_params"])
        # Initialize the grid search
        xgb_grid = GridSearchCV(estimator=xgb_reg, 
                                param_grid=self.train_params["grid_search_params"],
                                scoring="neg_mean_absolute_percentage_error", 
                                cv=cv, 
                                n_jobs=parallel_jobs,
                                verbose=True)
        # Train the model
        xgb_grid.fit(X=X_train, y=y_train, 
                     **self.train_params["fit_params"])
        # Set the best parameters and estimator
        self._best_params = xgb_grid.best_params_
        self._model = xgb_grid.best_estimator_
        if save:
            save_model(model=self._model,model_name=model_name)
            
    def tscv(self, validator: str="time_series_split", k: int=5, test_size: int=81):
        """tscv function decides on the cross validation technique and returns the cross validation
        parameter of interest in GridSearchCV. 

        Args:
            validator (str): validator . Defaults to "time_series_split".
            k (int, optional): Number of folds. Defaults to 5.
            test_size (int, optional): Size of the test set at each iteration through cross validation. Defaults to 81.

        Returns:
            TimeSeriesSplit Generator | int: _description_
        """
        if validator == "time_series_split":
            from sklearn.model_selection import TimeSeriesSplit
            tscv = TimeSeriesSplit(gap=0, max_train_size=None, n_splits=k, test_size=test_size)
            return tscv
        else:
            print("k-fold cross validation")
            return k