import pickle
from typing import Any

# Save the model
def save_model(model, model_name: str, folder_name: str="models"):
    """This method saves the model object with given model name to given folder.

    Args:
        model (Any): Model to be saved.
        model_name (str): Name of the model.
        folder_name (str, optional): Folder of the model to be saved. Defaults to "models".
    """
    pickle.dump(model, open(f"{folder_name}+/{model_name}", 'wb'))

# Load the model
def load_model(model_name:str ,folder_name: str="models")-> Any:
    """This method loads the model object with given model name from given folder.

    Args:
        model_name (str): Name of the model.
        folder_name (str, optional): Folder of the model to be loaded. Defaults to "models".

    Returns:
        Any: Returns the model object. 
    """
    model = pickle.load(open(f"{folder_name}/{model_name}", 'rb'))
    return model 