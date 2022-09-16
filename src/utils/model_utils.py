import pickle

# Save the model
def save_model(model, model_name: str, path: str="models/"):
    pickle.dump(model, open(path+model_name, 'wb'))

# Load the model
def load_model(model_name:str ,path: str="models/"):
    model = pickle.load(open(path+model_name, 'rb'))
    return model 