import yaml

def get_config(file_name: str="config", folder_name: str="configs")->dict:
    """This method reads file with .yaml extension for given file name 
    and returns its content in dictionary format.  

    Args:
        file_name (str, optional): Name of the file of interest. Defaults to "config".
        folder_name (str, optional): Name of the folder that contains file of interest. Defaults to "configs".
    Returns:
        _type_: _description_
    """
    with open(f"{folder_name}/{file_name}.yaml", "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        yamlfile.close()
    return config