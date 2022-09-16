import yaml

def get_config():
    with open("configs/config.yaml", "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        yamlfile.close()
    return config