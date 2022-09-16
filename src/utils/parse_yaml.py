import yaml

def parse_yaml(path):
    '''
    Parse yaml file
    :param:path to yaml file
    :return: connection information (dict)
    '''
    with open(path, 'r') as stream:
        try:
            return(yaml.safe_load(stream))
        except yaml.YAMLError as exc:
            print(exc)