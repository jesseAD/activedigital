import yaml

file_path = 'config.yaml'
client_alias = 'deepspace'

def read_config_file(config_file_path):
    with open(config_file_path, 'r') as config_file:
        config_data = yaml.safe_load(config_file)
    
    return config_data

def get_data_by_client(client):
    config = read_config_file(file_path)
    return config[client]

get_data_by_client(client_alias)