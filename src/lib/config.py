import yaml

def read_config_file(file_path = 'src/lib/config.yaml'):
    with open(file_path, 'r') as config_file:
        config_data = yaml.safe_load(config_file)
    
    return config_data
