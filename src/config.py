import yaml
import os

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)

def read_config_file(file_path = current_directory+'/config.yaml'):
  with open(file_path, 'r') as config_file:
    config_data = yaml.safe_load(config_file)
  
  return config_data
