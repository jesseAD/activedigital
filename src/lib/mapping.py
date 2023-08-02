import yaml
import os

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)

class Mapping:
    def __init__(self):
        self.mapping_data = None

    def read_mapping_file(self, file_path = current_directory+'/mapping.yaml'):
        with open(file_path, 'r') as mapping_file:
            mapping_data = yaml.safe_load(mapping_file)
        
        self.mapping_data = mapping_data

    def mapping(self, exchange, positions = None, instrument = None):
        self.read_mapping_file()

        if exchange not in self.mapping_data.keys():
            if positions is not None:
                return positions
            else:
                return instrument

        if positions is not None:
            if 'position' not in self.mapping_data[exchange].keys():
                return positions
            
            new_positions = []
            mapping_data = self.mapping_data[exchange]['position']
            
            for position in positions:
                new_postion = {}
                for _key, _value in position.items():
                    if type(_value) is dict:
                        new_postion[_key] = {}

                        if _key in mapping_data.keys():
                            for __key, __value in _value.items():
                                if __key in mapping_data[_key].keys():
                                    new_postion[_key][mapping_data[_key][__key]] = __value
                                else:
                                    new_postion[_key][__key] = __value
                        else:
                            for __key, __value in _value.items():
                                new_postion[_key][__key] = __value
                    else:
                        if _key in mapping_data.keys():
                            new_postion[mapping_data[_key]] = _value
                        else:
                            new_postion[_key] = _value
                new_positions.append(new_postion)

            return new_positions

        elif instrument is not None:
            if 'instrument' not in self.mapping_data[exchange].keys():
                return instrument
            
            new_instrument = {}
            mapping_data = self.mapping_data[exchange]['instrument']
            for _key, _value in instrument.items():
                if type(_value) is dict:
                    new_instrument[_key] = {}

                    if _key in mapping_data.keys():
                        for __key, __value in _value.items():
                            if __key in mapping_data[_key].keys():
                                new_instrument[_key][mapping_data[_key][__key]] = __value
                            else:
                                new_instrument[_key][__key] = __value
                    else:
                        for __key, __value, in _value.items():
                            new_instrument[_key][__key] = __value
                else:
                    if _key in mapping_data.keys():
                        new_instrument[mapping_data[_key]] = _value
                    else:
                        new_instrument[_key] = _value
            
            return new_instrument

        else:
            return None
