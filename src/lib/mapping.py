import yaml
import os

current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)


class Mapping:
    def __init__(self):
        with open(current_directory + "/mapping.yaml", "r") as mapping_file:
            mapping_data = yaml.safe_load(mapping_file)

        self.mapping_data = mapping_data

    def mapping_positions(self, exchange, positions=None):
        if exchange not in self.mapping_data.keys():
            return positions

        if positions is not None:
            if "position" not in self.mapping_data[exchange].keys():
                return positions

            new_positions = []
            mapping_data = self.mapping_data[exchange]["position"]

            for position in positions:
                new_postion = {}
                for _key, _value in position.items():
                    if type(_value) is dict:
                        new_postion[_key] = {}

                        if _key in mapping_data.keys():
                            for __key, __value in _value.items():
                                if __key in mapping_data[_key].keys():
                                    new_postion[_key][
                                        mapping_data[_key][__key]
                                    ] = __value
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

    def mapping_instruments(self, exchange, instrument=None):
        if exchange not in self.mapping_data.keys():
            return instrument

        if instrument is not None:
            if "instrument" not in self.mapping_data[exchange].keys():
                return instrument

            new_instrument = {}
            mapping_data = self.mapping_data[exchange]["instrument"]
            for _key, _value in instrument.items():
                if _key in mapping_data.keys():
                    new_instrument[mapping_data[_key]] = _value
                else:
                    new_instrument[_key] = _value

            return new_instrument

    def mapping_transactions(self, exchange, transactions=None):
        if exchange not in self.mapping_data.keys():
            return transactions

        if transactions is not None:
            if "transaction" not in self.mapping_data[exchange].keys():
                return transactions

            new_transactions = []
            mapping_data = self.mapping_data[exchange]["transaction"]

            for transaction in transactions:
                new_transaction = {}
                for _key, _value in transaction.items():
                    # if _key == "type":
                    #     if exchange == "okx":
                    #         if ("_" + _value) in mapping_data["values"]["type"].keys():
                    #             new_transaction[_key] = mapping_data["values"]["type"][
                    #                 "_" + _value
                    #             ]
                    #         else:
                    #             new_transaction[_key] = _value
                    #     else:
                    #         new_transaction[_key] = _value

                    # elif _key == "subType":
                    #     if exchange == "okx":
                    #         if ("_" + _value) in mapping_data["values"][
                    #             "subType"
                    #         ].keys():
                    #             new_transaction[_key] = mapping_data["values"][
                    #                 "subType"
                    #             ]["_" + _value]
                    #         else:
                    #             new_transaction[_key] = _value
                    #     else:
                    #         new_transaction[_key] = _value

                    # elif _key == "incomeType":
                    #     if exchange == "binance":
                    #         if _value in mapping_data["values"].keys():
                    #             new_transaction[_key] = mapping_data["values"][_value]
                    #         else:
                    #             new_transaction[_key] = _value
                    #     else:
                    #         new_transaction[_key] = _value

                    # elif _key in mapping_data.keys():
                    #     new_transaction[mapping_data[_key]] = _value
                    # else:
                    #     new_transaction[_key] = _value
                    if _key in mapping_data.keys():
                        new_transaction[mapping_data[_key]] = _value
                    else:
                        new_transaction[_key] = _value
                new_transactions.append(new_transaction)

            return new_transactions

    def mapping_mark_price(self, exchange, mark_price=None):
        if exchange not in self.mapping_data.keys():
            return mark_price

        if mark_price is not None:
            if "mark_prices" not in self.mapping_data[exchange].keys():
                return mark_price

            new_mark_price = {}
            mapping_data = self.mapping_data[exchange]["mark_prices"]
            for _key, _value in mark_price.items():
                if _key in mapping_data.keys():
                    new_mark_price[mapping_data[_key]] = _value
                else:
                    new_mark_price[_key] = _value

            return new_mark_price
        
    def mapping_fills(self, exchange, fills=None):
        if exchange not in self.mapping_data.keys():
            return fills

        if fills is not None:
            if "fills" not in self.mapping_data[exchange].keys():
                return fills

            new_fills = []
            mapping_data = self.mapping_data[exchange]["fills"]

            for item in fills:
                new_fill = {}
                for _key, _value in item.items():
                    if _key in mapping_data.keys():
                        new_fill[mapping_data[_key]] = _value
                    else:
                        new_fill[_key] = _value
                
                new_fills.append(new_fill)

            return new_fills
