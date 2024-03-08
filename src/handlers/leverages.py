from datetime import datetime, timezone

from src.config import read_config_file

config = read_config_file()

class Leverages:
    def __init__(self, db, collection):

        self.runs_db = db['runs']
        self.positions_db = db['positions']
        self.split_positions_db = db['split_positions']
        self.balances_db = db['balances']
        self.tickers_db = db['tickers']
        self.leverages_db = db['leverages']

    def get(
        self,
        client,
        exchange: str = None,
        account: str = None,
        logger=None
    ):        
        run_ids = self.runs_db.find({}).sort('_id', -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item['runid']
            except:
                pass
        
        try:
            query = {}
            if exchange:
                query["venue"] = exchange
            
            # fetch latest position, balance, tickers
            ticker_value = self.tickers_db.find(query)
            for item in ticker_value:
                try: 
                    latest_ticker = item['ticker_value']
                except Exception as e:
                    logger.error(client + " " + exchange + " " + account + " leverages " + str(e))
                    # print("An error occurred in Leverages:", e)
                    return True  
                
            if client:
                query["client"] = client
            if account:
                query["account"] = account

            if config['clients'][client]['split_positions'] == True:
                position_value = self.split_positions_db.aggregate([
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$client', client
                                        ]
                                    }, {
                                        '$eq': [
                                            '$venue', exchange
                                        ]
                                    }, {
                                        '$eq': [
                                            '$account', account
                                        ]
                                    }
                                ]
                            }
                        }
                    }, {
                        '$project': {
                            'position_value': 1
                        }
                    }, {
                        '$group': {
                            '_id': None, 
                            'position_value': {
                                '$last': '$position_value'
                            }
                        }
                    }
                ])

                for item in position_value:
                    try:
                        latest_position = item['position_value']
                    except Exception as e:
                        logger.error(client + " " + exchange + " " + account + " leverages " + str(e))
                        # print("An error occurred in Leverages:", e)
                        return True
                
                try:
                    # max_notional = abs(float(max(latest_position, key=lambda x: abs(float(x['notional'])))['notional']))
                    max_notional = 0
                    for pair in latest_position:
                        max_notional += max([abs(item['notional']) for item in pair])
                except Exception as e:
                    logger.error(client + " " + exchange + " " + account + " leverages " + str(e))
                    # print("An error occurred in Leverages:", e)
                    return True
                
            else:
                position_value = self.positions_db.aggregate([
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$client', client
                                        ]
                                    }, {
                                        '$eq': [
                                            '$venue', exchange
                                        ]
                                    }, {
                                        '$eq': [
                                            '$account', account
                                        ]
                                    }
                                ]
                            }
                        }
                    }, {
                        '$project': {
                            'position_value': 1
                        }
                    }, {
                        '$group': {
                            '_id': None, 
                            'position_value': {
                                '$last': '$position_value'
                            }
                        }
                    }
                ])
                
                for item in position_value:
                    try:
                        latest_position = item['position_value']
                    except Exception as e:
                        logger.error(client + " " + exchange + " " + account + " leverages " + str(e))
                        # print("An error occurred in Leverages:", e)
                        return True
                    
                try:
                    # max_notional = abs(float(max(latest_position, key=lambda x: abs(float(x['notional'])))['notional']))
                    max_notional = 0
                    for item in latest_position:
                        max_notional += abs(float(item['notional']))
                except Exception as e:
                    logger.error(client + " " + exchange + " " + account + " leverages " + str(e))
                    # print("An error occurred in Leverages:", e)
                    return True
            
            balance_valule = self.balances_db.aggregate([
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$eq': [
                                        '$client', client
                                    ]
                                }, {
                                    '$eq': [
                                        '$venue', exchange
                                    ]
                                }, {
                                    '$eq': [
                                        '$account', account
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'balance_value': 1,
                    }
                }, {
                    '$group': {
                        '_id': None, 
                        'balance_value': {
                            '$last': '$balance_value'
                        }
                    }
                }
            ])

            for item in balance_valule:
                try: 
                    latest_balance = item['balance_value']
                except Exception as e:
                    logger.error(client + " " + exchange + " " + account + " leverages " + str(e))
                    # print("An error occurred in Leverages:", e)
                    return True
                
            base_currency = config["clients"][client]["subaccounts"][exchange]["base_ccy"]

            balance_in_base_currency = 0
            try:
                if base_currency == "USDT":
                    balance_in_base_currency = latest_balance['base'] / latest_ticker['USDT/USD']['last']
                elif base_currency == "USD":
                    balance_in_base_currency = latest_balance['base'] 
                else:
                    balance_in_base_currency = latest_balance['base'] / (latest_ticker['USDT/USD']['last'] * latest_ticker[base_currency + '/USDT']['last'])
            except Exception as e:
                logger.error(client + " " + exchange + " " + account + " leverages " + str(e))
                # print("An error occurred in Leverages:", e)
                return True

            # try:
            #     balance_in_base_currency = 0
            #     for currency, balance in latest_balance.items():
            #         if currency == base_currency:
            #             balance_in_base_currency += balance
            #         else:
            #             balance_in_base_currency += latest_ticker * balance
            # except Exception as e:
            #     print("An error occurred in Leverages:", e)
            #     return False

            leverage_value = {
                "client": client,
                "venue": exchange,
                "account": account,
                "timestamp": datetime.now(timezone.utc),
            }
            try:
                leverage_value['leverage'] = max_notional / balance_in_base_currency
            except Exception as e:
                logger.error(client + " " + exchange + " " + account + " leverages " + str(e))
                # print("An error occurred in Leverages:", e)
                return True
            
            leverage_value["runid"] = latest_run_id
            
            if config['leverages']['store_type'] == "timeseries":
                self.leverages_db.insert_one(leverage_value)
            elif config['leverages']['store_type'] == "snapshot":
                self.leverages_db.update_one(
                    {
                        "client": leverage_value["client"],
                        "venue": leverage_value["venue"],
                        "account": leverage_value["account"]
                    },
                    { "$set": {
                        "leverage": leverage_value["leverage"],
                        "timestamp": leverage_value["timestamp"],
                        "runid": leverage_value["runid"]
                    }},
                    upsert=True
                )

            return True
        
        except Exception as e:
            logger.error(client + " " + exchange + " " + account + " leverages " + str(e))
            return True