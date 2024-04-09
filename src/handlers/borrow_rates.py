from datetime import datetime, timezone
import ccxt 

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper

config = read_config_file()


class BorrowRates:
    def __init__(self, db, collection):

        self.runs_db = db[config['mongodb']['database']]['runs']
        self.borrow_rates_db = db[config['mongodb']['database']]['borrow_rates']

    # def get(
    #     self,
    #     active: bool = None,
    #     spot: str = None,
    #     future: str = None,
    #     perp: str = None,
    #     exchange: str = None,
    #     account: str = None,
    #     code: str = None,
    # ):
    #     results = []

    #     pipeline = [
    #         {"$sort": {"_id": -1}},
    #     ]

    #     if active is not None:
    #         pipeline.append({"$match": {"active": active}})
    #     if spot:
    #         pipeline.append({"$match": {"spotMarket": spot}})
    #     if future:
    #         pipeline.append({"$match": {"futureMarket": future}})
    #     if perp:
    #         pipeline.append({"$match": {"perpMarket": perp}})
    #     if exchange:
    #         pipeline.append({"$match": {"venue": exchange}})
    #     if account:
    #         pipeline.append({"$match": {"account": account}})
    #     if code:
    #         pipeline.append({"$match": {"code": code}})

    #     try:
    #         results = self.borrow_rates_db.aggregate(pipeline)
    #         return results

    #     except Exception as e:
    #         log.error(e)

    def create(
        self,
        exch = None,
        exchange: str = None,
        code: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        borrowRatesValue: str = None,
        vipLoanRatesValue: str = None,
        logger=None,
        secrets={},
    ):

        if borrowRatesValue is None:
            try:
                if exch == None:
                    exch = Exchange(exchange).exch()
                
                borrowRatesValue = {}

                query = {}
                if exchange:
                    query["venue"] = exchange
                query["code"] = code
                query["market/vip"] = "market"

            
                borrow_rate_values = self.borrow_rates_db.aggregate([
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$venue', exchange
                                        ]
                                    }, {
                                        '$eq': [
                                            '$code', code
                                        ]
                                    }, {
                                        '$eq': [
                                            '$market/vip', 'market'
                                        ]
                                    }
                                ]
                            }
                        }
                    }, {
                        '$project': {
                            'borrow_rates_value': 1
                        }
                    }, {
                        '$group': {
                            '_id': None, 
                            'borrow_rates_value': {
                                '$last': '$borrow_rates_value'
                            }
                        }
                    }
                ])

                current_values = None
                for item in borrow_rate_values:
                    current_values = item["borrow_rates_value"]

                try:
                    if current_values is None:
                        if exchange == "okx":
                            borrowRatesValue = OKXHelper().get_borrow_rates(
                                exch=exch, limit=92, code=code
                            )
                        elif exchange == "binance":
                            borrowRatesValue = Helper().get_borrow_rates(
                                exch=exch, limit=92, code=code
                            )
                        elif exchange == "bybit":
                            res = BybitHelper().get_borrow_rate(
                                exch=exch, code=code
                            )
                            if res != None:
                                borrowRatesValue = [res]
                            else:
                                borrowRatesValue = []
                    else:
                        last_time = int(current_values["timestamp"])
                        if exchange == "okx":
                            borrowRatesValue = OKXHelper().get_borrow_rates(
                                exch=exch, limit=92, code=code, since=last_time+1
                            )
                        elif exchange == "binance":
                            borrowRatesValue = Helper().get_borrow_rates(
                                exch=exch, limit=92, code=code, since=last_time+1
                            )
                        elif exchange == "bybit":                            
                            res = BybitHelper().get_borrow_rate(
                                exch=exch, code=code
                            )
                            if res != None:
                                borrowRatesValue = [res]
                            else:
                                borrowRatesValue = []
                    
                    if len(borrowRatesValue) > 0:
                        scalar = 1
                        if config['borrow_rates']['period'][exchange] != "yearly":
                            scalar = 365

                        if exchange == "okx":
                            borrow_rate = OKXHelper().get_borrow_rate(
                                exch=exch, params={"ccy": code}
                            )["data"][0]["interestRate"]
                            for item in borrowRatesValue:
                                item["nextBorrowRate"] = float(borrow_rate) * 24 * 365 / scalar
                                item['scalar'] = scalar

                        elif exchange == "binance":
                            borrow_rate = Helper().get_borrow_rate(
                                exch=exch, params={"assets": code, "isIsolated": False}
                            )[0]["nextHourlyInterestRate"]
                            for item in borrowRatesValue:
                                item["nextBorrowRate"] = float(borrow_rate) * 24 * 365 / scalar
                                item['scalar'] = scalar      

                        elif exchange == "bybit":
                            for item in borrowRatesValue:
                                item['scalar'] = scalar 
                except ccxt.BadSymbol as e:
                    if logger == None:
                        print(exchange +  " borrow rates " + str(e))
                        print("Unable to collect borrow rates for " + exchange)
                    else:
                        logger.warning(exchange +  " borrow rates " + str(e))
                        logger.error("Unable to collect borrow rates for " + exchange)

                    return True
        
            except ccxt.AuthenticationError as e:
                if logger == None:
                    print(exchange + " borrow rates " + str(e))
                else:
                    logger.warning(exchange + " borrow rates " + str(e))

                for _client in config['clients']:
                    if exchange in config['clients'][_client]['subaccounts']:
                        for _account in config['clients'][_client]['subaccounts'][exchange]:
                            print(_client + exchange + _account)
                            
                            if _account != "base_ccy":
                                spec = _client.upper() + "_" + exchange.upper() + "_" + _account.upper() + "_"
                                API_KEY = secrets[spec + "API_KEY"]
                                API_SECRET = secrets[spec + "API_SECRET"]
                                PASSPHRASE = None
                                if exchange == "okx":
                                    PASSPHRASE = secrets[spec + "PASSPHRASE"]

                                exch = Exchange(exchange, _account, API_KEY, API_SECRET, PASSPHRASE).exch()

                                borrowRatesValue = {}

                                query = {}
                                if exchange:
                                    query["venue"] = exchange
                                query["code"] = code
                                query["market/vip"] = "market"

                                try:
                                    borrow_rate_values = self.borrow_rates_db.aggregate([
                                        {
                                            '$match': {
                                                '$expr': {
                                                    '$and': [
                                                        {
                                                            '$eq': [
                                                                '$venue', exchange
                                                            ]
                                                        }, {
                                                            '$eq': [
                                                                '$code', code
                                                            ]
                                                        }, {
                                                            '$eq': [
                                                                '$market/vip', 'market'
                                                            ]
                                                        }
                                                    ]
                                                }
                                            }
                                        }, {
                                            '$project': {
                                                'borrow_rates_value': 1
                                            }
                                        }, {
                                            '$group': {
                                                '_id': None, 
                                                'borrow_rates_value': {
                                                    '$last': '$borrow_rates_value'
                                                }
                                            }
                                        }
                                    ])

                                    current_values = None
                                    for item in borrow_rate_values:
                                        current_values = item["borrow_rates_value"]

                                    if current_values is None:
                                        if exchange == "okx":
                                            borrowRatesValue = OKXHelper().get_borrow_rates(
                                                exch=exch, limit=92, code=code
                                            )
                                        else:
                                            borrowRatesValue = Helper().get_borrow_rates(
                                                exch=exch, limit=92, code=code
                                            )
                                    else:
                                        last_time = int(current_values["timestamp"])
                                        if exchange == "okx":
                                            borrowRatesValue = OKXHelper().get_borrow_rates(
                                                exch=exch, limit=92, code=code, since=last_time+1
                                            )
                                        else:
                                            borrowRatesValue = Helper().get_borrow_rates(
                                                exch=exch, limit=92, code=code, since=last_time+1
                                            )
                                    
                                    if len(borrowRatesValue) > 0:
                                        scalar = 1
                                        if config['borrow_rates']['period'][exchange] != "yearly":
                                            scalar = 365

                                        if exchange == "okx":
                                            borrow_rate = OKXHelper().get_borrow_rate(
                                                exch=exch, params={"ccy": code}
                                            )["data"][0]["interestRate"]
                                            for item in borrowRatesValue:
                                                item["nextBorrowRate"] = float(borrow_rate) * 24 * 365 / scalar
                                                item['scalar'] = scalar

                                        elif exchange == "binance":
                                            borrow_rate = Helper().get_borrow_rate(
                                                exch=exch, params={"assets": code, "isIsolated": False}
                                            )[0]["nextHourlyInterestRate"]
                                            for item in borrowRatesValue:
                                                item["nextBorrowRate"] = float(borrow_rate) * 24 * 365 / scalar
                                                item['scalar'] = scalar  
                                except ccxt.ExchangeError as e:
                                    if logger == None:
                                        print(exchange + " borrow rates " + str(e))
                                        print("Unable to collect borrow rates for " + exchange)
                                    else:
                                        logger.warning(exchange + " borrow rates " + str(e))
                                        logger.error("Unable to collect borrow rates for " + exchange)

                                    return True
                                except ccxt.NetworkError as e:  
                                    if logger == None:
                                        print(exchange + " borrow rates " + str(e))
                                    else:
                                        logger.warning(exchange + " borrow rates " + str(e))

                                    return False

                                break
                                                      
                    break

                pass

            except ccxt.ExchangeError as e:
                if logger == None:
                    print(exchange + " borrow rates " + str(e))
                    print("Unable to collect borrow rates for " + exchange)
                else:
                    logger.warning(exchange + " borrow rates " + str(e))
                    logger.error("Unable to collect borrow rates for " + exchange)

                return True
            except ccxt.NetworkError as e:
                if logger == None:
                    print(exchange + " borrow rates " + str(e))
                else:
                    logger.warning(exchange + " borrow rates " + str(e))

                return False
        
        if exchange == "okx" and vipLoanRatesValue is None:
            try:
                vipLoanRatesValue = {}
                spec = config['vip_loan_rates']['client'].upper() + "_" + exchange.upper() + "_" + config['vip_loan_rates']['account'].upper() + "_"
                API_KEY = secrets[spec + "API_KEY"]
                API_SECRET = secrets[spec + "API_SECRET"]
                PASSPHRASE = secrets[spec + "PASSPHRASE"]

                _exch = Exchange(exchange, config['vip_loan_rates']['account'], API_KEY, API_SECRET, PASSPHRASE).exch()
                vipLoanRatesValue['info'] = OKXHelper().get_vip_loan_rate(
                    exch=_exch,
                    params={'type': 1, 'ccy': code}
                )['data'][0]['records'][0]
                vipLoanRatesValue['rate'] = float(vipLoanRatesValue['info']['rate'])
                vipLoanRatesValue['code'] = code
                vipLoanRatesValue['scalar'] = 365
                vipLoanRatesValue['timestamp'] = int(datetime.timestamp(datetime.now(timezone.utc)) * 1000)

            except ccxt.NetworkError as e:
                if logger == None:
                    print(exchange + " borrow rates " + str(e))
                else:
                    logger.warning(exchange + " borrow rates " + str(e))
            
            except ccxt.ExchangeError as e:
                if logger == None:
                    print(exchange + " borrow rates " + str(e))
                else:
                    logger.warning(exchange + " borrow rates " + str(e))
            
            except Exception as e:
                if logger == None:
                    print(exchange + " borrow rates " + str(e))
                else:
                    logger.warning(exchange + " borrow rates " + str(e))

        if len(borrowRatesValue) <= 0 and (vipLoanRatesValue == {} or vipLoanRatesValue is None):
            if logger == None:
                print("Empty borrow rates for " + exchange)
                print("Unable to collect borrow rates for " + exchange)
            else:
                logger.warning("Empty borrow rates for " + exchange)
                logger.error("Unable to collect borrow rates for " + exchange)

            return True

        borrow_rates = []

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass

        for item in borrowRatesValue:
            new_value = {
                "venue": exchange,
                "borrow_rates_value": item,
                "code": code,
                "market/vip": "market",
                "active": True,
                "entry": False,
                "exit": False,
                "timestamp": datetime.now(timezone.utc),
            }

            if spot:
                new_value["spotMarket"] = spot
            if future:
                new_value["futureMarket"] = future
            if perp:
                new_value["perpMarket"] = perp

            new_value["runid"] = latest_run_id

            borrow_rates.append(new_value)
        
        del borrowRatesValue

        if vipLoanRatesValue != {} and vipLoanRatesValue is not None:
            new_value = {
                "venue": exchange,
                "borrow_rates_value": vipLoanRatesValue,
                "code": code,
                "market/vip": "vip",
                "active": True,
                "entry": False,
                "exit": False,
                "timestamp": datetime.now(timezone.utc),
            }

            if spot:
                new_value["spotMarket"] = spot
            if future:
                new_value["futureMarket"] = future
            if perp:
                new_value["perpMarket"] = perp

            new_value["runid"] = latest_run_id

            borrow_rates.append(new_value)

        try:
            self.borrow_rates_db.insert_many(borrow_rates)

            del borrow_rates

            if logger == None:
                print("Collected borrow rates for " + exchange)
            else:
                logger.info("Collected borrow rates for " + exchange)

            return True

        except Exception as e:
            if logger == None:
                print(exchange +" borrow rates " + str(e))
                print("Unable to collect borrow rates for " + exchange)
            else:
                logger.error(exchange +" borrow rates " + str(e))
                logger.error("Unable to collect borrow rates for " + exchange)

            return True
