import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone
import ccxt 
import time

# from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper
# from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()


class BorrowRates:
    def __init__(self, db, collection):
        # if os.getenv("mode") == "testing":
        #     self.runs_db = MongoDB(config["mongo_db"], "runs")
        #     self.borrow_rates_db = MongoDB(config["mongo_db"], db)
        # else:
        #     self.borrow_rates_db = database_connector(db)
        #     self.runs_db = database_connector("runs")

        self.runs_db = db['runs']
        self.borrow_rates_db = db['borrow_rates']

    def close_db(self):
        if os.getenv("mode") == "testing":
            self.runs_db.close()
            self.borrow_rates_db.close()
        else:
            self.borrow_rates_db.database.client.close()
            self.runs_db.database.client.close()

    def get(
        self,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        exchange: str = None,
        account: str = None,
        code: str = None,
    ):
        results = []

        pipeline = [
            {"$sort": {"_id": -1}},
        ]

        if active is not None:
            pipeline.append({"$match": {"active": active}})
        if spot:
            pipeline.append({"$match": {"spotMarket": spot}})
        if future:
            pipeline.append({"$match": {"futureMarket": future}})
        if perp:
            pipeline.append({"$match": {"perpMarket": perp}})
        if exchange:
            pipeline.append({"$match": {"venue": exchange}})
        if account:
            pipeline.append({"$match": {"account": account}})
        if code:
            pipeline.append({"$match": {"code": code}})

        try:
            results = self.borrow_rates_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

    def create(
        self,
        exch = None,
        exchange: str = None,
        code: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        borrowRatesValue: str = None,
        codes: str = None,
        back_off = {},
        logger=None
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

            
                borrow_rate_values = (
                    self.borrow_rates_db.find(query).sort("_id", -1).limit(1)
                )

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
                    logger.warning(exchange +  " borrow rates " + str(e))
                    # print("An error occurred in Borrow Rates:", e)
                    return True

            # except ccxt.InvalidNonce as e:
            #     print("Hit rate limit", e)
            #     time.sleep(back_off[exchange] / 1000.0)
            #     back_off[exchange] *= 2
            #     return True
        
            except ccxt.AuthenticationError as e:
                logger.warning(exchange + " borrow rates " + str(e))
                # print("An error occurred in Borrow Rates:", e)

                for _client in config['clients']:
                    if exchange in config['clients'][_client]['subaccounts']:
                        for _account in config['clients'][_client]['subaccounts'][exchange]:
                            print(_client + exchange + _account)
                            
                            if _account.startswith('sub'):
                                spec = _client.upper() + "_" + exchange.upper() + "_" + _account.upper() + "_"
                                API_KEY = os.getenv(spec + "API_KEY")
                                API_SECRET = os.getenv(spec + "API_SECRET")
                                PASSPHRASE = None
                                if exchange == "okx":
                                    PASSPHRASE = os.getenv(spec + "PASSPHRASE")

                                exch = Exchange(exchange, _account, API_KEY, API_SECRET, PASSPHRASE).exch()

                                borrowRatesValue = {}

                                query = {}
                                if exchange:
                                    query["venue"] = exchange
                                query["code"] = code

                                try:
                                    borrow_rate_values = (
                                        self.borrow_rates_db.find(query).sort("_id", -1).limit(1)
                                    )

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
                                    logger.warning(exchange + " borrow rates " + str(e))
                                    # print("An error occurred in Borrow Rates:", e)
                                    return True
                                except ccxt.NetworkError as e:  
                                    logger.warning(exchange + " borrow rates " + str(e))
                                    return False

                                break
                                                      
                    break

                pass

            except ccxt.ExchangeError as e:
                logger.warning(exchange + " borrow rates " + str(e))
                # print("An error occurred in Borrow Rates:", e)
                return True
            except ccxt.NetworkError as e:
                logger.warning(exchange + " borrow rates " + str(e))
                return False
        
        # back_off[exchange] = config['dask']['back_off']

        if len(borrowRatesValue) <= 0:
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

        try:
            self.borrow_rates_db.insert_many(borrow_rates)

            del borrow_rates

            return True

            # return borrow_rates

        except Exception as e:
            logger.error(exchange +" borrow rates " + str(e))
            return True
