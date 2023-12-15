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


class FundingRates:
    def __init__(self, db, collection):
        # if os.getenv("mode") == "testing":
        #     self.runs_db = MongoDB(config["mongo_db"], "runs")
        #     self.funding_rates_db = MongoDB(config["mongo_db"], db)
        # else:
        #     self.funding_rates_db = database_connector(db)
        #     self.runs_db = database_connector("runs")

        self.runs_db = db['runs']
        self.funding_rates_db = db['funding_rates']
        self.borrow_rates_db = db['borrow_rates']
        self.long_funding_db = db['long_funding']
        self.short_funding_db = db['short_funding']

    def get(
        self,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        exchange: str = None,
        symbol: str = None,
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
        if symbol:
            pipeline.append({"$match": {"symbol": symbol}})

        try:
            results = self.funding_rates_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

    def create(
        self,
        client: str = None,
        exch = None,
        exchange: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        fundingRatesValue: str = None,
        symbols: str = None,
        back_off = {},
        logger=None
    ):
        if symbols is None:
            if exchange == "binance":
                symbols = config["funding_rates"]["symbols"]["binance_usdt"] + config["funding_rates"]["symbols"]["binance_usd"]
                # symbols_d = config["funding_rates"]["symbols"]["binance_usd"]
            elif exchange == "okx":
                symbols = config["funding_rates"]["symbols"]["okx_usdt"] + config["funding_rates"]["symbols"]["okx_usd"]
                # symbols_d = config["funding_rates"]["symbols"]["okx_usd"]
            elif exchange == "bybit":
                symbols = config["funding_rates"]["symbols"]["bybit_usdt"] + config["funding_rates"]["symbols"]["bybit_usd"]
                # symbols_d = config["funding_rates"]["symbols"]["bybit_usd"]

        if fundingRatesValue is None:
            if exch == None:
                exch = Exchange(exchange).exch()
            
            fundingRatesValue = {}
            scalar = 1

            for symbol in symbols:
                query = {}
                if exchange:
                    query["venue"] = exchange
                query["symbol"] = symbol.split(":")[0]

                try:
                    funding_rate_values = (
                        self.funding_rates_db.find(query).sort("_id", -1).limit(1)
                    )

                    current_values = None
                    for item in funding_rate_values:
                        current_values = item["funding_rates_value"]

                    last_time = 0
                    if current_values is None:
                        if exchange == "okx":
                            fundingRatesValue[symbol] = OKXHelper().get_funding_rates(
                                exch=exch, limit=100, symbol=symbol
                            )

                            funding_rate = OKXHelper().get_funding_rate(
                                exch=exch,
                                symbol=symbol
                            )
                            current_funding_rate = {}
                            current_funding_rate["info"] = funding_rate['info']
                            current_funding_rate["symbol"] = symbol
                            current_funding_rate["fundingRate"] = funding_rate["fundingRate"]
                            current_funding_rate["info"]["realizedRate"] = funding_rate["fundingRate"]
                            current_funding_rate["timestamp"] = funding_rate["fundingTimestamp"]
                            current_funding_rate["datetime"] = funding_rate["fundingDatetime"]
                            fundingRatesValue[symbol].append(current_funding_rate)

                        elif exchange == "binance":
                            fundingRatesValue[symbol] = Helper().get_funding_rates(
                                exch=exch, limit=100, symbol=symbol
                            )
                        elif exchange == "bybit":
                            fundingRatesValue[symbol] = BybitHelper().get_funding_rates(
                                exch=exch, limit=100, symbol=symbol
                            )
                    else:
                        last_time = int(current_values['timestamp']) + 1
                        if exchange == "okx":
                            fundingRatesValue[symbol] = OKXHelper().get_funding_rates(
                                exch=exch, limit=100, symbol=symbol, since=last_time + 28800000
                            )

                            if len(fundingRatesValue[symbol]) > 0 or (datetime.now(timezone.utc).timestamp() * 1000) > (last_time + 28800000):
                                funding_rate = OKXHelper().get_funding_rate(
                                    exch=exch,
                                    symbol=symbol
                                )

                                current_funding_rate = {}
                                current_funding_rate["info"] = funding_rate['info']
                                current_funding_rate["symbol"] = symbol
                                current_funding_rate["fundingRate"] = funding_rate["fundingRate"]
                                current_funding_rate["info"]["realizedRate"] = funding_rate["fundingRate"]
                                current_funding_rate["timestamp"] = funding_rate["fundingTimestamp"]
                                current_funding_rate["datetime"] = funding_rate["fundingDatetime"]
                                fundingRatesValue[symbol].append(current_funding_rate)

                        elif exchange == "binance":
                            fundingRatesValue[symbol] = Helper().get_funding_rates(
                                exch=exch, limit=100, symbol=symbol, since=last_time
                            )

                        elif exchange == "bybit":
                            fundingRatesValue[symbol] = BybitHelper().get_funding_rates(
                                exch=exch, limit=100, symbol=symbol, since=last_time #params={'startTime': last_time, 'endTime': datetime.now(timezone.utc).timestamp()}
                            )

                    if len(fundingRatesValue[symbol]) > 0:
                        if exchange == "okx":
                            funding_rate = OKXHelper().get_funding_rate(exch=exch, symbol=symbol)

                            scalar = 1

                            if config["funding_rates"]["period"] == "daily":
                                scalar = 365
                            elif config["funding_rates"]["period"] == "interval":
                                scalar = 24 * 365 * 3600000
                                scalar /= int(funding_rate["nextFundingTimestamp"]) - int(
                                    funding_rate["fundingTimestamp"]
                                )

                            for item in fundingRatesValue[symbol]:
                                item["nextFundingRate"] = funding_rate["nextFundingRate"]
                                item["nextFundingTime"] = funding_rate["nextFundingTimestamp"]
                                item["base"] = symbol.split("/")[0]
                                item["quote"] = symbol.split("/")[1].split(":")[0]
                                item["scalar"] = scalar

                                if (
                                    config["funding_rates"][exchange]["valid"]
                                    == "valid_to"
                                ):
                                    item["timestamp"] = int(item["timestamp"]) - (
                                        int(funding_rate["nextFundingTimestamp"])
                                        - int(funding_rate["fundingTimestamp"])
                                    )
                                    item["nextFundingTime"] = int(item["nextFundingTime"]) - (
                                        int(funding_rate["nextFundingTimestamp"])
                                        - int(funding_rate["fundingTimestamp"])
                                    )

                        elif exchange == "binance":
                            funding_rate = Helper().get_funding_rate(
                                exch=exch, symbol=symbol
                            )

                            scalar = 1

                            if config["funding_rates"]["period"] == "daily":
                                scalar = 365
                            elif config["funding_rates"]["period"] == "interval":
                                scalar = 24 * 365 * 3600000
                                scalar /= int(funding_rate["fundingTimestamp"]) - int(
                                    fundingRatesValue[symbol][-1]["timestamp"]
                                )

                            for item in fundingRatesValue[symbol]:
                                item["nextFundingRate"] = funding_rate["fundingRate"]
                                item["nextFundingTime"] = funding_rate["fundingTimestamp"]
                                item["base"] = symbol.split("/")[0]
                                item["quote"] = symbol.split("/")[1].split(":")[0]
                                item["scalar"] = scalar
                        
                        elif exchange == "bybit":
                            funding_rate = BybitHelper().get_funding_rate(
                                exch=exch, symbol=symbol
                            )

                            scalar = 1

                            if config["funding_rates"]["period"] == "daily":
                                scalar = 365
                            elif config["funding_rates"]["period"] == "interval":
                                scalar = 24 * 365 * 3600000
                                scalar /= int(funding_rate["fundingTimestamp"]) - int(
                                    fundingRatesValue[symbol][-1]["timestamp"]
                                )

                            for item in fundingRatesValue[symbol]:
                                item["nextFundingRate"] = funding_rate["fundingRate"]
                                item["nextFundingTime"] = funding_rate["fundingTimestamp"]
                                item["base"] = symbol.split("/")[0]
                                item["quote"] = symbol.split("/")[1].split(":")[0]
                                item["scalar"] = scalar

                        try:
                            run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

                            latest_run_id = 0
                            for item in run_ids:
                                try:
                                    latest_run_id = item["runid"]
                                except:
                                    pass
                            
                            borrow_ccy = "USDT" if fundingRatesValue[symbol][-1]['quote'] == "USDT" else fundingRatesValue[symbol][-1]['base']
                            
                            pipeline = []
                            pipeline.append({"$match": {"venue": exchange}})
                            pipeline.append({"$match": {"code": borrow_ccy}})
                            pipeline.append({"$match": {"borrow_rates_value.timestamp": {"$gte": max(last_time - 1, fundingRatesValue[symbol][-1]["timestamp"] - 28800000), "$lt": fundingRatesValue[symbol][-1]["timestamp"]}}})

                            borrow_rates = list(self.borrow_rates_db.aggregate(pipeline))
                            borrow_rate = 0
                            try:
                                borrow_rate = sum([item['borrow_rates_value']['rate'] * item['borrow_rates_value']['scalar'] for item in borrow_rates]) / len(borrow_rates)
                            except:
                                pass
                            
                            long_fundings = []

                            for i in range(1, 7):
                                for base_ccy in config['funding_rates']['base_ccy']:
                                    if base_ccy == borrow_ccy:
                                        funding = i * fundingRatesValue[symbol][-1]["fundingRate"] * fundingRatesValue[symbol][-1]["scalar"] - (i-1) * borrow_rate
                                    else:
                                        funding = i * fundingRatesValue[symbol][-1]["fundingRate"] * fundingRatesValue[symbol][-1]["scalar"] - i * borrow_rate

                                    long_fundings.append({
                                        'venue': exchange,
                                        'base': fundingRatesValue[symbol][-1]['base'],
                                        'quote': fundingRatesValue[symbol][-1]['quote'],
                                        'base_ccy': base_ccy,
                                        'n': i,
                                        'long_funding_value': {
                                            'symbol': fundingRatesValue[symbol][-1]['base'] + "/" + fundingRatesValue[symbol][-1]['quote'],
                                            'funding': funding,
                                            'timestamp': fundingRatesValue[symbol][-1]["timestamp"]
                                        },
                                        'runid': latest_run_id,
                                        'timestamp': datetime.now(timezone.utc)
                                    })

                            self.long_funding_db.insert_many(long_fundings)

                            pipeline = []
                            pipeline.append({"$match": {"venue": exchange}})
                            pipeline.append({"$match": {"code": symbol.split("/")[0]}})
                            pipeline.append({"$match": {"borrow_rates_value.timestamp": {"$gte": max(last_time - 1, fundingRatesValue[symbol][-1]["timestamp"] - 28800000), "$lt": fundingRatesValue[symbol][-1]["timestamp"]}}})

                            borrow_rates = list(self.borrow_rates_db.aggregate(pipeline))
                            borrow_rate = 0
                            try:
                                borrow_rate = sum([item['borrow_rates_value']['rate'] * item['borrow_rates_value']['scalar'] for item in borrow_rates]) / len(borrow_rates)
                            except:
                                pass

                            short_fundings = []

                            for i in range(1, 7):
                                short_fundings.append({
                                    'venue': exchange,
                                    'base': fundingRatesValue[symbol][-1]['base'],
                                    'quote': fundingRatesValue[symbol][-1]['quote'],
                                    'n': i,
                                    'short_funding_value': {
                                        'symbol': fundingRatesValue[symbol][-1]['base'] + "/" + fundingRatesValue[symbol][-1]['quote'],
                                        'funding': -i * fundingRatesValue[symbol][-1]["fundingRate"] - (i-1) * borrow_rate,
                                        'timestamp': fundingRatesValue[symbol][-1]["timestamp"]
                                    },
                                    'runid': latest_run_id,
                                    'timestamp': datetime.now(timezone.utc)
                                })

                            self.short_funding_db.insert_many(short_fundings)

                        except Exception as e:
                            logger.warning(exchange + " spot funding " + str(e))

                    else:
                        if exchange == "binance":
                            funding_rate = Helper().get_funding_rate(
                                exch=exch, symbol=symbol
                            )

                            self.funding_rates_db.update_one(
                                {
                                    "venue": exchange,
                                    "symbol": symbol.split(":")[0],
                                    "funding_rates_value.timestamp": current_values['timestamp']
                                },
                                {
                                    "$set": {
                                        "funding_rates_value.nextFundingRate": funding_rate['fundingRate']
                                    }
                                },
                                upsert=False
                            )
                        elif exchange == "bybit":
                            funding_rate = BybitHelper().get_funding_rate(
                                exch=exch, symbol=symbol
                            )

                            self.funding_rates_db.update_one(
                                {
                                    "venue": exchange,
                                    "symbol": symbol.split(":")[0],
                                    "funding_rates_value.timestamp": current_values['timestamp']
                                },
                                {
                                    "$set": {
                                        "funding_rates_value.nextFundingRate": funding_rate['fundingRate']
                                    }
                                },
                                upsert=False
                            )
                        elif exchange == "okx":
                            funding_rate = OKXHelper().get_funding_rate(exch=exch, symbol=symbol)

                            self.funding_rates_db.update_one(
                                {
                                    "venue": exchange,
                                    "symbol": symbol.split(":")[0],
                                    "funding_rates_value.timestamp": current_values['timestamp']
                                },
                                {
                                    "$set": {
                                        "funding_rates_value.nextFundingRate": funding_rate['nextFundingRate']
                                    }
                                },
                                upsert=False
                            )
                        pass

                # except ccxt.InvalidNonce as e:
                #     print("Hit rate limit", e)
                #     time.sleep(back_off[exchange] / 1000.0)
                #     back_off[exchange] *= 2
                #     return True
                
                except ccxt.ExchangeError as e:
                    logger.warning(exchange + " funding rates " + str(e))
                    # print("An error occurred in Funding Rates:", e)
                    pass

            # for symbol in symbols_d:
            #     if exchange == "binance":
            #         query = {}
            #         if exchange:
            #             query["venue"] = exchange
            #         query["symbol"] = symbol

            #         try:
            #             funding_rate_values = (
            #                 self.funding_rates_db.find(query).sort("_id", -1).limit(1)
            #             )

            #             current_values = None
            #             for item in funding_rate_values:
            #                 current_values = item["funding_rates_value"]

            #             if current_values is None:
            #                 fundingRatesValue[symbol] = Helper().get_funding_rates(
            #                     exch=exch, symbol=symbol, limit=100
            #                 )
            #             else:
            #                 last_time = int(current_values["timestamp"]) + 1

            #                 fundingRatesValue[symbol] = Helper().get_funding_rates(
            #                     exch=exch, symbol=symbol, limit=100, since=last_time
            #                 )

            #             if len(fundingRatesValue[symbol]) > 0:
            #                 funding_rate = Helper().get_funding_rate(
            #                     exch=exch, symbol=symbol
            #                 )

            #                 scalar = 1

            #                 if config["funding_rates"]["period"] == "daily":
            #                     scalar = 365
            #                 elif config["funding_rates"]["period"] == "interval":
            #                     scalar = 24 * 365 * 3600000
            #                     scalar /= int(funding_rate["fundingTimestamp"]) - int(
            #                         fundingRatesValue[symbol][-1]["timestamp"]
            #                     )

            #                 for item in fundingRatesValue[symbol]:
            #                     item["nextFundingRate"] = funding_rate["fundingRate"]
            #                     item["nextFundingTime"] = funding_rate["fundingTimestamp"]
            #                     item["base"] = symbol.split("/")[0]
            #                     item["quote"] = "USD"
            #                     item["scalar"] = scalar
                        
                    
            #         except ccxt.InvalidNonce as e:
            #             print("Hit rate limit", e)
            #             time.sleep(back_off[exchange] / 1000.0)
            #             back_off[exchange] *= 2
            #             return False
                
            #         except Exception as e:
            #             print("An error occurred in Funding Rates:", e)
            #             pass

            #     # elif exchange == "bybit":
            #     #     query = {}
            #     #     if exchange:
            #     #         query["venue"] = exchange
            #     #     query["symbol"] = symbol

            #     #     try:
            #     #         funding_rate_values = (
            #     #             self.funding_rates_db.find(query).sort("_id", -1).limit(1)
            #     #         )

            #     #         current_values = None
            #     #         for item in funding_rate_values:
            #     #             current_values = item["funding_rates_value"]

            #     #         if current_values is None:
            #     #             fundingRatesValue[symbol] = BybitHelper().get_funding_rates(
            #     #                 symbol=symbol, limit=100, exch=exch
            #     #             )
            #     #             print(fundingRatesValue[symbol])
            #     #         else:
            #     #             last_time = int(current_values["timestamp"]) + 1

            #     #             fundingRatesValue[symbol] = Helper().get_funding_rates_dapi(
            #     #                 exch=exch,
            #     #                 params={
            #     #                     "limit": 100,
            #     #                     "symbol": symbol,
            #     #                     "startTime": last_time,
            #     #                 },
            #     #             )

            #     #         for item in fundingRatesValue[symbol]:
            #     #             item["timestamp"] = item["fundingTime"]
            #     #             item["fundingRate"] = float(item["fundingRate"])
            #     #             item["base"] = item["symbol"][:-8]
            #     #             item["quote"] = "USD"
            #     #             item.pop("fundingTime", None)
            #     #             if scalar is not None:
            #     #                 item["scalar"] = scalar
                    
            #     #     except ccxt.InvalidNonce as e:
            #     #         print("Hit rate limit", e)
            #     #         time.sleep(back_off[exchange] / 1000.0)
            #     #         back_off[exchange] *= 2
            #     #         return False
                
            #     #     except Exception as e:
            #     #         print("An error occurred in Funding Rates:", e)
            #     #         pass
                    
            #     elif exchange == "okx":
            #         query = {}
            #         if exchange:
            #             query["venue"] = exchange
            #         query["symbol"] = symbol

            #         try:
            #             funding_rate_values = (
            #                 self.funding_rates_db.find(query).sort("_id", -1).limit(1)
            #             )

            #             current_values = None
            #             for item in funding_rate_values:
            #                 current_values = item["funding_rates_value"]

            #             if current_values is None:
            #                 fundingRatesValue[symbol] = OKXHelper().get_funding_rates(
            #                     exch=exch, limit=100, symbol=symbol
            #                 )
            #             else:
            #                 last_time = int(current_values["timestamp"]) + 1

            #                 fundingRatesValue[symbol] = OKXHelper().get_funding_rates(
            #                     exch=exch, limit=100, symbol=symbol, since=last_time
            #                 )

            #             if len(fundingRatesValue[symbol]) > 0:
            #                 funding_rate = OKXHelper().get_funding_rate(
            #                     exch=exch, symbol=symbol
            #                 )

            #                 current_funding_rate = {}

            #                 current_funding_rate["info"] = funding_rate['info']
            #                 current_funding_rate["symbol"] = symbol
            #                 current_funding_rate["fundingRate"] = funding_rate["fundingRate"]
            #                 current_funding_rate["info"]["realizedRate"] = funding_rate["fundingRate"]
            #                 current_funding_rate["timestamp"] = funding_rate["fundingTimestamp"]
            #                 current_funding_rate["datetime"] = funding_rate["fundingDatetime"]
            #                 fundingRatesValue[symbol].append(current_funding_rate)

            #                 scalar = 1

            #                 if config["funding_rates"]["period"] == "daily":
            #                     scalar = 365
            #                 elif config["funding_rates"]["period"] == "interval":
            #                     scalar = 24 * 365 * 3600000
            #                     scalar /= int(funding_rate["nextFundingTimestamp"]) - int(
            #                         funding_rate["fundingTimestamp"]
            #                     )

            #                 for item in fundingRatesValue[symbol]:
            #                     item["nextFundingRate"] = funding_rate["nextFundingRate"]
            #                     item["nextFundingTime"] = funding_rate["nextFundingTimestamp"]
            #                     item["fundingRate"] = float(item["fundingRate"])
            #                     item["base"] = symbol.split("/")[0]
            #                     item["quote"] = "USD"
            #                     item.pop("fundingTime", None)
            #                     item["scalar"] = scalar
            #                     if config["funding_rates"][exchange]["valid"] == "valid_to":
            #                         item["timestamp"] = int(item["timestamp"]) - (
            #                             int(funding_rate["nextFundingTimestamp"])
            #                             - int(funding_rate["fundingTimestamp"])
            #                         )
            #                         item["nextFundingTime"] = int(item["nextFundingTime"]) - (
            #                             int(funding_rate["nextFundingTimestamp"])
            #                             - int(funding_rate["fundingTimestamp"])
            #                         )

            #         except ccxt.InvalidNonce as e:
            #             print("Hit rate limit", e)
            #             time.sleep(back_off[exchange] / 1000.0)
            #             back_off[exchange] *= 2
            #             return False
                
            #         except Exception as e:
            #             print("An error occurred in Funding Rates:", e)
            #             pass

        # back_off[exchange] = config['dask']['back_off']
        
        # if 'symbols_d' in globals() or 'symbols_d' in locals():
        #     symbols = symbols + symbols_d

        flag = False
        for symbol in symbols:
            if symbol in fundingRatesValue.keys():
                if len(fundingRatesValue[symbol]) > 0:
                    flag = True
                    break

        if flag == False:
            return True

        funding_rates = []

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass

        for symbol in symbols:
            if symbol in fundingRatesValue.keys():
                for item in fundingRatesValue[symbol]:
                    new_value = {
                        "venue": exchange,
                        "funding_rates_value": item,
                        "symbol": item['base'] + "/" + item['quote'],
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

                    funding_rates.append(new_value)

        del fundingRatesValue

        if len(funding_rates) <= 0:
            return True

        try:
            self.funding_rates_db.insert_many(funding_rates)

            del funding_rates

            return True

            # return funding_rates

        except Exception as e:
            logger.error(exchange + " funding rates " + str(e))
            return True
