import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone
import ccxt 
import time

from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper
from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()


class FundingRates:
    def __init__(self, db):
        if os.getenv("mode") == "testing":
            self.runs_db = MongoDB(config["mongo_db"], "runs")
            self.funding_rates_db = MongoDB(config["mongo_db"], db)
        else:
            self.funding_rates_db = database_connector(db)
            self.runs_db = database_connector("runs")

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

                            if len(fundingRatesValue[symbol]) > 0 or datetime.now(timezone.utc).timestamp() > (last_time + 28800000):
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
                    print("An error occurred in Funding Rates:", e)
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
            log.error(e)
            return True
