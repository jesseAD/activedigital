import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper
from src.handlers.helpers import OKXHelper
from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()


class FundingRates:
    def __init__(self, db):
        if config["mode"] == "testing":
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
        account: str = None,
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

        try:
            results = self.funding_rates_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

    def create(
        self,
        client,
        exchange: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        fundingRatesValue: str = None,
        symbols: str = None,
    ):
        if symbols is None:
            symbols = config["funding_rates"]["symbols"]

        if fundingRatesValue is None:
            spec = exchange.upper() + "_" + sub_account.upper() + "_"
            API_KEY = os.getenv(spec + "API_KEY")
            API_SECRET = os.getenv(spec + "API_SECRET")
            exch = Exchange(exchange, sub_account, API_KEY, API_SECRET).exch()

            fundingRatesValue = {}

            for symbol in symbols:
                query = {}
                if client:
                    query["client"] = client
                if exchange:
                    query["venue"] = exchange
                if sub_account:
                    query["account"] = sub_account
                query["symbol"] = symbol.split(":")[0]

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
                    else:
                        fundingRatesValue[symbol] = Helper().get_funding_rates(
                            exch=exch, limit=100, symbol=symbol
                        )
                else:
                    last_time = int(current_values["info"]["fundingTime"])
                    if exchange == "okx":
                        fundingRatesValue[symbol] = OKXHelper().get_funding_rates(
                            exch=exch, limit=100, symbol=symbol, since=last_time
                        )
                    else:
                        fundingRatesValue[symbol] = Helper().get_funding_rates(
                            exch=exch, limit=100, symbol=symbol, since=last_time
                        )
                if len(fundingRatesValue[symbol]) > 0:
                    if exchange == "okx":
                        funding_rate = OKXHelper().get_funding_rate(
                            exch=exch,
                            params={
                                "instId": fundingRatesValue[symbol][0]["info"]["instId"]
                            },
                        )["data"][0]

                        for item in fundingRatesValue[symbol]:
                            item["nextFundingRate"] = funding_rate["nextFundingRate"]
                            item["nextFundingTime"] = funding_rate["nextFundingTime"]

                    elif exchange == "binance":
                        funding_rate = Helper().get_mark_prices(
                            exch=exch,
                            params={
                                "symbol": fundingRatesValue[symbol][0]["info"]["symbol"]
                            },
                        )

                        for item in fundingRatesValue[symbol]:
                            item["nextFundingRate"] = funding_rate["lastFundingRate"]
                            item["nextFundingTime"] = funding_rate["nextFundingTime"]

        flag = False
        for symbol in symbols:
            if len(fundingRatesValue[symbol]) > 0:
                flag = True
                break

        if flag == False:
            return []

        funding_rates = []

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass

        for symbol in symbols:
            for item in fundingRatesValue[symbol]:

                new_value = {
                    "client": client,
                    "venue": exchange,
                    "account": "Main Account",
                    "funding_rates_value": item,
                    "symbol": symbol.split(":")[0],
                    "active": True,
                    "entry": False,
                    "exit": False,
                    "timestamp": datetime.now(timezone.utc),
                }

                if sub_account:
                    new_value["account"] = sub_account
                if spot:
                    new_value["spotMarket"] = spot
                if future:
                    new_value["futureMarket"] = future
                if perp:
                    new_value["perpMarket"] = perp

                new_value["runid"] = latest_run_id

                funding_rates.append(new_value)

        if len(funding_rates) <= 0:
            return False

        try:
            self.funding_rates_db.insert_many(funding_rates)

            return funding_rates

        except Exception as e:
            log.error(e)
            return False
