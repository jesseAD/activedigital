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


class BorrowRates:
    def __init__(self, db):
        if config["mode"] == "testing":
            self.runs_db = MongoDB(config["mongo_db"], "runs")
            self.borrow_rates_db = MongoDB(config["mongo_db"], db)
        else:
            self.borrow_rates_db = database_connector(db)
            self.runs_db = database_connector("runs")

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
        client,
        exchange: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        borrowRatesValue: str = None,
        codes: str = None,
    ):
        if codes is None:
            codes = config["borrow_rates"]["codes"]

        if borrowRatesValue is None:
            spec = client.upper() + "_" + exchange.upper() + "_" + sub_account.upper() + "_"
            API_KEY = os.getenv(spec + "API_KEY")
            API_SECRET = os.getenv(spec + "API_SECRET")
            PASSPHRASE = None
            if exchange == "okx":
                PASSPHRASE = os.getenv(spec + "PASSPHRASE")

            exch = Exchange(exchange, sub_account, API_KEY, API_SECRET, PASSPHRASE).exch()

            borrowRatesValue = {}

            for code in codes:
                query = {}
                if client:
                    query["client"] = client
                if exchange:
                    query["venue"] = exchange
                if sub_account:
                    query["account"] = sub_account
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
                            borrowRatesValue[code] = OKXHelper().get_borrow_rates(
                                exch=exch, limit=92, code=code
                            )
                        else:
                            borrowRatesValue[code] = Helper().get_borrow_rates(
                                exch=exch, limit=92, code=code
                            )
                    else:
                        last_time = int(current_values["timestamp"])
                        if exchange == "okx":
                            borrowRatesValue[code] = OKXHelper().get_borrow_rates(
                                exch=exch, limit=92, code=code, since=last_time
                            )
                        else:
                            borrowRatesValue[code] = Helper().get_borrow_rates(
                                exch=exch, limit=92, code=code, since=last_time
                            )
                    if len(borrowRatesValue[code]) > 0:
                        scalar = 1
                        if config['borrow_rates']['period'] != "yearly":
                            scalar = 365

                        if exchange == "okx":
                            borrow_rate = OKXHelper().get_borrow_rate(
                                exch=exch, params={"ccy": code}
                            )["data"][0]["interestRate"]
                            for item in borrowRatesValue[code]:
                                item["nextBorrowRate"] = borrow_rate
                                item['scalar'] = scalar

                        elif exchange == "binance":
                            borrow_rate = Helper().get_borrow_rate(
                                exch=exch, params={"assets": code, "isIsolated": False}
                            )[0]["nextHourlyInterestRate"]
                            for item in borrowRatesValue[code]:
                                item["nextBorrowRate"] = borrow_rate
                                item['scalar'] = scalar
                except:
                    pass

        flag = False
        for code in codes:
            if code in borrowRatesValue.keys():
                if len(borrowRatesValue[code]) > 0:
                    flag = True
                    break

        if flag == False:
            return []

        borrow_rates = []

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass

        for code in codes:
            if code in borrowRatesValue.keys():
                for item in borrowRatesValue[code]:
                    new_value = {
                        "client": client,
                        "venue": exchange,
                        "account": "Main Account",
                        "borrow_rates_value": item,
                        "code": code,
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

                    borrow_rates.append(new_value)

        try:
            self.borrow_rates_db.insert_many(borrow_rates)

            return borrow_rates

        except Exception as e:
            log.error(e)
            return False
