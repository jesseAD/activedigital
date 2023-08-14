import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.lib.mapping import Mapping
from src.config import read_config_file
from src.handlers.helpers import Helper
from src.handlers.helpers import OKXHelper
from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()


class Transactions:
    def __init__(self, db):
        if config["mode"] == "testing":
            self.runs_db = MongoDB(config["mongo_db"], "runs")
            self.transactions_db = MongoDB(config["mongo_db"], db)
        else:
            self.runs_db = database_connector("runs")
            self.transactions_db = database_connector(db)

    def get(
        self,
        client,
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
        if client:
            pipeline.append({"$match": {"client": client}})
        if exchange:
            pipeline.append({"$match": {"venue": exchange}})
        if account:
            pipeline.append({"$match": {"account": account}})

        try:
            results = self.transactions_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

    def create(
        self,
        client,
        exchange: str = None,
        positionType: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        transaction_value: str = None,
        symbol: str = None,
    ):
        if transaction_value is None:
            spec = exchange.upper() + "_" + sub_account.upper() + "_"
            API_KEY = os.getenv(spec + "API_KEY")
            API_SECRET = os.getenv(spec + "API_SECRET")
            exch = Exchange(exchange, sub_account, API_KEY, API_SECRET).exch()

            if config["transactions"]["store_type"] == "snapshot":
                if exchange == "okx":
                    transactions = OKXHelper().get_transactions(exch=exch)
                    transaction_value = Mapping().mapping_transactions(
                        exchange=exchange, transactions=transactions
                    )

                elif exchange == "binance":
                    futures_trades = Helper().get_future_transactions(
                        exch=exch,
                        params={"limit": 500}
                    )
                    futures_trades = Mapping().mapping_transactions(
                        exchange=exchange, transactions=futures_trades
                    )
                    spot_trades = Helper().get_spot_transactions(
                        exch=exch,
                        params={"symbol": symbol}
                    )

                    transaction_value = {"future": futures_trades, "spot": spot_trades}

            elif config["transactions"]["store_type"] == "timeseries":
                query = {}
                if client:
                    query["client"] = client
                if exchange:
                    query["venue"] = exchange
                if sub_account:
                    query["account"] = sub_account
                transactions_values = self.transactions_db.find(query)

                current_values = None
                for item in transactions_values:
                    current_values = item["transaction_value"]

                if current_values is None:
                    if exchange == "okx":
                        transactions = OKXHelper().get_transactions(exch=exch)
                        transaction_value = Mapping().mapping_transactions(
                            exchange=exchange, transactions=transactions
                        )

                    elif exchange == "binance":
                        futures_trades = Helper().get_future_transactions(
                            exch=exch,
                            params={"limit": 500}
                        )
                        futures_trades = Mapping().mapping_transactions(
                            exchange=exchange, transactions=futures_trades
                        )
                        spot_trades = Helper().get_spot_transactions(
                            exch=exch,
                            params={"symbol": symbol, "limit": 500}
                        )

                        transaction_value = {
                            "future": futures_trades,
                            "spot": spot_trades,
                        }
                else:
                    if exchange == "okx":
                        if config["transactions"]["fetch_type"] == "id":
                            last_id = current_values[0]["billId"]
                            transactions = OKXHelper().get_transactions(
                                exch=exch,
                                params={"before": last_id}
                            )
                            transactions = Mapping().mapping_transactions(
                                exchange=exchange, transactions=transactions
                            )
                        elif config["transactions"]["fetch_type"] == "time":
                            last_time = current_values[0]["timestamp"]
                            transactions = OKXHelper().get_transactions(
                                exch=exch,
                                params={"begin": last_time}
                            )
                            transactions = Mapping().mapping_transactions(
                                exchange=exchange, transactions=transactions
                            )

                        transaction_value = transactions + current_values

                    elif exchange == "binance":
                        if len(current_values["future"]) != 0:
                            last_time = current_values["future"][0]["timestamp"]
                            futures_trades = Helper().get_future_transactions(
                                exch=exch,
                                params={
                                    "startTime": last_time,
                                    "limit": 500,
                                    "symbol": symbol,
                                }
                            )
                            futures_trades = Mapping().mapping_transactions(
                                exchange=exchange, transactions=futures_trades
                            )
                            futures_trades += current_values["future"]
                        else:
                            futures_trades = Helper().get_future_transactions(
                                exch=exch,
                                params={"limit": 500, "symbol": symbol}
                            )
                            futures_trades = Mapping().mapping_transactions(
                                exchange=exchange, transactions=futures_trades
                            )

                        if len(current_values["spot"]) != 0:
                            if config["transactions"]["fetch_type"] == "id":
                                last_id = current_values["spot"][0]["id"]
                                spot_trades = Helper().get_spot_transactions(
                                    exch=exch,
                                    params={
                                        "fromId": last_id,
                                        "symbol": symbol,
                                        "limit": 500,
                                    }
                                )
                            elif config["transactions"]["fetch_type"] == "time":
                                last_time = current_values["spot"][0]["time"]
                                spot_trades = Helper().get_spot_transactions(
                                    exch=exch,
                                    params={
                                        "startTime": last_time,
                                        "symbol": symbol,
                                        "limit": 500,
                                    }
                                )
                        else:
                            spot_trades = Helper().get_spot_transactions(
                                exch=exch,
                                params={"symbol": symbol, "limit": 500}
                            )

                        transaction_value = {
                            "future": futures_trades,
                            "spot": spot_trades,
                        }

        current_time = datetime.now(timezone.utc)
        transaction = {
            "client": client,
            "venue": exchange,
            "account": "Main Account",
            "transaction_value": transaction_value,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": current_time,
        }

        if sub_account:
            transaction["account"] = sub_account
        if spot:
            transaction["spotMarket"] = spot
        if future:
            transaction["futureMarket"] = future
        if perp:
            transaction["perpMarket"] = perp

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass
        transaction["runid"] = latest_run_id

        if config["transactions"]["store_type"] == "snapshot":
            # get latest transaction data
            query = {}
            if client:
                query["client"] = client
            if exchange:
                query["venue"] = exchange
            if sub_account:
                query["account"] = sub_account

            transactions_values = self.transactions_db.find(query).sort("_id", -1)

            latest_run_id = -1
            latest_value = None
            for item in transactions_values:
                if latest_run_id < item["runid"]:
                    latest_run_id = item["runid"]
                    latest_value = item["transaction_value"]

            if latest_value == transaction["transaction_value"]:
                print("same transaction")
                return False

        try:
            self.transactions_db.update_one(
                {
                    "client": transaction["client"],
                    "venue": transaction["venue"],
                    "account": transaction["account"],
                },
                {
                    "$set": {
                        "transaction_value": transaction["transaction_value"],
                        "timestamp": transaction["timestamp"],
                        "runid": transaction["runid"],
                        "active": transaction["active"],
                        "entry": transaction["entry"],
                        "exit": transaction["exit"],
                    }
                },
                upsert=True,
            )

            # log.debug(f"transaction created: {transaction}")
            return transaction
        except Exception as e:
            log.error(e)
            return False
