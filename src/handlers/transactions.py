import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

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

    def get_last_funding_payments(
        self,
        client,
        exchange,
        account,
        start_date,
        field_name,
    ):
        pipeline = [
            {"$sort": {"_id": -1}},
        ]
        pipeline.append({"$match": {"client": client}})
        pipeline.append({"$match": {"venue": exchange}})
        pipeline.append({"$match": {"account": account}})
        pipeline.append(
            {
                "$match": {
                    "$or": [
                        {"transaction_value.incomeType": "FUNDING_FEE"},
                        {"transaction_value.subType": "FUNDING_FEE"},
                        {"transaction_value.type": "FUNDING_FEE"},
                    ]
                }
            }
        )
        pipeline.append(
            {"$match": {"transaction_value.timestamp": {"$gte": start_date}}}
        )

        pipeline.append(
            {
                "$group": {
                    "_id": {
                        "client": "$client",
                        "venue": "$venue",
                        "account": "$account",
                    },
                    "client": {"$last": "$client"},
                    "venue": {"$last": "$venue"},
                    "account": {"$last": "$account"},
                    field_name: {"$sum": "$transaction_value.income"},
                    "timestamp": {"$last": "$timestamp"},
                }
            }
        )

        pipeline.append({"$project": {"_id": 0}})

        pipeline.append(
            {"$merge": {"into": "funding_payments", "whenMatched": "replace"}}
        )

        try:
            self.transactions_db.aggregate(pipeline)

        except Exception as e:
            log.error(e)

    def get_last_borrow_payments(
        self,
        client,
        exchange,
        account,
        start_date,
        field_name,
    ):
        pipeline = [
            {"$sort": {"_id": -1}},
        ]
        pipeline.append({"$match": {"client": client}})
        pipeline.append({"$match": {"venue": exchange}})
        pipeline.append({"$match": {"account": account}})
        pipeline.append(
            {
                "$match": {
                    "$or": [
                        {"transaction_value.incomeType": "BORROW"},
                        {"transaction_value.subType": "BORROW"},
                        {"transaction_value.type": "BORROW"},
                    ]
                }
            }
        )
        pipeline.append(
            {"$match": {"transaction_value.timestamp": {"$gte": start_date}}}
        )

        pipeline.append(
            {
                "$group": {
                    "_id": {
                        "client": "$client",
                        "venue": "$venue",
                        "account": "$account",
                    },
                    "client": {"$last": "$client"},
                    "venue": {"$last": "$venue"},
                    "account": {"$last": "$account"},
                    field_name: {"$sum": "$transaction_value.income"},
                    "timestamp": {"$last": "$timestamp"},
                }
            }
        )

        pipeline.append({"$project": {"_id": 0}})

        pipeline.append(
            {"$merge": {"into": "borrow_payments", "whenMatched": "replace"}}
        )

        try:
            self.transactions_db.aggregate(pipeline)

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
                        exch=exch, params={"limit": 100}
                    )
                    futures_trades = Mapping().mapping_transactions(
                        exchange=exchange, transactions=futures_trades
                    )
                    spot_trades = Helper().get_spot_transactions(
                        exch=exch, params={"symbol": symbol}
                    )
                    spot_trades = Mapping().mapping_transactions(
                        exchange=exchange, transactions=spot_trades
                    )

                    transaction_value = {"future": futures_trades, "spot": spot_trades}

            elif config["transactions"]["store_type"] == "timeseries":
                if exchange == "okx":
                    query = {}
                    if client:
                        query["client"] = client
                    if exchange:
                        query["venue"] = exchange
                    if sub_account:
                        query["account"] = sub_account

                    transactions_values = (
                        self.transactions_db.find(query)
                        .sort("transaction_value.timestamp", -1)
                        .limit(1)
                    )

                    current_value = None
                    for item in transactions_values:
                        current_value = item["transaction_value"]

                    if current_value is None:
                        transactions = OKXHelper().get_transactions(exch=exch)
                        transaction_value = Mapping().mapping_transactions(
                            exchange=exchange, transactions=transactions
                        )
                    else:
                        if config["transactions"]["fetch_type"] == "id":
                            last_id = current_value["billId"]
                            transactions = OKXHelper().get_transactions(
                                exch=exch, params={"before": last_id}
                            )
                            transaction_value = Mapping().mapping_transactions(
                                exchange=exchange, transactions=transactions
                            )
                        elif config["transactions"]["fetch_type"] == "time":
                            last_time = current_value["timestamp"]
                            transactions = OKXHelper().get_transactions(
                                exch=exch, params={"begin": last_time}
                            )
                            transaction_value = Mapping().mapping_transactions(
                                exchange=exchange, transactions=transactions
                            )
                elif exchange == "binance":
                    query = {}
                    if client:
                        query["client"] = client
                    if exchange:
                        query["venue"] = exchange
                    if sub_account:
                        query["account"] = sub_account
                    query["trade_type"] = "future"

                    transactions_values = (
                        self.transactions_db.find(query)
                        .sort("transaction_value.timestamp", -1)
                        .limit(1)
                    )

                    current_value = None
                    for item in transactions_values:
                        current_value = item["transaction_value"]

                    if current_value is None:
                        futures_trades = Helper().get_future_transactions(
                            exch=exch, params={"limit": 100}
                        )
                        futures_trades = Mapping().mapping_transactions(
                            exchange=exchange, transactions=futures_trades
                        )

                        transaction_value = {
                            "future": futures_trades,
                        }
                    else:
                        last_time = current_value["timestamp"]
                        futures_trades = Helper().get_future_transactions(
                            exch=exch,
                            params={
                                "startTime": last_time,
                                "limit": 100,
                                "symbol": symbol,
                            },
                        )
                        futures_trades = Mapping().mapping_transactions(
                            exchange=exchange, transactions=futures_trades
                        )

                        transaction_value = {
                            "future": futures_trades,
                        }

                    query["trade_type"] = "spot"

                    transactions_values = (
                        self.transactions_db.find(query)
                        .sort("transaction_value.timestamp", -1)
                        .limit(1)
                    )

                    current_value = None
                    for item in transactions_values:
                        current_value = item["transaction_value"]

                    if current_value is None:
                        spot_trades = Helper().get_spot_transactions(
                            exch=exch, params={"symbol": symbol, "limit": 100}
                        )
                        spot_trades = Mapping().mapping_transactions(
                            exchange=exchange, transactions=spot_trades
                        )

                        transaction_value["spot"] = spot_trades

                    else:
                        if config["transactions"]["fetch_type"] == "id":
                            last_id = current_value["id"]
                            spot_trades = Helper().get_spot_transactions(
                                exch=exch,
                                params={
                                    "fromId": last_id,
                                    "symbol": symbol,
                                    "limit": 100,
                                },
                            )

                        elif config["transactions"]["fetch_type"] == "time":
                            last_time = current_value["timestamp"]
                            spot_trades = Helper().get_spot_transactions(
                                exch=exch,
                                params={
                                    "startTime": last_time,
                                    "symbol": symbol,
                                    "limit": 100,
                                },
                            )

                        spot_trades = Mapping().mapping_transactions(
                            exchange=exchange, transactions=spot_trades
                        )
                        transaction_value["spot"] = spot_trades

        current_time = datetime.now(timezone.utc)
        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass

        if config["transactions"]["store_type"] == "snapshot":
            transaction = {
                "client": client,
                "venue": exchange,
                "account": "Main Account",
                "transaction_value": transaction_value,
                "runid": latest_run_id,
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

        elif config["transactions"]["store_type"] == "timeseries":
            transaction = []

            if exchange == "okx":
                if len(transaction_value) == 0:
                    return False

                for item in transaction_value:
                    item['timestamp'] = int(item["timestamp"])
                    item['income'] = float(item["income"])
                    new_value = {
                        "client": client,
                        "venue": exchange,
                        "account": "Main Account",
                        "transaction_value": item,
                        "runid": latest_run_id,
                        "active": True,
                        "entry": False,
                        "exit": False,
                        "timestamp": current_time,
                    }
                    
                    if sub_account:
                        new_value["account"] = sub_account
                    if spot:
                        new_value["spotMarket"] = spot
                    if future:
                        new_value["futureMarket"] = future
                    if perp:
                        new_value["perpMarket"] = perp

                    transaction.append(new_value)

            if exchange == "binance":
                if len(transaction_value["future"]) > 0:
                    for item in transaction_value["future"]:
                        item['timestamp'] = int(item["timestamp"])
                        item['income'] = float(item["income"])
                        new_value = {
                            "client": client,
                            "venue": exchange,
                            "account": "Main Account",
                            "transaction_value": item,
                            "trade_type": "future",
                            "runid": latest_run_id,
                            "active": True,
                            "entry": False,
                            "exit": False,
                            "timestamp": current_time,
                        }
                        if sub_account:
                            new_value["account"] = sub_account
                        if spot:
                            new_value["spotMarket"] = spot
                        if future:
                            new_value["futureMarket"] = future
                        if perp:
                            new_value["perpMarket"] = perp

                        transaction.append(new_value)

                if len(transaction_value["spot"]) > 0:
                    for item in transaction_value["spot"]:
                        item['timestamp'] = int(item["timestamp"])
                        # item['income'] = float(item["income"])
                        new_value = {
                            "client": client,
                            "venue": exchange,
                            "account": "Main Account",
                            "transaction_value": item,
                            "trade_type": "spot",
                            "runid": latest_run_id,
                            "active": True,
                            "entry": False,
                            "exit": False,
                            "timestamp": current_time,
                        }
                        if sub_account:
                            new_value["account"] = sub_account
                        if spot:
                            new_value["spotMarket"] = spot
                        if future:
                            new_value["futureMarket"] = future
                        if perp:
                            new_value["perpMarket"] = perp

                        transaction.append(new_value)

                if len(transaction) == 0:
                    return False

        try:
            if config["transactions"]["store_type"] == "snapshot":
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

            elif config["transactions"]["store_type"] == "timeseries":
                self.transactions_db.insert_many(transaction)

            start_date = int((current_time - timedelta(days=28)).timestamp() * 1000)
            self.get_last_funding_payments(
                client=client,
                exchange=exchange,
                account=sub_account,
                start_date=start_date,
                field_name="last_28d",
            )
            self.get_last_borrow_payments(
                client=client,
                exchange=exchange,
                account=sub_account,
                start_date=start_date,
                field_name="last_28d",
            )

            # log.debug(f"transaction created: {transaction}")
            return transaction
        except Exception as e:
            log.error(e)
            return False
