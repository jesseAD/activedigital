import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper
from src.handlers.helpers import OKXHelper
from src.lib.mapping import Mapping
from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()


class Fills:
    def __init__(self, db):
        if config["mode"] == "testing":
            self.runs_db = MongoDB(config["mongo_db"], "runs")
            self.fills_db = MongoDB(config["mongo_db"], db)
        else:
            self.fills_db = database_connector(db)
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
            results = self.fills_db.aggregate(pipeline)
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
        fillsValue: str = None,
        symbols: str = None,
    ):
        if symbols is None:
            symbols = config["fills"]["symbols"][exchange]

        if fillsValue is None:
            spec = exchange.upper() + "_" + sub_account.upper() + "_"
            API_KEY = os.getenv(spec + "API_KEY")
            API_SECRET = os.getenv(spec + "API_SECRET")
            exch = Exchange(exchange, sub_account, API_KEY, API_SECRET).exch()

            fillsValue = {}

            for symbol in symbols:
                query = {}
                if client:
                    query["client"] = client
                if exchange:
                    query["venue"] = exchange
                if sub_account:
                    query["account"] = sub_account
                query["symbol"] = symbol

                fills_values = self.fills_db.find(query).sort("_id", -1).limit(1)

                current_value = None
                for item in fills_values:
                    current_value = item["fills_value"]

                if current_value is None:
                    if exchange == "okx":
                        fillsValue[symbol] = Mapping().mapping_fills(
                            exchange=exchange,
                            fills=OKXHelper().get_fills(
                                exch=exch,
                                params={
                                    "instType": "SWAP",
                                    "instId": symbol,
                                    "limit": 100,
                                },
                            ),
                        )
                    else:
                        fillsValue[symbol] = Mapping().mapping_fills(
                            exchange=exchange,
                            fills=Helper().get_fills(
                                exch=exch, params={"symbol": symbol, "limit": 100}
                            ),
                        )
                else:
                    if config["fills"]["fetch_type"] == "time":
                        last_time = int(current_value["timestamp"])
                        if exchange == "okx":
                            fillsValue[symbol] = Mapping().mapping_fills(
                                exchange=exchange,
                                fills=OKXHelper().get_fills(
                                    exch=exch,
                                    params={
                                        "instType": "SWAP",
                                        "instId": symbol,
                                        "limit": 100,
                                        "begin": last_time,
                                    },
                                ),
                            )
                        else:
                            fillsValue[symbol] = Mapping().mapping_fills(
                                exchange=exchange,
                                fills=Helper().get_fills(
                                    exch=exch,
                                    params={
                                        "symbol": symbol,
                                        "limit": 100,
                                        "startTime": last_time,
                                    },
                                ),
                            )
                    elif config["fills"]["fetch_type"] == "id":
                        last_id = int(current_value["id"])
                        if exchange == "okx":
                            fillsValue[symbol] = Mapping().mapping_fills(
                                exchange=exchange,
                                fills=OKXHelper().get_fills(
                                    exch=exch,
                                    params={
                                        "instType": "SWAP",
                                        "instId": symbol,
                                        "limit": 100,
                                        "before": last_id,
                                    },
                                ),
                            )
                        else:
                            fillsValue[symbol] = Mapping().mapping_fills(
                                exchange=exchange,
                                fills=Helper().get_fills(
                                    exch=exch,
                                    params={
                                        "symbol": symbol,
                                        "limit": 100,
                                        "fromId": last_id,
                                    },
                                ),
                            )
                            
        fills = []

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass

        for symbol in symbols:
            for item in fillsValue[symbol]:
                new_value = {
                    "client": client,
                    "venue": exchange,
                    "account": "Main Account",
                    "fills_value": item,
                    "symbol": symbol,
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

                fills.append(new_value)

        try:
            self.fills_db.insert_many(fills)

            return fills
        except Exception as e:
            log.error(e)
            return False
