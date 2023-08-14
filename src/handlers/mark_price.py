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


class MarkPrices:
    def __init__(self, db):
        if config["mode"] == "testing":
            self.runs_db = MongoDB(config["mongo_db"], "runs")
            self.positions_db = MongoDB(config["mongo_db"], "positions")
            self.mark_prices_db = MongoDB(config["mongo_db"], db)
        else:
            self.runs_db = database_connector("runs")
            self.positions_db = database_connector("positions")
            self.mark_prices_db = database_connector(db)

    def get(
        self,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        exchange: str = None,
        account: str = None,
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
        if account:
            pipeline.append({"$match": {"account": account}})
        if symbol:
            pipeline.append({"$match": {"symbol": symbol}})

        try:
            results = self.mark_prices_db.aggregate(pipeline)
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
        markPriceValue: str = None,
    ):
        if markPriceValue is None:
            spec = exchange.upper() + "_" + sub_account.upper() + "_"
            API_KEY = os.getenv(spec + "API_KEY")
            API_SECRET = os.getenv(spec + "API_SECRET")
            exch = Exchange(exchange, sub_account, API_KEY, API_SECRET).exch()
            if exchange == "okx":
                markPriceValue = Mapping().mapping_mark_price(
                    exchange=exchange,
                    mark_price=OKXHelper().get_mark_prices(
                        exch=exch,
                        params={"instType": "SWAP", "instId": "BTC-USDT-SWAP"},
                    ),
                )
            else:
                markPriceValue = Mapping().mapping_mark_price(
                    exchange=exchange,
                    mark_price=Helper().get_mark_prices(
                        exch=exch, params={"symbol": "BTCUSDT"}
                    ),
                )

        mark_price = {
            "client": client,
            "venue": exchange,
            "account": "Main Account",
            "mark_price_value": markPriceValue,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }

        if sub_account:
            mark_price["account"] = sub_account
        if spot:
            mark_price["spotMarket"] = spot
        if future:
            mark_price["futureMarket"] = future
        if perp:
            mark_price["perpMarket"] = perp

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass
        mark_price["runid"] = latest_run_id

        # get latest mark_prices data
        query = {}
        if client:
            query["client"] = client
        if exchange:
            query["venue"] = exchange
        if sub_account:
            query["account"] = sub_account

        mark_price_values = self.mark_prices_db.find(query).sort("runid", -1).limit(1)

        latest_run_id = -1
        latest_value = None
        for item in mark_price_values:
            if latest_run_id < item["runid"]:
                latest_run_id = item["runid"]
                latest_value = item["mark_price_value"]

        if latest_value == mark_price["mark_price_value"]:
            print("same mark price")
            return False

        try:
            latest_positions = (
                self.positions_db.find(
                    {
                        "client": mark_price["client"],
                        "venue": mark_price["venue"],
                        "account": mark_price["account"],
                    }
                )
                .sort("runid", -1)
                .limit(1)
            )

            latest_position = None
            for item in latest_positions:
                latest_position = item

            if (
                latest_position is not None
                and len(latest_position["position_value"]) != 0
            ):
                latest_position["position_value"][0]["markPrice"] = mark_price[
                    "mark_price_value"
                ]["markPrice"]

            self.positions_db.update_one(
                {
                    "client": latest_position["client"],
                    "venue": latest_position["venue"],
                    "account": latest_position["account"],
                    "runid": latest_position["runid"],
                },
                {"$set": {"position_value": latest_position["position_value"]}},
            )

            if config["mark_prices"]["store_type"] == "timeseries":
                self.mark_prices_db.insert_one(mark_price)
            elif config["mark_prices"]["store_type"] == "snapshot":
                self.mark_prices_db.update_one(
                    {
                        "client": mark_price["client"],
                        "venue": mark_price["venue"],
                        "account": mark_price["account"],
                        "symbol": mark_price["symbol"],
                    },
                    {
                        "$set": {
                            "mark_price_value": mark_price["mark_price_value"],
                            "active": mark_price["active"],
                            "entry": mark_price["entry"],
                            "exit": mark_price["exit"],
                            "timestamp": mark_price["timestamp"],
                            "runid": mark_price["runid"],
                        }
                    },
                    upsert=True,
                )

            return mark_price
        except Exception as e:
            log.error(e)
            return False
