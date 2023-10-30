import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import time
import ccxt
import pdb
from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.lib.mapping import Mapping
from src.lib.unhedged import get_unhedged
from src.config import read_config_file
from src.handlers.helpers import Helper
from src.handlers.helpers import OKXHelper
from src.handlers.helpers import BybitHelper
from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()


class Positions:
    def __init__(self, db):
        if os.getenv("mode") == "testing":
            self.runs_db = MongoDB(config["mongo_db"], "runs")
            self.tickers_db = MongoDB(config["mongo_db"], "tickers")
            self.balances_db = MongoDB(config["mongo_db"], "balances")
            self.split_positions_db = MongoDB(config["mongo_db"], "split_positions")
            self.positions_db = MongoDB(config["mongo_db"], db)
        else:
            self.runs_db = database_connector("runs")
            self.tickers_db = database_connector("tickers")
            self.balances_db = database_connector("balances")
            self.split_positions_db = database_connector("split_positions")
            self.positions_db = database_connector(db)

    def get(
        self,
        client,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        position_type: str = None,
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
        if position_type:
            pipeline.append({"$match": {"positionType": position_type}})
        if client:
            pipeline.append({"$match": {"client": client}})
        if exchange:
            pipeline.append({"$match": {"venue": exchange}})
        if account:
            pipeline.append({"$match": {"account": account}})

        try:
            results = self.positions_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

    def create(
        self,
        client,
        exch=None,
        exchange: str = None,
        positionType: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        position_value: str = None,
        back_off={},
    ):
        if position_value is None:
            if exch == None:
                spec = (client.upper() + "_" + exchange.upper() + "_" + sub_account.upper() + "_")
                API_KEY = os.getenv(spec + "API_KEY")
                API_SECRET = os.getenv(spec + "API_SECRET")
                PASSPHRASE = None
                if exchange == "okx":
                    PASSPHRASE = os.getenv(spec + "PASSPHRASE")

                exch = Exchange(
                    exchange, sub_account, API_KEY, API_SECRET, PASSPHRASE
                ).exch()

            try:
                if exchange == "okx":
                    position_value = OKXHelper().get_positions(exch=exch)

                elif exchange == "binance":
                    if config['clients'][client]['funding_payments'][exchange][sub_account]['margin_mode'] == 'portfolio':
                        position_value = Helper().get_pm_positions(exch=exch)
                        for item in position_value:
                            item['info'] = {**item}
                            item['marginMode'] = "cross"
                    else:
                        position_value = Helper().get_positions(exch=exch)

                elif exchange == "bybit":
                    position_value = BybitHelper().get_positions(exch=exch)

                position_value = Mapping().mapping_positions(exchange=exchange, positions=position_value)

            except ccxt.InvalidNonce as e:
                print("Hit rate limit", e)
                time.sleep(
                    back_off[client + "_" + exchange + "_" + sub_account] / 1000.0
                )
                back_off[client + "_" + exchange + "_" + sub_account] *= 2
                return False

            except Exception as e:
                print("An error occurred in Positions:", e)
                return False
                

        try:
            position_info = []
            liquidation_buffer = None

            if exchange == "okx":
                try:
                    cross_margin_ratio = float(
                        OKXHelper().get_cross_margin_ratio(exch=exch)
                    )
                    liquidation_buffer = OKXHelper().calc_liquidation_buffer(
                        exchange=exchange, mgnRatio=cross_margin_ratio
                    )

                except ccxt.InvalidNonce as e:
                    print("Hit rate limit", e)
                    time.sleep(
                        back_off[client + "_" + exchange + "_" + sub_account] / 1000.0
                    )
                    back_off[client + "_" + exchange + "_" + sub_account] *= 2
                    return False

                except Exception as e:
                    print("An error occurred in Positions:", e)
                    pass

            elif exchange == "bybit":
                try:
                    cross_margin_ratio = float(
                        BybitHelper().get_cross_margin_ratio(exch=exch)
                    )
                    liquidation_buffer = BybitHelper().calc_liquidation_buffer(
                        exchange=exchange, mgnRatio=cross_margin_ratio
                    )

                except ccxt.InvalidNonce as e:
                    print("Hit rate limit", e)
                    time.sleep(
                        back_off[client + "_" + exchange + "_" + sub_account] / 1000.0
                    )
                    back_off[client + "_" + exchange + "_" + sub_account] *= 2
                    return False

                except Exception as e:
                    print("An error occurred in Positions:", e)
                    pass

            elif exchange == "binance":
                if config['clients'][client]['funding_payments'][exchange][sub_account]['margin_mode'] == 'portfolio':
                    try:
                        cross_margin_ratio = float(
                            Helper().get_pm_cross_margin_ratio(exch=exch)
                        )
                        liquidation_buffer = Helper().calc_liquidation_buffer(
                            exchange=exchange, mgnRatio=cross_margin_ratio
                        )

                    except ccxt.InvalidNonce as e:
                        print("Hit rate limit", e)
                        time.sleep(
                            back_off[client + "_" + exchange + "_" + sub_account] / 1000.0
                        )
                        back_off[client + "_" + exchange + "_" + sub_account] *= 2
                        return False

                    except Exception as e:
                        print("An error occurred in Positions:", e)
                        pass
                else:
                    try:
                        cross_margin_ratio = 1e10
                        for position in position_value:
                            if position['initialMargin'] > 0:
                                cross_margin_ratio = (position['marginRatio'] 
                                    if cross_margin_ratio > position['marginRatio'] else cross_margin_ratio)
                        liquidation_buffer = Helper().calc_liquidation_buffer(
                            exchange=exchange, mgnRatio=cross_margin_ratio
                        )

                    except ccxt.InvalidNonce as e:
                        print("Hit rate limit", e)
                        time.sleep(
                            back_off[client + "_" + exchange + "_" + sub_account] / 1000.0
                        )
                        back_off[client + "_" + exchange + "_" + sub_account] *= 2
                        return False

                    except Exception as e:
                        print("An error occurred in Positions:", e)
                        pass

            for value in position_value:
                if float(value["initialMargin"]) > 0:
                    if config['clients'][client]['funding_payments'][exchange][sub_account]['margin_mode'] == 'non_portfolio':
                        # portfolio = None
                        # if exchange == "binance":
                        #     try:
                        #         if (
                        #             config["clients"][client]["funding_payments"][exchange][
                        #                 sub_account
                        #             ]["margin_mode"] == "non_portfolio"
                        #         ):
                        #             portfolio = Helper().get_non_portfolio_margin(
                        #                 exch=exch,
                        #                 params={"symbol": value["info"]["symbol"]},
                        #             )
                        #         elif (
                        #             config["clients"][client]["funding_payments"][exchange][
                        #                 sub_account
                        #             ]["margin_mode"] == "portfolio"
                        #         ):
                        #             portfolio = Helper().get_portfolio_margin(
                        #                 exch=exch, params={"symbol": "USDT"}
                        #             )
                        #             portfolio = [
                        #                 item
                        #                 for item in portfolio
                        #                 if float(item["balance"]) != 0
                        #             ]

                        #     except ccxt.InvalidNonce as e:
                        #         print("Hit rate limit", e)
                        #         time.sleep(
                        #             back_off[client + "_" + exchange + "_" + sub_account]
                        #             / 1000.0
                        #         )
                        #         back_off[client + "_" + exchange + "_" + sub_account] *= 2
                        #         return False

                        #     except Exception as e:
                        #         print("An error occurred in Positions:", e)
                        #         pass

                        # value["margin"] = portfolio
                        if exchange == "bybit":
                            value['symbol'] = value['base'] + value['quote'] + "-PERP"
                            value["liquidationBuffer"] = liquidation_buffer
                        else:
                            value["base"] = value["symbol"].split("/")[0]
                            value["quote"] = (
                                value["symbol"].split("-")[0].split("/")[1].split(":")[0]
                            )
                            value['symbol'] = value['base'] + value['quote'] + "-PERP"
                            value["liquidationBuffer"] = liquidation_buffer

                            if value["quote"] == "USD":
                                tickers = list(
                                    self.tickers_db.find({"venue": exchange})
                                    .sort("_id", -1)
                                    .limit(1)
                                )[0]["ticker_value"]

                                value["notional"] = float(
                                    value["notional"]
                                ) * Helper().calc_cross_ccy_ratio(
                                    value["base"],
                                    config["clients"][client]["funding_payments"][exchange][
                                        "base_ccy"
                                    ],
                                    tickers,
                                )
                                value["unrealizedPnl"] = float(
                                    value["unrealizedPnl"]
                                ) * Helper().calc_cross_ccy_ratio(
                                    value["base"],
                                    config["clients"][client]["funding_payments"][exchange][
                                        "base_ccy"
                                    ],
                                    tickers,
                                )
                    else:
                        value["base"] = value["symbol"].split("_")[0].split("USD")[0]
                        value["quote"] = "USD" + value["symbol"].split("_")[0].split("USD")[1]
                        value['symbol'] = value['base'] + value['quote'] + "-PERP"
                        value['side'] = "long" if float(value['positionAmt']) > 0 else "short"
                        
                        value["liquidationBuffer"] = liquidation_buffer

                        if value["quote"] == "USD":
                            tickers = list(
                                self.tickers_db.find({"venue": exchange})
                                .sort("_id", -1)
                                .limit(1)
                            )[0]["ticker_value"]

                            value["notional"] = float(
                                value["notional"]
                            ) * Helper().calc_cross_ccy_ratio(
                                value["base"],
                                config["clients"][client]["funding_payments"][exchange][
                                    "base_ccy"
                                ],
                                tickers,
                            )
                            value["unrealizedPnl"] = float(
                                value["unrealizedPnl"]
                            ) * Helper().calc_cross_ccy_ratio(
                                value["base"],
                                config["clients"][client]["funding_payments"][exchange][
                                    "base_ccy"
                                ],
                                tickers,
                            )

                    position_info.append(value)
            
            if exchange == "binance":
                for position in position_info:
                    try:
                        position["markPrice"] = Helper().get_mark_prices(
                            exch=exch, symbol=position['base'] + position['quote']
                        )["markPrice"]
                    except Exception as e:
                        print("An error occurred in Positions:", e)
                        pass

        except ccxt.InvalidNonce as e:
            print("Hit rate limit", e)
            time.sleep(back_off[client + "_" + exchange + "_" + sub_account] / 1000.0)
            back_off[client + "_" + exchange + "_" + sub_account] *= 2
            return False

        except Exception as e:
            print("An error occurred in Positions:", e)
            pass

        del position_value
        
        back_off[client + "_" + exchange + "_" + sub_account] = config["dask"]["back_off"]

        # print(get_unhedged(position_info))
        if config['clients'][client]['split_positions'] == True:
            query = {}

            if exchange:
                query["venue"] = exchange

            ticker_values = self.tickers_db.find(query).sort("_id", -1).limit(1)

            ticker = None
            for item in ticker_values:
                ticker = item["ticker_value"]

            if client:
                query["client"] = client            
            if sub_account:
                query["account"] = sub_account

            balance_values = self.balances_db.find(query).sort("_id", -1).limit(1)

            balance = None
            for item in balance_values:
                timestamp = item['timestamp']
                balance = item["balance_value"]

            spot_positions = []

            for _key, _val in balance.items():
                if _key != "base":
                    spot_position = {}
                    spot_position['base'] = _key
                    spot_position['quote'] = _key
                    spot_position['symbol'] = _key
                    spot_position['contracts'] = _val
                    spot_position['avgPrice'] = 0
                    spot_position['leverage'] = 0
                    spot_position['unrealizedPnl'] = 0
                    spot_position['marginMode'] = None
                    spot_position['timestamp'] = int(timestamp.timestamp() * 1000)
                    spot_position['side'] = "long" if _val > 0 else "short"
                    spot_position['markPrice'] = 1 if _key == "USDT" else ticker[_key + "/USDT"]['last']
                    spot_position['notional'] = spot_position['markPrice'] * spot_position['contracts']

                    spot_positions.append(spot_position)

            current_time = datetime.now(timezone.utc)
            split_position = {
                "client": client,
                "venue": exchange,
                "account": "Main Account",
                "position_value": get_unhedged(position_info, spot_positions),
                "active": True,
                "entry": False,
                "exit": False,
                "timestamp": current_time,
            }
            if sub_account:
                split_position["account"] = sub_account
            if spot:
                split_position["spotMarket"] = spot
            if future:
                split_position["futureMarket"] = future
            if perp:
                split_position["perpMarket"] = perp
            run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
            latest_run_id = 0
            for item in run_ids:
                try:
                    latest_run_id = item["runid"] + 1
                except:
                    pass
            split_position["runid"] = latest_run_id

            try:
                self.split_positions_db.insert_one(split_position)
            except Exception as e:
                log.error(e)
                return False
            

        current_time = datetime.now(timezone.utc)
        position = {
            "client": client,
            "venue": exchange,
            # "positionType": positionType.lower(),
            "account": "Main Account",
            "position_value": position_info,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": current_time,
        }

        del position_info

        if sub_account:
            position["account"] = sub_account
        if spot:
            position["spotMarket"] = spot
        if future:
            position["futureMarket"] = future
        if perp:
            position["perpMarket"] = perp

        # get latest positions data
        query = {}
        if client:
            query["client"] = client
        if exchange:
            query["venue"] = exchange
        if sub_account:
            query["account"] = sub_account

        position_values = self.positions_db.find(query).sort("_id", -1).limit(1)

        latest_run_id = -1
        latest_value = None
        for item in position_values:
            if latest_run_id < item["runid"]:
                latest_run_id = item["runid"]
                latest_value = item["position_value"]

        if latest_value == position["position_value"]:
            print("same position")
            return False

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"] + 1
            except:
                pass

        try:
            position["runid"] = latest_run_id

            if config["positions"]["store_type"] == "timeseries":
                self.positions_db.insert_one(position)
            elif config["positions"]["store_type"] == "snapshot":
                self.positions_db.update_one(
                    {
                        "client": position["client"],
                        "venue": position["venue"],
                        "account": position["account"],
                    },
                    {
                        "$set": {
                            "position_value": position["position_value"],
                            "active": position["active"],
                            "entry": position["entry"],
                            "exit": position["exit"],
                            "timestamp": position["timestamp"],
                            "runid": position["runid"],
                        }
                    },
                    upsert=True,
                )

            # log.debug(f"Position created: {position}")

            del position

            # return position
        except Exception as e:
            log.error(e)
            return False

    def entry(self, account: str = None, status: bool = True):
        # get all positions with account
        positions = Positions.get(active=True, account=account)

        for position in positions:
            try:
                self.positions_db.update(
                    {"_id": position["_id"]},
                    {"$set": {"entry": status}},
                )
                log.debug(
                    f"position in account entry {account} has been set to {status}"
                )
            except Exception as e:
                log.error(e)
                return False

        return True

    def exit(self, account: str = None, status: bool = False):
        # get all positions with account
        positions = Positions.get(active=True, account=account)

        for position in positions:
            if position["entry"] is False:
                log.debug(
                    f"position in account {account} has not been entered, skipping"
                )
                continue
            try:
                self.positions_db.update(
                    {"_id": position["_id"]},
                    {"$set": {"exit": status}},
                )
                log.debug(
                    f"Position in account exit {account} has been set to {status}"
                )
            except Exception as e:
                log.error(e)
                return False

        return True

    def update(self, account: str = None, **kwargs: dict):
        # get all positions with account
        positions = Positions.get(account=account)

        for position in positions:
            try:
                self.positions_db.update_one(
                    {"_id": position["_id"]},
                    {"$set": kwargs},
                )
                log.debug(f"Position in account {account} has been updated")
            except Exception as e:
                log.error(e)
                return False

        return True
