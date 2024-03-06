import os
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


class Balances:
    def __init__(self, db, collection):

        self.runs_db = db['runs']
        self.tickers_db = db['tickers']
        self.balances_db = db['balances']

    def get(
        self,
        active: bool = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        position_type: str = None,
        client: str = None,
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
            results = self.balances_db.aggregate(pipeline)
            return results

        except Exception as e:
            log.error(e)

    def create(
        self,
        client,
        exch=None,
        exchange: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        balanceValue: str = None,
        back_off={},
        logger=None
    ):
        if balanceValue is None:
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

            repayments = {}
            loan_pools = {
                'vip_loan': 0,
                'market_loan': 0
            }

            try:
                if exchange == "okx":
                    balanceValue, repayments, max_loan = OKXHelper().get_balances(exch=exch)
                    loan_pools['market_loan'] = OKXHelper().get_market_loan_pool(exch=exch)
                    loan_pools['vip_loan'] = OKXHelper().get_VIP_loan_pool(exch=exch)

                elif exchange == "binance":
                    if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'portfolio':
                        balanceValue = Helper().get_pm_balances(exch=exch)
                    else:
                        balanceValue = Helper().get_balances(exch=exch)
                        
                elif exchange == "bybit":
                    balanceValue = BybitHelper().get_balances(exch=exch)
                
            # except ccxt.InvalidNonce as e:
            #     print("Hit rate limit", e)
            #     time.sleep(
            #         back_off[client + "_" + exchange + "_" + sub_account] / 1000.0
            #     )
            #     back_off[client + "_" + exchange + "_" + sub_account] *= 2
            #     return True

            except ccxt.ExchangeError as e:
                logger.warning(client + " " + exchange + " " + sub_account + " balances " + str(e))
                # print("An error occurred in Balances:", e)
                return True

        # back_off[client + "_" + exchange + "_" + sub_account] = config["dask"]["back_off"]

        balanceValue = {_key: balanceValue[_key] for _key in balanceValue if balanceValue[_key] != 0.0}

        query = {}
        if exchange:
            query["venue"] = exchange
        ticker_values = self.tickers_db.find(query)

        for item in ticker_values:
            ticker_value = item["ticker_value"]

        base_balance = 0
        if exchange == "binance":
            try:
                wallet_balances = Helper().get_wallet_balances(exch=exch)
                for item in wallet_balances:
                    base_balance += float(item['balance']) * Helper().calc_cross_ccy_ratio(
                        "BTC", "USD", ticker_value
                    )
            except ccxt.ExchangeError as e:
                logger.warning(client + " " + exchange + " " + sub_account + " balances " + str(e))
                # print("An error occurred in Balances:", e)
                pass
        else:
            for _key, _value in balanceValue.items():
                base_balance += _value * Helper().calc_cross_ccy_ratio(
                    _key,
                    config["clients"][client]["subaccounts"][exchange]["base_ccy"],
                    ticker_value,
                )

        balanceValue["base"] = base_balance

        balance = {
            "client": client,
            "venue": exchange,
            "account": "Main Account",
            "balance_value": balanceValue,
            "repayments": repayments,
            "loan_pools": loan_pools,
            "base_ccy": config["clients"][client]["subaccounts"][exchange]["base_ccy"],
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }

        if sub_account:
            balance["account"] = sub_account
        if spot:
            balance["spotMarket"] = spot
        if future:
            balance["futureMarket"] = future
        if perp:
            balance["perpMarket"] = perp
        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass

        balance["runid"] = latest_run_id

        # get latest balances data
        # query = {}
        # if client:
        #     query["client"] = client
        # if exchange:
        #     query["venue"] = exchange
        # if sub_account:
        #     query["account"] = sub_account

        # balances_values = self.balances_db.find(query).sort("runid", -1).limit(1)

        # latest_run_id = -1
        # latest_value = None
        # for item in balances_values:
        #     if latest_run_id < item["runid"]:
        #         latest_run_id = item["runid"]
        #         latest_value = item["balance_value"]

        # if latest_value == balance["balance_value"]:
        #     logger.info(client + " " + exchange + " " + sub_account + " " + "same balance")
        #     # print("same balance")
        #     return True

        try:
            if config["balances"]["store_type"] == "timeseries":
                self.balances_db.insert_one(balance)
            elif config["balances"]["store_type"] == "snapshot":
                self.balances_db.update_one(
                    {
                        "client": balance["client"],
                        "venue": balance["venue"],
                        "account": balance["account"],
                    },
                    {
                        "$set": {
                            "balance_value": balance["balance_value"],
                            "active": balance["active"],
                            "entry": balance["entry"],
                            "exit": balance["exit"],
                            "timestamp": balance["timestamp"],
                            "runid": balance["runid"],
                        }
                    },
                    upsert=True,
                )

            return True
        
        except Exception as e:
            # print("An error occurred in Balances:", e)
            logger.error(client + " " + exchange + " " + sub_account + " balances " + str(e))
            return True

    