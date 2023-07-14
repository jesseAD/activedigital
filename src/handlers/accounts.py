import os
# from turtle import position
from dotenv import load_dotenv
import pandas as pd
from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange


load_dotenv()
log = Log()
# MONGO_URI = os.getenv("MONGO_URI")
API_KEY = os.getenv("BINANCE_SUBACCOUNT1_API_KEY")
API_SECRET = os.getenv("BINANCE_SUBACCOUNT1_API_SECRET")
positions_db = MongoDB("production", "positions")


class Accounts:
    def get_subaccounts():
        exch = Exchange(account=None, key=API_KEY, secret=API_SECRET).exch()
        subaccounts = exch.private_get_subaccounts()
        accounts = [account["nickname"] for account in subaccounts["result"]]
        return accounts

    def get(exchange: str = None, account: str = None, value: bool = None):
        data = {}
        spec = exchange.upper() + "_" + account.upper() + "_"
        API_KEY = os.getenv(spec + "API_KEY")
        API_SECRET = os.getenv(spec + "API_SECRET")
        exch = Exchange(exchange, account, API_KEY, API_SECRET).exch()
        print("exch: ", exch)
        try:
            account = exch.privateGetAccount()
            data = account

            # if value:
            #     data = float(account["result"]["totalAccountValue"])

        except Exception as e:
            log.error(e)

        return data

    def get_trades(account: str = None):
        exch = Exchange(account, API_KEY, API_SECRET).exch()
        start = "2022-10-03T00:00:00Z"
        trades = None

        try:
            data = exch.fetch_my_trades(limit=1000)
            trades = pd.DataFrame(data)
        except Exception as e:
            log.error(e)

        if trades is None:
            log.debug(f"No trades found for account {account}")
            return None

        trades_cleaned = pd.json_normalize(trades["info"])
        trades = pd.concat([trades.drop(["info"], axis=1), trades_cleaned], axis=1)
        trades["price"] = trades["price"].astype(float)
        trades["size"] = trades["size"].astype(float)
        trades_cleaned = trades_cleaned.drop(["id", "fee", "feeCurrency"], axis=1)
        trades_cleaned["account"] = account

        return trades_cleaned

    def get_funding(account: str = None):
        exch = Exchange(account, API_KEY, API_SECRET).exch()
        funding = None

        try:
            data = exch.private_get_funding_payments()
            funding = pd.DataFrame(data["result"])
        except Exception as e:
            log.error(e)

        if funding.empty:
            return None

        funding["time"] = pd.to_datetime(funding["time"])
        funding["account"] = account
        funding.set_index("time", inplace=True)
        funding.drop(["id", "rate"], axis=1, inplace=True)
        funding["payment"] = (
            funding["payment"].astype(float).apply(lambda x: -x if x > 0 else -x)
        )

        return funding

    def create(nickname: str = None, amount: float = 0.0):
        exch = Exchange(account=None, key=API_KEY, secret=API_SECRET).exch()
        data = None

        if amount > 10000:
            log.error("Amount cannot be greater than 1000")
            return False

        try:

            create_account_data = exch.private_post_subaccounts(
                params={"nickname": nickname}
            )

            account_name = create_account_data["result"]["nickname"]

            transfer_funds_data = exch.private_post_subaccounts_transfer(
                params={
                    "coin": "USD",
                    "size": amount,
                    "source": "main",
                    "destination": account_name,
                }
            )

            transfered_size = transfer_funds_data["result"]["size"]

            log.info(f'Account "{account_name}" created with {transfered_size} USD')

            return True

        except Exception as e:
            log.error(e)

        return False
