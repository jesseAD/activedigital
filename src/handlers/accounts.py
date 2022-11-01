import os
from turtle import position
from dotenv import load_dotenv
import pandas as pd
from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange


load_dotenv()
log = Log()
MONGO_URI = os.getenv("MONGO_URI")
FTX_API_KEY = os.getenv("FTX_API_KEY")
FTX_API_SECRET = os.getenv("FTX_API_SECRET")
positions_db = MongoDB("hg", "positions", MONGO_URI)


class Accounts:
    def get_subaccounts():
        ftx = Exchange(account=None, key=FTX_API_KEY, secret=FTX_API_SECRET).ftx()
        subaccounts = ftx.private_get_subaccounts()
        accounts = [account['nickname'] for account in subaccounts['result']]
        return accounts

    def get(account: str = None, value: bool = None):
        data = {}

        ftx = Exchange(account, FTX_API_KEY, FTX_API_SECRET).ftx()

        try:
            account = ftx.privateGetAccount()

            data = account["result"]

            if value:
                data = float(account["result"]["totalAccountValue"])

        except Exception as e:
            log.error(e)

        return data

    def get_trades(account: str = None):
        ftx = Exchange(account, FTX_API_KEY, FTX_API_SECRET).ftx()
        start = "2022-10-03T00:00:00Z"
        trades = None

        try:
            data = ftx.fetch_my_trades(limit=1000)
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
        trades_cleaned['account'] = account

        return trades_cleaned

    def get_funding(account: str = None):
        ftx = Exchange(account, FTX_API_KEY, FTX_API_SECRET).ftx()
        funding = None

        try:
            data = ftx.private_get_funding_payments()
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
