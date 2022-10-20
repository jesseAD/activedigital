import os
from xmlrpc.client import boolean
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
    def get(account: str = None, value: boolean = None):
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
            data = ftx.fetch_my_trades(limit=None)
            trades = pd.DataFrame(data)
        except Exception as e:
            log.error(e)

        if trades is None:
            log.debug(f"No trades found for account {account}")
            return None

        trades_cleaned = pd.json_normalize(trades["info"])
        trades_cleaned = trades_cleaned.drop(["id", "side", "fee", "price"], axis=1)
        trades = pd.concat([trades.drop(["info"], axis=1), trades_cleaned], axis=1)
        trades["price"] = trades["price"].astype(float)
        trades["size"] = trades["size"].astype(float)

        # create index based on Market and side
        trades_cleaned.set_index(["market", "side"], inplace=True)
        trades_cleaned = trades_cleaned.drop(["id", "fee", "feeCurrency"], axis=1)

        trades["symbol"] = trades["symbol"].str.replace("/USD:USD", "-PERP")
        trades.drop(["fees"], axis=1, inplace=True)

        return trades_cleaned

    def get_funding(account: str = None):
        ftx = Exchange(account, FTX_API_KEY, FTX_API_SECRET).ftx()
        funding = None

        try:
            data = ftx.private_get_funding_payments()
            funding = pd.DataFrame(data)
        except Exception as e:
            log.error(e)

        if funding.empty:
            return None

        funding["time"] = pd.to_datetime(funding["time"])
        funding.set_index("time", inplace=True)
        funding.drop(["id", "rate"], axis=1, inplace=True)
        funding["payment"] = (
            funding["payment"].astype(float).apply(lambda x: -x if x > 0 else -x)
        )
        # TODO: HAVE AHMAD EXPLAIN THIS NEXT LINE
        # funding = funding[funding["future"] == PERP]

        return funding
