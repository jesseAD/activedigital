import os
# from turtle import position
from dotenv import load_dotenv
from src.lib.log import Log
from src.lib.exchange import Exchange


load_dotenv()
log = Log()
MONGO_URI = os.getenv("MONGO_URI")
FTX_API_KEY = os.getenv("FTX_API_KEY")
FTX_API_SECRET = os.getenv("FTX_API_SECRET")

load_dotenv()

log = Log()


class tickers:
    def spots():
        ftx = Exchange(account=None, key=FTX_API_KEY, secret=FTX_API_SECRET).ftx()
        markets = ftx.fetch_markets()

        spot_list = []

        for x in markets:
            # check if type is spot and margin is True and add to list
            if x["type"] == "spot":
                name = x["base"]
                id = x["id"]
                spot_list.append({"name": name, "id": id})

        return spot_list

    def swaps():
        ftx = Exchange(account=None, key=FTX_API_KEY, secret=FTX_API_SECRET).ftx()
        markets = ftx.fetch_markets()

        market_list = []
        # check if market id has -PERP in it

        for market in markets:
            # check if market id has -PERP in it
            if "-PERP" in market["id"]:
                name = market["id"]
                market_list.append({"name": name, "id": name})

        return market_list

    def futures():
        ftx = Exchange(account=None, key=FTX_API_KEY, secret=FTX_API_SECRET).ftx()

        markets = ftx.fetch_markets()
        market_list = []

        for market in markets:
            if market["type"] == "future":
                name = market["id"]
                market_list.append(
                    {
                        "id": name,
                        "name": name,
                    }
                )

        return market_list

class Helper():
    # default method for Binance
    def get_positions(self, exch):
        return exch.privateGetAccount()

class OKXHelper(Helper):
    # override get_positions() method for OKX
    def get_positions(self, exch):
        return exch.private_get_account_positions()