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

# default helper for Binance
class Helper():
    def get_positions(self, exch):
        return exch.fetch_account_positions(params={'type':'future'})
    
    def get_balances(self, exch):
        all_balances = exch.fetch_balance()['total']
        result = dict()
        for currency, balance in all_balances.items():
            if float(balance) != 0:
                result[currency] = balance

        return result
    
    def get_instruments(self, exch):
        return exch.fetch_markets({'symbol': "BTCUSDT"})[0]['info']
    
    def get_bid_ask(self, exch, symbol):
        order_book = exch.fetch_l2_order_book(symbol)
        best_bid = order_book['bids'][0][0]  # price of the highest bid
        best_ask = order_book['asks'][0][0]  # price of the lowest ask

        return {
            'bid': best_bid,
            'ask': best_ask
        }
    
    def get_tickers(self, exch):
        return exch.fetch_tickers()
    
    def get_borrow_rates(self, exch, code, limit = None, since = None, params = {}):
        return exch.fetch_borrow_rate_history(code=code, limit=limit, since=since, params=params)
    
    def get_funding_rates(self, exch, symbol, limit = None, since = None, params = {}):
        return exch.fetch_funding_rate_history(symbol=symbol, limit=limit, since=since, params=params)
    
    def get_portfolio_margin(self, exch, params={}):
        return exch.fapiprivate_get_balance(params)
    
    def get_non_portfolio_margin(self, exch, params={}):
        return exch.fapiprivatev2_get_positionrisk(params)[0]
    
    def get_mark_prices(self, exch, params={}):
        return exch.fapipublic_get_premiumindex(params)
    
    def get_future_transactions(self, exch, params={}):
        return exch.fapiprivate_get_income(params)
    
    def get_spot_transactions(self, exch, params={}):
        return exch.private_get_mytrades(params)
    
    def get_fills(self, exch, params={}):
        return exch.fapiprivate_get_usertrades(params)

class OKXHelper(Helper):
    def get_positions(self, exch):
        return exch.fetch_positions(params={'type':'swap'})
    
    def get_mark_prices(self, exch, params={}):
        return exch.public_get_public_mark_price(params)['data'][0]
    
    def get_transactions(self, exch, params={}):
        return exch.private_get_account_bills_archive(params)['data']
    
    def get_fills(self, exch, params={}):
        return exch.private_get_trade_fills_history(params)['data']

class CoinbaseHelper():
    def get_usdt2usd_ticker(self, exch):
        return exch.fetch_ticker('USDT/USD')