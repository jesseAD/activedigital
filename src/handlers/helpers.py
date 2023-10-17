import os

# from turtle import position
from src.config import read_config_file
from dotenv import load_dotenv
from src.lib.log import Log
from src.lib.exchange import Exchange


config = read_config_file()
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
class Helper:
    def get_positions(self, exch):
        return exch.fetch_account_positions(params={"type": "future"})
    
    def get_pm_positions(self, exch):
        position_value = []

        try:
            position_value += exch.papi_get_um_positionrisk(params={"type": "future"})
        except:
            pass

        try:
            position_value += exch.papi_get_cm_positionrisk(params={"type": "future"})
        except:
            pass

        return position_value

    def get_balances(self, exch):
        all_balances = exch.fetch_balance()["total"]
        result = dict()
        for currency, balance in all_balances.items():
            if float(balance) != 0:
                result[currency] = balance

        return result
    
    def get_pm_balances(self, exch):
        balances = exch.papi_get_balance()
        balances = [item for item in balances if item['totalWalletBalance'] != 0.0]

        balance_value = {}
        for item in balances:
            balance_value[item['asset']] = float(item['totalWalletBalance'])

        return balance_value

    def get_instruments(self, exch):
        # return exch.fetch_markets({'symbol': "BTCUSDT"})[0]['info']
        return exch.fetch_markets()

    def get_bid_ask(self, exch, symbol):
        order_book = exch.fetch_order_book(symbol)
        best_bid = order_book["bids"][0][0]  # price of the highest bid
        best_ask = order_book["asks"][0][0]  # price of the lowest ask

        return {
            "bid": best_bid,
            "ask": best_ask,
            "mid_point": (best_ask + best_bid) / 2.0,
        }

    def get_tickers(self, exch, params={}):
        return exch.fetch_tickers(params=params)

    def get_borrow_rates(self, exch, code, limit=None, since=None, params={}):
        return exch.fetch_borrow_rate_history(
            code=code, limit=limit, since=since, params=params
        )

    def get_borrow_rate(self, exch, params={}):
        return exch.sapi_get_margin_next_hourly_interest_rate(params=params)

    def get_funding_rates(self, exch, symbol, limit=None, since=None, params={}):
        return exch.fetch_funding_rate_history(
            symbol=symbol, limit=limit, since=since, params=params
        )

    def get_funding_rates_dapi(self, exch, params={}):
        return exch.dapipublic_get_fundingrate(params=params)

    def get_portfolio_margin(self, exch, params={}):
        return exch.fapiprivatev2_get_balance(params)

    def get_non_portfolio_margin(self, exch, params={}):
        return exch.fapiprivatev2_get_positionrisk(params)[0]

    def get_mark_prices(self, exch, params={}):
        return exch.fapipublic_get_premiumindex(params)

    def get_future_transactions(self, exch, params={}):
        return exch.fapiprivate_get_income(params)

    def get_spot_transactions(self, exch, params={}):
        return exch.private_get_mytrades(params)
    
    def get_um_transactions(self, exch, params={}):
        return exch.papi_get_um_income(params=params)

    def get_cm_transactions(self, exch, params={}):
        return exch.papi_get_cm_income(params=params)

    def get_fills(self, exch, symbol=None, since=None, limit=None, params={}):
        return exch.fetch_my_trades(symbol=symbol, since=since, limit=limit, params=params)
        # return exch.fapiprivate_get_usertrades(params)
    
    def get_pm_fills(self, exch, symbol, params={}):
        fills = []

        try:
            params['pair'] = symbol
            fills += exch.papi_get_cm_usertrades(params)
        except Exception as e:
            print("An error occurred in PM Fills:", e)
            pass
        try:
            params['symbol'] = symbol
            fills += exch.papi_get_um_usertrades(params)        
        except Exception as e:
            print("An error occurred in PM Fills:", e)
            pass

        return fills
    
    def get_pm_cross_margin_ratio(self, exch):
        return exch.papi_get_account()['uniMMR']

    def calc_liquidation_buffer(self, exchange, mgnRatio):
        return max(0, min(
            1, config["liquidation"]["scalar"][exchange] * (mgnRatio - config["liquidation"]['threshold'][exchange])
        ))
        # if exchange == "okx":
        #     return min(
        #         1,
        #         config["liquidation"]["scalar"][exchange]
        #         * (mgnRatio * config["liquidation"]['threshold'][exchange]),
        #     )
        
    def calc_cross_ccy_ratio(self, src_ccy, dest_ccy, tickers):
        if src_ccy == dest_ccy:
            return 1.0
        
        if src_ccy == "USD":
            if dest_ccy == "USDT":
                return 1.0 / tickers['USDT/USD']['last']
            elif (dest_ccy + "/USDT") not in tickers:
                return 0.0
            else:
                return 1.0 / tickers['USDT/USD']['last'] / tickers[dest_ccy + "/USDT"]['last']
            
        if src_ccy == "USDT":
            if dest_ccy == "USD":
                return tickers['USDT/USD']['last']
            elif (dest_ccy + "/USDT") not in tickers:
                return 0.0
            else:
                return 1.0 / tickers[dest_ccy + "/USDT"]['last']
            
        if (src_ccy + "/USDT") not in tickers:
            return 0.0
            
        if dest_ccy == "USD":
            return tickers['USDT/USD']['last'] * tickers[src_ccy + "/USDT"]['last']
        
        if dest_ccy == "USDT":
            return tickers[src_ccy + "/USDT"]['last']
        
        if (dest_ccy + "/USDT") not in tickers:
            return 0.0
        
        return tickers[src_ccy + "/USDT"]['last'] / tickers[dest_ccy + "/USDT"]['last']


class OKXHelper(Helper):
    def get_positions(self, exch):
        return exch.fetch_positions(params={"type": "swap"})

    def get_mark_prices(self, exch, params={}):
        return exch.public_get_public_mark_price(params)["data"][0]

    def get_transactions(self, exch, params={}):
        return exch.private_get_account_bills_archive(params)["data"]

    def get_funding_rate(self, exch, params={}):
        return exch.public_get_public_funding_rate(params)

    def get_borrow_rate(self, exch, params={}):
        return exch.private_get_account_interest_rate(params=params)

    def get_index_prices(self, exch, params={}):
        return exch.public_get_market_index_tickers(params)

    def get_cross_margin_ratio(self, exch):
        return exch.private_get_account_balance()["data"][0]["mgnRatio"]
    

class BybitHelper(Helper):
    pass

class CoinbaseHelper:
    def get_usdt2usd_ticker(self, exch):
        return exch.fetch_ticker("USDT/USD")
