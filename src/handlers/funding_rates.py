from datetime import datetime, timezone
import ccxt 

from src.lib.exchange import Exchange
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper, HuobiHelper

config = read_config_file()


class FundingRates:
    def __init__(self, db, collection):

        self.runs_db = db[config['mongodb']['database']]['runs']
        self.funding_rates_db = db[config['mongodb']['database']][collection]
        self.borrow_rates_db = db[config['mongodb']['database']]['borrow_rates']
        self.long_funding_db = db[config['mongodb']['database']]['long_funding']
        self.short_funding_db = db[config['mongodb']['database']]['short_funding']

    # def get(
    #     self,
    #     active: bool = None,
    #     spot: str = None,
    #     future: str = None,
    #     perp: str = None,
    #     exchange: str = None,
    #     symbol: str = None,
    # ):
    #     results = []

    #     pipeline = [
    #         {"$sort": {"_id": -1}},
    #     ]

    #     if active is not None:
    #         pipeline.append({"$match": {"active": active}})
    #     if spot:
    #         pipeline.append({"$match": {"spotMarket": spot}})
    #     if future:
    #         pipeline.append({"$match": {"futureMarket": future}})
    #     if perp:
    #         pipeline.append({"$match": {"perpMarket": perp}})
    #     if exchange:
    #         pipeline.append({"$match": {"venue": exchange}})
    #     if symbol:
    #         pipeline.append({"$match": {"symbol": symbol}})

    #     try:
    #         results = self.funding_rates_db.aggregate(pipeline)
    #         return results

    #     except Exception as e:
    #         log.error(e)

    def create(
        self,
        exch = None,
        exchange: str = None,
        symbol: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        fundingRatesValue: str = None,
        symbols: str = None,
        logger=None
    ):

        if fundingRatesValue is None:
            if exch == None:
                exch = Exchange(exchange).exch()
            
            fundingRatesValue = {}
            scalar = 1

            try:
                funding_rate_values = self.funding_rates_db.aggregate([
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$venue', exchange
                                        ]
                                    }, {
                                        '$eq': [
                                            '$symbol', symbol.split(":")[0]
                                        ]
                                    }
                                ]
                            }
                        }
                    }, {
                        '$project': {
                            'funding_rates_value': 1
                        }
                    }, {
                        '$group': {
                            '_id': None, 
                            'funding_rates_value': {
                                '$last': '$funding_rates_value'
                            }
                        }
                    }
                ])

                current_values = None
                for item in funding_rate_values:
                    current_values = item["funding_rates_value"]

                last_time = 0
                if current_values is None:
                    if exchange == "okx":
                        fundingRatesValue = OKXHelper().get_funding_rates(
                            exch=exch, limit=100, symbol=symbol
                        )

                        funding_rate = OKXHelper().get_funding_rate(
                            exch=exch,
                            symbol=symbol
                        )
                        current_funding_rate = {}
                        current_funding_rate["info"] = funding_rate['info']
                        current_funding_rate["symbol"] = symbol
                        current_funding_rate["fundingRate"] = funding_rate["fundingRate"]
                        current_funding_rate["info"]["realizedRate"] = funding_rate["fundingRate"]
                        current_funding_rate["timestamp"] = funding_rate["fundingTimestamp"]
                        current_funding_rate["datetime"] = funding_rate["fundingDatetime"]
                        fundingRatesValue.append(current_funding_rate)

                    elif exchange == "binance":
                        fundingRatesValue = Helper().get_funding_rates(
                            exch=exch, limit=100, symbol=symbol
                        )
                    elif exchange == "bybit":
                        fundingRatesValue = BybitHelper().get_funding_rates(
                            exch=exch, limit=100, symbol=symbol
                        )
                    elif exchange == "huobi":
                        fundingRatesValue = HuobiHelper().get_funding_rates(
                            exch=exch, limit=100, symbol=symbol
                        )
                else:
                    last_time = int(current_values['timestamp']) + 1
                    if exchange == "okx":
                        fundingRatesValue = OKXHelper().get_funding_rates(
                            exch=exch, limit=100, symbol=symbol, since=last_time# + 28800000
                        )

                        # if len(fundingRatesValue) > 0 or (datetime.now(timezone.utc).timestamp() * 1000) > (last_time + 28800000):
                        #     funding_rate = OKXHelper().get_funding_rate(
                        #         exch=exch,
                        #         symbol=symbol
                        #     )

                        #     current_funding_rate = {}
                        #     current_funding_rate["info"] = funding_rate['info']
                        #     current_funding_rate["symbol"] = symbol
                        #     current_funding_rate["fundingRate"] = funding_rate["fundingRate"]
                        #     current_funding_rate["info"]["realizedRate"] = funding_rate["fundingRate"]
                        #     current_funding_rate["timestamp"] = funding_rate["fundingTimestamp"]
                        #     current_funding_rate["datetime"] = funding_rate["fundingDatetime"]
                        #     fundingRatesValue.append(current_funding_rate)

                    elif exchange == "binance":
                        fundingRatesValue = Helper().get_funding_rates(
                            exch=exch, limit=100, symbol=symbol, since=last_time
                        )

                    elif exchange == "bybit":
                        fundingRatesValue = BybitHelper().get_funding_rates(
                            exch=exch, limit=100, symbol=symbol, since=last_time #params={'startTime': last_time, 'endTime': datetime.now(timezone.utc).timestamp()}
                        )

                    elif exchange == "huobi":
                        fundingRatesValue = HuobiHelper().get_funding_rates(
                            exch=exch, limit=100, symbol=symbol, since=last_time
                        )

                if len(fundingRatesValue) > 0:
                    if exchange == "okx":
                        funding_rate = OKXHelper().get_funding_rate(exch=exch, symbol=symbol)

                        scalar = 1

                        if config["funding_rates"]["period"] == "daily":
                            scalar = 365
                        elif config["funding_rates"]["period"] == "interval":
                            scalar = 24 * 365 * 3600000
                            scalar /= int(funding_rate["nextFundingTimestamp"]) - int(
                                funding_rate["fundingTimestamp"]
                            )

                        for item in fundingRatesValue:
                            item["nextFundingRate"] = funding_rate["fundingRate"]
                            item["nextFundingTime"] = funding_rate["fundingTimestamp"]
                            item["base"] = symbol.split("/")[0]
                            item["quote"] = symbol.split("/")[1].split(":")[0]
                            item["scalar"] = scalar

                    elif exchange == "binance":
                        funding_rate = Helper().get_funding_rate(
                            exch=exch, symbol=symbol
                        )

                        scalar = 1

                        if config["funding_rates"]["period"] == "daily":
                            scalar = 365
                        elif config["funding_rates"]["period"] == "interval":
                            scalar = 24 * 365 * 3600000
                            scalar /= int(funding_rate["fundingTimestamp"]) - int(
                                fundingRatesValue[-1]["timestamp"]
                            )

                        for item in fundingRatesValue:
                            item["nextFundingRate"] = funding_rate["fundingRate"]
                            item["nextFundingTime"] = funding_rate["fundingTimestamp"]
                            item["base"] = symbol.split("/")[0]
                            item["quote"] = symbol.split("/")[1].split(":")[0]
                            item["scalar"] = scalar
                    
                    elif exchange == "bybit":
                        funding_rate = BybitHelper().get_funding_rate(
                            exch=exch, symbol=symbol
                        )

                        scalar = 1

                        if config["funding_rates"]["period"] == "daily":
                            scalar = 365
                        elif config["funding_rates"]["period"] == "interval":
                            scalar = 24 * 365 * 3600000
                            scalar /= int(funding_rate["fundingTimestamp"]) - int(
                                fundingRatesValue[-1]["timestamp"]
                            )

                        for item in fundingRatesValue:
                            item["nextFundingRate"] = funding_rate["fundingRate"]
                            item["nextFundingTime"] = funding_rate["fundingTimestamp"]
                            item["base"] = symbol.split("/")[0]
                            item["quote"] = symbol.split("/")[1].split(":")[0]
                            item["scalar"] = scalar

                    elif exchange == "huobi":
                        funding_rate = HuobiHelper().get_funding_rate(
                            exch=exch, symbol=symbol
                        )

                        scalar = 1

                        if config["funding_rates"]["period"] == "daily":
                            scalar = 365
                        elif config["funding_rates"]["period"] == "interval":
                            scalar = 24 * 365 * 3600000
                            scalar /= int(funding_rate["fundingTimestamp"]) - int(
                                fundingRatesValue[-1]["timestamp"]
                            )

                        for item in fundingRatesValue:
                            item["nextFundingRate"] = funding_rate["fundingRate"]
                            item["nextFundingTime"] = funding_rate["fundingTimestamp"]
                            item["base"] = symbol.split("/")[0]
                            item["quote"] = symbol.split("/")[1].split(":")[0]
                            item["scalar"] = scalar

                    try:
                        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

                        latest_run_id = 0
                        for item in run_ids:
                            try:
                                latest_run_id = item["runid"]
                            except:
                                pass
                        
                        borrow_ccy = "USDT" if fundingRatesValue[-1]['quote'] == "USDT" else fundingRatesValue[-1]['base']
                        
                        borrow_rates = list(self.borrow_rates_db.find({
                            "$and": [
                                {"venue": exchange},
                                {"code": borrow_ccy},
                                {"borrow_rates_value.timestamp": {"$gte": max(last_time - 1, fundingRatesValue[-1]["timestamp"] - 28800000), "$lt": fundingRatesValue[-1]["timestamp"]}}
                            ]
                        }))
                        market_borrow_rate = 0
                        vip_borrow_rate = 0
                        try:
                            market_borrow_rates = [item for item in borrow_rates if item['market/vip'] == "market"]
                            market_borrow_rate = sum([item['borrow_rates_value']['rate'] * item['borrow_rates_value']['scalar'] for item in market_borrow_rates]) / len(market_borrow_rates)
                        except:
                            pass

                        long_fundings = []

                        for i in range(1, 7):
                            for base_ccy in config['funding_rates']['base_ccy']:
                                if base_ccy == borrow_ccy:
                                    funding = i * fundingRatesValue[-1]["fundingRate"] * fundingRatesValue[-1]["scalar"] - (i-1) * market_borrow_rate
                                else:
                                    funding = i * fundingRatesValue[-1]["fundingRate"] * fundingRatesValue[-1]["scalar"] - i * market_borrow_rate

                                long_fundings.append({
                                    'venue': exchange,
                                    'base': fundingRatesValue[-1]['base'],
                                    'quote': fundingRatesValue[-1]['quote'],
                                    'base_ccy': base_ccy,
                                    'n': i,
                                    'long_funding_value': {
                                        'symbol': fundingRatesValue[-1]['base'] + "/" + fundingRatesValue[-1]['quote'],
                                        'funding': funding,
                                        'timestamp': fundingRatesValue[-1]["timestamp"]
                                    },
                                    'market/vip': "market",
                                    'runid': latest_run_id,
                                    'timestamp': datetime.now(timezone.utc)
                                })

                        if exchange == "okx":
                            try:
                                vip_borrow_rates = [item for item in borrow_rates if item['market/vip'] == "vip"]
                                vip_borrow_rate = sum([item['borrow_rates_value']['rate'] * item['borrow_rates_value']['scalar'] for item in vip_borrow_rates]) / len(vip_borrow_rates)
                            except:
                                pass

                            for i in range(1, 7):
                                for base_ccy in config['funding_rates']['base_ccy']:
                                    if base_ccy == borrow_ccy:
                                        funding = i * fundingRatesValue[-1]["fundingRate"] * fundingRatesValue[-1]["scalar"] - (i-1) * vip_borrow_rate
                                    else:
                                        funding = i * fundingRatesValue[-1]["fundingRate"] * fundingRatesValue[-1]["scalar"] - i * vip_borrow_rate

                                    long_fundings.append({
                                        'venue': exchange,
                                        'base': fundingRatesValue[-1]['base'],
                                        'quote': fundingRatesValue[-1]['quote'],
                                        'base_ccy': base_ccy,
                                        'n': i,
                                        'long_funding_value': {
                                            'symbol': fundingRatesValue[-1]['base'] + "/" + fundingRatesValue[-1]['quote'],
                                            'funding': funding,
                                            'timestamp': fundingRatesValue[-1]["timestamp"]
                                        },
                                        'market/vip': "vip",
                                        'runid': latest_run_id,
                                        'timestamp': datetime.now(timezone.utc)
                                    })

                        self.long_funding_db.insert_many(long_fundings)

                        borrow_rates = list(self.borrow_rates_db.find({
                            "$and": [
                                {"venue": exchange},
                                {"code": symbol.split("/")[0]},
                                {"borrow_rates_value.timestamp": {"$gte": max(last_time - 1, fundingRatesValue[-1]["timestamp"] - 28800000), "$lt": fundingRatesValue[-1]["timestamp"]}}
                            ]
                        }))
                        market_borrow_rate = 0
                        vip_borrow_rate = 0
                        try:
                            market_borrow_rates = [item for item in borrow_rates if item['market/vip'] == "market"]
                            market_borrow_rate = sum([item['borrow_rates_value']['rate'] * item['borrow_rates_value']['scalar'] for item in market_borrow_rates]) / len(market_borrow_rates)
                        except:
                            pass

                        short_fundings = []

                        for i in range(1, 7):
                            short_fundings.append({
                                'venue': exchange,
                                'base': fundingRatesValue[-1]['base'],
                                'quote': fundingRatesValue[-1]['quote'],
                                'n': i,
                                'short_funding_value': {
                                    'symbol': fundingRatesValue[-1]['base'] + "/" + fundingRatesValue[-1]['quote'],
                                    'funding': -i * (fundingRatesValue[-1]["fundingRate"] + market_borrow_rate),
                                    'timestamp': fundingRatesValue[-1]["timestamp"]
                                },
                                'market/vip': "market",
                                'runid': latest_run_id,
                                'timestamp': datetime.now(timezone.utc)
                            })

                        if exchange == "okx":
                            try:
                                vip_borrow_rates = [item for item in borrow_rates if item['market/vip'] == "vip"]
                                vip_borrow_rate = sum([item['borrow_rates_value']['rate'] * item['borrow_rates_value']['scalar'] for item in vip_borrow_rates]) / len(vip_borrow_rates)
                            except:
                                pass

                            for i in range(1, 7):
                                short_fundings.append({
                                    'venue': exchange,
                                    'base': fundingRatesValue[-1]['base'],
                                    'quote': fundingRatesValue[-1]['quote'],
                                    'n': i,
                                    'short_funding_value': {
                                        'symbol': fundingRatesValue[-1]['base'] + "/" + fundingRatesValue[-1]['quote'],
                                        'funding': -i * (fundingRatesValue[-1]["fundingRate"] + vip_borrow_rate),
                                        'timestamp': fundingRatesValue[-1]["timestamp"]
                                    },
                                    'market/vip': "vip",
                                    'runid': latest_run_id,
                                    'timestamp': datetime.now(timezone.utc)
                                })

                        self.short_funding_db.insert_many(short_fundings)

                    except Exception as e:
                        if logger == None:
                            print(exchange + " spot funding " + str(e))
                        else:
                            logger.warning(exchange + " spot funding " + str(e))

                else:
                    if exchange == "binance":
                        funding_rate = Helper().get_funding_rate(
                            exch=exch, symbol=symbol
                        )

                        self.funding_rates_db.update_one(
                            {
                                "venue": exchange,
                                "symbol": symbol.split(":")[0],
                                "funding_rates_value.timestamp": current_values['timestamp']
                            },
                            {
                                "$set": {
                                    "funding_rates_value.nextFundingRate": funding_rate['fundingRate']
                                }
                            },
                            upsert=False
                        )
                    elif exchange == "bybit":
                        funding_rate = BybitHelper().get_funding_rate(
                            exch=exch, symbol=symbol
                        )

                        self.funding_rates_db.update_one(
                            {
                                "venue": exchange,
                                "symbol": symbol.split(":")[0],
                                "funding_rates_value.timestamp": current_values['timestamp']
                            },
                            {
                                "$set": {
                                    "funding_rates_value.nextFundingRate": funding_rate['fundingRate']
                                }
                            },
                            upsert=False
                        )
                    elif exchange == "okx":
                        funding_rate = OKXHelper().get_funding_rate(exch=exch, symbol=symbol)

                        self.funding_rates_db.update_one(
                            {
                                "venue": exchange,
                                "symbol": symbol.split(":")[0],
                                "funding_rates_value.timestamp": current_values['timestamp']
                            },
                            {
                                "$set": {
                                    "funding_rates_value.nextFundingRate": funding_rate['fundingRate']
                                }
                            },
                            upsert=False
                        )
                    elif exchange == "huobi":
                        funding_rate = HuobiHelper().get_funding_rate(exch=exch, symbol=symbol)

                        self.funding_rates_db.update_one(
                            {
                                "venue": exchange,
                                "symbol": symbol.split(":")[0],
                                "funding_rates_value.timestamp": current_values['timestamp']
                            },
                            {
                                "$set": {
                                    "funding_rates_value.nextFundingRate": funding_rate['fundingRate']
                                }
                            },
                            upsert=False
                        )
                    pass
            
            except ccxt.ExchangeError as e:
                if logger == None:
                    print(exchange + " funding rates " + str(e))
                    print("Unable to collect funding rates for " + exchange)
                else:
                    logger.warning(exchange + " funding rates " + str(e))
                    logger.error("Unable to collect funding rates for " + exchange)

                return True
            except ccxt.NetworkError as e:
                if logger == None:
                    print(exchange + " funding rates " + str(e))
                else:
                    logger.warning(exchange + " funding rates " + str(e))

                return False

        if len(fundingRatesValue) <= 0:
            return True

        funding_rates = []

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)

        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass

        for item in fundingRatesValue:
            new_value = {
                "venue": exchange,
                "funding_rates_value": item,
                "symbol": item['base'] + "/" + item['quote'],
                "active": True,
                "entry": False,
                "exit": False,
                "timestamp": datetime.now(timezone.utc),
            }

            if spot:
                new_value["spotMarket"] = spot
            if future:
                new_value["futureMarket"] = future
            if perp:
                new_value["perpMarket"] = perp

            new_value["runid"] = latest_run_id

            funding_rates.append(new_value)

        del fundingRatesValue

        if len(funding_rates) <= 0:
            if logger == None:
                print("Empty funding rates for " + exchange)
                print("Unable to collect funding rates for " + exchange)
            else:
                logger.info("Empty funding rates for " + exchange)
                logger.error("Unable to collect funding rates for " + exchange)

            return True

        try:
            self.funding_rates_db.insert_many(funding_rates)

            del funding_rates

            if logger == None:
                print("Collected funding rates for " + exchange)
            else:
                logger.info("Collected funding rates for " + exchange)

            return True

        except Exception as e:
            if logger == None:
                print(exchange + " funding rates " + str(e))
                print("Unable to collect funding rates for " + exchange)
            else:
                logger.error(exchange + " funding rates " + str(e))
                logger.error("Unable to collect funding rates for " + exchange)

            return True
