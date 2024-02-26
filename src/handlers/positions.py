import os, json
from dotenv import load_dotenv
from datetime import datetime, timezone
import time
import ccxt
import pdb
# from src.lib.db import MongoDB
from src.lib.log import Log
from src.lib.exchange import Exchange
from src.lib.mapping import Mapping
from src.lib.unhedged import get_unhedged
from src.config import read_config_file
from src.handlers.helpers import Helper, OKXHelper, BybitHelper
# from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()


class Positions:
    def __init__(self, db, collection):

        self.runs_db = db['runs']
        self.tickers_db = db['tickers']
        self.balances_db = db['balances']
        self.funding_rates_db = db['funding_rates']
        self.lifetime_funding_db = db['lifetime_funding']
        self.split_positions_db = db['split_positions']
        self.positions_db = db['positions']
        self.mark_prices_db = db['mark_prices']
        self.price_changes_db = db['open_positions_price_change']

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
        logger=None
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

                    # logger.info("okx positions: " + json.dumps(position_value))

                elif exchange == "binance":
                    if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'portfolio':
                        position_value = Helper().get_pm_positions(exch=exch)
                        for item in position_value:
                            item['info'] = {**item}
                            item['marginMode'] = "cross"
                    else:
                        position_value = Helper().get_positions(exch=exch)
                        for item in position_value:
                            item['info'] = {**item}
                            item['marginMode'] = "cross"

                        # if client == "lucid":
                        #     logger.info("lucid positions: " + json.dumps(position_value))

                elif exchange == "bybit":
                    position_value = BybitHelper().get_positions(exch=exch)

                position_value = Mapping().mapping_positions(exchange=exchange, positions=position_value)

            # except ccxt.InvalidNonce as e:
            #     print("Hit rate limit", e)
            #     time.sleep(
            #         back_off[client + "_" + exchange + "_" + sub_account] / 1000.0
            #     )
            #     back_off[client + "_" + exchange + "_" + sub_account] *= 2
            #     return True

            except ccxt.ExchangeError as e:
                logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
                # print("An error occurred in Positions:", e)
                return True
                
        try:
            position_info = []
            liquidation_buffer = None
            tickers = list(self.tickers_db.find({"venue": exchange}))[0]["ticker_value"]

            if exchange == "okx":
                try:
                    cross_margin_ratio = float(
                        OKXHelper().get_cross_margin_ratio(exch=exch)
                    )
                    liquidation_buffer = OKXHelper().calc_liquidation_buffer(
                        exchange=exchange, mgnRatio=cross_margin_ratio
                    )

                # except ccxt.InvalidNonce as e:
                #     print("Hit rate limit", e)
                #     time.sleep(
                #         back_off[client + "_" + exchange + "_" + sub_account] / 1000.0
                #     )
                #     back_off[client + "_" + exchange + "_" + sub_account] *= 2
                #     return True

                except ccxt.ExchangeError as e:
                    logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
                    # print("An error occurred in Positions:", e)
                    pass
                except Exception as e:
                    logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
                    pass

            elif exchange == "bybit":
                try:
                    cross_margin_ratio = float(
                        BybitHelper().get_cross_margin_ratio(exch=exch)
                    )
                    liquidation_buffer = BybitHelper().calc_liquidation_buffer(
                        exchange=exchange, mgnRatio=cross_margin_ratio
                    )

                # except ccxt.InvalidNonce as e:
                #     print("Hit rate limit", e)
                #     time.sleep(
                #         back_off[client + "_" + exchange + "_" + sub_account] / 1000.0
                #     )
                #     back_off[client + "_" + exchange + "_" + sub_account] *= 2
                #     return True

                except ccxt.ExchangeError as e:
                    logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
                    # print("An error occurred in Positions:", e)
                    pass
                except Exception as e:
                    logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
                    pass

            elif exchange == "binance":
                if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'portfolio':
                    try:
                        cross_margin_ratio = float(
                            Helper().get_pm_cross_margin_ratio(exch=exch)
                        )
                        liquidation_buffer = Helper().calc_liquidation_buffer(
                            exchange=exchange, mgnRatio=cross_margin_ratio
                        )

                    # except ccxt.InvalidNonce as e:
                    #     print("Hit rate limit", e)
                    #     time.sleep(
                    #         back_off[client + "_" + exchange + "_" + sub_account] / 1000.0
                    #     )
                    #     back_off[client + "_" + exchange + "_" + sub_account] *= 2
                    #     return True

                    except ccxt.ExchangeError as e:
                        logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
                        # print("An error occurred in Positions:", e)
                        pass
                    except Exception as e:
                        logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
                        pass
                else:
                    try:
                        liquidation1 = Helper().calc_liquidation_buffer(
                            exchange=exchange, mgnRatio=Helper().get_cross_margin_ratio(exch=exch)
                        )
                        liquidation2 = Helper().calc_liquidation_buffer(
                            exchange=exchange+"_cm", mgnRatio=Helper().get_cm_margin_ratio(exch=exch)
                        )
                        liquidation3 = Helper().calc_liquidation_buffer(
                            exchange=exchange+"_um", mgnRatio=Helper().get_um_margin_ratio(exch=exch)
                        )
                        liquidation_buffer = min(liquidation1, liquidation2, liquidation3)

                    # except ccxt.InvalidNonce as e:
                    #     print("Hit rate limit", e)
                    #     time.sleep(
                    #         back_off[client + "_" + exchange + "_" + sub_account] / 1000.0
                    #     )
                    #     back_off[client + "_" + exchange + "_" + sub_account] *= 2
                    #     return True

                    except ccxt.ExchangeError as e:
                        logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
                        # print("An error occurred in Positions:", e)
                        pass
                    except Exception as e:
                        logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
                        pass

            for value in position_value:
                # if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'non_portfolio':
                if exchange != "binance":
                        # portfolio = None
                        # if exchange == "binance":
                        #     try:
                        #         if (
                        #             config["clients"][client]["subaccounts"][exchange][
                        #                 sub_account
                        #             ]["margin_mode"] == "non_portfolio"
                        #         ):
                        #             portfolio = Helper().get_non_portfolio_margin(
                        #                 exch=exch,
                        #                 params={"symbol": value["info"]["symbol"]},
                        #             )
                        #         elif (
                        #             config["clients"][client]["subaccounts"][exchange][
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
                    try:
                        if exchange == "bybit":
                            value['symbol'] = value['base'] + value['quote'] + "-PERP"
                            value["liquidationBuffer"] = liquidation_buffer
                        else:
                            if ":" in value['symbol']:
                                value["base"] = value["symbol"].split("/")[0]
                                value["quote"] = (
                                    value["symbol"].split("-")[0].split("/")[1].split(":")[0]
                                )
                            else:
                                value['base'] = value["symbol"].split("-")[0].split("/")[0]
                                value['quote'] = value['symbol'].split("-")[1] if "-" in value['symbol'] else value['symbol'].split("_")[0].split("/")[1]

                            value['symbol'] = value['base'] + value['quote'] + "-PERP"
                            value["liquidationBuffer"] = liquidation_buffer

                            if value["quote"] == "USD":
                                value["notional"] = float(
                                    value["notional"]
                                ) * Helper().calc_cross_ccy_ratio(
                                    value["base"],
                                    config["clients"][client]["subaccounts"][exchange][
                                        "base_ccy"
                                    ],
                                    tickers,
                                )
                                value["unrealizedPnl"] = float(
                                    value["unrealizedPnl"]
                                ) * Helper().calc_cross_ccy_ratio(
                                    value["base"],
                                    config["clients"][client]["subaccounts"][exchange][
                                        "base_ccy"
                                    ],
                                    tickers,
                                )
                        position_info.append(value)

                    except Exception as e:
                        logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))

                else:
                    if (float(value["initialMargin"]) != 0.0  if config['clients'][client]['subaccounts'][exchange][sub_account]['margin_mode'] == 'non_portfolio' else True):
                        try:
                            value["base"] = value["symbol"].split("_")[0].split("USD")[0]
                            value["quote"] = "USD" + value["symbol"].split("_")[0].split("USD")[1]
                            value['symbol'] = value['base'] + value['quote'] + "-PERP"
                            value['side'] = "long" if float(value['contracts']) > 0 else "short"
                            
                            value["liquidationBuffer"] = liquidation_buffer

                            if value["quote"] == "USD":
                                value["notional"] = float(
                                    value["notional"]
                                ) * Helper().calc_cross_ccy_ratio(
                                    value["base"],
                                    config["clients"][client]["subaccounts"][exchange][
                                        "base_ccy"
                                    ],
                                    tickers,
                                )
                                value["unrealizedPnl"] = float(
                                    value["unrealizedPnl"]
                                ) * Helper().calc_cross_ccy_ratio(
                                    value["base"],
                                    config["clients"][client]["subaccounts"][exchange][
                                        "base_ccy"
                                    ],
                                    tickers,
                                )

                            position_info.append(value)

                        except Exception as e:
                            logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
            
            if exchange == "binance":
                for position in position_info:
                    try:
                        mark_prices = self.mark_prices_db.find({'venue': exchange, 'symbol': position['base'] + "/" + position['quote']}).sort('_id', -1).limit(1)
                        for item in mark_prices:
                            try:
                                mark_price = item['mark_price_value']['markPrice']
                            except:
                                pass
                            
                        position["markPrice"] = mark_price
                    except ccxt.ExchangeError as e:
                        logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
                        # print("An error occurred in Positions:", e)
                        pass

        # except ccxt.InvalidNonce as e:
        #     print("Hit rate limit", e)
        #     time.sleep(back_off[client + "_" + exchange + "_" + sub_account] / 1000.0)
        #     back_off[client + "_" + exchange + "_" + sub_account] *= 2
        #     return True

        except ccxt.ExchangeError as e:
            logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
            # print("An error occurred in Positions:", e)
            pass

        except Exception as e:
            logger.warning(client + " " + exchange + " " + sub_account + " positions " + str(e))
            pass

        if client == "lucid" and len(position_info) < 13:
            logger.info(json.dumps(position_value))
            logger.info(json.dumps(exch.fapiprivatev2_get_account()))
        
        del position_value
        
        # back_off[client + "_" + exchange + "_" + sub_account] = config["dask"]["back_off"]

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"] + 1
            except:
                pass

        # open positions price changes

        price_changes = []
        for position in position_info:
            try:
                ticker = float(tickers[position['base'] + "/USDT"]['last'])
                mark_klines = exch.fetch_mark_ohlcv(symbol = position['base'] + "/USDT", timeframe = '2h', limit=1)

                price_change = {
                    'client': client,
                    'venue': exchange,
                    'account': sub_account,
                    'base': position['base'],
                    'symbol': position['symbol'],
                    'price_change': (ticker - mark_klines[0][1]) / mark_klines[0][1] * 100,
                    'runid': latest_run_id,
                    'timestamp': datetime.now(timezone.utc)
                }
                price_changes.append(price_change)

            except Exception as e:
                logger.warning(client + " " + exchange + " " + sub_account + " price changes " + str(e))
                pass

        try:
            self.price_changes_db.insert_many(price_changes)

        except Exception as e:
            logger.warning(client + " " + exchange + " " + sub_account + " price changes " + str(e))
            pass

        del price_changes

        # life time funding rates

        query = {}
        query['client'] = client
        query['venue'] = exchange
        query['account'] = sub_account

        lifetime_funding_values = list(self.lifetime_funding_db.find(query))
        lifetime_funding_values.sort(key = lambda x: x['symbol'])
        position_info.sort(key = lambda x: x['symbol'])

        fundings = []
        current_time = int(datetime.now(timezone.utc).timestamp() * 1000)
        i = 0
        j = 0
        while True:
            try:
                if i == len(position_info) and j == len(lifetime_funding_values):
                    break
                if i == len(position_info):
                    funding = {
                        'client': client,
                        'venue': exchange,
                        'account': sub_account,
                        'symbol': lifetime_funding_values[j]['symbol'],
                        'funding': lifetime_funding_values[j]['funding'],
                        'state': "closed",
                        'open_close_time': lifetime_funding_values[j]['open_close_time'],
                        'last_time': lifetime_funding_values[j]['last_time'],
                        'base': lifetime_funding_values[j]['base'],
                        'quote': lifetime_funding_values[j]['quote'],
                    }
                    if lifetime_funding_values[j]['state'] == "closed":
                        if lifetime_funding_values[j]['open_close_time'] > lifetime_funding_values[j]['last_time']:
                            query = {
                                'venue': exchange,
                                'symbol': lifetime_funding_values[j]['base'] + "/USDT" if lifetime_funding_values[j]['quote'] == "USDT" else lifetime_funding_values[j]['base'] + "/USD"
                            }
                            funding_rates = self.funding_rates_db.find(query).sort("_id", -1).limit(1)
                            funding_rate = 0.0
                            funding_time = 0
                            for item in funding_rates:
                                funding_rate = item['funding_rates_value']['fundingRate']
                                funding_time = item['funding_rates_value']['timestamp']

                            if funding_time > lifetime_funding_values[j]['last_time']:
                                funding['last_time'] = lifetime_funding_values[j]['open_close_time']
                                funding['funding'] += (funding_rate * (lifetime_funding_values[j]['open_close_time'] - lifetime_funding_values[j]['last_time']) / 28800000)
                    else:
                        funding['open_close_time'] = current_time

                    fundings.append(funding)
                    j += 1

                elif j == len(lifetime_funding_values):
                    fundings.append({
                        'client': client,
                        'venue': exchange,
                        'account': sub_account,
                        'symbol': position_info[i]['symbol'],
                        'funding': 0.0,
                        'state': "open",
                        'open_close_time': current_time,
                        'last_time': current_time,
                        'base': position_info[i]['base'],
                        'quote': position_info[i]['quote']
                    })
                    position_info[i]['lifetime_funding_rates'] = 0.0
                    i += 1

                else:
                    if position_info[i]['symbol'] == lifetime_funding_values[j]['symbol']:
                        if lifetime_funding_values[j]['state'] == "closed":
                            if lifetime_funding_values[j]['open_close_time'] > lifetime_funding_values[j]['last_time']:
                                fundings.append({
                                    'client': client,
                                    'venue': exchange,
                                    'account': sub_account,
                                    'symbol': lifetime_funding_values[j]['symbol'],
                                    'funding': lifetime_funding_values[j]['funding'],
                                    'state': "open",
                                    'open_close_time': current_time - lifetime_funding_values[j]['open_close_time'] + lifetime_funding_values[j]['last_time'],
                                    'last_time': lifetime_funding_values[j]['last_time'],
                                    'base': lifetime_funding_values[j]['base'],
                                    'quote': lifetime_funding_values[j]['quote']
                                })
                            else:
                                fundings.append({
                                    'client': client,
                                    'venue': exchange,
                                    'account': sub_account,
                                    'symbol': lifetime_funding_values[j]['symbol'],
                                    'funding': lifetime_funding_values[j]['funding'],
                                    'state': "open",
                                    'open_close_time': current_time,
                                    'last_time': current_time,
                                    'base': lifetime_funding_values[j]['base'],
                                    'quote': lifetime_funding_values[j]['quote']
                                })
                            position_info[i]['lifetime_funding_rates'] = lifetime_funding_values[j]['funding']
                        else:
                            query = {
                                'venue': exchange,
                                'symbol': position_info[i]['base'] + "/USDT" if position_info[i]['quote'] == "USDT" else position_info[i]['base'] + "/USD"
                            }
                            funding_rates = self.funding_rates_db.find(query).sort("_id", -1).limit(1)
                            funding_rate = 0.0
                            funding_time = 0
                            for item in funding_rates:
                                funding_rate = item['funding_rates_value']['fundingRate']
                                funding_time = item['funding_rates_value']['timestamp']

                            if lifetime_funding_values[j]['open_close_time'] < funding_time and lifetime_funding_values[j]['last_time'] < funding_time:
                                fundings.append({
                                    'client': client,
                                    'venue': exchange,
                                    'account': sub_account,
                                    'symbol': position_info[i]['symbol'],
                                    'funding': (lifetime_funding_values[j]['funding'] + funding_rate 
                                                * (funding_time - max(lifetime_funding_values[j]['open_close_time'], lifetime_funding_values[j]['last_time'])) / 28800000),
                                    'state': "open",
                                    'open_close_time': lifetime_funding_values[j]['open_close_time'],
                                    'last_time': funding_time,
                                    'base': lifetime_funding_values[j]['base'],
                                    'quote': lifetime_funding_values[j]['quote']
                                })
                                position_info[i]['lifetime_funding_rates'] = fundings[-1]['funding']
                            else:
                                position_info[i]['lifetime_funding_rates'] = lifetime_funding_values[j]['funding']

                        i += 1
                        j += 1

                    elif position_info[i]['symbol'] < lifetime_funding_values[j]['symbol']:
                        fundings.append({
                            'client': client,
                            'venue': exchange,
                            'account': sub_account,
                            'symbol': position_info[i]['symbol'],
                            'funding': 0.0,
                            'state': "open",
                            'open_close_time': current_time,
                            'last_time': current_time,
                            'base': position_info[i]['base'],
                            'quote': position_info[i]['quote']
                        })
                        position_info[i]['lifetime_funding_rates'] = 0.0
                        i += 1

                    elif position_info[i]['symbol'] > lifetime_funding_values[j]['symbol']:
                        funding = {
                            'client': client,
                            'venue': exchange,
                            'account': sub_account,
                            'symbol': lifetime_funding_values[j]['symbol'],
                            'funding': lifetime_funding_values[j]['funding'],
                            'state': "closed",
                            'open_close_time': lifetime_funding_values[j]['open_close_time'],
                            'last_time': lifetime_funding_values[j]['last_time'],
                            'base': lifetime_funding_values[j]['base'],
                            'quote': lifetime_funding_values[j]['quote'],
                        }
                        if lifetime_funding_values[j]['state'] == "closed":
                            if lifetime_funding_values[j]['open_close_time'] > lifetime_funding_values[j]['last_time']:
                                query = {
                                    'venue': exchange,
                                    'symbol': lifetime_funding_values[j]['base'] + "/USDT" if lifetime_funding_values[j]['quote'] == "USDT" else lifetime_funding_values[j]['base'] + "/USD"
                                }
                                funding_rates = self.funding_rates_db.find(query).sort("_id", -1).limit(1)
                                funding_rate = 0.0
                                funding_time = 0
                                for item in funding_rates:
                                    funding_rate = item['funding_rates_value']['fundingRate']
                                    funding_time = item['funding_rates_value']['timestamp']

                                if funding_time > lifetime_funding_values[j]['last_time']:
                                    funding['last_time'] = lifetime_funding_values[j]['open_close_time']
                                    funding['funding'] += (funding_rate * (lifetime_funding_values[j]['open_close_time'] - lifetime_funding_values[j]['last_time']) / 28800000)
                        else:
                            funding['open_close_time'] = current_time

                        fundings.append(funding)
                        j += 1

            except Exception as e:
                logger.warning(client + " " + exchange + " " + sub_account + " lifetime fundings " + str(e))

        for item in fundings:
            try:
                self.lifetime_funding_db.update_one(
                    {
                        'client': client,
                        'venue': exchange,
                        'account': sub_account,
                        'symbol': item['symbol']
                    },
                    {"$set": {
                        'state': item['state'],
                        'funding': item['funding'],
                        'open_close_time': item['open_close_time'],
                        'last_time': item['last_time'],
                        'base': item['base'],
                        'quote': item['quote']
                    }},
                    upsert=True
                )
            except Exception as e:
                logger.error(client + " " + exchange + " " + sub_account + " lifetime fundings " + str(e))
                # print("An error occurred in Lifetime Funding:", e)

        # calculate unhedged

        if config['clients'][client]['split_positions'] == True:
            query = {}

            if client:
                query["client"] = client            
            if sub_account:
                query["account"] = sub_account
            if exchange:
                query["venue"] = exchange

            balance_values = self.balances_db.find(query).sort("_id", -1).limit(1)

            balance = None
            for item in balance_values:
                timestamp = item['timestamp']
                balance = item["balance_value"]

            spot_positions = []

            if balance != None:
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
                        spot_position['lifetime_funding_rates'] = 0
                        spot_position['marginMode'] = None
                        spot_position['timestamp'] = int(timestamp.timestamp() * 1000)
                        spot_position['side'] = "long" if _val > 0 else "short"
                        spot_position['markPrice'] = 1 if _key == "USDT" else (0 if (_key + "/USDT") not in tickers else tickers[_key + "/USDT"]['last'])
                        spot_position['notional'] = spot_position['markPrice'] * spot_position['contracts']

                        spot_positions.append(spot_position)

            split_positions = get_unhedged(position_info, spot_positions)

            hedged_exclusion_positive = config['clients'][client]['subaccounts'][exchange][sub_account]['hedged_exclusion_positive']
            hedged_exclusion_negative = config['clients'][client]['subaccounts'][exchange][sub_account]['hedged_exclusion_negative']
            i = 0
            while i < len(split_positions):
                if len(split_positions[i]) == 1:
                    if split_positions[i][0]['position'] > 0 and split_positions[i][0]['base'] in hedged_exclusion_positive:
                        if hedged_exclusion_positive[split_positions[i][0]['base']] == 0:
                            split_positions.pop(i)
                            i -= 1
                        else:
                            split_positions[i][0]['notional'] *= (1 - hedged_exclusion_positive[split_positions[i][0]['base']] / split_positions[i][0]['position'])
                            split_positions[i][0]['unrealizedPnl'] *= (1 - hedged_exclusion_positive[split_positions[i][0]['base']] / split_positions[i][0]['position'])
                            split_positions[i][0]['unhedgedAmount'] *= (1 - hedged_exclusion_positive[split_positions[i][0]['base']] / split_positions[i][0]['position'])
                            split_positions[i][0]['position'] -= hedged_exclusion_positive[split_positions[i][0]['base']]

                    elif split_positions[i][0]['position'] < 0 and split_positions[i][0]['base'] in hedged_exclusion_negative:
                        if hedged_exclusion_negative[split_positions[i][0]['base']] == 0:
                            split_positions.pop(i)
                            i -= 1
                        else:
                            split_positions[i][0]['notional'] *= (1 + hedged_exclusion_negative[split_positions[i][0]['base']] / split_positions[i][0]['position'])
                            split_positions[i][0]['unrealizedPnl'] *= (1 + hedged_exclusion_negative[split_positions[i][0]['base']] / split_positions[i][0]['position'])
                            split_positions[i][0]['unhedgedAmount'] *= (1 + hedged_exclusion_negative[split_positions[i][0]['base']] / split_positions[i][0]['position'])
                            split_positions[i][0]['position'] += hedged_exclusion_negative[split_positions[i][0]['base']]

                i += 1
            
            current_time = datetime.now(timezone.utc)
            split_position = {
                "client": client,
                "venue": exchange,
                "account": "Main Account",
                "position_value": split_positions,
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
            
            split_position["runid"] = latest_run_id

            try:
                self.split_positions_db.insert_one(split_position)
                
            except Exception as e:
                logger.error(client + " " + exchange + " " + sub_account + " split positions " + str(e))
                return True
            

        current_time = datetime.now(timezone.utc)
        position = {
            "client": client,
            "venue": exchange,
            # "positionType": positionType.lower(),
            "account": "Main Account",
            "position_value": position_info,
            "alert_threshold": config['positions']['alert_threshold'],
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
        # query = {}
        # if client:
        #     query["client"] = client
        # if exchange:
        #     query["venue"] = exchange
        # if sub_account:
        #     query["account"] = sub_account

        # position_values = self.positions_db.find(query).sort("_id", -1).limit(1)

        # latest_run_id = -1
        # latest_value = None
        # for item in position_values:
        #     if latest_run_id < item["runid"]:
        #         latest_run_id = item["runid"]
        #         latest_value = item["position_value"]

        # if latest_value == position["position_value"]:
        #     logger.info(client + " " + exchange + " " + sub_account + " positions " + "same position")
        #     # print("same position")
        #     return True

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

            return True

            # return position
        except Exception as e:
            logger.error(client + " " + exchange + " " + sub_account + " positions " + str(e))
            return True

    # def entry(self, account: str = None, status: bool = True):
    #     # get all positions with account
    #     positions = Positions.get(active=True, account=account)

    #     for position in positions:
    #         try:
    #             self.positions_db.update(
    #                 {"_id": position["_id"]},
    #                 {"$set": {"entry": status}},
    #             )
    #             log.debug(
    #                 f"position in account entry {account} has been set to {status}"
    #             )
    #         except Exception as e:
    #             log.error(e)
    #             return False

    #     return True

    # def exit(self, account: str = None, status: bool = False):
    #     # get all positions with account
    #     positions = Positions.get(active=True, account=account)

    #     for position in positions:
    #         if position["entry"] is False:
    #             log.debug(
    #                 f"position in account {account} has not been entered, skipping"
    #             )
    #             continue
    #         try:
    #             self.positions_db.update(
    #                 {"_id": position["_id"]},
    #                 {"$set": {"exit": status}},
    #             )
    #             log.debug(
    #                 f"Position in account exit {account} has been set to {status}"
    #             )
    #         except Exception as e:
    #             log.error(e)
    #             return False

    #     return True

    # def update(self, account: str = None, **kwargs: dict):
    #     # get all positions with account
    #     positions = Positions.get(account=account)

    #     for position in positions:
    #         try:
    #             self.positions_db.update_one(
    #                 {"_id": position["_id"]},
    #                 {"$set": kwargs},
    #             )
    #             log.debug(f"Position in account {account} has been updated")
    #         except Exception as e:
    #             log.error(e)
    #             return False

    #     return True
