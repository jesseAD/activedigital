import pymongo
from datetime import datetime, timezone, timedelta
from dotenv import dotenv_values

secrets = dotenv_values()

mongo_uri = 'mongodb+srv://activedigital:' + secrets['CLOUD_MONGO_PASSWORD'] + '@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)
# mongo_client = pymongo.MongoClient(None)
db = mongo_client['active_digital']

# collections = [
#   'balances', 'fills', 'positions', 'transactions', 'open_orders', 'leverages', 'lifetime_funding', 'mtd_pnls',
#   'bid_asks', 'borrow_rates', 'funding_rates', 'index_prices', 'instruments', 'long_funding', 'mark_prices',
#   'open_positions_price_change', 'roll_costs', 'runs', 'short_funding', 'split_positions'
# ]

# for collection in collections:
#   db[collection].delete_many({
#     'runid': {'$lte': 38}
#   })

# for collection in collections:
#   db[collection].update_many(
#     {'$or': [{'client': "nifty1USD"}, {'client': "nifty2"}]},
#     {'$set': {'client': 'nifty'}}
#   )

db['transactions_union'].delete_many({'client': "edison", 'venue': "okx", 'account': "submn1", 'runid': {'$lt': 71647}})

# print(list(db['tickers'].find({'venue': "binance"}))[0]['ticker_value']['DYDX/USDT'])
# db['balances'].update_many(
#   {},
#   {'$rename': {"vip_level": "tier"}}
# )

# data = list(db['positions_archive'].find())
# print("read")
# for i in range(0, len(data), 500):
#     db['positions_temp'].insert_many(data[i, i + 499])

# query = {'$and': [{'venue': 'bybit'}, {'fills_value.timestamp': {'$gte': 1710028800000}}]}
# query = {'$and': [{'client': 'lucid'}, {'venue': 'binance'}, {'account': 'subls1'}]}

# db['daily_returns'].delete_many({'client': "nifty", 'venue': "bybit", 'account': "subbasis1"})
# balances = db['balances'].find({
#   'client': "nifty",
#   'account': "subbasis2",
#   'venue': "bybit",
#   'timestamp': {
#     '$gte': datetime(2024, 5, 13, 23, 50),
#     '$lte': datetime(2024, 5, 15, 0)
#   }
# })
# print([item['balance_value']['base'] for item in balances])

# db['balances'].update_many(
#   {'runid': {'$gt': 66763}, 'client': "nifty", 'venue': "bybit", 'account': "subbasis1"},
#   {'$set': {'collateral': 7.5}}
# )

# db['leverages'].update_many(
#   {'venue': "binance", 'runid': {'$gte': 67175, '$lte': 67617}, 'client': "nifty", 'account': "subbasis1"},
#   {'$set': {'leverage': 0.000005786932240871664}}
# )

# db['transactions'].update_many(
#   {'venue': "bybit", 'runid': {'$lte': 25251}, 'trade_type': "commission"},
#   [{'$set': {
#     'transaction_value.fee': {'$subtract': [0, "$transaction_value.fee"]},
#     'transaction_value.fee_origin': {'$subtract': [0, "$transaction_value.fee_origin"]},
#     'transaction_value.fee_base': {'$subtract': [0, "$transaction_value.fee_base"]}
#   }}]
# )

# db['daily_returns'].delete_many({
#   'client': {'$ne': "shannon"},
  # 'venue': "deribit",
  # 'account': "submn1"
# })

# borrow_rates_db = db['borrow_rates']
# codes = list(borrow_rates_db.aggregate([
#   {'$match': {'venue': "binance"}},
#   {'$group': {
#     '_id': "$code",
#   }}
# ]))
# codes = [item['_id'] for item in codes]

# # print(codes)

# recent_codes = list(borrow_rates_db.aggregate([
#   {'$match': {'venue': "binance"}},
#   {'$match': {'timestamp': {'$gte': datetime.now() - timedelta(days=30)}}},
#   {'$group': {
#     '_id': "$code",
#   }}
# ]))
# recent_codes = [item['_id'] for item in recent_codes]
# print(recent_codes)

# for code in codes:
#   if code not in recent_codes:
#     print(code)
#     borrow_rates_db.delete_many({
#       'venue': "binance",
#       'code': code
#     })