import pymongo
from datetime import datetime, timezone
from dotenv import dotenv_values

secrets = dotenv_values()

mongo_uri = 'mongodb+srv://activedigital:' + secrets['CLOUD_MONGO_PASSWORD'] + '@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)
# mongo_client = pymongo.MongoClient(None)
db = mongo_client['active_digital']

collections = [
  'balances', 'fills', 'positions', 'transactions', 'open_orders', 'leverages', 'lifetime_funding', 'mtd_pnls',
  'bid_asks', 'borrow_rates', 'funding_rates', 'index_prices', 'instruments', 'long_funding', 'mark_prices',
  'open_positions_price_change', 'roll_costs', 'runs', 'short_funding', 'split_positions'
]

# for collection in collections:
#   db[collection].delete_many({
#     'runid': {'$lte': 38}
#   })

# for collection in collections:
#   db[collection].update_many(
#     {'$or': [{'client': "nifty1USD"}, {'client': "nifty2"}]},
#     {'$set': {'client': 'nifty'}}
#   )

# print(list(db['instruments'].find({'venue': "huobi"}))[0]['instrument_value'].keys())

# data = list(db['positions_archive'].find())
# print("read")
# for i in range(0, len(data), 500):
#     db['positions_temp'].insert_many(data[i, i + 499])

# query = {'$and': [{'venue': 'bybit'}, {'fills_value.timestamp': {'$gte': 1710028800000}}]}
# query = {'$and': [{'client': 'lucid'}, {'venue': 'binance'}, {'account': 'subls1'}]}

# db['transactions'].delete_many({'client': "kepler", 'venue': "bybit", 'account': "subbasis1", 'runid': 65312})
balances = db['balances'].find({
  'client': "nifty",
  'account': "subbasis2",
  'venue': "bybit",
  'timestamp': {
    '$gte': datetime(2024, 5, 13, 23, 50),
    '$lte': datetime(2024, 5, 15, 0)
  }
})
print([item['balance_value']['base'] for item in balances])

# db.update_many(
#     {'runid': {'$lt': 57106}},
#     {'$set': {'convert_ccy': 'USDT'}}
# )