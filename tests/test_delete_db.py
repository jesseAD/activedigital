import pymongo
from datetime import datetime, timezone

mongo_uri = 'mongodb+srv://activedigital:pwd@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)
db = mongo_client['active_digital']['roll_costs']

# collections = ['borrow_rates', 'fills', 
#             'funding_rates', 'transactions'
#             ]

# for collection in collections:
#     db[collection].delete_many(
#         {'runid': 51896}
#     )

# data = list(db['positions_archive'].find())
# print("read")
# for i in range(0, len(data), 500):
#     db['positions_temp'].insert_many(data[i, i + 499])

# query = {'$and': [{'venue': 'bybit'}, {'fills_value.timestamp': {'$gte': 1710028800000}}]}
# query = {'$and': [{'client': 'lucid'}, {'venue': 'binance'}, {'account': 'subls1'}]}

db.delete_many({'runid': {'$lte': 57230}})

# db.update_many(
#     {'runid': {'$lt': 57106}},
#     {'$set': {'convert_ccy': 'USDT'}}
# )