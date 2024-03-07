import pymongo
from datetime import datetime, timezone

mongo_uri = 'mongodb+srv://activedigital:8EnNmGsai9pD0gxq@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)
db = mongo_client['active_digital']

# collections = ['borrow_rates', 'fills', 
#             'funding_rates', 'transactions'
#             ]

# for collection in collections:
#     db[collection].delete_many(
#         {'runid': 51896}
#     )

data = list(db['positions_archive'].find())
print("read")
for i in range(0, len(data), 500):
    db['positions_temp'].insert_many(data[i, i + 499])

# query = {'$and': [{'client': 'nifty'}, {'venue': 'binance'}, {'runid': {'$gte': 53650}}]}
# query = {'$and': [{'client': 'lucid'}, {'venue': 'binance'}, {'account': 'subls1'}]}

# db.delete_many(query)

# db.update_many(
#     {'runid': {'$lte': 45162}},
#     {'$set': {'market/vip': 'market'}}
# )