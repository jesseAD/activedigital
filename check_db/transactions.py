import pymongo
from datetime import datetime, timezone
from dotenv import dotenv_values

secrets = dotenv_values()

mongo_uri = 'mongodb+srv://activedigital:' + secrets['CLOUD_MONGO_PASSWORD'] + '@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)

active_digital = mongo_client['active_digital']['transactions']
active_digita = mongo_client['active_digita']['transactions']

transactions1 = list(
  active_digital.find({'client': "edison", 'venue': "okx", 'account': "submn1", 'transaction_value.timestamp': {'$gt': 1713261653423, '$lte': 1713348053423}})
)
transactions2 = list(
  active_digita.find({'client': "edison", 'venue': "okx", 'account': "submn1", 'transaction_value.timestamp': {'$gt': 1713261653423, '$lte': 1713348053423}})
)

print("read")

print(len(transactions1))
print(len(transactions2))

for i in range(len(transactions1)):
  if transactions1[i]['transaction_value']['info'] != transactions2[i]['transaction_value']['info']:
    print("Not Same")
    print(i)
    # print(transactions1[i]['transaction_value'])
    # print(transactions2[i]['transaction_value'])