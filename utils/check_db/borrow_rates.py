import pymongo
from datetime import datetime, timezone
from dotenv import dotenv_values

secrets = dotenv_values()

mongo_uri = 'mongodb+srv://activedigital:' + secrets['CLOUD_MONGO_PASSWORD'] + '@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)

active_digital = mongo_client['active_digital']['borrow_rates']
active_digita = mongo_client['active_digita']['borrow_rates']

borrow_rates1 = list(
  active_digital.find({'venue': "bybit", 'vip/market': "market", 'borrow_rates_value.timestamp': {'$gt': 1713247100000, '$lte': 1713333500000}})
)
borrow_rates2 = list(
  active_digita.find({'venue': "bybit", 'vip/market': "market", 'borrow_rates_value.timestamp': {'$gt': 1713247100000, '$lte': 1713333500000}})
)

print("read")

dict1 = {}
for item in borrow_rates1:
  if item['code'] not in dict1:
    dict1[item['code']] = []

  dict1[item['code']].append(item['borrow_rates_value'])

dict2 = {}
for item in borrow_rates2:
  if item['code'] not in dict2:
    dict2[item['code']] = []

  dict2[item['code']].append(item['borrow_rates_value'])

for item in dict2:
  for i in range(len(dict2[item])):
    if (
      dict1[item][i]['currency'] != dict2[item][i]['currency'] or 
      dict1[item][i]['rate'] != dict2[item][i]['rate'] or
      dict1[item][i]['timestamp'] != dict2[item][i]['timestamp']
    ):
      print(dict1[item][i]['currency'] + " " + dict1[item][i]['datetime'])