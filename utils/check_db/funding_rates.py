import pymongo
from datetime import datetime, timezone
from dotenv import dotenv_values

secrets = dotenv_values()

mongo_uri = 'mongodb+srv://activedigital:' + secrets['CLOUD_MONGO_PASSWORD'] + '@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)

active_digital = mongo_client['active_digital']['funding_rates']
active_digita = mongo_client['active_digita']['funding_rates']

funding_rates1 = list(
  active_digital.find({'venue': "bybit", 'funding_rates_value.timestamp': {'$gt': 1713110400000, '$lte': 1713369600000}})
)
funding_rates2 = list(
  active_digita.find({'venue': "bybit", 'funding_rates_value.timestamp': {'$gt': 1713110400000, '$lte': 1713369600000}})
)

print("read")

dict1 = {}
for item in funding_rates1:
  if item['symbol'] not in dict1:
    dict1[item['symbol']] = []

  dict1[item['symbol']].append(item['funding_rates_value'])

dict2 = {}
for item in funding_rates2:
  if item['symbol'] not in dict2:
    dict2[item['symbol']] = []

  dict2[item['symbol']].append(item['funding_rates_value'])

for item in dict2:
  for i in range(len(dict2[item])):
    if (
      dict1[item][i]['symbol'] != dict2[item][i]['symbol'] or 
      dict1[item][i]['fundingRate'] != dict2[item][i]['fundingRate'] or
      dict1[item][i]['timestamp'] != dict2[item][i]['timestamp']
    ):
      print(dict1[item][i]['symbol'] + " " + dict1[item][i]['datetime'])