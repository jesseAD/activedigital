import pymongo
from datetime import datetime, timezone
from dotenv import dotenv_values

secrets = dotenv_values()

accounts = [
  {'client': "jess", 'venue': "okx", 'account': "subandyactivedigital"},
  {'client': "jess", 'venue': "okx", 'account': "subandypants"},
  {'client': "jess", 'venue': "okx", 'account': "subjessetf"},
  {'client': "jess", 'venue': "okx", 'account': "subadtest2"},
  {'client': "besttrader", 'venue': "okx", 'account': "sub1ap"},
  {'client': "besttrader", 'venue': "okx", 'account': "sub2aa"},
  {'client': "rundmc", 'venue': "okx", 'account': "subaccount1"},
  {'client': "rundmc", 'venue': "binance", 'account': "subaccount4"},
  {'client': "rundmc", 'venue': "binance", 'account': "subaccount6"},
  {'client': "edison", 'venue': "okx", 'account': "submn1"},
  {'client': "edisonhedge", 'venue': "okx", 'account': "symswap"},
  {'client': "quizzical", 'venue': "binance", 'account': "subls"},
]

mongo_uri = 'mongodb+srv://activedigital:' + secrets['CLOUD_MONGO_PASSWORD'] + '@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)

active_digital = mongo_client['active_digital']['fills']
active_digita = mongo_client['active_digita']['fills']

for account in accounts:
  fills1 = list(
    active_digital.find(
      {'client': account['client'], 'venue': account['venue'], 'account': account['account'], 'fills_value.timestamp': {'$gt': 1713261653423 if account['venue'] != "binance" else "1713261653423", '$lte': 1713348053423 if account['venue'] != "binance" else "1713348053423"}}
    ).sort([("fills_value.timestamp", 1), ("fills_value.id", 1)])
  )
  fills2 = list(
    active_digita.find(
      {'client': account['client'], 'venue': account['venue'], 'account': account['account'], 'fills_value.timestamp': {'$gt': 1713261653423 if account['venue'] != "binance" else "1713261653423", '$lte': 1713348053423 if account['venue'] != "binance" else "1713348053423"}}
    ).sort([("fills_value.timestamp", 1), ("fills_value.id", 1)])
  )

  print(account['client'] + " " + account['venue'] + " " + account['account'])

  print(len(fills1))
  print(len(fills2))

  for i in range(len(fills1)):
    if fills1[i]['fills_value'] != fills2[i]['fills_value']:
      print("Not Same")
      # print(i)
      print(fills1[i]['fills_value'])
      print(fills2[i]['fills_value'])