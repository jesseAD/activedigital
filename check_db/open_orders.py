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

active_digital = mongo_client['active_digital']['open_orders']
active_digita = mongo_client['active_digita']['open_orders']

for account in accounts:
  try:
    open_orders1 = list(active_digital.find(
      {'client': account['client'], 'venue': account['venue'], 'account': account['account']}
    ).sort("_id", -1).limit(1))[0]['open_orders_value']
    open_orders2 = list(active_digita.find(
      {'client': account['client'], 'venue': account['venue'], 'account': account['account']}
    ).sort("_id", -1).limit(1))[0]['open_orders_value']
  except:
    continue

  print(account['client'] + " " + account['venue'] + " " + account['account'])

  for i in range(len(open_orders1)):
    if open_orders1[i]['info'] != open_orders2[i]['info']:
      print("Not Same")