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

active_digital = mongo_client['active_digital']['positions']
active_digita = mongo_client['active_digita']['positions']

for account in accounts:
  try:
    position1 = list(active_digital.find(
      {'client': account['client'], 'venue': account['venue'], 'account': account['account']}
    ).sort("_id", -1).limit(1))[0]['position_value']
    position2 = list(active_digita.find(
      {'client': account['client'], 'venue': account['venue'], 'account': account['account']}
    ).sort("_id", -1).limit(1))[0]['position_value']
  except:
    continue

  print(account['client'] + " " + account['venue'] + " " + account['account'])

  for i in range(len(position1)):
    if position1[i]['symbol'] != position2[i]['symbol'] or position1[i]['contracts'] != position2[i]['contracts'] or position1[i]['leverage'] != position2[i]['leverage']:
      print("Not Same")