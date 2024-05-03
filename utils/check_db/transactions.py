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

ids = {
  'binance': "transaction_value.tranId",
  'okx': "transaction_value.billId",
  'bybit': ""
}

mongo_uri = 'mongodb+srv://activedigital:' + secrets['CLOUD_MONGO_PASSWORD'] + '@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'

mongo_client = pymongo.MongoClient(mongo_uri)

active_digital = mongo_client['active_digital']['transactions']
active_digita = mongo_client['active_digita']['transactions']

for account in accounts:
  transactions1 = list(
    active_digital.find(
      {'client': account['client'], 'venue': account['venue'], 'account': account['account'], 'transaction_value.timestamp': {'$gt': 1713261653423, '$lte': 1713348053423}}
    ).sort([("transaction_value.timestamp", 1), (ids[account['venue']], 1)])
  )
  transactions2 = list(
    active_digita.find(
      {'client': account['client'], 'venue': account['venue'], 'account': account['account'], 'transaction_value.timestamp': {'$gt': 1713261653423, '$lte': 1713348053423}}
    ).sort([("transaction_value.timestamp", 1), (ids[account['venue']], 1)])
  )

  print(account['client'] + " " + account['venue'] + " " + account['account'])

  print(len(transactions1))
  print(len(transactions2))

  for i in range(len(transactions1)):
    if transactions1[i]['transaction_value']['info'] != transactions2[i]['transaction_value']['info']:
      print("Not Same")
      # print(i)
      print(transactions1[i]['transaction_value'])
      print(transactions2[i]['transaction_value'])