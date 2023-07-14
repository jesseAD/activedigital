from datetime import datetime, timezone
from pymongo import MongoClient
from log import Log

log = Log()

class DataCollector:
    def __init__(self, mongo_host, mongo_db, mongo_port, client, exchange='', collection='', account='', helper='', apikey='', apisecret='', script=''):
        self.mongo_host = mongo_host
        self.mongo_db = mongo_db
        self.mongo_port = mongo_port
        self.client = client
        self.exchange = exchange
        self.collection = collection
        self.account = account
        self.helper = helper
        self.apikey = apikey
        self.apisecret = apisecret
        self.script = script

        mongo_client = MongoClient(self.mongo_host, self.mongo_port)
        self.db = mongo_client[self.mongo_db]
