from datetime import datetime, timezone
from pymongo import MongoClient
from log import Log

log = Log()

class DataCollector:
    def __init__(self, mongo_host, mongo_db, mongo_port, client, exchange='', collection='', helper='', apikey='', apisecret='', script=''):
        self.mongo_host = mongo_host
        self.mongo_db = mongo_db
        self.mongo_port = mongo_port
        self.client = client
        self.exchange = exchange
        self.collection = collection
        self.helper = helper
        self.apikey = apikey
        self.apisecret = apisecret
        self.script = script

        mongo_client = MongoClient(self.mongo_host, self.mongo_port)
        self.db = mongo_client[self.mongo_db]

    def get(
            client: str = None, 
            exchange: str = None, 
            collection: str = None, 
            active: bool = None
    ):
        results = []
        pipeline = []

        if client:
            pipeline.append({"$match": {"client": client}})
        if exchange:
            pipeline.append({"$match": {"exchange": exchange}})
        if collection:
            pipeline.append({"$match": {"collection": collection}})
        if active is not None:
            pipeline.append({"$match": {"active": active}})

        try:
            results = DataCollector.db.aggregate(pipeline)
        
        except Exception as e:
            log.error(e)

        return results

    def create():
        data = {
            "exchange": DataCollector.exchange,
            "collection": DataCollector.collection,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }

        try:
            DataCollector.db.insert(data)
            log.debug(f"{DataCollector.collection} created: {data}")
            return True
        except Exception as e:
            log.error(e)
            return False

        return False
    
    def entry(status: bool = True):
        collectors = DataCollector.get(
            client=DataCollector.client,
            exchange=DataCollector.exchange,
            collection=DataCollector.collection,
            active=True)

        for collector in collectors:
            try:
                DataCollector.db.update(
                    {"_id": collector["_id"]},
                    {"entry": status},
                )
            except Exception as e:
                log.error(e)
                return False

        return True
    
    def exit(status: bool = False):
    
        collectors = DataCollector.get(
            client=DataCollector.client,
            exchange=DataCollector.exchange,
            collection=DataCollector.collection,
            active=True)

        for collector in collectors:
            if collector["entry"] is False:
                continue
            try:
                DataCollector.db.update(
                    {"_id": collector["_id"]},
                    {"exit": status},
                )
            except Exception as e:
                log.error(e)
                return False

        return True