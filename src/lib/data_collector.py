class DataCollector:
    def __init__(self, mongo_host, mongo_port, client, exchange='', collection='', account='', helper='', apikey='', apisecret='', script=''):
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.client = client
        self.exchange = exchange
        self.collection = collection
        self.account = account
        self.helper = helper
        self.apikey = apikey
        self.apisecret = apisecret
        self.script = script