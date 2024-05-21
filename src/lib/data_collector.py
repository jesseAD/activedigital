class DataCollector:
  def __init__(self, client, exch=None, exchange='', account=''):
    self.client = client
    self.exchange = exchange
    self.account = account
    self.exch = exch