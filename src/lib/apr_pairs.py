from datetime import datetime, timezone, timedelta

def get_expiry(timestamp):
  return round(
    int(timestamp - datetime.now(timezone.utc).timestamp() * 1000) / 86400000
  )

def filter_insts(insts={}):
  bases = ["BTC", "ETH"]
  quotes = ["USD", "USDT", "USDC"]

  for _exchange, _inst in insts.items():
    _inst = {item: _inst[item] for item in _inst if _inst[item]['base'] in bases and _inst[item]['quote'] in quotes}

    _inst = {
      item: _inst[item] 
      for item in _inst 
      if (
        _inst[item]['type'] == "future" and 
        _inst[item]['expiry'] >= datetime(datetime.now().year, datetime.now().month, datetime.now().day).timestamp() * 1000) or
        _inst[item]['type'] == "spot" or _inst[item]['type'] == "swap"
    }

    if _exchange == "okx":
      _inst = {item: _inst[item] for item in _inst if _inst[item]['future'] == True or _inst[item]['type'] == "spot" or _inst[item]['type'] == "swap"}

    elif _exchange == "deribit":
      _inst = {item: _inst[item] for item in _inst if _inst[item]['info']['kind'] == "future" or _inst[item]['type'] == "spot" or _inst[item]['type'] == "swap"}

    for item in _inst:
      if _inst[item]['type'] == "spot" or _inst[item]['type'] == "swap":
        _inst[item]['expiry'] = int((datetime.now() + timedelta(hours=1)).timestamp() * 1000)
    
    insts[_exchange] = _inst

  return insts

def make_pairs(insts={}):
  pairs = []

  for _exchange, _inst in insts.items():
    for _x in _inst.keys():
      for _y in _inst.keys():
        if _x != _y and _inst[_x]['base'] == _inst[_y]['base'] and _inst[_x]['expiry'] < _inst[_y]['expiry']:
          pairs.append({
            'leg1': {
              'exchange': _exchange,
              'symbol': _inst[_x]['symbol'],
              'type': "Spot" if _inst[_x]['spot'] else ("Swap" if _inst[_x]['swap'] else ("Linear" if _inst[_x]['linear'] else "Inverse")),
              'expiry': get_expiry(_inst[_x]['expiry'])
            },
            'leg2': {
              'exchange': _exchange,
              'symbol': _inst[_y]['symbol'],
              'type': "Spot" if _inst[_y]['spot'] else ("Swap" if _inst[_y]['swap'] else ("Linear" if _inst[_y]['linear'] else "Inverse")),
              'expiry': get_expiry(_inst[_y]['expiry'])
            }
          })

  for exch1 in insts.keys():
    for exch2 in insts.keys():
      if exch1 != exch2:
        for _x in insts[exch1].keys():
          for _y in insts[exch2].keys():
            if insts[exch1][_x]['base'] == insts[exch2][_y]['base'] and insts[exch1][_x]['expiry'] < insts[exch2][_y]['expiry']:
              pairs.append({
              'leg1': {
                'exchange': exch1,
                'symbol': insts[exch1][_x]['symbol'],
                'type': "Spot" if insts[exch1][_x]['spot'] else ("Swap" if insts[exch1][_x]['swap'] else ("Linear" if insts[exch1][_x]['linear'] else "Inverse")),
                'expiry': get_expiry(insts[exch1][_x]['expiry'])
              },
              'leg2': {
                'exchange': exch2,
                'symbol': insts[exch2][_y]['symbol'],
                'type': "Spot" if insts[exch2][_y]['spot'] else ("Swap" if insts[exch2][_y]['swap'] else ("Linear" if insts[exch2][_y]['linear'] else "Inverse")),
                'expiry': get_expiry(insts[exch2][_y]['expiry'])
              }
            })

  return pairs