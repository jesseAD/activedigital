import json
import math
from collections import defaultdict

def get_decimal_places(number):
  str_num = str(number)
  if '.' in str_num: # check if it is a decimal
    return len(str_num.split('.')[1]) # return number of digits after .
  else:
    return 0 # if it's not a decimal, return 0
  
def get_unhedged(perp=[], spot=[]):
  new_positions = []
  for position in spot:
    new_position = {}
    new_position['notional'] = abs(float(position['notional']))
    new_position['position_decimal'] = get_decimal_places(position['contracts'])
    new_position['position'] = float(position['contracts'])
    new_position['markPrice'] = float(position['markPrice'])

    if 'avgPrice' in position:
      if position['avgPrice'] != None:
        new_position['avgPrice'] = float(position['avgPrice'])
      else:
        new_position['avgPrice'] = 0
    else:
      new_position['avgPrice'] = float(position['info']['avgPrice'])

    try:
      new_position['leverage'] = float(position['leverage'])
    except:
      new_position['leverage'] = 0

    try:
      new_position['unrealizedPnl'] = float(position['unrealizedPnl'])
    except:
      new_position['unrealizedPnl'] = 0  

    try:
      new_position['liquidationPriceChange'] = float(position['liquidationPriceChange'])
    except:
      new_position['liquidationPriceChange'] = 0

    new_position['side'] = position['side'] 
    new_position['base'] = position['base']
    new_position['symbol'] = position['symbol']
    new_position['timestamp'] = position['timestamp']
    new_position['marginMode'] = position['marginMode']

    if 'lifetime_funding_rates' in position:
      new_position['lifetime_funding_rates'] = position['lifetime_funding_rates']
    else:
      new_position['lifetime_funding_rates'] = 0

    new_positions.append(new_position)

  spot = new_positions

  new_positions = []
  for position in perp:
    new_position = {}
    new_position['notional'] = abs(float(position['notional']))
    new_position['position_decimal'] = get_decimal_places(position['contracts'])
    new_position['position'] = float(position['contracts'])
    new_position['markPrice'] = float(position['markPrice'])
    if 'lifetime_funding_rates' in position:
      new_position['lifetime_funding_rates'] = position['lifetime_funding_rates']
    else:
      new_position['lifetime_funding_rates'] = 0

    if 'avgPrice' in position:
      if position['avgPrice'] != None:
        new_position['avgPrice'] = float(position['avgPrice'])
      else:
        new_position['avgPrice'] = 0
    else:
      new_position['avgPrice'] = 0

    try:
      new_position['leverage'] = float(position['leverage'])
    except:
      new_position['leverage'] = 0

    try:
      new_position['unrealizedPnl'] = float(position['unrealizedPnl'])
    except:
      new_position['unrealizedPnl'] = 0  

    try:
      new_position['liquidationPriceChange'] = float(position['liquidationPriceChange'])
    except:
      new_position['liquidationPriceChange'] = 0

    new_position['side'] = position['side'] 
    new_position['base'] = position['base']
    new_position['symbol'] = position['info']['symbol']
    new_position['timestamp'] = position['timestamp']
    new_position['marginMode'] = position['marginMode']

    if 'expiryDatetime' in position:
      new_position['expiryDatetime'] = position['expiryDatetime']

    new_positions.append(new_position)

  perp = new_positions

  perp.sort(key = lambda x: x['base'])

  groups = defaultdict(list)
  for position in perp:
    groups[position['base']].append(position)

  groups1 = defaultdict(list)
  for position in spot:
    groups1[position['base']].append(position)

  id = 1
  pairs = []
  for _key, _group in groups.items():
    long_count = len([item['side'] for item in _group if item['side'] == "long"])
    short_count = len([item['side'] for item in _group if item['side'] == "short"])

    if _key in groups1:
      for _x in groups1[_key]:
        if _x['side'] == "long":
          long_count += 1
        else:
          short_count += 1

    least = "long" if long_count <= short_count else "short"
    most = "long" if least == "short" else "short"    
    
    leasts = [item for item in _group if item['side'] == least]
    mosts = [item for item in _group if item['side'] == most]    

    mosts.sort(key = lambda x: x['notional'], reverse=True)
    
    if long_count == 0 or short_count == 0:
      sign = 1 if least == "long" else -1
      pairs.append([{
        **mosts[0],
        'id': id,
        'position': sum(item['position'] for item in mosts),
        'notional': sum(item['notional'] for item in mosts),
        'unrealizedPnl': sum(item['unrealizedPnl'] for item in mosts),
        'lifetime_funding_rates': sum(item['lifetime_funding_rates'] for item in mosts),
        'unhedgedAmount': sign * sum(item['notional'] for item in mosts),
      }])
      id += 1

      if _key in groups1:
        pairs.append([{
          **groups1[_key][0],
          'id': id,
          'unhedgedAmount': sign * groups1[_key][0]['notional']
        }])
        id += 1
      # for item in mosts:
      #   pairs.append([{
      #     **item,
      #     'id': id,
      #     'unhedgedAmount': item['notional'] if item['side'] == "long" else -item['notional']
      #   }])

      # print("-------" + __key)
      continue

    if _key in groups1:
      for _x in groups1[_key]:
        if _x['side'] == most:
          mosts.append(_x)
        else:
          cnt = len(mosts)
          for i in range(cnt-1, -1, -1):
            sign = 1 if least == "long" else -1
            if _x['notional'] >= mosts[i]['notional']:
              if i == 0:
                pairs.append([
                  {
                    **_x,
                    'id': id,
                    'unhedgedAmount': round((_x['notional'] - mosts[i]['notional']) * sign, 5)
                  },
                  {
                    **mosts[i],
                    'id': id,
                    'unhedgedAmount': round((_x['notional'] - mosts[i]['notional']) * sign, 5)
                  }
                ])
              else:
                ratio = round(_x['position'] * mosts[i]['notional'] / _x['notional'], _x['position_decimal']) / _x['position']
                pairs.append([
                  {
                    **_x,
                    'notional': round(_x['notional'] * ratio, 5),
                    'position': round(_x['position'] * ratio, 5),
                    'unrealizedPnl': round(_x['unrealizedPnl'] * ratio, 5),
                    'id': id,
                    'unhedgedAmount': round(sign * (_x['notional'] * ratio - mosts[i]['notional']), 5)
                  },
                  {
                    **mosts[i],
                    'id': id,
                    'unhedgedAmount': round(sign * (_x['notional'] * ratio - mosts[i]['notional']), 5)
                  }
                ])
                _x['notional'] = round((1 - ratio) * _x['notional'], 5)
                _x['position'] = round((1 - ratio) * _x['position'], 5)
                _x['unrealizedPnl'] = round((1 - ratio) * _x['unrealizedPnl'], 5)
              mosts = mosts[:-1]
              id += 1
            elif _x['notional'] > 0.0:
              pairs.append([
                {
                  **_x,
                  'id': id,
                  'unhedgedAmount': sign * (_x['notional'] - mosts[i]['notional'])
                },
                {
                  **mosts[i],
                  'id': id,
                  'unhedgedAmount': sign * (_x['notional'] - mosts[i]['notional'])
                }
              ])
              _x['notional'] = 0.0
              mosts = mosts[:-1]
              id += 1
            else:
              break

    if len(leasts) > 0:
      merged = {}
      precise = max(item['position_decimal'] for item in leasts)
      
      merged = leasts[0]
      merged['timestamp'] = max([item['timestamp'] for item in leasts])
      merged['avgPrice'] = round(sum([(item['avgPrice'] * item['notional']) for item in leasts]) / sum([item['notional'] for item in leasts]), precise)
      merged['leverage'] = round(sum([(item['leverage'] * item['notional']) for item in leasts]) / sum([item['notional'] for item in leasts]), precise)
      merged['marginMode'] = "mixed" if any([item['marginMode'] != "cross" for item in leasts]) else "cross"
      merged['markPrice'] = leasts[0]['markPrice']
      merged['notional'] = sum([item['notional'] for item in leasts])
      merged['position'] = sum([item['position'] for item in leasts])
      merged['side'] = least
      merged['symbol'] = leasts[0]['symbol']
      merged['unrealizedPnl'] = sum([item['unrealizedPnl'] for item in leasts])
      merged['lifetime_funding_rates'] = sum([item['lifetime_funding_rates'] for item in leasts])

      for item in mosts:
        sign = 1 if least == "long" else -1
        unhedged = 0
        notional = 0
        unrealized = 0
        position = 0
        lifetime_funding_rates = 0
        if merged['notional'] == 0.0:
          if item == mosts[-1]:
            pairs.append([{
              **item,
              'id': id,
              'notional': notional + item['notional'],
              'position': position + item['position'],
              'unrealizedPnl': unrealized + item['unrealizedPnl'],
              'lifetime_funding_rates': lifetime_funding_rates + item['lifetime_funding_rates'],
              'unhedgedAmount': -item['notional'] * sign + unhedged
            }])
          else:
            notional += item['notional']
            unrealized += item['unrealizedPnl']
            lifetime_funding_rates += item['lifetime_funding_rates']
            position += item['position']
            unhedged -= (item['notional'] * sign)
            
        elif merged['notional'] >= item['notional']:
          if item == mosts[-1]:
            pairs.append([
              {
                **merged,
                'id': id,
                'unhedgedAmount': round((merged['notional'] - item['notional']) * sign, 5)
              },
              {
                **item,
                'id': id,
                'unhedgedAmount': round((merged['notional'] - item['notional']) * sign, 5)
              }
            ])
          else:
            ratio = round(merged['position'] * item['notional'] / merged['notional'], precise) / merged['position']
            pairs.append([
              {
                **merged,
                'notional': round(merged['notional'] * ratio, 5),
                'position': round(merged['position'] * ratio, 5),
                'unrealizedPnl': round(merged['unrealizedPnl'] * ratio, 5),
                'lifetime_funding_rates': round(merged['lifetime_funding_rates'] * ratio, 5),
                'id': id,
                'unhedgedAmount': round(sign * (merged['notional'] * ratio - item['notional']), 5)
              },
              {
                **item,
                'id': id,
                'unhedgedAmount': round(sign * (merged['notional'] * ratio - item['notional']), 5)
              }
            ])
            merged['notional'] = round((1 - ratio) * merged['notional'], 5)
            merged['position'] = round((1 - ratio) * merged['position'], 5)
            merged['unrealizedPnl'] = round((1 - ratio) * merged['unrealizedPnl'], 5)
            merged['lifetime_funding_rates'] = round((1 - ratio) * merged['lifetime_funding_rates'], 5)
        else:
          pairs.append([
            {
            **merged,
            'id': id,
            'unhedgedAmount': round((merged['notional'] - item['notional']) * sign, 5)
            },
            {
            **item,
            'id': id,
            'unhedgedAmount': round((merged['notional'] - item['notional']) * sign, 5)
            }
          ])
          merged['notional'] = 0.0
        
        id += 1

  for _key, _group in groups1.items():
    if _key not in groups:
      sign = 1 if _group[0]['side'] == "long" else -1
      pairs.append([{
        **_group[0],
        'id': id,
        'position': sum([item['position'] for item in _group]),
        'unrealizedPnl': sum([item['unrealizedPnl'] for item in _group]),
        'notional': sum([item['notional'] for item in _group]),
        'unhedgedAmount': sum([item['notional'] for item in _group]) * sign
      }])

      id += 1

  for pair in pairs:
    min_notional = min([item['notional'] for item in pair])
    for position in pair:
      position['unhedgedAmount'] = abs(position['unhedgedAmount']) / min_notional

  return pairs
