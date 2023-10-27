import json
import math
from collections import defaultdict

def get_decimal_places(number):
    str_num = str(number)
    if '.' in str_num: # check if it is a decimal
        return len(str_num.split('.')[1]) # return number of digits after .
    else:
        return 0 # if it's not a decimal, return 0
  
def get_unhedged(positions):
    new_positions = []
    for position in positions:
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

        if position['leverage'] != None:
            new_position['leverage'] = float(position['leverage'])
        else:
            new_position['leverage'] = 0

        if position['unrealizedPnl'] != None:
            new_position['unrealizedPnl'] = float(position['unrealizedPnl'])
        else:
            new_position['unrealizedPnl'] = 0  

        new_position['side'] = position['side'] 
        new_position['base'] = position['base']
        new_position['symbol'] = position['symbol']
        new_position['timestamp'] = position['timestamp']
        new_position['marginMode'] = position['marginMode']
        new_position['markPrice'] = position['markPrice']

        new_positions.append(new_position)

    positions = new_positions

    positions.sort(key = lambda x: x['base'])

    groups = defaultdict(list)
    for position in positions:
        groups[position['base']].append(position)

    id = 1
    pairs = []
    for _key, _group in groups.items():
        long_count = len([item['side'] for item in _group if item['side'] == "long"])
        short_count = len([item['side'] for item in _group if item['side'] == "short"])

        least = "long" if long_count <= short_count else "short"
        most = "long" if least == "short" else "short"        
        
        leasts = [item for item in _group if item['side'] == least]
        mosts = [item for item in _group if item['side'] == most]
        mosts.sort(key = lambda x: x['notional'], reverse=True)
        
        if long_count == 0 or short_count == 0:
            for item in mosts:
                pairs.append([{
                    **item,
                    'id': id,
                    'unhedgedAmount': item['notional'] if item['side'] == "long" else -item['notional']
                }])
                id += 1

            continue

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

        for item in mosts:
            sign = 1 if least == "long" else -1
            if merged['notional'] == 0.0:
                pairs.append([{
                    **item,
                    'id': id,
                    'unhedgedAmount': -item['notional'] * sign
                }])
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

    return pairs
