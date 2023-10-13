import time
import hmac
import hashlib
from urllib.parse import urlencode

import requests

API_KEY = '6iXhfvj6EIfEBRGZwO2hsn4YJGkLzm2nfmo7lNvoRTDGK9ISZF2pVuz1FRLkWrUm'
SECRET_KEY = 'I41jLtrCeAS6qMouJ7p3C3Ve6iWHEcbGIRrc1M59t7RuFgaTlfK1OZAr35OYPjwV'
BASE_URL = 'https://papi.binance.com'  # Alternatively use https://testnet.binance.vision for sandbox environment

def get_server_time():
    """Get server time"""
    url = "https://api.binance.com/api/v3/time"
    r = requests.get(url)
    if r.status_code == 200:
        return r.json()['serverTime']

def hashing(query_string):
    return hmac.new(SECRET_KEY.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

def get_headers():
    headers = {
        'X-MBX-APIKEY': API_KEY
    }
    return headers

def get_balance():
    timestamp = get_server_time()
    query_string = 'timestamp=' + str(timestamp)
    url = BASE_URL + '/papi/v1/cm/income?' + query_string + '&signature=' + hashing(query_string)
    print(url)

    r = requests.get(url, headers=get_headers())
    return r.json()

# print(get_balance())
get_balance()