import os
import sys
import json
import time
from datetime import datetime, timezone
import pymongo
from dotenv import load_dotenv

# Add the project root to the Python path
current_file = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file)
target_dir = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(target_dir)

from src.lib.exchange import Exchange
from src.handlers.instantiator import instantiate, get_data_collectors
from src.handlers.balances import Balances
from src.config import read_config_file

# Load environment variables with override to ensure values are current
load_dotenv(override=True)

# Get MongoDB credentials from environment
MONGO_DB_USER = os.getenv('MONGO_DB_USER')
MONGO_DB_PASSWORD = os.getenv('MONGO_DB_PASSWORD')
print(f"MongoDB User: {MONGO_DB_USER}")

# Construct MongoDB URI using environment variables
mongo_uri = f"mongodb+srv://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@serverlessinstance0.bszsz.mongodb.net/active_digital?retryWrites=true&w=majority&appName=ServerlessInstance0"

# Connect to MongoDB
print("Connecting to MongoDB...")
db_client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
db = db_client['active_digital']
print("MongoDB connected!")

# Get secrets from .env
secrets = {}
with open('.env', 'r') as f:
    for line in f:
        if '=' in line and not line.startswith('#'):
            key, value = line.strip().split('=', 1)
            secrets[key] = value

def test_mendel_exchange_connection():
    """Test connecting to the Mendel account on Deribit"""
    print("\n=== Testing Mendel Exchange Connection ===")
    
    client = "mendel"
    exchange = "deribit"
    account = "volarb"
    
    print(f"Instantiating data collector for {client} {exchange} {account}...")
    data_collector = instantiate(secrets, client, 'subaccounts', exchange, account)
    
    if data_collector and data_collector.exch:
        print(f"Successfully instantiated data collector")
        
        try:
            # Test basic operations
            print(f"Fetching markets...")
            markets = data_collector.exch.fetch_markets()
            print(f"Successfully fetched {len(markets)} markets")
            
            print(f"Fetching balance...")
            balance = data_collector.exch.fetch_balance()
            
            if 'total' in balance:
                print("\nBalance info:")
                for currency, amount in balance['total'].items():
                    if amount > 0:
                        print(f"  {currency}: {amount}")
            
            print(f"Fetching open positions...")
            positions = data_collector.exch.fetch_positions()
            
            if positions:
                print(f"\nFound {len(positions)} open positions")
                for i, pos in enumerate(positions[:2]):
                    print(f"Position {i+1}: {pos['symbol']} - Size: {pos['notional']} - Side: {pos['side']}")
            else:
                print("No open positions found")
                
            return True
        except Exception as e:
            print(f"Error executing exchange operations: {str(e)}")
    else:
        print(f"Failed to instantiate data collector")
    
    return False

def test_mendel_data_collection():
    """Test collecting and storing data for the Mendel account"""
    print("\n=== Testing Mendel Data Collection ===")
    
    client = "mendel"
    exchange = "deribit"
    account = "volarb"
    
    print(f"Instantiating data collector for {client} {exchange} {account}...")
    data_collector = instantiate(secrets, client, 'subaccounts', exchange, account)
    
    if data_collector and data_collector.exch:
        print(f"Successfully instantiated data collector")
        
        try:
            # Test balances collection
            print("Collecting balances...")
            balances = Balances(db, 'balances')
            
            result = balances.create(
                client=client,
                exch=data_collector.exch,
                exchange=exchange,
                sub_account=account
            )
            print(f"Balances collection result: {result}")
            
            # Retrieve and display the collected balances
            balance_data = list(db['balances'].find(
                {"client": client, "venue": exchange, "account": account}
            ).sort("timestamp", -1).limit(1))
            
            if balance_data:
                print("\nStored balance data:")
                print(f"Timestamp: {balance_data[0]['timestamp']}")
                print(f"Currencies: {balance_data[0]['balance_value']}")
                return True
            else:
                print("No balance data found in database")
                
        except Exception as e:
            print(f"Error collecting data: {str(e)}")
    else:
        print(f"Failed to instantiate data collector")
    
    return False

if __name__ == "__main__":
    print(f"Testing Mendel data collection at {datetime.now(timezone.utc)}")
    
    try:
        # First test the exchange connection
        connection_success = test_mendel_exchange_connection()
        
        if connection_success:
            print("\nExchange connection successful!")
            
            # Then test data collection
            collection_success = test_mendel_data_collection()
            
            if collection_success:
                print("\nSUCCESS! Data collection for Mendel account is working properly.")
                print("The data_getter.py script should now be able to work with the Mendel account.")
            else:
                print("\nWarning: Exchange connection works, but data collection failed.")
        else:
            print("\nFailed to connect to the exchange. Please check your credentials.")
            
    except Exception as e:
        print(f"Error during testing: {str(e)}")
    finally:
        # Close the MongoDB connection
        db_client.close()
        print("\nTest completed.") 