import os
import pymongo
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

# Load environment variables
load_dotenv(override=True)

# Get MongoDB credentials
MONGO_DB_USER = os.getenv('MONGO_DB_USER')
MONGO_DB_PASSWORD = os.getenv('MONGO_DB_PASSWORD')

# Connect to MongoDB
mongo_uri = f"mongodb+srv://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@serverlessinstance0.bszsz.mongodb.net/active_digital?retryWrites=true&w=majority&appName=ServerlessInstance0"
client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
db = client['active_digital']

# Get time 24 hours ago
last_24h = datetime.now(timezone.utc) - timedelta(hours=24)

# Check for recent runs to see if the Docker container made any changes
print("\nRuns in the last 24 hours:")
print("-" * 80)
runs = list(db['runs'].find({"timestamp": {"$gte": last_24h}}).sort('timestamp', -1))
if runs:
    for run in runs:
        print(f"Run ID: {run.get('_id')}")
        print(f"Start time: {run.get('timestamp')}")
        print(f"End time: {run.get('end_timestamp')}")
        print(f"Status: {'Completed' if run.get('end_timestamp') else 'In Progress'}")
        print("-" * 80)
else:
    print("No runs found in the last 24 hours")
    print("-" * 80)

# Check for recent Mendel balances
print("\nMendel balances in the last 24 hours:")
print("-" * 80)
balances = list(db['balances'].find({"client": "mendel", "timestamp": {"$gte": last_24h}}).sort('timestamp', -1))
if balances:
    for balance in balances:
        print(f"Timestamp: {balance.get('timestamp')}")
        print(f"Account: {balance.get('account')}")
        print(f"Venue: {balance.get('venue')}")
        print(f"Balance values: {balance.get('balance_value')}")
        print("-" * 80)
else:
    print("No Mendel balance updates in the last 24 hours")
    print("-" * 80)

# Check for recent Mendel positions
print("\nMendel positions in the last 24 hours:")
print("-" * 80)
positions = list(db['positions'].find({"client": "mendel", "timestamp": {"$gte": last_24h}}).sort('timestamp', -1))
if positions:
    for position in positions:
        print(f"Timestamp: {position.get('timestamp')}")
        print(f"Account: {position.get('account')}")
        print(f"Venue: {position.get('venue')}")
        position_values = position.get('position_value', [])
        print(f"Position count: {len(position_values)}")
        
        if position_values:
            print("\nFirst position:")
            print(position_values[0])
        print("-" * 80)
else:
    print("No Mendel position updates in the last 24 hours")
    print("-" * 80)

# Close the MongoDB connection
client.close()
print("MongoDB connection closed") 