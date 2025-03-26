import os
import pymongo
from dotenv import load_dotenv
from datetime import datetime, timezone

# Load environment variables
load_dotenv(override=True)

# Get MongoDB credentials
MONGO_DB_USER = os.getenv('MONGO_DB_USER')
MONGO_DB_PASSWORD = os.getenv('MONGO_DB_PASSWORD')

# Connect to MongoDB
mongo_uri = f"mongodb+srv://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@serverlessinstance0.bszsz.mongodb.net/active_digital?retryWrites=true&w=majority&appName=ServerlessInstance0"
client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
db = client['active_digital']

def print_separator():
    print("-" * 80)

# Check for Mendel balances
print("\nMendel Balances:")
print_separator()
balances = list(db['balances'].find({'client': 'mendel'}).sort('timestamp', -1).limit(5))
if balances:
    for balance in balances:
        print(f"Timestamp: {balance.get('timestamp')}")
        print(f"Account: {balance.get('account')}")
        print(f"Venue: {balance.get('venue')}")
        print(f"Balance values: {balance.get('balance_value')}")
        print_separator()
else:
    print("No balance data found for Mendel account")
    print_separator()

# Check for Mendel positions
print("\nMendel Positions:")
print_separator()
positions = list(db['positions'].find({'client': 'mendel'}).sort('timestamp', -1).limit(5))
if positions:
    for position in positions:
        print(f"Timestamp: {position.get('timestamp')}")
        print(f"Account: {position.get('account')}")
        print(f"Venue: {position.get('venue')}")
        position_values = position.get('position_value', [])
        print(f"Position count: {len(position_values)}")
        
        if position_values:
            print("\nFirst few positions:")
            for i, pos in enumerate(position_values[:3]):
                print(f"Position {i+1}: {pos}")
                if i < len(position_values) - 1 and i < 2:
                    print("---")
        print_separator()
else:
    print("No position data found for Mendel account")
    print_separator()

# Check for recent runs
print("\nMost Recent Runs:")
print_separator()
runs = list(db['runs'].find().sort('timestamp', -1).limit(3))
if runs:
    for run in runs:
        print(f"Run ID: {run.get('_id')}")
        print(f"Start time: {run.get('timestamp')}")
        print(f"End time: {run.get('end_timestamp')}")
        print(f"Status: {'Completed' if run.get('end_timestamp') else 'In Progress'}")
        print_separator()
else:
    print("No run data found")
    print_separator()

# Close the MongoDB connection
client.close()
print("MongoDB connection closed") 