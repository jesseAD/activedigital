#!/usr/bin/env python3
import pymongo

# MongoDB connection string from the backup script
uri = "mongodb+srv://mathele:JtSWwQlBrmwvl6TO@mongodbcluster.nzphth1.mongodb.net/active_digital?retryWrites=true&w=majority"

# Connect to MongoDB
print("Connecting to old MongoDB database...")
client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
db = client['active_digital']

try:
    # List all collections
    collections = db.list_collection_names()
    print("\nCollections in the old database:")
    for i, collection in enumerate(sorted(collections), 1):
        print(f"{i}. {collection}")
    
    # Compare with backup list
    backup_collections = [
        "positions", "balances", "transactions", "transactions_union", 
        "fills", "leverages", "split_positions", "daily_returns", 
        "instruments", "tickers", "funding_contributions", 
        "open_positions_price_change", "borrow_rates", "open_orders", 
        "mark_prices", "index_prices", "bid_asks", "runs"
    ]
    
    print("\nChecking which collections are not in the backup list:")
    missing_from_backup = set(collections) - set(backup_collections)
    for i, collection in enumerate(sorted(missing_from_backup), 1):
        print(f"{i}. {collection}")
    
    # Check collection counts
    print("\nDocument counts for each collection:")
    for collection in sorted(collections):
        count = db[collection].count_documents({})
        print(f"{collection}: {count} documents")
        
except Exception as e:
    print(f"Error: {str(e)}")
finally:
    client.close()
    print("\nConnection closed.") 