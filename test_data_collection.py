import os
import sys
import pymongo
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# Get MongoDB credentials from environment
MONGO_DB_USER = os.getenv('MONGO_DB_USER')
MONGO_DB_PASSWORD = os.getenv('MONGO_DB_PASSWORD')

# Construct MongoDB URI
MONGO_URI = f"mongodb+srv://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@serverlessinstance0.bszsz.mongodb.net/active_digital?retryWrites=true&w=majority&appName=ServerlessInstance0"

def test_mongodb_connection():
    """Test MongoDB connection and basic operations"""
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(MONGO_URI)
        db = client['active_digital']
        
        # Test listing collections
        collections = db.list_collection_names()
        print("Available collections:", collections)
        
        # Test reading from a few collections
        for collection_name in ['instruments', 'positions', 'balances']:
            if collection_name in collections:
                count = db[collection_name].count_documents({})
                print(f"{collection_name} collection has {count} documents")
        
        return True
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return False

if __name__ == "__main__":
    print("Testing MongoDB connection...")
    if test_mongodb_connection():
        print("MongoDB connection test successful!")
    else:
        print("MongoDB connection test failed!") 