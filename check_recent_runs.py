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

print("\nMost recent runs:")
print("-" * 80)

runs = list(db['runs'].find().sort('timestamp', -1).limit(5))
if runs:
    for run in runs:
        print(f"Run ID: {run.get('_id')}")
        print(f"Start time: {run.get('timestamp')}")
        print(f"End time: {run.get('end_timestamp')}")
        status = "Completed" if run.get('end_timestamp') else "In Progress"
        print(f"Status: {status}")
        print("-" * 80)
else:
    print("No run records found")
    print("-" * 80)

# Close the MongoDB connection
client.close()
print("MongoDB connection closed") 