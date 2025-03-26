#!/bin/bash
# Script to test Docker setup with a minimal script

# Go to the project root directory
cd "$(dirname "$0")/.." || exit 1

# Create a simple test script
cat > test_setup.py << 'EOF'
import os
import sys
import pymongo
from dotenv import load_dotenv
from datetime import datetime, timezone

# Load environment variables
print("Loading environment variables...")
load_dotenv(override=True)

# Get MongoDB credentials
print("Checking MongoDB credentials...")
mongo_user = os.getenv("MONGO_DB_USER")
mongo_password = os.getenv("MONGO_DB_PASSWORD")

if not mongo_user or not mongo_password:
    print("ERROR: MongoDB credentials not found in environment variables")
    sys.exit(1)

print(f"MongoDB User: {mongo_user}")
print(f"MongoDB Password: {'*' * len(mongo_password)}")

# Check for Mendel account credentials
print("\nChecking Mendel account credentials...")
mendel_key = os.getenv("MENDEL_DERIBIT_VOLARB_API_KEY")
mendel_secret = os.getenv("MENDEL_DERIBIT_VOLARB_API_SECRET")

if not mendel_key or not mendel_secret:
    print("ERROR: Mendel account credentials not found in environment variables")
    sys.exit(1)

print(f"Mendel API Key: {mendel_key}")
print(f"Mendel API Secret: {'*' * len(mendel_secret)}")

# Try to connect to MongoDB
print("\nAttempting to connect to MongoDB...")
try:
    mongo_uri = f"mongodb+srv://{mongo_user}:{mongo_password}@serverlessinstance0.bszsz.mongodb.net/active_digital?retryWrites=true&w=majority&appName=ServerlessInstance0"
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    # The ismaster command is cheap and does not require auth
    client.admin.command('ismaster')
    print("MongoDB connection successful!")
    
    # Check available databases
    dbs = client.list_database_names()
    print(f"Available databases: {', '.join(dbs)}")
    
    # Close the connection
    client.close()
except Exception as e:
    print(f"ERROR: Failed to connect to MongoDB: {str(e)}")
    sys.exit(1)

# Check if log directory exists
print("\nChecking log directory...")
if os.path.exists("/data/log"):
    print("Log directory exists")
else:
    print("ERROR: Log directory not found")
    sys.exit(1)

# Test successful
print("\nAll tests passed! Docker setup is working correctly.")
print(f"Test completed at {datetime.now(timezone.utc)}")
EOF

# Update docker-compose.yml to use the test script
cat > docker-compose.test.yml << EOF
version: '3'

services:
  data_collector_test:
    build: 
      context: .
      dockerfile: Dockerfile
    container_name: active_digital_test
    volumes:
      - .:/app
      - ./.env:/.env
      - ./data:/data
    command: python /app/test_setup.py
EOF

# Run the test container
echo "Building and running test container..."
docker-compose -f docker-compose.test.yml up --build

# Cleanup
echo "Cleaning up..."
docker-compose -f docker-compose.test.yml down
rm -f test_setup.py
rm -f docker-compose.test.yml

echo "Test completed." 