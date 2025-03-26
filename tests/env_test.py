from dotenv import load_dotenv
load_dotenv()
import os

# Test reading a specific credential
key = "EDISON_DERIBIT_SEGTEST_API_KEY"
value = os.getenv(key)
print(f"{key}: {value}")