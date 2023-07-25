import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

def database_connector(collection):        
    mongo_password = os.getenv("CLOUD_MONGO_PASSWORD")
    connection_uri = 'mongodb+srv://activedigital:'+mongo_password+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
    cloud_mongo = MongoClient(connection_uri)
    leverages_cloud = cloud_mongo['active_digital'][collection]

    return leverages_cloud