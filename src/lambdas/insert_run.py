import json, os
import pymongo
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

def insert_run(event, context):
    secret_value = boto3.session.Session().client(service_name='secretsmanager', region_name = "eu-central-1").get_secret_value(SecretId="activedigital_secrets")
    secret_value_json = json.loads(secret_value['SecretString'])
    
    mongo_uri = 'mongodb+srv://activedigital:'+secret_value_json['CLOUD_MONGO_PASSWORD']+'@mongodbcluster.nzphth1.mongodb.net/?retryWrites=true&w=majority'
    db = pymongo.MongoClient(mongo_uri, maxPoolsize=1)['active_digita']
    
    runs_db = db['runs']
    
    current_time = datetime.now(timezone.utc)

    run_ids = runs_db.find({}).sort("_id", -1).limit(1)
    latest_run_id = 0
    for item in run_ids:
        try:
            latest_run_id = item["runid"] + 1
        except:
            pass
        
    runs_db.insert_one(
        {"start_time": current_time, "runid": latest_run_id}
    )
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
