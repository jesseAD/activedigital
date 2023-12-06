import os
from dotenv import load_dotenv
from datetime import datetime, timezone

# from src.lib.db import MongoDB
from src.lib.log import Log
# from src.lib.exchange import Exchange
# from src.lib.mapping import Mapping
from src.config import read_config_file
# from src.handlers.helpers import Helper
# from src.handlers.helpers import OKXHelper
# from src.handlers.database_connector import database_connector

load_dotenv()
log = Log()
config = read_config_file()


class Runs:
    def __init__(self, db, collection):
        # if os.getenv("mode") == "testing":
        #     self.runs_db = MongoDB(config["mongo_db"], db)
        # else:
        #     self.runs_db = database_connector(db)

        self.runs_db = db['runs']

    def close_db(self):
        if os.getenv("mode") == "testing":
            self.runs_db.close()
        else:
            self.runs_db.database.client.close()

    def get(self):
        try:
            run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
            latest_run_id = 0
            for item in run_ids:
                try:
                    latest_run_id = item["runid"]
                except:
                    pass

            return latest_run_id

        except Exception as e:
            log.error(e)

    def start(self, logger=None):
        current_time = datetime.now(timezone.utc)

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"] + 1
            except:
                pass

        try:
            self.runs_db.insert_one(
                {"start_time": current_time, "runid": latest_run_id}
            )

            # log.debug(f"Position created: {position}")
            return latest_run_id

        except Exception as e:
            logger.error(e)
            return False

    def end(self, logger=None):
        current_time = datetime.now(timezone.utc)

        run_ids = self.runs_db.find({}).sort("_id", -1).limit(1)
        latest_run_id = 0
        for item in run_ids:
            try:
                latest_run_id = item["runid"]
            except:
                pass

        try:
            self.runs_db.update_one(
                {"runid": latest_run_id},
                {"$set": {"end_time": current_time}},
            )

            # log.debug(f"Position created: {position}")
            return latest_run_id

        except Exception as e:
            logger.error(e)
            return False
