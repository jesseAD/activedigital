from datetime import datetime, timezone

from src.config import read_config_file

config = read_config_file()


class Runs:
  def __init__(self, db, collection):

    self.runs_db = db[collection]

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
      print(e)

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

      return True

    except Exception as e:
      if logger == None:
        print("Error in inserting a run: " + str(e))
      else:
        logger.error("Error in inserting a run: " + str(e))

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

      return True

    except Exception as e:
      if logger == None:
        print("Error in enclosing a run: " + str(e))
      else:
        logger.error("Error in enclosing a run: " + str(e))

      return False
