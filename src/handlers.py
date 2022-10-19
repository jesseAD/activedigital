from xmlrpc.client import boolean
import os
import warnings
from datetime import datetime, timezone
from dotenv import load_dotenv
from src.lib.db import MongoDB
from src.lib.exchange import Exchange
from src.lib.log import Log


warnings.filterwarnings("ignore")

load_dotenv()


# db setup
MONGO_URI = os.getenv("MONGO_URI")
FTX_API_KEY = os.getenv("FTX_API_KEY")
FTX_API_SECRET = os.getenv("FTX_API_SECRET")

log = Log()
positions_db = MongoDB("hg", "positions", MONGO_URI)


class Positions:
    def get(
        active: boolean = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
        position_type: str = None,
        account: str = None,
    ):
        results = []

        pipeline = [
            {"$sort": {"_id": -1}},
        ]

        if active is not None:
            pipeline.append({"$match": {"active": active}})
        if spot:
            pipeline.append({"$match": {"spotMarket": spot}})
        if future:
            pipeline.append({"$match": {"futureMarket": future}})
        if perp:
            pipeline.append({"$match": {"perpMarket": perp}})
        if position_type:
            pipeline.append({"$match": {"positionType": position_type}})
        if account:
            pipeline.append({"$match": {"account": account}})

        try:
            results = positions_db.aggregate(pipeline)

        except Exception as e:
            log.error(e)

        return results

    def create(
        positionType: str = "basis",
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
    ):

        accountValue = Accounts.get(account=sub_account, value=True)

        position = {
            "positionType": positionType,
            "account": "Main Account",
            "accountValue": accountValue,
            "active": True,
            "entry": False,
            "exit": False,
            "timestamp": datetime.now(timezone.utc),
        }

        if sub_account:
            position["account"] = sub_account
        if spot:
            position["spotMarket"] = spot
        if future:
            position["futureMarket"] = future
        if perp:
            position["perpMarket"] = perp

        try:
            positions_db.insert(position)
            log.debug(f"Position created: {position}")
        except Exception as e:
            log.error(e)

        return

    def update():
        # two main activities
        ## 1. check if entered is True, is so get the entry spread information and update the position object with the right information
        ## 2. check if exit is True, is so get the exit spread information and update the position object with the right information
        log.debug("Updating positions")
        pass

    def entry(account: str = None, status: boolean = True):
        # get all positions with account
        positions = Positions.get(active=True, account=account)

        for position in positions:
            positions_db.update(
                {"_id": position["_id"]},
                {"entry": status},
            )
            log.debug(f"position in account entry {account} has been set to {status}")

    def exit(account: str = None, status: boolean = False):
        # get all positions with account
        positions = Positions.get(active=True, account=account)

        for position in positions:
            positions_db.update(
                {"_id": position["_id"]},
                {"exit": status},
            )
            log.debug(f"Position in account exit {account} has been set to {status}")


class Accounts:
    def get(account: str = None, value: boolean = None):
        data = {}

        ftx = Exchange(account, FTX_API_KEY, FTX_API_SECRET).ftx()

        try:
            account = ftx.privateGetAccount()

            data = account["result"]

            if value:
                data = float(account["result"]["totalAccountValue"])

        except Exception as e:
            log.error(e)

        return data


if __name__ == "__main__":
    log.info("started")

    # Step 1: create position via some web ui which will fire off this function
    # Positions.create(
    #     positionType="basis",
    #     sub_account="FT4",
    #     spot="ETHW",
    #     perp="ETHW-PERP",
    # )

    # Step 2: manually enter the position

    # Step 3: set entry status because we finished entering via the web ui whcih will fire off this function
    Positions.entry(account="FT4", status=True)

    # Loop: always check to update positions with spread, pnls ect
    Positions.update()

    print("done")
