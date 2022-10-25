import os
from xmlrpc.client import boolean
from dotenv import load_dotenv
from datetime import datetime, timezone
from src.lib.db import MongoDB
from src.lib.log import Log
from src.handlers.accounts import Accounts

load_dotenv()
log = Log()
MONGO_URI = os.getenv("MONGO_URI")
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
        positionType: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
    ):

        accountValue = Accounts.get(account=sub_account, value=True)

        position = {
            "positionType": positionType,
            "account": "Main Account",
            "initialAccountValue": accountValue,
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
            return True
        except Exception as e:
            log.error(e)
            return False 

        return False

    def entry(account: str = None, status: boolean = True):
        # get all positions with account
        positions = Positions.get(active=True, account=account)

        for position in positions:
            try:
                positions_db.update(
                    {"_id": position["_id"]},
                    {"entry": status},
                )
                log.debug(
                    f"position in account entry {account} has been set to {status}"
                )
            except Exception as e:
                log.error(e)
                return False

        return True

    def exit(account: str = None, status: boolean = False):
        # get all positions with account
        positions = Positions.get(active=True, account=account)

        for position in positions:
            if position["entry"] is False:
                log.debug(
                    f"position in account {account} has not been entered, skipping"
                )
                continue
            try:
                positions_db.update(
                    {"_id": position["_id"]},
                    {"exit": status},
                )
                log.debug(
                    f"Position in account exit {account} has been set to {status}"
                )
            except Exception as e:
                log.error(e)
                return False

        return True

    def update(account: str = None, **kwargs: dict):
        # get all positions with account
        positions = Positions.get(account=account)

        for position in positions:
            try:
                positions_db.update(
                    {"_id": position["_id"]},
                    kwargs,
                )
                log.debug(f"Position in account {account} has been updated")
            except Exception as e:
                log.error(e)
                return False

        return True
