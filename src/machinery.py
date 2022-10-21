import warnings
from src.lib.log import Log
from src.handlers.accounts import Accounts
from src.handlers.positions import Positions
from src.handlers.analytics import General

warnings.filterwarnings("ignore")

log = Log()


class Machinery:
    def update():
        # two main activities
        ## 1. check if entered is True, is so get the entry spread information and update the position object with the right information
        ## 2. check if exit is True, is so get the exit spread information and update the position object with the right information
        log.debug("Updating positions")

    def entry_controller(account: str = None, status: bool = True):
        res = Positions.entry(account, status)
        if res:
            return True
        else:
            return False

    def exit_controller(account: str = None, status: bool = False):
        res = Positions.exit(account, status)
        if res:
            return True
        else:
            return False

    ## NOTE: USE THIS FUNCTION TO TEST STUFF by CALLING http://localhost:8000/test
    def test():
        trades = Accounts.get_trades("FT4")
        data = General.position_entry(trades)
        print(data)

        return data


if __name__ == "__main__":
    log.info("started testing machinery")
    # Step 1: create position via some web ui which will fire off this function
    # Positions.create(
    #     positionType="basis",
    #     sub_account="FT4",
    #     spot="ETHW",
    #     perp="ETHW-PERP",
    # )

    # Step 2: manually enter the position

    # Step 3: set entry status because we finished entering via the web ui whcih will fire off this function
    # Positions.entry(account="FT4", status=True)

    # # Loop: always check to update positions with spread, pnls ect
    # Positions.update()

    print("done")
