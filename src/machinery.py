import warnings
from src.lib.log import Log
from src.handlers.accounts import Accounts
from src.handlers.positions import Positions
from src.handlers.analytics import General

warnings.filterwarnings("ignore")

log = Log()


class Machinery:
    # -------------------
    # Accounts Machinery
    # -------------------
    def get_subaccounts():
        res = Accounts.get_subaccounts()
        return res

    # -------------------
    # Positions Machinery
    # -------------------
    def positions(active, spot, future, perp, position_type, account):
        positions = Positions.get(active, spot, future, perp, position_type, account)
        data = {
            "results": positions,
        }
        return data

    def create_position(
        positionType: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,
    ):
        res = Positions.create(
            positionType=positionType,
            sub_account=sub_account,
            spot=spot,
            future=future,
            perp=perp,
        )

        return res

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

    def update(account):

        active_positions = Positions.get(active=True)

        for position in active_positions:
            account = position["account"]
            if position["entry"] is False and position["exit"] is False:
                log.debug(
                    f"position in account {account} has just been created with no activity"
                )
                continue
            if position["entry"] is True:
                trades = Accounts.get_trades(account)
                # get the entry spread information
                entry_spread = General.position_entry(trades)
                # update the position object with the right information
                Positions.update(
                    account,
                    entry_spread=entry_spread,
                )
            elif position["exit"] is True:
                # get the exit spread information
                # exit_spread = General.position_exit(trades)
                # update the position object with the right information
                # Positions.update(
                #     account,
                #     exit_spread=exit_spread,
                # )
                pass
            elif position["exit"] is False and position["entry"] is True:
                # get the spread profit & loss and write it to the position object as spread_entry_profit
                # get the funding profit & loss and write it to the position object as funding_profit
                pass
            elif position["active"] is False:
                # the position is closed via the UI
                # get the spread profit & loss and write it to the position object as spread_entry_profit
                # get the funding profit & loss and write it to the position object as funding_profit
                # get the spread exit profit & loss and write it to the position object as spread_exit_profit
                # get the total profit & loss and write it to the position object as total_profit
                # write the start time and end time of the position to the position object (take entry_spread start and exit_sold end)
                pass

        return
