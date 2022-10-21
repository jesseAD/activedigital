import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from src.lib.log import Log
from src.lib.db import MongoDB

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
positions_db = MongoDB("hg", "positions", MONGO_URI)
log = Log()


class General:
    def position_entry(trades: pd.DataFrame):
        data = {}
        try:
            start = pd.to_datetime(trades["time"].min())
            end = pd.to_datetime(trades["time"].max())
            delta = end - start

            spot = trades.loc[trades["future"].isnull()]
            perp = trades.loc[trades["future"].notnull()]

            spot["price"] = spot["price"].astype(float)
            spot["size"] = spot["size"].astype(float)
            perp["price"] = perp["price"].astype(float)
            perp["size"] = perp["size"].astype(float)

            perp.loc[perp["side"] == "buy", "size"] = -perp["size"]
            spot.loc[spot["side"] == "sell", "size"] = -spot["size"]

            # get the start and end time delta in the trades
            start = pd.to_datetime(trades["time"].min())
            end = pd.to_datetime(trades["time"].max())
            delta = end - start

            delta = round(delta.total_seconds() / 60, 0)

            spot_avg_entry = np.average(spot["price"], weights=spot["size"])
            perp_avg_entry = np.average(perp["price"], weights=perp["size"])
            entry_spread = round((perp_avg_entry - spot_avg_entry) * 100, 6)
            spread_entry_gain_loss_percent = round((entry_spread / perp_avg_entry), 6)

            data = {
                "spot_avg_entry": spot_avg_entry,
                "perp_avg_entry": perp_avg_entry,
                "entry_spread": entry_spread,
                "spread_entry_gain_loss_percent": spread_entry_gain_loss_percent,
                "spot_market": spot["market"].unique()[0],
                "perp_market": perp["market"].unique()[0],
                "spot_size": spot["size"].sum(),
                "perp_size": perp["size"].sum(),
                "start": start,
                "end": end,
                "entry_total_time": delta,
            }

        except Exception as e:
            log.error(e)

        return data

    def funding_pnl(funding: pd.DataFrame):
        data = {}

        if funding is None:
            log.debug("No funding data found to perform analytics")
            return None

        account = funding["account"].unique()[0]

        try:
            # get the accountsize for the account from the positions db
            cursor = positions_db.find({"account": account})

            funding["pnl"] = funding["payment"]
            total_profit = round(funding["pnl"].sum(), 2)
            average_profit = round(funding["pnl"].mean(), 2)
            average_daily_profit = round(funding["pnl"].resample("D").sum().mean(), 2)
            daily_profit_percent = round(
                (average_daily_profit / cursor[0]["initialAccountValue"]) * 100, 2
            )

            data = {
                "account": account,
                "total_profit": total_profit,
                "average_profit": average_profit,
                "average_daily_profit": average_daily_profit,
                "daily_profit_percent": daily_profit_percent,
            }

        except Exception as e:
            log.error(e)

        return data
