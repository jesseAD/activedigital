from typing import Union
from fastapi import FastAPI

# import main from src
from src.machinery import Machinery


app = FastAPI()


@app.get("/")
def read_root():
    return {"app": "FTX Account Handler"}

@app.get("/positions/create")
def create_position(
        positionType: str = None,
        sub_account: str = None,
        spot: str = None,
        future: str = None,
        perp: str = None,):
    res = Machinery.create_position(positionType, sub_account, spot, future, perp)
    if res:
        return {"message": "Position created"}
    else:
        return {"message": "Position not created"}


@app.get("/positions/entry")
def entry_positions(account: str = None, status: bool = True):
    res = Machinery.entry_controller(account, status)

    if res:
        return {"message": f"Positions entry updated for account {account} to {status}"}
    else:
        return {"message": "Positions entry failed to update"}


@app.get("/positions/exit")
def exit_positions(account: str = None, status: bool = False):
    res = Machinery.exit_controller(account, status)

    if res:
        return {"message": f"Positions exit updated for account {account} to {status}"}
    else:
        return {"message": "Positions exit failed to update"}


# just a function to test stuff
@app.get("/update")
def update(account: str = None):
    data = Machinery.update(account)
    return data
