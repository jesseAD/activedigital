from typing import Union
from fastapi import FastAPI

# import main from src
from src.handlers import Positions, Accounts


app = FastAPI()


@app.get("/")
def read_root():
    return {"app": "FTX Account Handler"}


@app.get("/positions/update")
def update_positions():
    Positions.update()
    return {"message": "Positions updated"}


@app.get("/positions/entry")
def entry_positions(account: str = None, status: bool = True):
    Positions.entry(account, status)
    return {"message": "Positions entry updated"}


@app.get("/positions/exit")
def exit_positions(account: str = None, status: bool = False):
    Positions.exit(account, status)
    return {"message": "Positions exit updated"}
