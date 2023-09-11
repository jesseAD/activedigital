# Account Management Service

This python webserver helps manage binance and okx positions, accounts and public data for tracking purposes only.

## Contributing

Contributions are always welcome!

See `contributing.md` for ways to get started.

Please adhere to this project's `code of conduct`.

## Features

- Create, Update Positions

## Installation

Install my-project with npm

```bash
  pip install -r requirements.txt
```

## Run Locally

Clone the project

```bash
  git clone https://link-to-project
```

Go to the project directory

```bash
  cd my-project
```

Install dependencies

```bash
  pip install -r requirements.txt
```

Start the server

```bash
  uvicorn main:app --reload
```

or

```bash
make run
```

## Folder Structure

- main.py --> all of the API endpoints that are publicly exposed
- ./src/machinery --> all the logic for executing the public functions to deliver info to the apis
- ./src/handlers --> all the private classes and functions to get, clean and perform actions to data
- ./src/lib --> all the supporting functions needed in many handlers

## License

[GPL 3.0](https://choosealicense.com/licenses/gpl-3.0/)
