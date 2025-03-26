# Docker Setup for Active Digital Data Collector

This repository includes a Docker setup for running the Active Digital data collector in a containerized environment. This approach provides a consistent runtime environment and simplifies dependency management.

## Prerequisites

- Docker and Docker Compose installed on your machine
- An `.env` file in the root directory with all required environment variables

## Directory Structure

```
active-digital/
├── .env                # Environment variables (mounted into the container)
├── Dockerfile          # Instructions for building the Docker image
├── docker-compose.yml  # Docker Compose configuration
├── scripts/            # Helper scripts
│   └── entrypoint.sh   # Container entrypoint script
├── data/               # Data directory
│   └── log/            # Log files
└── src/                # Source code
```

## Environment Variables

The `.env` file should contain all necessary environment variables, including:

- `MONGO_DB_USER` - MongoDB username
- `MONGO_DB_PASSWORD` - MongoDB password
- API credentials for various accounts (e.g., `MENDEL_DERIBIT_VOLARB_API_KEY`)

## Running the Data Collector

To build and start the data collector container:

```bash
docker-compose up --build
```

To run it in the background:

```bash
docker-compose up --build -d
```

## Checking Logs

To view the logs from the container:

```bash
docker-compose logs -f data_collector
```

## Stopping the Container

To stop the container:

```bash
docker-compose down
```

## Debugging

If you need to debug the container, you can set `KEEP_CONTAINER_RUNNING=true` in the `docker-compose.yml` file. This will keep the container running even after the data collection process finishes, allowing you to connect to it:

```bash
docker exec -it active_digital_data_collector bash
```

## Data Persistence

- The `.env` file is mounted into the container, so any changes to environment variables will be reflected on the next container start
- The `data` directory is mounted to persist log files and other generated data
- The entire project directory is mounted at `/app` to ensure access to the latest code

## Customization

You can modify the Docker Compose configuration to adjust container resources, add additional volumes, or modify network settings as needed. 