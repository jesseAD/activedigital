#!/bin/bash
# Script to manage the Data Collector container

# Function to display usage information
function show_usage {
  echo "Usage: $0 [start|stop|restart|status|logs]"
  echo ""
  echo "Commands:"
  echo "  start   - Start the data collector container"
  echo "  stop    - Stop the data collector container"
  echo "  restart - Restart the data collector container"
  echo "  status  - Show the status of the data collector container"
  echo "  logs    - Show the logs from the data collector container"
  echo ""
}

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
  echo "Error: Docker is not installed or not in PATH"
  exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
  echo "Error: Docker Compose is not installed or not in PATH"
  exit 1
fi

# Go to the project root directory
cd "$(dirname "$0")/.." || exit 1

# Process command
case "$1" in
  start)
    echo "Starting data collector container..."
    docker-compose up --build -d
    ;;
  stop)
    echo "Stopping data collector container..."
    docker-compose down
    ;;
  restart)
    echo "Restarting data collector container..."
    docker-compose down
    docker-compose up --build -d
    ;;
  status)
    echo "Data collector container status:"
    docker-compose ps
    ;;
  logs)
    echo "Data collector container logs:"
    docker-compose logs -f data_collector
    ;;
  *)
    show_usage
    exit 1
    ;;
esac

exit 0 