#!/bin/bash

# Create log directory if it doesn't exist
mkdir -p /data/log

# Set environment variables from .env if mounted
if [ -f "/.env" ]; then
  echo "Loading environment variables from mounted .env file"
  set -a
  source /.env
  set +a
fi

# Run the data collection script
echo "Starting data collection process at $(date)"
python /app/src/data_getter.py

# Keep container running if needed for debugging
if [ "$KEEP_CONTAINER_RUNNING" = "true" ]; then
  echo "Keeping container running for debugging purposes"
  tail -f /dev/null
fi 