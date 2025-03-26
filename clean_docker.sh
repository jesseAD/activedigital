#!/bin/bash

# Script to clean up Docker resources and reclaim disk space

echo "=== Docker Cleanup Script ==="
echo "This script will remove unused Docker resources to free up disk space."

# 1. First stop all running containers
echo -e "\n[Step 1] Stopping all running Docker containers..."
sudo docker stop $(sudo docker ps -q) 2>/dev/null || echo "No running containers to stop."

# 2. Remove unused containers
echo -e "\n[Step 2] Removing unused containers..."
sudo docker container prune -f

# 3. Remove unused images
echo -e "\n[Step 3] Removing dangling images..."
sudo docker image prune -f

# 4. Remove all unused images (not just dangling ones)
echo -e "\n[Step 4] Removing all unused images..."
sudo docker image prune -a -f

# 5. Remove unused volumes
echo -e "\n[Step 5] Removing unused volumes..."
sudo docker volume prune -f

# 6. Remove unused networks
echo -e "\n[Step 6] Removing unused networks..."
sudo docker network prune -f

# 7. Full system prune (as a backup)
echo -e "\n[Step 7] Performing full system prune..."
sudo docker system prune -a -f --volumes

# 8. Restart Docker service
echo -e "\n[Step 8] Restarting Docker service..."
sudo systemctl restart docker

# 9. Check disk space after cleanup
echo -e "\n[Step 9] Current disk space usage:"
df -h /

echo -e "\nDocker cleanup completed!"
echo "You can now restart your Docker containers."
echo "To restart the data collection container, run: scripts/manage_data_collector.sh start" 