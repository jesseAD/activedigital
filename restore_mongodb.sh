#!/bin/bash

# New MongoDB connection string for the target database (where data will be restored)
TARGET_URI="mongodb+srv://mathlete76:NGGgtZM4JOKY5o3Z@serverlessinstance0.bszsz.mongodb.net/active_digital?retryWrites=true&w=majority"

# Backup directory path
BACKUP_DIR="mongodb_backup"

# Check if backup directory exists
if [ ! -d "$BACKUP_DIR/active_digital" ]; then
    echo "Error: Backup directory $BACKUP_DIR/active_digital not found."
    exit 1
fi

# First, let's create a temporary directory to hold modified metadata files
TMP_DIR="temp_metadata"
mkdir -p $TMP_DIR

echo "Removing storageEngine parameters from metadata files..."
# Process each metadata file to remove storageEngine parameters
for metadata_file in "$BACKUP_DIR/active_digital"/*.metadata.json; do
    collection_name=$(basename "$metadata_file" .metadata.json)
    echo "Processing metadata for $collection_name..."
    
    # Create a modified version without storageEngine parameters
    cat "$metadata_file" | grep -v "storageEngine" > "$TMP_DIR/${collection_name}.metadata.json"
    
    # Replace the original with our modified version
    cp "$TMP_DIR/${collection_name}.metadata.json" "$metadata_file"
done

# First try to restore the runs collection (most important for continuity)
echo "First restoring the runs collection to ensure run ID continuity..."
mongorestore --uri="$TARGET_URI" --db="active_digital" --collection="runs" --drop --noIndexRestore "$BACKUP_DIR/active_digital/runs.bson"

if [ $? -eq 0 ]; then
    echo "Successfully restored runs collection!"
else
    echo "Failed to restore runs collection. This may affect run ID continuity."
fi

# Get list of all collections from the backup directory
collections=$(find "$BACKUP_DIR/active_digital" -name "*.bson" | sed 's/.*\///;s/\.bson//')

# Restore each collection individually
for collection in $collections; do
    # Skip runs collection as we already tried to restore it
    if [ "$collection" == "runs" ]; then
        continue
    fi
    
    echo "Restoring $collection..."
    mongorestore --uri="$TARGET_URI" --db="active_digital" --collection="$collection" --drop --noIndexRestore "$BACKUP_DIR/active_digital/$collection.bson"
    
    if [ $? -eq 0 ]; then
        echo "Successfully restored $collection"
    else
        echo "Failed to restore $collection"
    fi
done

# Clean up temporary directory
rm -rf $TMP_DIR

echo "Database restoration process completed!"
echo "You can now restart your data collection process to continue with the correct run IDs." 