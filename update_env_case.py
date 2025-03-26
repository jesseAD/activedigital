#!/usr/bin/env python3
"""
Script to update the .env file to use uppercase for all environment variables
"""
import os
import re

def update_env_file():
    """Update the .env file to use uppercase for exchange and account names"""
    print("Updating .env file to use uppercase for all environment variables...")
    
    # Read the current .env file
    try:
        with open('.env', 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print("Error: .env file not found!")
        return False
    
    # Process each line
    updated_lines = []
    update_count = 0
    
    for line in lines:
        # Skip comments and empty lines
        if line.strip() == '' or line.strip().startswith('#'):
            updated_lines.append(line)
            continue
        
        # Check if the line has the format we're looking for (client_exchange_account_VAR=value)
        if '=' in line:
            key, value = line.strip().split('=', 1)
            
            # Look for variables with the pattern: MENDEL_deribit_volarb_API_KEY
            pattern = r'^([A-Z]+)_([a-z]+)_([a-z]+)_(.+)$'
            match = re.match(pattern, key)
            
            if match:
                # Extract components
                client = match.group(1)  # Already uppercase
                exchange = match.group(2)  # Lowercase
                account = match.group(3)  # Lowercase
                var_type = match.group(4)  # Typically uppercase
                
                # Create the new key with all uppercase parts
                new_key = f"{client}_{exchange.upper()}_{account.upper()}_{var_type}"
                
                if new_key != key:
                    print(f"Converting: {key} -> {new_key}")
                    updated_lines.append(f"{new_key}={value}\n")
                    update_count += 1
                else:
                    updated_lines.append(line)
            else:
                # Keep lines that don't match the pattern
                updated_lines.append(line)
        else:
            # Keep lines without equal sign
            updated_lines.append(line)
    
    if update_count == 0:
        print("No environment variables needed updating.")
        return True
    
    # Create a backup of the original .env file
    try:
        with open('.env.bak', 'w') as f:
            f.writelines(lines)
        print(f"Created backup of original .env file as .env.bak")
    except Exception as e:
        print(f"Warning: Could not create backup file: {str(e)}")
    
    # Write the updated .env file
    try:
        with open('.env', 'w') as f:
            f.writelines(updated_lines)
        print(f"Successfully updated {update_count} environment variables in .env file!")
        return True
    except Exception as e:
        print(f"Error writing to .env file: {str(e)}")
        return False

if __name__ == "__main__":
    update_env_file() 