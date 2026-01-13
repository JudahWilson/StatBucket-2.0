"""
Initialize staged database with DDL from production database

This script uses mysqldump to extract the schema from production database
and applies it to the staging database to ensure both have identical structure.
"""

import argparse
import sys
import os
import tempfile
import subprocess
from dotenv import load_dotenv
from .utils.common import print_success, print_error, print_info

# Load environment variables
load_dotenv()

def parse_db_url(url):
    """Parse database URL to extract connection parameters.
    
    Args:
        url (str): Database URL in format mysql+mysqlconnector://user:pass@host/db
        
    Returns:
        dict: Connection parameters for mysql command
    """
    # Remove protocol prefix
    url = url.replace('mysql+mysqlconnector://', '')
    
    # Split by @ to separate credentials from host/db
    credentials, host_db = url.split('@', 1)
    
    # Split credentials by : to get user and password
    user, password = credentials.split(':', 1)
    
    # Split host_db by / to get host and database
    host, database = host_db.split('/', 1)
    
    return {
        'host': host,
        'port': 3306,
        'user': user,
        'password': password,
        'database': database
    }


def dump_production_schema():
    """Extract schema from production database using mysqldump.
    
    Returns:
        str: Path to temporary file containing DDL
        
    Raises:
        subprocess.CalledProcessError: If mysqldump fails
    """
    try:
        print_info("Extracting schema from production database...")
        
        prod_url = os.environ["PROD_DB_URL"]
        prod_params = parse_db_url(prod_url)
        
        # Create temporary file for schema dump
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
        temp_file.close()
        
        # Build mysqldump command for schema only
        cmd = [
            'mysqldump',
            f'--host={prod_params["host"]}',
            f'--port={prod_params["port"]}',
            f'--user={prod_params["user"]}',
            f'--password={prod_params["password"]}',
            '--no-data',  # Schema only, no data
            '--routines',  # Include stored procedures and functions
            '--triggers',  # Include triggers
            '--single-transaction',  # Consistent snapshot
            prod_params['database']
        ]
        
        print_info(f"Running: mysqldump --host={prod_params['host']} --no-data {prod_params['database']}")
        
        with open(temp_file.name, 'w') as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True, check=True)
        
        print_success("Schema extracted successfully")
        return temp_file.name
        
    except subprocess.CalledProcessError as e:
        print_error(f"mysqldump failed: {e.stderr}")
        raise
    except Exception as e:
        print_error(f"Failed to dump production schema: {e}")
        raise


def create_staging_database():
    """Create staging database if it doesn't exist."""
    try:
        staging_url = os.environ["STAGING_DB_URL"]
        staging_params = parse_db_url(staging_url)
        
        print_info(f"Creating staging database '{staging_params['database']}' if it doesn't exist...")
        
        # Connect without specifying database to create it
        cmd = [
            'mariadb',
            f'--host={staging_params["host"]}',
            f'--port={staging_params["port"]}',
            f'--user={staging_params["user"]}',
            f'--password={staging_params["password"]}',
            '--execute',
            f"CREATE DATABASE IF NOT EXISTS {staging_params['database']}"
        ]
        
        result = subprocess.run(cmd, stderr=subprocess.PIPE, text=True, check=True)
        print_success(f"Staging database '{staging_params['database']}' ready")
        
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to create staging database: {e.stderr}")
        raise


def apply_schema_to_staging(schema_file):
    """Apply schema to staging database using mysql command.
    
    Args:
        schema_file (str): Path to SQL file containing DDL
        
    Raises:
        subprocess.CalledProcessError: If mysql command fails
    """
    try:
        print_info("Applying schema to staging database...")
        
        staging_url = os.environ["STAGING_DB_URL"] 
        staging_params = parse_db_url(staging_url)
        
        # Build mysql command
        cmd = [
            'mariadb',
            f'--host={staging_params["host"]}',
            f'--port={staging_params["port"]}',
            f'--user={staging_params["user"]}',
            f'--password={staging_params["password"]}',
            staging_params['database']
        ]
        
        print_info(f"Running: mariadb --host={staging_params['host']} {staging_params['database']}")
        
        with open(schema_file, 'r') as f:
            result = subprocess.run(cmd, stdin=f, stderr=subprocess.PIPE, text=True, check=True)
        
        print_success("Schema applied to staging database successfully")
        
    except subprocess.CalledProcessError as e:
        print_error(f"mariadb command failed: {e.stderr}")
        raise
    except Exception as e:
        print_error(f"Failed to apply schema to staging: {e}")
        raise





if __name__ == "__main__":
    """Initialize staged database according to production DDL"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    
    try:
        # Create staging database if it doesn't exist
        create_staging_database()
        
        # Dump production schema and apply to staging
        schema_file = None
        try:
            schema_file = dump_production_schema()
            apply_schema_to_staging(schema_file)
            print_success("Staged database initialization completed successfully")
        finally:
            # Clean up temporary file
            if schema_file and os.path.exists(schema_file):
                os.unlink(schema_file)
    
    except Exception as e:
        print_error(f"Database initialization failed: {e}")
        sys.exit(1)