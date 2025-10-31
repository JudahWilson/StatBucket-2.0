"""
Database initialization utilities

Helper functions for creating database tables from SQLAlchemy models.
Used internally by development setup scripts.
"""

import os
import sys
from pathlib import Path
from sqlalchemy import create_engine
from typing import Optional

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from nba_scraper.database_schema import Base
from .common import print_success, print_error, print_info

def initialize_database(database_url: str = 'sqlite:///nba_scraper.db', 
                       echo: bool = False,
                       skip_if_exists: bool = True) -> bool:
    """
    Initialize database from SQLAlchemy models
    
    Args:
        database_url: Database connection URL
        echo: Whether to echo SQL statements  
        skip_if_exists: Skip if SQLite database file already exists
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Check if database already exists (for SQLite)
        if skip_if_exists and database_url.startswith('sqlite:///'):
            db_file = database_url.replace('sqlite:///', '')
            if os.path.exists(db_file):
                print_info(f"Database file {db_file} already exists, skipping initialization")
                return True
        
        print_info(f"Creating database from SQLAlchemy models...")
        print_info(f"Database URL: {database_url}")
        
        # Create engine
        engine = create_engine(database_url, echo=echo)
        
        # Create all tables from Base metadata
        Base.metadata.create_all(engine)
        
        print_success("Database tables created successfully!")
        
        # Show what was created
        table_names = Base.metadata.tables.keys()
        print_info(f"Created {len(table_names)} tables:")
        for table_name in sorted(table_names):
            print(f"  - {table_name}")
        
        return True
        
    except Exception as e:
        print_error(f"Failed to create database: {e}")
        return False