"""Access the database via the `engine` variable provided in this module"""
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.environ['DB_URL'])
"""Production database"""
engine_staged = create_engine('sqlite:///staged_data.db')
"""Intermediate storage for review"""