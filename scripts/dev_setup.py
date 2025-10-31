"""
Development environment setup script

This script handles complete development environment configuration by
orchestrating other scripts for dependency installation and documentation setup.
"""

import sys
from pathlib import Path
from .utils.common import run_command, print_success, print_error, print_info
from .utils.database import initialize_database
import argparse

def main():
    """Complete development environment configuration"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    print_info("Setting up development environment...")
    
    # Install dependencies
    print_info("Installing dependencies...")
    result = run_command(["uv", "sync", "--all-extras"])
    if result.returncode == 0:
        print_success("Dependencies installed successfully")
    else:
        print_error("Failed to install dependencies")
        sys.exit(1)
    
    # Initialize database
    print_info("Initializing database...")
    if initialize_database(skip_if_exists=True):
        print_success("Database initialized successfully")
    else:
        print_error("Database initialization failed")
    
    # Setup and build documentation using combined docs script
    print_info("Setting up and building documentation...")
    docs_result = run_command(["uv", "run", "docs"], check=False)
    if docs_result.returncode == 0:
        print_success("Documentation setup and build completed")
    else:
        print_error("Documentation setup/build failed")
        print_info("You can set up docs later with 'uv run docs'")
    
    print_success("Development environment setup complete!")
    print_info("Run 'uv run list-scripts' to see all available commands")

if __name__ == "__main__":
    main()