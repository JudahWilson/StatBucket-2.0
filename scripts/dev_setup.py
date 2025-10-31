"""
Development environment setup script

This script handles complete development environment configuration by
orchestrating other scripts for dependency installation and documentation setup.
"""

import sys
from pathlib import Path
from .utils.common import run_command, print_success, print_error, print_info
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
    
    # Setup documentation using existing docs-setup script
    print_info("Setting up documentation...")
    docs_setup_result = run_command(["uv", "run", "docs-setup"], check=False)
    if docs_setup_result.returncode == 0:
        print_success("Documentation setup completed")
        
        # Build initial documentation using existing docs-build script
        print_info("Building initial documentation...")
        docs_build_result = run_command(["uv", "run", "docs-build"], check=False)
        if docs_build_result.returncode == 0:
            print_success("Initial documentation built successfully")
        else:
            print_info("Initial documentation build completed with warnings")
    else:
        print_error("Documentation setup failed")
        print_info("You can set up docs later with 'uv run docs-setup'")
    
    print_success("Development environment setup complete!")
    print_info(run_command('uv run list-scripts'.split(),capture_output=True).stdout)

if __name__ == "__main__":
    main()