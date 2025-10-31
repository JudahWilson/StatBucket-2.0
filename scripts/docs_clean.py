"""
Documentation cleanup script

This script removes all documentation build artifacts and cache files,
providing a clean slate for fresh documentation builds.
"""

import shutil
from pathlib import Path
from .utils.common import print_success, print_info
import argparse

def main():
    """Remove all documentation build artifacts and cache"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    print_info("Cleaning documentation build artifacts...")
    
    docs_dir = Path("docs")
    build_dir = docs_dir / "_build"
    
    if build_dir.exists():
        shutil.rmtree(build_dir)
        print_success("Cleaned documentation build directory")
    else:
        print_info("No build directory found to clean")
    
    # Clean any Sphinx cache
    doctree_dir = docs_dir / ".doctrees"
    if doctree_dir.exists():
        shutil.rmtree(doctree_dir)
        print_success("Cleaned Sphinx cache")
    
    print_success("Documentation cleanup complete!")

if __name__ == "__main__":
    main()