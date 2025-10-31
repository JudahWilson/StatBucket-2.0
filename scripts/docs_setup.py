"""
Documentation setup script

This script initializes Sphinx documentation structure with autodoc configuration
for extracting docstrings from the NBA scraper codebase.
"""

from pathlib import Path
from .utils.sphinx_helpers import setup_sphinx_structure, generate_api_docs
from .utils.common import print_success, print_error, print_info
import argparse

def main():
    """Initialize Sphinx documentation structure with autodoc configuration"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    print_info("Setting up Sphinx documentation...")
    
    try:
        # Create basic Sphinx structure
        docs_dir = setup_sphinx_structure()
        print_success("Created Sphinx directory structure")
        
        # Generate API documentation
        if generate_api_docs():
            print_success("Generated API documentation from source code")
        else:
            print_error("Failed to generate API docs (source code may be empty)")
        
        print_success("Documentation setup complete!")
        print_info(f"Documentation source created in: {docs_dir}")
        print_info("Run 'uv run docs-build' to generate HTML documentation")
        
    except Exception as e:
        print_error(f"Documentation setup failed: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()