"""
Documentation build script

This script generates HTML documentation from Python docstrings using Sphinx.
Processes all source code and creates browsable HTML documentation.
"""

import sys
from pathlib import Path
from .utils.common import run_command, print_success, print_error, print_info
import argparse

def main():
    """Generate HTML documentation from Python docstrings"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    print_info("Building documentation...")
    
    docs_dir = Path("docs")
    if not docs_dir.exists():
        print_error("No docs directory found. Run 'uv run docs-setup' first.")
        sys.exit(1)
    
    # Build HTML docs
    build_dir = docs_dir / "_build" / "html"
    cmd = [
        sys.executable, "-m", "sphinx.cmd.build",
        "-b", "html",
        str(docs_dir),
        str(build_dir)
    ]
    
    try:
        result = run_command(cmd, check=True, capture_output=True)
        print_success("Documentation built successfully!")
        print_info(f"Open {build_dir / 'index.html'} to view")
        
        if result.stdout:
            print_info("Build details:")
            print(result.stdout)
            
    except Exception as e:
        print_error(f"Documentation build failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()