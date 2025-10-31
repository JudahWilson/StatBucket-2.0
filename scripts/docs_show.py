"""
Documentation viewing script

Opens documentation in appropriate viewer based on type.
"""

import argparse
import sys
import webbrowser
from pathlib import Path
from scripts.utils.common import run_command, print_success, print_error, print_info

def main():
    """Open documentation in appropriate viewer"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--er', action='store_true',
                       help='Open ER diagram in VS Code')
    
    args = parser.parse_args()
    
    try:
        if args.er:
            # Open ER diagram in VS Code
            er_file = Path("docs/database.md")
            if not er_file.exists():
                print_error(f"ER diagram not found: {er_file}")
                print_info("Generate it first with: uv run docs-er-diagram")
                sys.exit(1)
            
            print_info("Opening ER diagram in VS Code...")
            result = run_command(["code", str(er_file)], check=False)
            if result.returncode == 0:
                print_success("ER diagram opened in VS Code")
            else:
                print_error("Failed to open VS Code")
                sys.exit(1)
        else:
            # Open HTML documentation in browser
            docs_file = Path("docs/_build/html/index.html").resolve()
            if not docs_file.exists():
                print_error("HTML documentation not found")
                print_info("Build it first with: uv run docs-build")
                sys.exit(1)
            
            print_info("Opening documentation in browser...")
            webbrowser.open(docs_file.as_uri())
            print_success("Documentation opened in browser")
            
    except Exception as e:
        print_error(f"Failed to open documentation: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()