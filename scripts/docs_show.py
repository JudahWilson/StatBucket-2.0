"""
Opens documentation in the browser.
"""

import sys
import webbrowser
from pathlib import Path
from scripts.utils.common import print_success, print_error, print_info

def main():
    """Open documentation in the browser."""    
    try:
        
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