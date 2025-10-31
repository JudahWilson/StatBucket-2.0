"""
Documentation script

Intelligent documentation management that sets up Sphinx structure if needed,
then builds HTML documentation from Python docstrings.
"""

import sys
from pathlib import Path
from .utils.sphinx_helpers import setup_sphinx_structure, generate_api_docs
from .utils.common import run_command, print_success, print_error, print_info
import argparse

def needs_setup() -> bool:
    """Check if documentation setup is needed"""
    docs_dir = Path("docs")
    conf_py = docs_dir / "conf.py"
    return not (docs_dir.exists() and conf_py.exists())

def setup_documentation() -> bool:
    """Set up Sphinx documentation structure"""
    try:
        print_info("Setting up Sphinx documentation...")
        
        # Create basic Sphinx structure
        docs_dir = setup_sphinx_structure()
        print_success("Created Sphinx directory structure")
        
        # Generate API documentation
        if generate_api_docs():
            print_success("Generated API documentation from source code")
        else:
            print_info("API docs generation completed (may be minimal for new project)")
        
        print_success("Documentation setup complete!")
        return True
        
    except Exception as e:
        print_error(f"Documentation setup failed: {e}")
        return False

def build_documentation() -> bool:
    """Build HTML documentation"""
    try:
        print_info("Building HTML documentation...")
        
        docs_dir = Path("docs")
        build_dir = docs_dir / "_build" / "html"
        
        cmd = [
            sys.executable, "-m", "sphinx.cmd.build",
            "-b", "html",
            str(docs_dir),
            str(build_dir)
        ]
        
        result = run_command(cmd, check=True, capture_output=True)
        print_success("Documentation built successfully!")
        print_info(f"Open {build_dir / 'index.html'} to view")
        
        if result.stdout:
            print_info("Build details:")
            print(result.stdout)
        
        return True
        
    except Exception as e:
        print_error(f"Documentation build failed: {e}")
        return False

def main():
    """Generate documentation with automatic setup if needed"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--setup-only', action='store_true',
                       help='Only set up documentation structure, do not build')
    parser.add_argument('--build-only', action='store_true', 
                       help='Only build documentation, do not set up')
    
    args = parser.parse_args()
    
    success = True
    
    # Setup if needed (unless build-only specified)
    if not args.build_only and needs_setup():
        success = setup_documentation()
    
    # Build unless setup-only specified
    if not args.setup_only and success:
        success = build_documentation()
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()