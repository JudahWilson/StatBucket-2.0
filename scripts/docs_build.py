"""
Documentation build script

Streamlined documentation workflow that automatically:
1. Cleans all build artifacts and cache
2. Sets up Sphinx structure if needed (or when forced with --force-setup)
3. Builds HTML documentation from Python docstrings

Provides a single command for complete documentation generation.
"""

import sys
import shutil
import os
import stat
from pathlib import Path
from .utils.sphinx_helpers import setup_sphinx_structure, generate_api_docs
from .utils.common import run_command, print_success, print_error, print_info
import argparse

def remove_readonly(func, path, _):
    """
    Error handler for Windows readonly file removal.
    
    This function is called when shutil.rmtree encounters permission errors,
    typically due to readonly or hidden file attributes on Windows.
    """
    try:
        # Clear readonly and hidden attributes
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except Exception as e:
        print_error(f"Failed to remove {path}: {e}")

def safe_rmtree(path):
    """
    Safely remove a directory tree, handling Windows permission issues.
    
    Args:
        path (Path): The directory path to remove
        
    Returns:
        bool: True if successful, False if failed
    """
    try:
        if os.name == 'nt':  # Windows
            shutil.rmtree(path, onerror=remove_readonly)
        else:  # Unix-like systems
            shutil.rmtree(path)
        return True
    except Exception as e:
        print_error(f"Failed to remove directory {path}: {e}")
        return False

def clean_documentation() -> bool:
    """
    Remove all documentation build artifacts and cache files.
    
    Returns:
        bool: True if successful, False if any files could not be removed
        
    Examples:
        >>> clean_documentation()  # Clean all build artifacts
        True
    """
    print_info("Cleaning documentation build artifacts...")
    
    docs_dir = Path("docs")
    build_dir = docs_dir / "_build"
    success = True
    
    if build_dir.exists():
        if safe_rmtree(build_dir):
            print_success("Cleaned documentation build directory")
        else:
            success = False
    else:
        print_info("No build directory found to clean")
    
    # Clean any Sphinx cache
    doctree_dir = docs_dir / ".doctrees"
    if doctree_dir.exists():
        if safe_rmtree(doctree_dir):
            print_success("Cleaned Sphinx cache")
        else:
            success = False
    
    if success:
        print_success("Documentation cleanup complete!")
    else:
        print_error("Some files could not be removed. You may need to run as administrator.")
    
    return success

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

def validate_doctests() -> bool:
    """Run doctest validation on the documentation"""
    try:
        print_info("Validating doctests in documentation...")
        
        docs_dir = Path("docs")
        doctest_dir = docs_dir / "_build" / "doctest"
        
        cmd = [
            sys.executable, "-m", "sphinx.cmd.build",
            "-b", "doctest",
            str(docs_dir),
            str(doctest_dir)
        ]
        
        result = run_command(cmd, check=True, capture_output=True)
        print_success("All doctests passed!")
        
        if result.stdout:
            print_info("Doctest details:")
            print(result.stdout)
        
        return True
        
    except Exception as e:
        print_error(f"Doctest validation failed: {e}")
        return False

def main():
    """Generate documentation with automatic clean, setup detection, and build"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--force-setup', action='store_true',
                       help='Force setup to run even if documentation structure already exists')
    parser.add_argument('--ignore-tests', action='store_true',
                       help='Skip doctest validation during build process')
    
    args = parser.parse_args()
    
    success = True
    
    # Always clean before building
    success = clean_documentation()
    
    if not success:
        print_error("Failed to clean documentation artifacts")
        sys.exit(1)
    
    # Setup if needed or forced
    if args.force_setup or needs_setup():
        success = setup_documentation()
        if not success:
            print_error("Failed to setup documentation structure")
            sys.exit(1)
    
    # Always build after cleaning (and setup if needed)
    success = build_documentation()
    
    if not success:
        print_error("Failed to build documentation")
        sys.exit(1)
    
    # Run doctest validation unless ignored
    if not args.ignore_tests:
        success = validate_doctests()
        if not success:
            print_error("Doctest validation failed")
            sys.exit(1)

if __name__ == "__main__":
    main()