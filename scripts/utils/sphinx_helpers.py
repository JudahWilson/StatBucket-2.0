"""Sphinx documentation utilities"""

import sys
from pathlib import Path
from .common import run_command, ensure_directory_exists, print_success, print_error

def create_sphinx_config():
    """Create Sphinx configuration with autodoc settings"""
    conf_content = '''
# Configuration file for the Sphinx documentation builder.

project = 'NBA Scraper'
copyright = '2025, NBA Scraper Team'
author = 'NBA Scraper Team'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx_autodoc_typehints',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', 'nba_scraper.rst']

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

# Autodoc settings
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

# Remove nba_scraper prefix from displayed names
add_module_names = False
modindex_common_prefix = ['nba_scraper.']

# Autodoc configuration for cleaner output
autodoc_typehints = 'description'
autodoc_typehints_description_target = 'documented'

# Custom processing to clean up all nba_scraper references
def process_signature(app, what, name, obj, options, signature, return_annotation):
    """Remove nba_scraper prefix from module names in signatures"""
    if signature:
        signature = signature.replace('nba_scraper.', '')
    return signature, return_annotation

def process_docstring(app, what, name, obj, options, lines):
    """Clean up module references in docstrings"""
    for i, line in enumerate(lines):
        lines[i] = line.replace('nba_scraper.', '')

def skip_member(app, what, name, obj, skip, options):
    """Control which members to document"""
    return skip

def setup(app):
    app.connect('autodoc-process-signature', process_signature)
    app.connect('autodoc-process-docstring', process_docstring)
    app.connect('autodoc-skip-member', skip_member)
'''
    return conf_content.strip()

def setup_sphinx_structure():
    """Initialize Sphinx documentation directory structure"""
    docs_dir = Path("docs")
    ensure_directory_exists(docs_dir)
    ensure_directory_exists(docs_dir / "_static")
    ensure_directory_exists(docs_dir / "_templates")
    
    # Create conf.py
    conf_path = docs_dir / "conf.py"
    with open(conf_path, 'w') as f:
        f.write(create_sphinx_config())
    
    # Create index.rst
    index_content = '''
NBA Scraper Documentation
========================

Welcome to NBA Scraper's documentation!

This system provides comprehensive tools for scraping NBA data from basketball-reference.com,
with dynamic schema management, error handling, and modular data processing.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex` 
* :ref:`search`
'''
    
    index_path = docs_dir / "index.rst"
    with open(index_path, 'w') as f:
        f.write(index_content.strip())
    
    return docs_dir

def generate_api_docs():
    """Generate API documentation from source code with flat structure"""
    docs_dir = Path("docs")
    
    # Generate module documentation with better options
    cmd = [
        sys.executable, "-m", "sphinx.ext.apidoc",
        "-f",           # Force overwrite
        "-e",           # Put each module on separate page  
        "--no-toc",     # Don't create a main toc file
        "-o", str(docs_dir), 
        "nba_scraper"
    ]
    
    result = run_command(cmd, check=False)
    
    # Create a custom modules index that lists modules directly
    if result.returncode == 0:
        create_flat_modules_index(docs_dir)
    
    return result.returncode == 0

def create_flat_modules_index(docs_dir):
    """Create a flat modules index without nesting and clean module titles"""
    
    # Find all generated .rst files for modules (excluding main nba_scraper.rst)
    module_files = []
    for rst_file in docs_dir.glob("nba_scraper.*.rst"):
        if rst_file.name != "nba_scraper.rst":
            module_name = rst_file.stem.replace("nba_scraper.", "")
            module_files.append((module_name, rst_file.stem))
            
            # Update each module file to have a cleaner title
            update_module_file_title(rst_file, module_name)
    
    # Sort modules alphabetically
    module_files.sort()
    
    # Create modules.rst with direct module references
    modules_content = '''API Reference
=============

.. toctree::
   :maxdepth: 2
   :caption: Modules:

'''
    
    for module_name, file_stem in module_files:
        modules_content += f"   {file_stem}\n"
    
    modules_path = docs_dir / "modules.rst"
    with open(modules_path, 'w') as f:
        f.write(modules_content)

def update_module_file_title(rst_file, clean_name):
    """Update individual module RST file to have cleaner title and content"""
    try:
        with open(rst_file, 'r') as f:
            content = f.read()
        
        # Replace the module title (first line and underline)
        lines = content.split('\n')
        if len(lines) >= 2 and lines[0].startswith('nba\\_scraper.'):
            # Create clean title
            clean_title = f"{clean_name.replace('_', ' ').title()} Module"
            full_module_name = f"nba_scraper.{clean_name}"
            
            # Replace the content with custom automodule configuration including module reference
            updated_content = f'''{clean_title}
{'=' * len(clean_title)}

.. note::
   **Module Path:** ``{full_module_name}``

.. automodule:: {full_module_name}
   :members:
   :show-inheritance:
   :undoc-members:
   
.. currentmodule:: {full_module_name}
'''
            
            # Write back the updated content
            with open(rst_file, 'w') as f:
                f.write(updated_content)
                
    except Exception:
        # If we can't update, that's okay - the file will still work
        pass