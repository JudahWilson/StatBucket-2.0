"""Sphinx documentation utilities"""

import fnmatch
from pathlib import Path
from .common import ensure_directory_exists, print_success, print_error, print_info

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

def parse_gitignore():
    """Parse .gitignore file and return patterns to exclude"""
    gitignore_path = Path(".gitignore")
    patterns = []
    
    if gitignore_path.exists():
        with open(gitignore_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    patterns.append(line)
    
    # Add common Python patterns if not already present
    default_patterns = ['__pycache__/', '*.pyc', '*.pyo', '.venv/', '*.egg-info/']
    for pattern in default_patterns:
        if pattern not in patterns:
            patterns.append(pattern)
    
    return patterns

def is_ignored(path, ignore_patterns):
    """Check if a path matches any gitignore patterns"""
    path_str = str(path).replace('\\', '/')
    
    for pattern in ignore_patterns:
        # Handle directory patterns
        if pattern.endswith('/'):
            if fnmatch.fnmatch(path_str + '/', pattern) or fnmatch.fnmatch(path_str, pattern[:-1]):
                return True
        # Handle file patterns
        elif fnmatch.fnmatch(path_str, pattern) or fnmatch.fnmatch(Path(path_str).name, pattern):
            return True
        # Handle patterns that might match parent directories
        elif '/' in pattern and fnmatch.fnmatch(path_str, pattern):
            return True
    
    return False

def discover_python_packages():
    """Discover Python packages respecting .gitignore, focusing on statbucket/"""
    ignore_patterns = parse_gitignore()
    packages = []
    
    # Focus on statbucket directory as per requirements
    statbucket_dir = Path("statbucket")
    if statbucket_dir.exists() and not is_ignored(statbucket_dir, ignore_patterns):
        packages.append("statbucket")
        print_info(f"Found package: statbucket")
        
        # Find subpackages
        for item in statbucket_dir.rglob("__init__.py"):
            package_dir = item.parent
            if not is_ignored(package_dir, ignore_patterns):
                # Convert path to module name
                module_name = str(package_dir).replace('\\', '.').replace('/', '.')
                if module_name not in packages:
                    packages.append(module_name)
                    print_info(f"Found subpackage: {module_name}")
    
    return packages

def generate_custom_rst_files():
    """Generate custom RST files for statbucket modules with correct paths"""
    docs_dir = Path("docs")
    
    # Manually create RST files for the statbucket package structure
    rst_files = [
        {
            'filename': 'statbucket.rst',
            'title': 'statbucket package',
            'module': 'statbucket',
            'submodules': ['statbucket.database', 'statbucket.scraping']
        },
        {
            'filename': 'statbucket.database.rst', 
            'title': 'statbucket.database module',
            'module': 'statbucket.database'
        },
        {
            'filename': 'statbucket.scraping.rst',
            'title': 'statbucket.scraping package', 
            'module': 'statbucket.scraping',
            'submodules': ['statbucket.scraping.base', 'statbucket.scraping.utils']
        },
        {
            'filename': 'statbucket.scraping.base.rst',
            'title': 'statbucket.scraping.base module',
            'module': 'statbucket.scraping.base'
        },
        {
            'filename': 'statbucket.scraping.utils.rst', 
            'title': 'statbucket.scraping.utils module',
            'module': 'statbucket.scraping.utils'
        }
    ]
    
    for rst_config in rst_files:
        create_rst_file(docs_dir, rst_config)
        print_info(f"Generated RST for {rst_config['module']}")
    
    return True

def create_rst_file(docs_dir, config):
    """Create individual RST file with proper configuration"""
    rst_path = docs_dir / config['filename']
    
    content = f"{config['title']}\n"
    content += "=" * len(config['title']) + "\n\n"
    
    # Add main module documentation
    content += f".. automodule:: {config['module']}\n"
    content += "   :members:\n"
    content += "   :undoc-members:\n" 
    content += "   :show-inheritance:\n\n"
    
    # Add submodules toctree if they exist
    if 'submodules' in config and config['submodules']:
        content += "Submodules\n"
        content += "----------\n\n"
        content += ".. toctree::\n"
        content += "   :maxdepth: 1\n\n"
        
        for submodule in config['submodules']:
            content += f"   {submodule}\n"
    
    with open(rst_path, 'w') as f:
        f.write(content)

def generate_api_docs():
    """Generate API documentation from discovered packages"""
    docs_dir = Path("docs")
    packages = discover_python_packages()
    
    if not packages:
        print_error("No Python packages found to document")
        return False
    
    print_info("Generating custom RST files with correct module paths...")
    success = generate_custom_rst_files()
    
    # Create a custom modules index that lists all discovered modules
    if success:
        create_dynamic_modules_index(docs_dir, packages)
    
    return success

def create_dynamic_modules_index(docs_dir, packages):
    """Create a dynamic modules index based on discovered packages"""
    
    module_files = []
    
    # Find all .rst files in docs directory that aren't index or conf
    for rst_file in docs_dir.glob("*.rst"):
        if rst_file.name not in ["index.rst", "modules.rst"]:
            module_name = rst_file.stem
            module_files.append((module_name, rst_file.stem))
            
            # Update each module file to have a cleaner title
            update_module_file_title(rst_file, module_name)
    
    # Sort modules alphabetically
    module_files.sort()
    
    # Create modules.rst with direct module references
    modules_content = '''StatBucket API Reference
=======================

.. toctree::
   :maxdepth: 2
   :caption: Modules:

'''
    
    for module_name, file_stem in module_files:
        modules_content += f"   {file_stem}\n"
    
    modules_path = docs_dir / "modules.rst"
    with open(modules_path, 'w') as f:
        f.write(modules_content)
    
    print_success(f"Created modules index with {len(module_files)} modules")

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