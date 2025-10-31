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