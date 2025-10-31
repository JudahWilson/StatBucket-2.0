"""
List available custom scripts from pyproject.toml

This script shows all custom scripts with their descriptions extracted from
the docstrings of the associated Python files.
"""

import toml
import sys
import ast
from pathlib import Path
import argparse

def extract_file_docstring(file_path):
    """Extract the module-level docstring from a Python file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            tree = ast.parse(f.read())
        
        # Get the first statement if it's a string (docstring)
        if (tree.body and isinstance(tree.body[0], ast.Expr) and 
            isinstance(tree.body[0].value, ast.Str)):
            return tree.body[0].value.s.strip()
        elif (tree.body and isinstance(tree.body[0], ast.Expr) and 
              isinstance(tree.body[0].value, ast.Constant) and 
              isinstance(tree.body[0].value.value, str)):
            return tree.body[0].value.value.strip()
    except Exception:
        pass
    return "No description available"

def get_script_description(script_name, script_path):
    """Get first sentence from the Python file's docstring"""
    # Parse the module path (e.g., "scripts.docs_setup:main")
    if ':' in script_path:
        module_path = script_path.split(':')[0]
    else:
        module_path = script_path
    
    # Convert module path to file path
    file_path = Path(module_path.replace('.', '/') + '.py')
    
    if file_path.exists():
        docstring = extract_file_docstring(file_path)
        if docstring:
            # Extract first sentence from docstring
            return extract_first_sentence(docstring)
        return "No description available"
    
    return "File not found"

def extract_first_sentence(text):
    """Extract the meaningful first sentence from a text block"""
    if not text:
        return "No description available"
    
    # Split into lines and find the actual description
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    # Skip the first line if it's just a title (ends with "script")
    start_line = 0
    if lines and lines[0].lower().endswith('script'):
        start_line = 1
    
    # Join remaining lines and clean up
    description_text = ' '.join(lines[start_line:])
    cleaned_text = ' '.join(description_text.split())
    
    # Find the first sentence (ends with . ! or ?)
    for i, char in enumerate(cleaned_text):
        if char in '.!?' and i < len(cleaned_text) - 1:
            # Make sure it's not an abbreviation (simple check)
            if cleaned_text[i + 1] == ' ' and (i == 0 or cleaned_text[i - 1] != ' '):
                sentence = cleaned_text[:i + 1].strip()
                # Limit sentence length for readability
                if len(sentence) > 80:
                    return sentence[:77] + "..."
                return sentence
    
    # If no sentence ending found, return first 80 characters or the whole text
    if len(cleaned_text) > 80:
        return cleaned_text[:77] + "..."
    return cleaned_text

def main():
    """List all custom scripts defined in pyproject.toml with descriptions from docstrings"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        print("❌ No pyproject.toml found in current directory")
        sys.exit(1)
    
    try:
        with open(pyproject_path, 'r') as f:
            content = f.read()
        
        # Parse TOML
        config = toml.loads(content)
        scripts = config.get('project', {}).get('scripts', {})
        
        if not scripts:
            print("📝 No custom scripts defined in pyproject.toml")
            return
        
        print("🚀 Available Custom Scripts:")
        print("=" * 50)
        
        # Group scripts by category
        dev_scripts = []
        doc_scripts = []
        other_scripts = []
        
        for script_name, script_path in scripts.items():
            desc = get_script_description(script_name, script_path)
            
            if script_name.startswith('docs-'):
                doc_scripts.append((script_name, desc))
            elif script_name in ['dev-setup']:
                dev_scripts.append((script_name, desc))
            else:
                other_scripts.append((script_name, desc))
        
        # Display grouped scripts
        if dev_scripts:
            print("\n📦 Development Environment:")
            for name, desc in dev_scripts:
                print(f"  uv run {name:<15} {desc}")
        
        if doc_scripts:
            print("\n📚 Documentation Scripts:")
            for name, desc in doc_scripts:
                print(f"  uv run {name:<15} {desc}")
        
        if other_scripts:
            print("\n🔧 Other Scripts:")
            for name, desc in other_scripts:
                print(f"  uv run {name:<15} {desc}")
        
        print(f"\n💡 Total: {len(scripts)} custom scripts available")
        
    except Exception as e:
        print(f"❌ Error reading pyproject.toml: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()