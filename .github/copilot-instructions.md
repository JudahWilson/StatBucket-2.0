<general-instructions>
- For every file you update, ask me for permission while describing what you are accomplishing and why briefly
- you are crazy. you do too much without asking me. please ask me for permission before writing code.
- you do way more than i ask. if you have ambiguities on how to perfectly accomplish my goal, please ask me more information
- all solutions should be proportional to the problem at hand and not any more crazy.
- Every command you run (besides posix bash commands) give me a summary of what they are doing.
- Don't assume anything without asking me.
- Before everytime you write code that implements a design decision you or I haven't mentioned to each other, first explain to me and ask my permission.
- Don't every use dummy data to fill in for code. please use TODO comments to indicate intervation in my part is needed.
- Be straightforward with meeting the goals. don't add too much that isn't relavent to the overall goal.
- utilize uv scripts and add docstrings when creating a new script
  - keep a 1-1 relationship between uv scripts in the toml and the python files. If they share logic they are allowed to store it in a common folder of an appropriate name. make sure the file has a docstring at the top
  - all scripts must utilize the below code in the main function to automatically support the -h and --help flags
  ```python
  parser = argparse.ArgumentParser(description=__doc__)
  args = parser.parse_args()
  ```
- Don't reinvent the wheel everytime you code. Understand the project and use existing code where appropriate.
- no emojis
- Communicate Efficiently and straitforward. No extra words that emulate emotional response. Just strait facts and info like a robot.
</general-instructions>
<documentation-instructions>
1. Document all Python packages/modules not in .gitignore via Sphinx
2. Exclude common patterns: __pycache__, *.pyc, .venv, etc.
3. Include docstrings for all public classes, functions, and methods
4. Generate module-level documentation with package structure
5. Each documented thing should allow an option to click to view it's source code
6. For now only doc the statbucket folder
7. Don't worry about private methods starting with _
8. Set up tests in docstrings so they can be validated using doctests
</documentation-instructions>
