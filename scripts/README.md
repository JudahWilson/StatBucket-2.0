- keep a 1-1 relationship between uv scripts in the toml and the python files. - shared logic can be stored in the utils folder
- document the top of the python file with a docstring
- all scripts must utilize the below code in the main function to automatically support the -h and --help flags
```python
parser = argparse.ArgumentParser(description=__doc__)
args = parser.parse_args()
```