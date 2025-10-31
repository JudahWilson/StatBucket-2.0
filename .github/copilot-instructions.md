# Instructions

- you are crazy. you do too much without asking me. please ask me for permission before writing code.
- you do way more than i ask. if you have ambiguities on how to perfectly accomplish my goal, please ask me more information
- all solutions should be proportional to the project at hand and not any more crazy.
- Every command you run (besides posix bash commands) give me a summary of what they are doing.
- Don't assume anything without asking me.
- Everytime you write code that implements a design decision you or I haven't mentioned to each other, first explain to me and ask my permission.
- Don't every use dummy data to fill in for code. please use TODO comments to indicate intervation in my part is needed.
- Be straightforward with meeting the goals. don't add too much that isn't relavent to the overall goal.
- utilize uv scripts and add docstrings when creating a new script
  - keep a 1-1 relationship between uv scripts in the toml and the python files. If they share logic they are allowed to store it in a common folder of an appropriate name. make sure the file has a docstring at the top
  - all scripts must utilize the below in the main function to automatically support the -h and --help flags
  ```python
  parser = argparse.ArgumentParser(description=__doc__)
  args = parser.parse_args()
  ```
- Don't reinvent the wheel everytime you code. Understand the project and use existing code where appropriate.
- no emojis

# Overall requirements

## Web Scraping Requirements

- Scrape HTML from basketball-reference.com
- Handle relationships between datasets (stat lines → players, games → seasons, etc.)
- Focus on building comprehensive scraping solutions rather than individual session limits
- Build towards a complete NBA historical dataset for analysis

## Dynamic Schema Management

- Add database columns dynamically as new data fields are discovered
- Apply Python functions to back-update existing data when reformatting is needed
- Maintain migration history for schema changes
- Use standard SQL with dynamic schema updates
- Implement automated migrations via ORM when possible

## Error Handling & Monitoring

- Stop scraping when columns are added/removed unexpectedly
- Record exact column changes for feedback system
- Allow manual review and schema adjustment after automated detection
- Provide alerts for schema mismatches requiring attention

## Special Column Processing

- Handle regex expressions to parse numbers and URL components
- Extract href URLs and link text into separate columns from single <td> elements
- Support one-to-many column mapping (1 <td> → 2+ columns or related tables)
- Default to text extraction with opt-in custom column handlers by name

## Database Architecture

- Choose optimal database for storing relationships between datasets
- Support relational data structure for comprehensive NBA data
- Handle complex entity relationships efficiently

## Modular Data Processing

- Separate functions for each dataset type
- Call dataset functions independently/on-demand
- Split HTML download logic from data extraction logic per dataset
- Download HTML dynamically if not present
- Save to intermediate test tables by default
- Option to save directly to destination tables when specified
- Pick up from where the last scrape left off per what is in the database already
