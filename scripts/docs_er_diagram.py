"""
Generate Mermaid ER diagram from SQLAlchemy models viewable in markdown preview
"""

import sys
from pathlib import Path
import argparse

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from nba_scraper.database_schema import Base

def main():
    """Generate Mermaid ER diagram"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    print("Generating Mermaid ER diagram...")
    
    output = ["```mermaid", "erDiagram"]
    
    # Generate entities and relationships
    for table_name, table in Base.metadata.tables.items():
        entity_name = table_name.upper()
        
        # Add entity with columns
        output.append(f"    {entity_name} {{")
        for column in table.columns:
            col_type = "string"
            if "int" in str(column.type).lower():
                col_type = "int"
            elif "date" in str(column.type).lower():
                col_type = "date"
            
            pk = " PK" if column.primary_key else ""
            fk = " FK" if column.foreign_keys else ""
            output.append(f"        {col_type} {column.name}{pk}{fk}")
        output.append("    }")
        output.append("")
    
    # Add relationships
    for table_name, table in Base.metadata.tables.items():
        for fk in table.foreign_keys:
            ref_table = fk.column.table.name
            output.append(f"    {ref_table.upper()} ||--o{{ {table_name.upper()} : \"references\"")
    
    output.append("```")
    
    # Write to file
    Path("docs/database.md").write_text("\n".join(output))
    print("Mermaid ER diagram saved to: database.md")
    print("It is viewable in the markdown preview")

if __name__ == "__main__":
    main()