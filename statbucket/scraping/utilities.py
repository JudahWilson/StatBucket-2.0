import pandas as pd
from bs4 import Tag

def parse_table_BR(contents: str | Tag) -> pd.DataFrame:
    """Get a DataFrame from an HTML table but preserve the a tags."""
    contents = str(contents)
    contents = contents.replace('<a ', '***a ').replace('</a>', '***/a>')
    df = pd.read_html(contents)[0]
    df = df.replace('***a ', '<a ').replace('***/a>', '</a>', regex=True)
    return df