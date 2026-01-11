import pandas as pd

def _parse_table_BR(contents: str) -> pd.DataFrame:
    """Get a DataFrame from an HTML table but preserve the a tags."""
    contents = contents.replace('<a ', '***a ').replace('</a>', '***/a>')
    df = pd.read_html(contents)[0]