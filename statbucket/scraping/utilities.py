import pandas as pd
from bs4 import Tag, BeautifulSoup


def parse_table_BR(contents: str | Tag) -> pd.DataFrame:
    """Get a DataFrame from an HTML table but preserve the a tags."""

    contents_str = str(contents)
    soup = BeautifulSoup(contents_str, "html.parser")

    for link in soup.find_all("a"):
        href = link.get("href", "")
        text = link.get_text()
        # Replace link with a formatted string that preserves both text and href
        link.replace_with(f"LINK[{text}|{href}]")

    df = pd.read_html(str(soup))[0]

    return df