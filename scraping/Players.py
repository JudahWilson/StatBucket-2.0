from typing import Any
from scraping.base import BaseScraper
from scraping.utilities import parse_table_BR
from playwright.sync_api import Page



class Players(BaseScraper):
    def __init__(
        self,
        range_start: Any = None,
        range_end: Any = None,
        override_html_cache: bool = False,
        webpage: Page | None = None,
    ):
        super().__init__(
            table_name="players",
            sid_column="br_id",
            range_start=range_start,
            range_end=range_end,
            override_html_cache=override_html_cache,
            webpage=webpage,
        )
        self._letters = [chr(i) for i in range(ord("a"), ord("z") + 1)]
        """Letters a-z except x"""
        self._letters.pop(self._letters.index("x"))  # No players with last names starting with X

    def _scrape(self, sid: str | None = None):
        first_letter = "a" if sid is None else sid[0].lower()
        start_index = self._letters.index(first_letter)
        for letter in self._letters[start_index:]:
            players_soup = self._html(f"players/{letter}/", "table#players")
            players_df = parse_table_BR(players_soup)
            breakpoint()


    
    def _persist(self):
        pass
    