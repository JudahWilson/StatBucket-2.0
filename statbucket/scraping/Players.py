from typing import Any
from statbucket.scraping.base import BaseScraper, get_soup


class Players(BaseScraper):
    def __init__(
        self,
        range_start: Any = None,
        range_end: Any = None,
        override_html_cache: bool = False,
    ):
        super().__init__(
            table_name="players",
            sid_column="br_id",
            range_start=range_start,
            range_end=range_end,
            override_html_cache=override_html_cache,
        )
        self.letters = [chr(i) for i in range(ord("a"), ord("z") + 1)]
        """Letters a-z except x"""
        self.letters.pop(self.letters.index("x"))  # No players with last names starting with X

    def _scrape(self, sid: str | None = None):
        first_letter = "a" if sid is None else sid[0].lower()
        start_index = self.letters.index(first_letter)
        for letter in self.letters[start_index:]:
            players_rows = self._html(f"players/{letter}/", "tbody > tr:not(.thead)")
            for row in players_rows:
                

    
    def _persist(self):
        pass
    