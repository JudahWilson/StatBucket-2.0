from typing import Any
from statbucket.scraping.base import BaseScraper


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

