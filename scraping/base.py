"""Base scraper class that all scrapers should inherit from."""

import os
import time
from typing import Any
from bs4 import BeautifulSoup, Tag
from abc import ABC, abstractmethod
import pandas as pd
# from mother import engine, engine_staged
import warnings
from playwright.sync_api import Page

# Ignores any UserWarning that starts with this text
warnings.filterwarnings(
    "ignore",
    message="Pandas doesn't allow columns to be created via a new attribute name",
)


class BaseScraperInternals(pd.DataFrame):
    """A base for the internal functionality of all scrapers."""

    BASE_URL = "https://www.basketball-reference.com"
    """The base URL for Basketball Reference."""
    WEBSCRAPE_DEBOUNCER = 4
    """Time in seconds to wait between web requests to avoid error 429 (Too Many Requests)."""

    def __init__(
        self,
        table_name: str,
        sid_column: str,
        range_start: Any = None,
        range_end: Any = None,
        override_html_cache: bool = False,
        webpage: Page | None = None,
    ):
        """Each BaseScraper instance represents a scraper for a DB table.

        Args:
            table_name (str): The database table name where it is being saved
            sid_column (str): The column name that uniquely identifies a row
            range_start (Any, optional): The starting point of the range of
                pages to be scraped. Defaults to None.
            range_end (Any, optional): The ending point of the range of pages
                to be scraped. Defaults to None.
            override_html_cache (bool, optional): Whether to override the HTML
                cache when getting HTML content. Defaults to False.
        """
        # Initialize as empty DataFrame
        super().__init__()

        # Store scraper-specific attributes
        self._table_name = table_name
        self._sid_column = sid_column
        self._range_start = range_start
        self._range_end = range_end
        self._override_html_cache = override_html_cache
        self._webpage = webpage

    @classmethod
    def _html_cache_path(cls, slug: str):
        """Get the path of the cached HTML file for a given URL.

        Args:
            slug (str): The path of the website the HTML content is from.
        """
        cache_dir = "html_cache"
        slug_path = slug.replace("/", "\\")
        if slug_path.endswith("\\"):
            slug_path = slug_path[:-1]
        cache_path= os.path.join(cache_dir, slug_path)
        cache_folder = os.path.dirname(cache_path)
        if not os.path.exists(cache_folder):
            os.makedirs(cache_folder,exist_ok=True)
        return cache_path + ".html"

    @classmethod
    def _is_html_cached(cls, slug: str) -> bool:
        """Check if the HTML content for the given URL is already cached.

        Args:
            slug (str): The path of the website the HTML content is from.
        """
        if os.path.exists(cls._html_cache_path(slug)):
            return True
        return False

    def _html(self, slug: str, selector: str | None = None) -> Tag | None:
        """Use this function to save any HTML in self._get_html

        Args:
            slug (str): URL slug of content
            selector (str | None): The HTML selector
        """
        soup = None
        if slug.startswith("/"):
            slug = slug[1:]
        if not self._is_html_cached(slug) or self._override_html_cache:
            # Get soup from web and cache it
            print(f"{self.BASE_URL}/{slug}")
            assert self._webpage is not None, (
                "Webpage must be provided to the scraper class to scrape HTML"
            )
            response = self._webpage.goto(f"{self.BASE_URL}/{slug}")
            time.sleep(self.WEBSCRAPE_DEBOUNCER)
            if response.status < 200 or response.status > 299:
                raise Exception(
                    f"Error getting data from {slug}. Status "
                    + str(response.status)
                )

            if selector:
                soup = BeautifulSoup(response.text(), "html.parser").select_one(selector)
            else:
                soup = BeautifulSoup(response.text(), "html.parser")
            with open(self._html_cache_path(slug), "w", encoding="utf-8") as f:
                f.write(str(soup))
        else:
            # Load soup from cache
            with open(self._html_cache_path(slug), "r", encoding="utf-8") as f:
                soup = BeautifulSoup(f.read(), "html.parser")
                if selector:
                    soup = soup.select_one(selector)
        return soup

    def _stage_rows(self, data: dict | pd.DataFrame | list[dict]):
        """Save rows of data into the staging database.

        Args:
            data (dict | pd.DataFrame): The data of the row being staged
        """
        # Convert to DataFrame if dict
        if isinstance(data, dict):
            data = pd.DataFrame([data])
        elif isinstance(data, list):
            data = pd.DataFrame(data)

        # Remove existing rows of the staged SIDs
        sid_values = data[self._sid_column].tolist()
        self.__delete_rows(SIDs=sid_values, engine=engine_staged)

        # Save new row
        data.to_sql(self._table_name, engine_staged, if_exists="append", index=False)

    def _get_latest_sid(self, staged: bool = True) -> Any | None:
        """Get the latest SID from either the staging or production database.
        The result must be in the range defined by range_start and range_end.

        Args:
            staged (bool, optional): Whether to get from the staging database.
                Defaults to True.
        """
        range_filter = ""
        if self._range_start and self._range_end:
            range_filter = f"BETWEEN '{self._range_start}' AND '{self._range_end}'"
        elif self._range_start:
            range_filter = f">= '{self._range_start}'"
        elif self._range_end:
            range_filter = f"<= '{self._range_end}'"

        engine_to_use = engine_staged if staged else engine
        with engine_to_use.connect() as conn:
            result = (
                conn.execute(
                    text(
                        f"SELECT MAX({self._sid_column}) as latest_sid FROM {self._table_name}"
                        + (
                            f" WHERE {self._sid_column} {range_filter}"
                            if range_filter
                            else ""
                        )
                    )
                )
                .mappings()
                .fetchone()
            )
            return (
                result["latest_sid"]
                if result and result["latest_sid"] is not None
                else None
            )

    def __delete_rows(self, SIDs: list[str], engine):
        """Delete multiple rows from the staging database.

        Args:
            SIDs (list[str]): The SIDs of the rows being deleted
            engine: The database engine to use for the deletion
        """
        with engine.connect() as conn:
            conn.execute(
                text(
                    f"DELETE FROM {self._table_name} WHERE {self._sid_column} IN :sids"
                ),
                {"sids": tuple(SIDs)},
            )
            conn.commit()


class BaseScraperInterface(ABC, BaseScraperInternals):
    """The interface that must be implemented by all scraper classes."""

    @abstractmethod
    def _scrape(self, sid: Any = None):
        """Extract data rows from the HTML. **PLEASE** do the following:

        1. Determine where we left off using the parameter `sid`
        2. use _html to get any HTML needed
        3. use self._stage_rows or self._stage_row for all extracted data
        """
        pass

    @abstractmethod
    def _persist(self):
        """Persist the staged data into the production database"""
        with engine_staged.connect() as staged_conn:
            staged_data = pd.read_sql(f"SELECT * FROM {self._table_name}", staged_conn)

        # Remove any rows in production DB that are being replaced by staged data
        staged_sids = staged_data[self._sid_column].tolist()
        self.__delete_rows(SIDs=staged_sids, engine=engine)
        with engine.connect() as prod_conn:
            prod_conn.execute(
                text(
                    f"DELETE FROM {self._table_name} WHERE {self._sid_column} IN :sids"
                ),
                {"sids": tuple(staged_sids)},
            )
            prod_conn.commit()

        # Insert staged data into production DB
        with engine.connect() as prod_conn:
            staged_data.to_sql(
                self._table_name, prod_conn, if_exists="append", index=False
            )
            prod_conn.commit()

        # Clear staged data
        self.clear_staged()


class BaseScraper(BaseScraperInterface):
    """The base scraper class. The interface methods and internal methods are
    inherited. This also extends DataFrame"""

    def run(self):
        """Run the scraper end-to-end: get HTML, parse HTML, stage data,
        and persist data."""
        latest_sid = self._get_latest_sid()
        self._scrape(latest_sid)
        self._persist()

    def clear_staged(self, filter: str = ""):
        """Remove the data from this class' table from the staged DB

        Args:
            filter (str, optional): SQL valid where expression (not including
                "where")
        """
        with engine_staged.connect() as conn:
            conn.execute(
                text(
                    f"DELETE FROM {self._table_name}{(' WHERE ' + filter) if filter else ''}"
                )
            )
            conn.commit()

    def load(self, sql_filter: str | None = None, staged: bool = False) -> None:
        """Load the DataFrame with data from the database within
        self._range_start and self._range_end, if set.

        Args:
            sql_filter (string): SQL valid where expression (not including
                where)
            staged (bool, optional): Whether to load from the staging database.
                Defaults to False.
        """
        sql = f"select * from {self._table_name}"
        if sql_filter or self._range_start or self._range_end:
            where_clauses = []
            if sql_filter:
                where_clauses.append(sql_filter)
            if self._range_start and self._range_end:
                where_clauses.append(
                    f"{self._sid_column} BETWEEN '{self._range_start}' AND '{self._range_end}'"
                )
            elif self._range_start:
                where_clauses.append(f"{self._sid_column} >= '{self._range_start}'")
            elif self._range_end:
                where_clauses.append(f"{self._sid_column} <= '{self._range_end}'")
            sql += " WHERE " + " AND ".join(where_clauses)
        new_data = pd.read_sql(
            sql,
            engine_staged if staged else engine,
        )

        # Clear existing data and update with new data
        self.drop(self.index, inplace=True)

        if not new_data.empty:
            # Update the DataFrame's data, index, and columns in place
            for col in new_data.columns:
                self[col] = new_data[col].values
            self.index = new_data.index

    def staged(self) -> pd.DataFrame:
        """Get the staged data as a DataFrame.

        Returns:
            pd.DataFrame: The staged data
        """
        with engine_staged.connect() as conn:
            staged_data = pd.read_sql(f"SELECT * FROM {self._table_name}", conn)
        return staged_data

    def get_latest_staged_sid(self) -> Any:
        """Get the latest staged SID from the staging database.

        Returns:
            Any: The latest staged SID
        """
        return self._get_latest_sid(staged=True)

    def get_latest_production_sid(self) -> Any:
        """Get the latest production SID from the production database.

        Returns:
            Any: The latest production SID
        """
        return self._get_latest_sid(staged=False)
