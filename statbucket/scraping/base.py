"""Base scraper class that all scrapers should inherit from."""

import os
import time
from typing import Any
from bs4 import BeautifulSoup
import requests
from abc import ABC, abstractmethod
import pandas as pd
from database import engine, engine_staged
from sqlalchemy import text


def html_cache_path(url: str):
    """Get the path of the cached HTML file for a given URL.

    Args:
        url (str): URL of the HTML content's source.

    Raises:
        NotImplementedError: Needs to be implemented.
    """
    raise NotImplementedError


def is_html_cached(url: str) -> bool:
    """Check if the HTML content for the given URL is already cached.

    Args:
        url (str): URL of the HTML content's source.
    """
    if os.path.exists(html_cache_path(url)):
        return True
    return False


def get_soup(url, selector: str | None = None) -> BeautifulSoup:
    """
    Get a BeautifulSoup object from a url

    Args:
        url (str): the url to get the soup from

    Raises:
        Exception: A failure to get the data from the url

    Returns:
        BeautifulSoup: the soup object
    """
    print(url)
    response = requests.get(url)
    time.sleep(WEBSCRAPE_DEBOUNCER)
    if response.status_code < 200 or response.status_code > 299:
        raise Exception(
            f"Error getting data from {url}. Status " + str(response.status_code)
        )

    if selector:
        return BeautifulSoup(response.text, "html.parser").select_one(selector)
    else:
        return BeautifulSoup(response.text, "html.parser")


class BaseScraper(pd.DataFrame, ABC):
    """A base class for all data being scraped that inherits from DataFrame.

    the @abstractmethod decorated functions are the user-implemented functions

    Any '_' prefixed functions are helper functions for internal use.

    Any '__' prefixed functions shouldn't be called by a UD function.

    The remaining functions and properties are user-accessible for interacting
    with the scraper.

    Args:
        ABC (_type_): _description_
    """

    def __init__(
        self,
        table_name: str,
        sid_column: str,
        range_start: Any = None,
        range_end: Any = None,
        override_html_cache: bool = False,
    ):
        """Each BaseScraper instance represents a scraper for a DB table.

        Args:
            table_name (str): The database table name where it is being saved
            sid_column (str): The column name that uniquely identifies a row
            range_start (Any, optional): The starting point of the range of
                pages to be scraped. Defaults to None.
            range_end (Any, optional): The ending point of the range of pages
                to be scraped. Defaults to None.
        """
        # Initialize as empty DataFrame
        super().__init__()
        
        # Store scraper-specific attributes
        self._table_name = table_name
        self._sid_column = sid_column
        self._range_start = range_start
        self._range_end = range_end
        self._override_html_cache = override_html_cache

    ######################################
    #region PUBLIC FUNCTIONS
    ######################################
    def run(self):
        """Run the scraper end-to-end: get HTML, parse HTML, stage data,
        and persist data."""
        self._get_html()
        self._extract_data_from_html()
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

    def refresh_dataframe(
        self, sql_filter: str | None = None
    ) -> None:
        """Refresh the DataFrame with data from the database

        Args:
            sql_filter (string): SQL valid where expression (not including
                where)
        """
        new_data = pd.read_sql(
            f"select * from {self._table_name}{('where ' + sql_filter) if sql_filter else ''}",
            engine,
        )
        
        # Clear existing data and update with new data
        self.drop(self.index, inplace=True)
        
        if not new_data.empty:
            # Update the DataFrame's data, index, and columns in place
            for col in new_data.columns:
                self[col] = new_data[col].values
            self.index = new_data.index

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

    # endregion

    #####################################
    #region UD FUNCTIONS
    #####################################
    @abstractmethod
    def _get_html(self):
        """Get all html needed for all data rows. **PLEASE** use self._save_html to save the HTML
        content."""
        pass

    @abstractmethod
    def _extract_data_from_html(self):
        """Extract data rows from the HTML. **PLEASE** do the following:

        1. Determine where we left off using self._get_latest_staged_sid()
        2. use self._stage_rows or self._stage_row for all extracted data =
        """
        pass

    # endregion

    #####################################
    #region INTERNAL UTILITIES
    #####################################
    def _save_html(self, url: str, selector: str | None = None):
        """Use this function to save any HTML in self._get_html

        Args:
            url (str): URL slug of content
            selector (str | None): The HTML selector
        """
        if not is_html_cached(url) or self._override_html_cache:
            html_content = get_soup(url)
            if selector:
                html_content = str(html_content.select_one(selector))
            else:
                html_content = str(html_content)
            with open(html_cache_path(url), "+a") as f:
                f.write(html_content)

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
        self.__delete_rows(
            SIDs=sid_values,
            engine=engine_staged
        )

        # Save new row
        data.to_sql(self._table_name, engine_staged, if_exists="append", index=False)

    def _persist(self):
        """Persist the staged data into the production database"""
        with engine_staged.connect() as staged_conn:
            staged_data = pd.read_sql(f"SELECT * FROM {self._table_name}", staged_conn)

        # Remove any rows in production DB that are being replaced by staged data
        staged_sids = staged_data[self._sid_column].tolist()
        self.__delete_rows(
            SIDs=staged_sids,
            engine=engine
        )
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

    def _get_latest_sid(self, staged: bool = True) -> Any:
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
            result = conn.execute(
                text(
                    f"SELECT MAX({self._sid_column}) as latest_sid FROM {self._table_name}"
                    + (
                        f" WHERE {self._sid_column} {range_filter}"
                        if range_filter
                        else ""
                    )
                )
            ).fetchone()
            return (
                result["latest_sid"]
                if result and result["latest_sid"] is not None
                else None
            )

    # endregion


    ######################################
    #region "UNDER THE HOOD" UTILITIES
    ######################################
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


WEBSCRAPE_DEBOUNCER = 4
"""Time in seconds to wait between web requests to avoid error 429 (Too Many Requests)."""
