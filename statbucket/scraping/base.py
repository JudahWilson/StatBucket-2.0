"""Base scraper class that all scrapers should inherit from."""
# TODO define handy "get last processed html/staged/persisted data?"
from statbucket.scraping.utils import html_cache_path
from abc import ABC, abstractmethod
import pandas as pd
from database import engine, engine_staged


class BaseScraper(ABC):
    def __init__(self, base_url: str, table_name: str):
        """Each BaseScraper instance represents a scraper for a DB table.

        Args:
            base_url (str): The common part of the url for all pages being
                scraped for this class
            table_name (str): The database table name where it is being saved
        """
        self._base_url = base_url
        # TODO validate table name
        self._table_name = table_name
        self._df: pd.DataFrame = pd.DataFrame()

    def _cache_html(self, html_content: str, url_slug: str):
        """Cache the html content. The file saving the contents

        Args:
            html_content (str): The HTML content
            url_slug (str): The part of the url that comes after self.base_url
        """
        with open(html_cache_path(url_slug), "+a") as f:
            f.write(html_content)

    def _stage_row(self, data: dict | pd.DataFrame, replace_filter: str = ''):
        """Save row of data into the staging database.

        Args:
            data (dict | pd.DataFrame): The data of the row being staged
            replace_filter (str, optional): If this is replacing a staged row,
                this is the valid where expression (excluding "where"). Defaults
                to ''.
        """
        # Remove existing row if replace_filter is provided
        if replace_filter:
            with engine_staged.connect() as conn:
                conn.execute(f"DELETE FROM {self._table_name} WHERE {replace_filter}")
                conn.commit()
        
        pd.DataFrame(data).to_sql(self._table_name, engine_staged)

    def clear_staged(self, filter: str = ''):
        """Remove the data from this class' table from the staged DB
        
        Args:
            filter (str, optional): SQL valid where expression (not including
                where)
        """
        with engine_staged.connect() as conn:
            conn.execute(f"DELETE FROM {self._table_name}{(' WHERE ' + filter) if filter else ''}")
            conn.commit()
    
    def df(self, sql_filter: str | None = None, force_refresh: bool = False):
        """Get a DataFrame of the data this class has scraped

        Args:
            sql_filter (string): SQL valid where expression (not including 
                where)
            force_refresh (bool, optional): Force the data to be pulled again 
                even if it was already acquired
        """ 
        # Check if content in DB
        if self._df.empty or force_refresh:
            self._df = pd.read_sql(
                f"select * from {self._table_name}{('where ' + sql_filter) if sql_filter else ''}",
                engine,
            )
        return self._df

    @abstractmethod
    def _download_page(self, url_slug: str) -> str:
        """Download one page's worth of content. **PLEASE** cache the results 
        with self._cache_html

        Args:
            url_slug (str): the unique part of the url after self.base_url
        """
        pass

    @abstractmethod
    def download(self) -> str:
        """Top level function to download all content in all pages needed for
        this class"""
        pass

    @abstractmethod
    def parse(self) -> dict:
        """Top level parsing function. **PLEASE** add data to staging DB with
        self._stage_row"""
        pass

    def persist(self):
        """Persist the staged data into the production database"""
        with engine_staged.connect() as staged_conn:
            staged_data = pd.read_sql(f"SELECT * FROM {self._table_name}", staged_conn)
        
        with engine.connect() as prod_conn:
            staged_data.to_sql(self._table_name, prod_conn, if_exists='append', index=False)
            prod_conn.commit()

        with engine_staged.connect() as staged_conn:
            staged_conn.execute(f"DELETE FROM {self._table_name}")
            staged_conn.commit()