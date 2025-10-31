"""
NBA Data Scraping Framework - Core Classes

Provides base classes for HTML downloading, data extraction, column handling,
and database operations with retry logic, rate limiting, and error handling.
"""

import time
import re
import logging
import requests
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Callable, Union
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass
from bs4 import BeautifulSoup, Tag
import json
from datetime import datetime
import hashlib
import os

# Conditional import for SQLAlchemy (will be available when needed)
try:
    from sqlalchemy.orm import Session
except ImportError:
    Session = Any  # Type placeholder

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ColumnHandler:
    """Configuration for handling specific table columns"""
    name: str
    target_columns: List[str]  # Column names to populate
    extractor: Callable[[Tag], Dict[str, Any]]  # Function to extract data from cell
    validator: Optional[Callable[[Any], bool]] = None  # Validation function
    description: str = ""
    
class RateLimiter:
    """Rate limiting for web requests"""
    def __init__(self, requests_per_second: float = 0.25, burst_size: int = 1):
        self.requests_per_second = requests_per_second
        self.burst_size = burst_size
        self.tokens = burst_size
        self.last_refill = time.time()
    
    def wait_if_needed(self):
        """Wait if rate limit would be exceeded"""
        now = time.time()
        elapsed = now - self.last_refill
        
        # Refill tokens based on elapsed time
        self.tokens = min(self.burst_size, 
                         self.tokens + elapsed * self.requests_per_second)
        self.last_refill = now
        
        if self.tokens < 1:
            sleep_time = (1 - self.tokens) / self.requests_per_second
            logger.info(f"Rate limiting: sleeping {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            self.tokens = 0
        else:
            self.tokens -= 1

class HTMLDownloader:
    """Handle HTML downloading with caching, retries, and rate limiting"""
    
    def __init__(self, 
                 cache_dir: str = "html_cache",
                 rate_limiter: Optional[RateLimiter] = None,
                 max_retries: int = 3,
                 retry_delay: float = 1.0,
                 timeout: int = 30,
                 user_agent: Optional[str] = None):
        self.cache_dir = cache_dir
        self.rate_limiter = rate_limiter or RateLimiter(requests_per_second=0.25)  # 1 request every 4 seconds
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        
        # Create cache directory
        os.makedirs(cache_dir, exist_ok=True)
        
        # Session for connection reuse
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def _get_cache_path(self, url: str) -> str:
        """Generate cache file path for URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{url_hash}.html")
    
    def _load_from_cache(self, url: str) -> Optional[str]:
        """Load HTML from cache if available and fresh"""
        cache_path = self._get_cache_path(url)
        if os.path.exists(cache_path):
            # Check if cache is less than 24 hours old for dynamic data
            cache_age = time.time() - os.path.getmtime(cache_path)
            if cache_age < 86400:  # 24 hours
                logger.info(f"Loading from cache: {url}")
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return f.read()
        return None
    
    def _save_to_cache(self, url: str, html: str):
        """Save HTML to cache"""
        cache_path = self._get_cache_path(url)
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                f.write(html)
            logger.debug(f"Cached HTML for: {url}")
        except Exception as e:
            logger.warning(f"Failed to cache HTML: {e}")
    
    def download(self, url: str, use_cache: bool = True, force_download: bool = False) -> str:
        """
        Download HTML with caching, retries, and rate limiting
        
        Args:
            url: URL to download
            use_cache: Whether to use cached version if available
            force_download: Force download even if cache exists
            
        Returns:
            HTML content as string
            
        Raises:
            Exception: If download fails after all retries
        """
        logger.info(f"Downloading: {url}")
        
        # Try cache first (unless forcing download)
        if use_cache and not force_download:
            cached_html = self._load_from_cache(url)
            if cached_html:
                return cached_html
        
        # Download with retries
        for attempt in range(self.max_retries + 1):
            try:
                # Rate limiting
                self.rate_limiter.wait_if_needed()
                
                # Make request
                logger.debug(f"HTTP request attempt {attempt + 1}/{self.max_retries + 1}: {url}")
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                html = response.text
                
                # Cache the result
                if use_cache:
                    self._save_to_cache(url, html)
                
                logger.info(f"Successfully downloaded: {url}")
                return html
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                
                if attempt < self.max_retries:
                    sleep_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"All download attempts failed for: {url}")
                    raise Exception(f"Failed to download {url} after {self.max_retries + 1} attempts: {e}")

class DataExtractor:
    """Extract structured data from HTML tables"""
    
    def __init__(self, column_handlers: Optional[Dict[str, ColumnHandler]] = None):
        self.column_handlers = column_handlers or {}
        self.default_extractors = self._setup_default_extractors()
    
    def _setup_default_extractors(self) -> Dict[str, Callable]:
        """Setup default data extractors for common patterns"""
        return {
            'text': lambda cell: {'value': cell.get_text(strip=True)},
            'number': lambda cell: self._extract_number(cell),
            'percentage': lambda cell: self._extract_percentage(cell),
            'url_and_text': lambda cell: self._extract_url_and_text(cell),
            'player_link': lambda cell: self._extract_player_link(cell),
            'team_link': lambda cell: self._extract_team_link(cell),
        }
    
    def _extract_number(self, cell: Tag) -> Dict[str, Any]:
        """Extract numeric value from cell"""
        text = cell.get_text(strip=True)
        
        # Handle common basketball-reference formatting
        if text in ['', '-', 'N/A']:
            return {'value': None}
        
        # Remove commas and extract number
        cleaned = re.sub(r'[,\s]', '', text)
        
        # Try integer first, then float
        try:
            if '.' in cleaned:
                return {'value': float(cleaned)}
            else:
                return {'value': int(cleaned)}
        except ValueError:
            logger.warning(f"Could not parse number: {text}")
            return {'value': None, 'raw_text': text}
    
    def _extract_percentage(self, cell: Tag) -> Dict[str, Any]:
        """Extract percentage value from cell"""
        text = cell.get_text(strip=True)
        
        if text in ['', '-', 'N/A']:
            return {'value': None}
        
        # Remove % sign and convert to decimal
        cleaned = text.replace('%', '').strip()
        try:
            return {'value': float(cleaned) / 100.0 if '%' in text else float(cleaned)}
        except ValueError:
            logger.warning(f"Could not parse percentage: {text}")
            return {'value': None, 'raw_text': text}
    
    def _extract_url_and_text(self, cell: Tag) -> Dict[str, Any]:
        """Extract both URL and display text from cell with link"""
        link = cell.find('a')
        if link:
            href = str(link.get('href', '') or '')
            return {
                'url': href,
                'text': link.get_text(strip=True),
                'full_url': urljoin('https://www.basketball-reference.com', href) if href else ''
            }
        else:
            text = cell.get_text(strip=True)
            return {'url': None, 'text': text, 'full_url': None}
    
    def _extract_player_link(self, cell: Tag) -> Dict[str, Any]:
        """Extract player information from cell with player link"""
        result = self._extract_url_and_text(cell)
        
        if result['url']:
            # Extract player ID from URL like '/players/j/jamesle01.html'
            match = re.search(r'/players/[a-z]/([^/]+)\.html', result['url'])
            if match:
                result['player_id'] = match.group(1)
        
        return result
    
    def _extract_team_link(self, cell: Tag) -> Dict[str, Any]:
        """Extract team information from cell with team link"""
        result = self._extract_url_and_text(cell)
        
        if result['url']:
            # Extract team abbreviation from URL like '/teams/BOS/2024.html'
            match = re.search(r'/teams/([A-Z]+)/', result['url'])
            if match:
                result['team_abbrev'] = match.group(1)
        
        return result
    
    def extract_table_data(self, 
                          soup: BeautifulSoup, 
                          table_id: Optional[str] = None,
                          table_class: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract data from HTML table
        
        Args:
            soup: BeautifulSoup object of the HTML
            table_id: ID of the table to extract
            table_class: Class of the table to extract
            
        Returns:
            List of dictionaries representing table rows
        """
        # Find table
        if table_id:
            table = soup.find('table', id=table_id)
        elif table_class:
            table = soup.find('table', class_=table_class)
        else:
            table = soup.find('table')
        
        if not table:
            logger.warning("No table found")
            return []
        
        # Extract headers
        headers = self._extract_headers(table)
        if not headers:
            logger.warning("No headers found in table")
            return []
        
        # Extract data rows
        rows = []
        tbody = table.find('tbody') or table
        
        for row in tbody.find_all('tr'):
            # Skip header rows
            if row.find('th') and not row.find('td'):
                continue
                
            row_data = self._extract_row_data(row, headers)
            if row_data:
                rows.append(row_data)
        
        logger.info(f"Extracted {len(rows)} rows with {len(headers)} columns")
        return rows
    
    def _extract_headers(self, table: Tag) -> List[str]:
        """Extract column headers from table"""
        headers = []
        
        # Look for headers in thead or first tr
        thead = table.find('thead')
        if thead:
            header_row = thead.find('tr')
        else:
            header_row = table.find('tr')
        
        if header_row:
            for cell in header_row.find_all(['th', 'td']):
                # Handle data-stat attribute (common in basketball-reference)
                header_name = cell.get('data-stat')
                if not header_name:
                    header_name = cell.get_text(strip=True).lower().replace(' ', '_')
                
                headers.append(header_name)
        
        return headers
    
    def _extract_row_data(self, row: Tag, headers: List[str]) -> Optional[Dict[str, Any]]:
        """Extract data from a single table row"""
        cells = row.find_all(['td', 'th'])
        
        if len(cells) == 0:
            return None
        
        row_data = {}
        
        for i, cell in enumerate(cells):
            if i >= len(headers):
                break
                
            header = headers[i]
            
            # Use custom column handler if available
            if header in self.column_handlers:
                handler = self.column_handlers[header]
                try:
                    extracted = handler.extractor(cell)
                    
                    # Validate if validator provided
                    if handler.validator and not handler.validator(extracted):
                        logger.warning(f"Validation failed for {header}: {extracted}")
                    
                    # Map to target columns
                    for target_col in handler.target_columns:
                        if target_col in extracted:
                            row_data[target_col] = extracted[target_col]
                        
                except Exception as e:
                    logger.error(f"Error processing column {header}: {e}")
                    row_data[header] = {'value': None, 'error': str(e)}
            else:
                # Use default extraction
                data_stat = cell.get('data-stat', header)
                
                # Try to determine data type and extract accordingly
                if self._is_numeric_cell(cell):
                    row_data[data_stat] = self._extract_number(cell)['value']
                elif self._has_link(cell):
                    link_data = self._extract_url_and_text(cell)
                    row_data[data_stat] = link_data['text']
                    row_data[f"{data_stat}_url"] = link_data['url']
                    row_data[f"{data_stat}_full_url"] = link_data['full_url']
                else:
                    row_data[data_stat] = cell.get_text(strip=True)
        
        return row_data if row_data else None
    
    def _is_numeric_cell(self, cell: Tag) -> bool:
        """Check if cell contains numeric data"""
        text = cell.get_text(strip=True)
        if not text or text in ['-', 'N/A']:
            return True  # Treat as potential numeric (null)
        
        # Remove common formatting
        cleaned = re.sub(r'[,\s%]', '', text)
        try:
            float(cleaned)
            return True
        except ValueError:
            return False
    
    def _has_link(self, cell: Tag) -> bool:
        """Check if cell contains a link"""
        return cell.find('a') is not None

class BaseScraper(ABC):
    """Abstract base class for NBA data scrapers"""
    
    def __init__(self, 
                 downloader: HTMLDownloader,
                 extractor: DataExtractor,
                 session_maker: Callable[[], Any]):
        self.downloader = downloader
        self.extractor = extractor
        self.session_maker = session_maker
        self.name = self.__class__.__name__
        
        # Setup logging for this scraper
        self.logger = logging.getLogger(f"{__name__}.{self.name}")
    
    @abstractmethod
    def get_urls_to_scrape(self, **kwargs) -> List[str]:
        """Return list of URLs to scrape for this dataset"""
        pass
    
    @abstractmethod
    def extract_data_from_html(self, html: str, url: str) -> List[Dict[str, Any]]:
        """Extract structured data from HTML"""
        pass
    
    @abstractmethod
    def save_to_database(self, data: List[Dict[str, Any]], session: Any, **kwargs) -> int:
        """Save extracted data to database"""
        pass
    
    def scrape_dataset(self, 
                      use_cache: bool = True, 
                      save_to_intermediate: bool = True,
                      save_to_destination: bool = False,
                      **kwargs) -> Dict[str, Any]:
        """
        Main method to scrape a complete dataset
        
        Args:
            use_cache: Whether to use cached HTML
            save_to_intermediate: Whether to save to intermediate table
            save_to_destination: Whether to save directly to destination tables
            **kwargs: Additional arguments passed to URL generation and saving
            
        Returns:
            Dictionary with scraping results
        """
        start_time = datetime.now()
        urls = self.get_urls_to_scrape(**kwargs)
        total_records = 0
        failed_urls = []
        
        self.logger.info(f"Starting scrape of {len(urls)} URLs for {self.name}")
        
        session = self.session_maker()
        
        try:
            for i, url in enumerate(urls, 1):
                self.logger.info(f"Processing URL {i}/{len(urls)}: {url}")
                
                try:
                    # Download HTML
                    html = self.downloader.download(url, use_cache=use_cache)
                    
                    # Extract data
                    data = self.extract_data_from_html(html, url)
                    
                    if data:
                        if save_to_intermediate:
                            records_saved = self._save_to_intermediate(data, url, session)
                            self.logger.info(f"Saved {records_saved} records to intermediate table")
                        
                        if save_to_destination:
                            records_saved = self.save_to_database(data, session, **kwargs)
                            self.logger.info(f"Saved {records_saved} records to destination tables")
                        
                        total_records += len(data)
                    else:
                        self.logger.warning(f"No data extracted from {url}")
                        
                except Exception as e:
                    self.logger.error(f"Failed to process {url}: {e}")
                    failed_urls.append((url, str(e)))
                    continue
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            self.logger.error(f"Scraping failed with error: {e}")
            raise
        finally:
            session.close()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        results = {
            'scraper': self.name,
            'start_time': start_time,
            'end_time': end_time,
            'duration_seconds': duration.total_seconds(),
            'urls_processed': len(urls) - len(failed_urls),
            'urls_failed': len(failed_urls),
            'failed_urls': failed_urls,
            'total_records': total_records,
            'success': len(failed_urls) == 0
        }
        
        self.logger.info(f"Scraping completed: {results}")
        return results
    
    def _save_to_intermediate(self, data: List[Dict[str, Any]], url: str, session: Any) -> int:
        """Save data to intermediate table"""
        from database_schema import IntermediateData, ScrapingJob
        import uuid
        
        # Create or get scraping job
        job_id = str(uuid.uuid4())
        job = ScrapingJob(
            job_id=job_id,
            dataset_name=self.name,
            url=url,
            status='completed',
            completed_at=datetime.now(),
            records_scraped=len(data)
        )
        session.add(job)
        
        # Save data
        for record in data:
            intermediate = IntermediateData(
                job_id=job_id,
                table_name=self.name.lower(),
                source_url=url,
                raw_data=record
            )
            session.add(intermediate)
        
        session.flush()
        return len(data)

# Common column handlers for basketball-reference.com
def create_basketball_reference_handlers() -> Dict[str, ColumnHandler]:
    """Create common column handlers for basketball-reference.com"""
    
    def extract_player_stats(cell: Tag) -> Dict[str, Any]:
        """Extract player name and ID from player cell"""
        link = cell.find('a')
        if link:
            player_name = link.get_text(strip=True)
            href = str(link.get('href', '') or '')
            # Extract player ID from URL like '/players/j/jamesle01.html'
            match = re.search(r'/players/[a-z]/([^/]+)\.html', href)
            player_id = match.group(1) if match else None
            
            return {
                'player_name': player_name,
                'player_br_id': player_id,
                'player_url': href
            }
        else:
            return {'player_name': cell.get_text(strip=True)}
    
    def extract_team_stats(cell: Tag) -> Dict[str, Any]:
        """Extract team name and abbreviation from team cell"""
        link = cell.find('a')
        if link:
            team_name = link.get_text(strip=True)
            href = str(link.get('href', '') or '')
            # Extract team abbreviation from URL like '/teams/BOS/2024.html'
            match = re.search(r'/teams/([A-Z]+)/', href)
            team_abbrev = match.group(1) if match else None
            
            return {
                'team_name': team_name,
                'team_abbreviation': team_abbrev,
                'team_url': href
            }
        else:
            return {'team_name': cell.get_text(strip=True)}
    
    return {
        'player': ColumnHandler(
            name='player',
            target_columns=['player_name', 'player_br_id', 'player_url'],
            extractor=extract_player_stats,
            description='Player name and basketball-reference ID'
        ),
        'team': ColumnHandler(
            name='team', 
            target_columns=['team_name', 'team_abbreviation', 'team_url'],
            extractor=extract_team_stats,
            description='Team name and abbreviation'
        ),
        'tm': ColumnHandler(  # Alternative team column name
            name='tm',
            target_columns=['team_name', 'team_abbreviation', 'team_url'], 
            extractor=extract_team_stats,
            description='Team name and abbreviation (tm column)'
        )
    }

if __name__ == "__main__":
    # Example usage
    downloader = HTMLDownloader(rate_limiter=RateLimiter(requests_per_second=0.5))
    handlers = create_basketball_reference_handlers()
    extractor = DataExtractor(column_handlers=handlers)
    
    # Test download
    html = downloader.download("https://www.basketball-reference.com/leagues/NBA_2024.html")
    print(f"Downloaded {len(html)} characters")
    
    # Test extraction
    soup = BeautifulSoup(html, 'html.parser')
    data = extractor.extract_table_data(soup, table_id='per_game-team')
    print(f"Extracted {len(data)} rows")
    if data:
        print("Sample row:", data[0])