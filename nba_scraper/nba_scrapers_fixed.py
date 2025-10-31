"""
NBA Specific Scrapers - Simplified Version

Core NBA data extractors that work with the basketball-reference.com website structure.
Focuses on essential functionality for team stats, player stats, and standings.
"""

import logging
import re
from typing import Dict, List, Any, Optional, Callable, Union
from datetime import datetime
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

class BasketballReferenceExtractor:
    """Base class for basketball-reference.com data extraction"""
    
    def clean_column_name(self, name: str) -> str:
        """Clean column names for database storage"""
        # Convert to lowercase and replace spaces/special chars with underscores
        # TODO can i preserve the column name instead? using quotes?
        cleaned = re.sub(r'[^\w\s]', '', name.lower())
        cleaned = re.sub(r'\s+', '_', cleaned.strip())
        return cleaned
    
    def convert_value(self, value: str) -> Union[str, int, float, None]:
        """Convert string values to appropriate types"""
        if not value or value == '':
            return None
        
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Return as string
        return value.strip()
    
    def safe_get_text(self, element: Optional[Tag]) -> str:
        """Safely get text from BeautifulSoup element"""
        if element is None:
            return ""
        return element.get_text(strip=True)
    
    def safe_get_href(self, element: Optional[Tag]) -> str:
        """Safely get href attribute from element"""
        if element is None:
            return ""
        href = element.get('href')
        return str(href) if href else ""

class NBATeamStatsExtractor(BasketballReferenceExtractor):
    """Extract NBA team statistics from league pages"""
    
    def extract_team_stats(self, html: str, url: str) -> List[Dict[str, Any]]:
        """Extract team statistics from NBA league page"""
        
        soup = BeautifulSoup(html, 'html.parser')
        data = []
        
        # Find team stats table - try multiple possible IDs
        table = None
        possible_ids = ['per_game-team', 'team-stats-per_game', 'stats']
        
        for table_id in possible_ids:
            table = soup.find('table', id=table_id)
            if table:
                logger.debug(f"Found team stats table with id: {table_id}")
                break
        
        if not table:
            logger.warning("No team stats table found")
            return data
        
        # Extract headers
        headers = []
        thead = table.find('thead')
        if thead:
            header_row = thead.find('tr')
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    header_text = self.safe_get_text(th)
                    if header_text and header_text not in ['Rk', '']:
                        headers.append(self.clean_column_name(header_text))
        
        if not headers:
            logger.warning("No headers found in team stats table")
            return data
        
        # Extract data rows
        tbody = table.find('tbody')
        if not tbody:
            logger.warning("No tbody found in team stats table")
            return data
        
        for row in tbody.find_all('tr'):
            # Skip header rows that might be in tbody
            row_class = row.get('class')
            if row_class and isinstance(row_class, list) and 'thead' in row_class:
                continue
            
            row_data = {}
            cells = row.find_all(['td', 'th'])
            
            for i, cell in enumerate(cells):
                if i >= len(headers):
                    break
                
                header = headers[i]
                cell_text = self.safe_get_text(cell)
                
                # Handle team names with links
                if header == 'team':
                    link = cell.find('a')
                    if link:
                        row_data[header] = self.safe_get_text(link)
                        href = self.safe_get_href(link)
                        row_data['team_url'] = href
                        
                        # Extract team abbreviation from URL
                        if href:
                            team_match = re.search(r'/teams/([A-Z]{3})/', href)
                            if team_match:
                                row_data['team_abbr'] = team_match.group(1)
                    else:
                        row_data[header] = cell_text
                else:
                    row_data[header] = self.convert_value(cell_text)
            
            # Only add rows that have team data
            if row_data and ('team' in row_data or 'team_abbr' in row_data):
                # Add metadata
                row_data['_source_url'] = url
                row_data['_extraction_time'] = datetime.now().isoformat()
                data.append(row_data)
        
        logger.info(f"Extracted {len(data)} team records from {url}")
        return data

class NBAPlayerStatsExtractor(BasketballReferenceExtractor):
    """Extract NBA player statistics"""
    
    def extract_player_stats(self, html: str, url: str) -> List[Dict[str, Any]]:
        """Extract player statistics from NBA pages"""
        
        soup = BeautifulSoup(html, 'html.parser')
        data = []
        
        # Find player stats table
        table = None
        possible_ids = ['per_game_stats', 'totals_stats', 'advanced_stats', 'stats']
        
        for table_id in possible_ids:
            table = soup.find('table', id=table_id)
            if table:
                logger.debug(f"Found player stats table with id: {table_id}")
                break
        
        if not table:
            logger.warning("No player stats table found")
            return data
        
        # Extract headers
        headers = []
        thead = table.find('thead')
        if thead:
            # Handle multi-level headers by taking the last row
            header_rows = thead.find_all('tr')
            header_row = header_rows[-1] if header_rows else None
            
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    header_text = self.safe_get_text(th)
                    if header_text and header_text not in ['Rk']:
                        headers.append(self.clean_column_name(header_text))
        
        if not headers:
            logger.warning("No headers found in player stats table")
            return data
        
        # Extract data rows
        tbody = table.find('tbody')
        if not tbody:
            logger.warning("No tbody found in player stats table")
            return data
        
        for row in tbody.find_all('tr'):
            # Skip header rows
            row_class = row.get('class')
            if row_class and isinstance(row_class, list) and 'thead' in row_class:
                continue
            
            row_data = {}
            cells = row.find_all(['td', 'th'])
            
            for i, cell in enumerate(cells):
                if i >= len(headers):
                    break
                
                header = headers[i]
                cell_text = self.safe_get_text(cell)
                
                # Handle player names with links
                if header == 'player':
                    link = cell.find('a')
                    if link:
                        row_data[header] = self.safe_get_text(link)
                        href = self.safe_get_href(link)
                        row_data['player_url'] = href
                        
                        # Extract player ID from URL
                        if href:
                            player_match = re.search(r'/players/[a-z]/([^.]+)\.html', href)
                            if player_match:
                                row_data['player_id'] = player_match.group(1)
                    else:
                        row_data[header] = cell_text
                
                # Handle team abbreviations with links
                elif header in ['tm', 'team']:
                    link = cell.find('a')
                    if link:
                        row_data[header] = self.safe_get_text(link)
                        href = self.safe_get_href(link)
                        row_data['team_url'] = href
                        
                        # Extract team abbreviation
                        if href:
                            team_match = re.search(r'/teams/([A-Z]{3})/', href)
                            if team_match:
                                row_data['team_abbr'] = team_match.group(1)
                    else:
                        row_data[header] = cell_text
                
                else:
                    row_data[header] = self.convert_value(cell_text)
            
            # Only add rows that have player data
            if row_data and ('player' in row_data or 'player_id' in row_data):
                # Add metadata
                row_data['_source_url'] = url
                row_data['_extraction_time'] = datetime.now().isoformat()
                data.append(row_data)
        
        logger.info(f"Extracted {len(data)} player records from {url}")
        return data

class NBAStandingsExtractor(BasketballReferenceExtractor):
    """Extract NBA standings"""
    
    def extract_standings(self, html: str, url: str) -> List[Dict[str, Any]]:
        """Extract standings from NBA league page"""
        
        soup = BeautifulSoup(html, 'html.parser')
        data = []
        
        # Find standings tables (Eastern and Western Conference)
        conference_tables = []
        
        # Look for conference-specific tables
        for conf in ['E', 'W']:
            table = soup.find('table', id=f'confs_standings_{conf}')
            if table:
                conference_name = 'Eastern' if conf == 'E' else 'Western'
                conference_tables.append((conference_name, table))
        
        # If no conference tables found, look for general standings
        if not conference_tables:
            table = soup.find('table', id='standings')
            if table:
                conference_tables.append(('', table))
        
        if not conference_tables:
            logger.warning("No standings tables found")
            return data
        
        for conference, table in conference_tables:
            # Extract headers
            headers = []
            thead = table.find('thead')
            if thead:
                header_row = thead.find('tr')
                if header_row:
                    for th in header_row.find_all(['th', 'td']):
                        header_text = self.safe_get_text(th)
                        if header_text:
                            headers.append(self.clean_column_name(header_text))
            
            if not headers:
                continue
            
            # Extract team standings
            tbody = table.find('tbody')
            if not tbody:
                continue
            
            for row in tbody.find_all('tr'):
                row_data = {}
                cells = row.find_all(['td', 'th'])
                
                for i, cell in enumerate(cells):
                    if i >= len(headers):
                        break
                    
                    header = headers[i]
                    cell_text = self.safe_get_text(cell)
                    
                    # Handle team names with links
                    if 'team' in header.lower():
                        link = cell.find('a')
                        if link:
                            row_data['team'] = self.safe_get_text(link)
                            href = self.safe_get_href(link)
                            row_data['team_url'] = href
                            
                            # Extract team abbreviation
                            if href:
                                team_match = re.search(r'/teams/([A-Z]{3})/', href)
                                if team_match:
                                    row_data['team_abbr'] = team_match.group(1)
                        else:
                            row_data['team'] = cell_text
                    else:
                        row_data[header] = self.convert_value(cell_text)
                
                # Add conference info and metadata
                if row_data and ('team' in row_data or 'team_abbr' in row_data):
                    if conference:
                        row_data['conference'] = conference
                    row_data['_source_url'] = url
                    row_data['_extraction_time'] = datetime.now().isoformat()
                    data.append(row_data)
        
        logger.info(f"Extracted {len(data)} standings records from {url}")
        return data

# URL generators for NBA data
def generate_nba_season_urls(seasons: List[int]) -> Dict[str, List[str]]:
    """Generate URLs for different NBA data types by season"""
    
    urls = {
        'team_stats': [],
        'player_stats': [],
        'standings': []
    }
    
    for season in seasons:
        # Team stats from league page
        urls['team_stats'].append(f"https://www.basketball-reference.com/leagues/NBA_{season}.html")
        
        # Player stats from per-game page
        urls['player_stats'].append(f"https://www.basketball-reference.com/leagues/NBA_{season}_per_game.html")
        
        # Standings
        urls['standings'].append(f"https://www.basketball-reference.com/leagues/NBA_{season}_standings.html")
    
    return urls

# Main scraping coordinator
class NBAScraper:
    """Main NBA data scraper coordinator"""
    
    def __init__(self, 
                 downloader: Any,
                 session_maker: Callable,
                 rate_limit: float = 1.0):
        self.downloader = downloader
        self.session_maker = session_maker
        self.rate_limit = rate_limit
        
        # Initialize extractors
        self.team_extractor = NBATeamStatsExtractor()
        self.player_extractor = NBAPlayerStatsExtractor()
        self.standings_extractor = NBAStandingsExtractor()
        
        logger.info("NBA Scraper initialized")
    
    def scrape_season_data(self, 
                          season: int,
                          data_types: Optional[List[str]] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Scrape data for a specific season
        
        Args:
            season: NBA season year (e.g., 2024 for 2023-24 season)
            data_types: List of data types to scrape ['team_stats', 'player_stats', 'standings']
        """
        
        if data_types is None:
            data_types = ['team_stats', 'player_stats', 'standings']
        
        results = {}
        urls = generate_nba_season_urls([season])
        
        for data_type in data_types:
            if data_type not in urls:
                logger.warning(f"Unknown data type: {data_type}")
                continue
            
            logger.info(f"Scraping {data_type} for {season} season")
            
            try:
                url = urls[data_type][0]  # First (and only) URL for this season
                html = self.downloader.download(url)
                
                if data_type == 'team_stats':
                    data = self.team_extractor.extract_team_stats(html, url)
                elif data_type == 'player_stats':
                    data = self.player_extractor.extract_player_stats(html, url)
                elif data_type == 'standings':
                    data = self.standings_extractor.extract_standings(html, url)
                else:
                    data = []
                
                results[data_type] = data
                logger.info(f"Successfully scraped {len(data)} {data_type} records")
                
            except Exception as e:
                logger.error(f"Failed to scrape {data_type} for season {season}: {e}")
                results[data_type] = []
        
        return results
    
    def scrape_multiple_seasons(self, 
                               seasons: List[int],
                               data_types: Optional[List[str]] = None) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """Scrape data for multiple seasons"""
        
        all_results = {}
        
        for season in seasons:
            logger.info(f"Scraping season {season}")
            season_results = self.scrape_season_data(season, data_types)
            all_results[str(season)] = season_results
        
        return all_results

if __name__ == "__main__":
    # Example usage and testing
    from scraping_framework import HTMLDownloader, RateLimiter
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    # Setup
    engine = create_engine("sqlite:///nba_test.db")
    SessionMaker = sessionmaker(bind=engine)
    
    downloader = HTMLDownloader(rate_limiter=RateLimiter(requests_per_second=0.5))
    
    # Create scraper
    scraper = NBAScraper(downloader, SessionMaker)
    
    # Test with a recent season
    print("Testing NBA scraper...")
    
    # Generate URLs for testing
    test_urls = generate_nba_season_urls([2024])
    print(f"Generated URLs for 2024 season:")
    for data_type, urls in test_urls.items():
        print(f"  {data_type}: {urls[0]}")
    
    # Test individual extractors (would need actual HTML)
    print(f"\nNBA Scraper ready for seasons: {[2023, 2024]}")
    print("Use scraper.scrape_season_data(2024) to scrape current season")