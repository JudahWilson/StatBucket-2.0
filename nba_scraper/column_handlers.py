"""
Advanced Column Handler System

Provides regex-based column handlers for parsing complex data patterns,
splitting single TD cells into multiple columns, and handling basketball-reference.com
specific data formats.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Callable, Tuple, Union
from dataclasses import dataclass
from bs4 import Tag, BeautifulSoup
from datetime import datetime, date
from urllib.parse import urljoin
import json

logger = logging.getLogger(__name__)

@dataclass 
class ColumnHandlerConfig:
    """Configuration for a column handler"""
    name: str
    pattern: Optional[str] = None  # Regex pattern to match against cell content
    target_columns: Optional[List[str]] = None  # Columns to populate from this handler
    extractor_func: Optional[Callable] = None  # Custom extraction function
    validator_func: Optional[Callable] = None  # Validation function
    default_value: Any = None  # Default value if extraction fails
    required: bool = False  # Whether this column is required
    description: str = ""
    priority: int = 0  # Higher priority handlers run first

class RegexColumnExtractor:
    """Extract data using regex patterns"""
    
    def __init__(self):
        self.patterns = self._setup_common_patterns()
    
    def _setup_common_patterns(self) -> Dict[str, Dict[str, Any]]:
        """Setup common regex patterns for basketball data"""
        return {
            'player_season_stats': {
                'pattern': r'(\d+\.?\d*)\s*\/\s*(\d+\.?\d*)\s*\/\s*(\d+\.?\d*)',
                'columns': ['points', 'rebounds', 'assists'],
                'types': [float, float, float],
                'description': 'Extract PPG/RPG/APG format'
            },
            'shooting_percentage': {
                'pattern': r'(\d+)-(\d+)',  # Made-Attempted format
                'columns': ['made', 'attempted'],
                'types': [int, int],
                'description': 'Extract made-attempted shooting stats'
            },
            'player_height': {
                'pattern': r'(\d+)-(\d+)',  # Feet-Inches format
                'columns': ['feet', 'inches'],
                'types': [int, int],
                'description': 'Extract height in feet-inches'
            },
            'game_score': {
                'pattern': r'(\d+)-(\d+)',  # Team1-Team2 score
                'columns': ['home_score', 'away_score'],
                'types': [int, int], 
                'description': 'Extract game score'
            },
            'date_format': {
                'pattern': r'(\d{4})-(\d{2})-(\d{2})',  # YYYY-MM-DD
                'columns': ['year', 'month', 'day'],
                'types': [int, int, int],
                'description': 'Extract date components'
            },
            'plus_minus': {
                'pattern': r'([+-]?\d+)',  # +/- with optional sign
                'columns': ['plus_minus'],
                'types': [int],
                'description': 'Extract plus/minus values'
            },
            'win_loss_record': {
                'pattern': r'(\d+)-(\d+)',  # Wins-Losses
                'columns': ['wins', 'losses'],
                'types': [int, int],
                'description': 'Extract win-loss record'
            },
            'playoff_series': {
                'pattern': r'(\d+)-(\d+)',  # Series wins-losses
                'columns': ['series_wins', 'series_losses'],
                'types': [int, int],
                'description': 'Extract playoff series record'
            }
        }
    
    def extract_with_pattern(self, 
                           text: str, 
                           pattern_name: str) -> Optional[Dict[str, Any]]:
        """Extract data using a named pattern"""
        if pattern_name not in self.patterns:
            logger.warning(f"Unknown pattern: {pattern_name}")
            return None
        
        pattern_info = self.patterns[pattern_name]
        pattern = pattern_info['pattern']
        columns = pattern_info['columns']
        types = pattern_info.get('types', [str] * len(columns))
        
        match = re.search(pattern, text)
        if not match:
            return None
        
        result = {}
        for i, (col_name, col_type) in enumerate(zip(columns, types)):
            if i < len(match.groups()):
                try:
                    raw_value = match.group(i + 1)
                    result[col_name] = col_type(raw_value) if raw_value else None
                except (ValueError, TypeError) as e:
                    logger.warning(f"Type conversion failed for {col_name}: {e}")
                    result[col_name] = raw_value if 'raw_value' in locals() else None
        
        return result
    
    def extract_with_custom_pattern(self, 
                                  text: str,
                                  pattern: str,
                                  column_names: List[str],
                                  column_types: Optional[List[type]] = None) -> Optional[Dict[str, Any]]:
        """Extract data using a custom regex pattern"""
        if column_types is None:
            column_types = [str] * len(column_names)
        
        match = re.search(pattern, text)
        if not match:
            return None
        
        result = {}
        for i, (col_name, col_type) in enumerate(zip(column_names, column_types)):
            if i < len(match.groups()):
                try:
                    raw_value = match.group(i + 1)
                    result[col_name] = col_type(raw_value) if raw_value else None
                except (ValueError, TypeError) as e:
                    logger.warning(f"Type conversion failed for {col_name}: {e}")
                    result[col_name] = raw_value if 'raw_value' in locals() else None
        
        return result

class BasketballReferenceHandlers:
    """Specialized handlers for basketball-reference.com data patterns"""
    
    def __init__(self):
        self.regex_extractor = RegexColumnExtractor()
        self.base_url = "https://www.basketball-reference.com"
    
    def handle_player_name_with_link(self, cell: Tag) -> Dict[str, Any]:
        """Handle player name with link to player page"""
        link = cell.find('a')
        text = cell.get_text(strip=True)
        
        if link:
            href = str(link.get('href', '') or '')
            full_url = urljoin(self.base_url, href)
            
            # Extract player ID from URL
            player_id_match = re.search(r'/players/[a-z]/([^/]+)\.html', href)
            player_id = player_id_match.group(1) if player_id_match else None
            
            return {
                'player_name': text,
                'player_br_id': player_id,
                'player_url': href,
                'player_full_url': full_url
            }
        else:
            return {'player_name': text}
    
    def handle_team_abbreviation_with_link(self, cell: Tag) -> Dict[str, Any]:
        """Handle team abbreviation with link to team page"""
        link = cell.find('a')
        text = cell.get_text(strip=True)
        
        if link:
            href = str(link.get('href', '') or '')
            full_url = urljoin(self.base_url, href)
            
            # Extract team abbreviation and year
            team_match = re.search(r'/teams/([A-Z]+)/(\d{4})\.html', href)
            if team_match:
                team_abbrev = team_match.group(1)
                season_year = int(team_match.group(2))
            else:
                team_abbrev = text
                season_year = None
            
            return {
                'team_name': text,
                'team_abbreviation': team_abbrev,
                'season_year': season_year,
                'team_url': href,
                'team_full_url': full_url
            }
        else:
            return {'team_abbreviation': text}
    
    def handle_game_date_with_link(self, cell: Tag) -> Dict[str, Any]:
        """Handle game date with link to box score"""
        link = cell.find('a')
        text = cell.get_text(strip=True)
        
        result = {'game_date_text': text}
        
        # Try to parse date
        try:
            # Handle different date formats
            if re.match(r'\d{4}-\d{2}-\d{2}', text):
                game_date = datetime.strptime(text, '%Y-%m-%d').date()
            elif re.match(r'\w+ \d+, \d{4}', text):  # "Jan 15, 2024"
                game_date = datetime.strptime(text, '%b %d, %Y').date()
            else:
                game_date = None
            
            if game_date:
                result['game_date'] = game_date.isoformat()
        except ValueError:
            result['game_date'] = None
        
        if link:
            href = str(link.get('href', '') or '')
            result['box_score_url'] = href
            result['box_score_full_url'] = urljoin(self.base_url, href)
            
            # Extract game ID from box score URL
            game_id_match = re.search(r'/boxscores/(\w+)\.html', href)
            if game_id_match:
                result['game_id'] = game_id_match.group(1)
        
        return result
    
    def handle_shooting_stats(self, cell: Tag) -> Dict[str, Any]:
        """Handle shooting statistics in made-attempted format"""
        text = cell.get_text(strip=True)
        
        if text in ['', '-', 'N/A']:
            return {
                'made': None,
                'attempted': None,
                'percentage': None
            }
        
        # Try to extract made-attempted format
        extracted = self.regex_extractor.extract_with_pattern(text, 'shooting_percentage')
        if extracted:
            made = extracted['made']
            attempted = extracted['attempted']
            percentage = made / attempted if attempted > 0 else None
            
            return {
                'made': made,
                'attempted': attempted,
                'percentage': percentage
            }
        else:
            # Try to parse as just a percentage
            try:
                if '%' in text:
                    percentage = float(text.replace('%', '')) / 100.0
                else:
                    percentage = float(text)
                
                return {
                    'made': None,
                    'attempted': None,
                    'percentage': percentage
                }
            except ValueError:
                return {
                    'made': None,
                    'attempted': None,
                    'percentage': None,
                    'raw_text': text
                }
    
    def handle_record_with_percentage(self, cell: Tag) -> Dict[str, Any]:
        """Handle win-loss record with winning percentage"""
        text = cell.get_text(strip=True)
        
        if text in ['', '-', 'N/A']:
            return {'wins': None, 'losses': None, 'win_percentage': None}
        
        # Extract record (wins-losses)
        record_match = re.search(r'(\d+)-(\d+)', text)
        if record_match:
            wins = int(record_match.group(1))
            losses = int(record_match.group(2))
            total_games = wins + losses
            win_percentage = wins / total_games if total_games > 0 else None
            
            return {
                'wins': wins,
                'losses': losses,
                'win_percentage': win_percentage
            }
        else:
            return {'wins': None, 'losses': None, 'win_percentage': None, 'raw_text': text}
    
    def handle_playoff_series_result(self, cell: Tag) -> Dict[str, Any]:
        """Handle playoff series results"""
        text = cell.get_text(strip=True)
        link = cell.find('a')
        
        result = {'series_result_text': text}
        
        if link:
            href = str(link.get('href', '') or '')
            result['series_stats_url'] = href
        
        # Parse series result (e.g., "Boston Celtics over Miami Heat (4-1)")
        series_match = re.search(r'(.+?) over (.+?) \((\d+)-(\d+)\)', text)
        if series_match:
            result.update({
                'winner': series_match.group(1).strip(),
                'loser': series_match.group(2).strip(),
                'winner_wins': int(series_match.group(3)),
                'loser_wins': int(series_match.group(4))
            })
        
        return result
    
    def handle_advanced_stats_cell(self, cell: Tag) -> Dict[str, Any]:
        """Handle cells with advanced statistics"""
        text = cell.get_text(strip=True)
        
        if text in ['', '-', 'N/A']:
            return {'value': None}
        
        try:
            # Handle different numeric formats
            if '%' in text:
                value = float(text.replace('%', '')) / 100.0
            else:
                value = float(text)
            
            return {'value': value}
        except ValueError:
            return {'value': None, 'raw_text': text}

class AdvancedColumnHandlerSystem:
    """Advanced column handler system with regex and custom handlers"""
    
    def __init__(self):
        self.handlers: Dict[str, ColumnHandlerConfig] = {}
        self.bbref_handlers = BasketballReferenceHandlers()
        self._setup_default_handlers()
    
    def _setup_default_handlers(self):
        """Setup default column handlers"""
        
        # Player name with link handler
        self.register_handler(ColumnHandlerConfig(
            name='player',
            target_columns=['player_name', 'player_br_id', 'player_url', 'player_full_url'],
            extractor_func=self.bbref_handlers.handle_player_name_with_link,
            description='Extract player name and ID from cell with link'
        ))
        
        # Team abbreviation handler
        self.register_handler(ColumnHandlerConfig(
            name='team',
            target_columns=['team_name', 'team_abbreviation', 'season_year', 'team_url', 'team_full_url'],
            extractor_func=self.bbref_handlers.handle_team_abbreviation_with_link,
            description='Extract team info from cell with link'
        ))
        
        # Game date handler
        self.register_handler(ColumnHandlerConfig(
            name='date_game',
            target_columns=['game_date_text', 'game_date', 'box_score_url', 'box_score_full_url', 'game_id'],
            extractor_func=self.bbref_handlers.handle_game_date_with_link,
            description='Extract game date and box score link'
        ))
        
        # Field goal shooting handler
        self.register_handler(ColumnHandlerConfig(
            name='fg',
            target_columns=['fg_made', 'fg_attempted', 'fg_percentage'],
            extractor_func=lambda cell: self._rename_keys(
                self.bbref_handlers.handle_shooting_stats(cell),
                {'made': 'fg_made', 'attempted': 'fg_attempted', 'percentage': 'fg_percentage'}
            ),
            description='Extract field goal shooting stats'
        ))
        
        # Three-point shooting handler
        self.register_handler(ColumnHandlerConfig(
            name='fg3',
            target_columns=['fg3_made', 'fg3_attempted', 'fg3_percentage'],
            extractor_func=lambda cell: self._rename_keys(
                self.bbref_handlers.handle_shooting_stats(cell),
                {'made': 'fg3_made', 'attempted': 'fg3_attempted', 'percentage': 'fg3_percentage'}
            ),
            description='Extract three-point shooting stats'
        ))
        
        # Free throw shooting handler
        self.register_handler(ColumnHandlerConfig(
            name='ft',
            target_columns=['ft_made', 'ft_attempted', 'ft_percentage'],
            extractor_func=lambda cell: self._rename_keys(
                self.bbref_handlers.handle_shooting_stats(cell),
                {'made': 'ft_made', 'attempted': 'ft_attempted', 'percentage': 'ft_percentage'}
            ),
            description='Extract free throw shooting stats'
        ))
        
        # Win-loss record handler
        self.register_handler(ColumnHandlerConfig(
            name='wins_losses',
            target_columns=['wins', 'losses', 'win_percentage'],
            extractor_func=self.bbref_handlers.handle_record_with_percentage,
            description='Extract win-loss record and percentage'
        ))
    
    def register_handler(self, config: ColumnHandlerConfig):
        """Register a new column handler"""
        self.handlers[config.name] = config
        logger.info(f"Registered column handler: {config.name}")
    
    def register_regex_handler(self, 
                             name: str,
                             pattern: str,
                             target_columns: List[str],
                             column_types: List[type] = None,
                             description: str = ""):
        """Register a regex-based column handler"""
        
        def regex_extractor(cell: Tag) -> Dict[str, Any]:
            text = cell.get_text(strip=True)
            result = self.bbref_handlers.regex_extractor.extract_with_custom_pattern(
                text, pattern, target_columns, column_types or [str] * len(target_columns)
            )
            return result or {}
        
        config = ColumnHandlerConfig(
            name=name,
            pattern=pattern,
            target_columns=target_columns,
            extractor_func=regex_extractor,
            description=description
        )
        
        self.register_handler(config)
    
    def process_cell(self, cell: Tag, column_name: str) -> Dict[str, Any]:
        """Process a cell using the appropriate handler"""
        # Check if we have a specific handler for this column
        if column_name in self.handlers:
            handler = self.handlers[column_name]
            try:
                result = handler.extractor_func(cell)
                
                # Validate if validator provided
                if handler.validator_func and not handler.validator_func(result):
                    logger.warning(f"Validation failed for {column_name}: {result}")
                
                return result or {}
                
            except Exception as e:
                logger.error(f"Handler {column_name} failed: {e}")
                return {'error': str(e)}
        
        # Fall back to default processing
        return self._default_cell_processing(cell)
    
    def _default_cell_processing(self, cell: Tag) -> Dict[str, Any]:
        """Default processing for cells without specific handlers"""
        text = cell.get_text(strip=True)
        
        # Check if cell has a link
        link = cell.find('a')
        if link:
            href = str(link.get('href', '') or '')
            return {
                'text': text,
                'url': href,
                'full_url': urljoin(self.bbref_handlers.base_url, href)
            }
        
        # Try to parse as number
        if self._is_numeric(text):
            return {'value': self._parse_number(text)}
        
        return {'value': text}
    
    def _is_numeric(self, text: str) -> bool:
        """Check if text represents a numeric value"""
        if not text or text in ['-', 'N/A', '']:
            return True  # Treat as potential numeric (null)
        
        # Remove common formatting
        cleaned = re.sub(r'[,\s%+\-]', '', text)
        try:
            float(cleaned)
            return True
        except ValueError:
            return False
    
    def _parse_number(self, text: str) -> Optional[Union[int, float]]:
        """Parse text as a number"""
        if not text or text in ['-', 'N/A', '']:
            return None
        
        try:
            # Handle percentages
            if '%' in text:
                return float(text.replace('%', '')) / 100.0
            
            # Remove commas
            cleaned = text.replace(',', '')
            
            # Try integer first, then float
            if '.' in cleaned:
                return float(cleaned)
            else:
                return int(cleaned)
                
        except ValueError:
            return None
    
    def _rename_keys(self, data: Dict[str, Any], key_map: Dict[str, str]) -> Dict[str, Any]:
        """Rename keys in a dictionary"""
        if not data:
            return {}
        
        result = {}
        for old_key, new_key in key_map.items():
            if old_key in data:
                result[new_key] = data[old_key]
        
        # Copy any keys not in the map
        for key, value in data.items():
            if key not in key_map:
                result[key] = value
        
        return result
    
    def get_handler_names(self) -> List[str]:
        """Get list of all registered handler names"""
        return list(self.handlers.keys())
    
    def get_handler_info(self, name: str) -> Optional[ColumnHandlerConfig]:
        """Get information about a specific handler"""
        return self.handlers.get(name)

# Easy-to-use function for creating custom handlers
def create_regex_handler(pattern: str, 
                        target_columns: List[str],
                        column_types: List[type] = None,
                        description: str = "") -> ColumnHandlerConfig:
    """Create a regex-based column handler configuration"""
    
    def extractor(cell: Tag) -> Dict[str, Any]:
        text = cell.get_text(strip=True)
        regex_extractor = RegexColumnExtractor()
        result = regex_extractor.extract_with_custom_pattern(
            text, pattern, target_columns, column_types or [str] * len(target_columns)
        )
        return result or {}
    
    return ColumnHandlerConfig(
        name=f"regex_{hash(pattern)}",  # Generate unique name
        pattern=pattern,
        target_columns=target_columns,
        extractor_func=extractor,
        description=description
    )

if __name__ == "__main__":
    # Example usage
    handler_system = AdvancedColumnHandlerSystem()
    
    # Register custom regex handler for height
    handler_system.register_regex_handler(
        name='height',
        pattern=r'(\d+)-(\d+)',
        target_columns=['height_feet', 'height_inches'],
        column_types=[int, int],
        description='Extract height in feet-inches format'
    )
    
    # Test with sample HTML
    html = '<td><a href="/players/j/jamesle01.html">LeBron James</a></td>'
    soup = BeautifulSoup(html, 'html.parser')
    cell = soup.find('td')
    
    result = handler_system.process_cell(cell, 'player')
    print("Processed player cell:", result)
    
    # List all handlers
    print("Available handlers:", handler_system.get_handler_names())