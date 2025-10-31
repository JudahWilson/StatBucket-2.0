"""
Dynamic Schema Management System

Handles dynamic column addition, schema change detection, and automated schema updates
for the NBA scraping system. Provides feedback mechanism when schema changes occur.
"""

import logging
from typing import Dict, List, Any, Optional, Type, Set, Callable
from sqlalchemy import (
    inspect, Column, Integer, String, Float, Date, DateTime, Boolean, Text, 
    MetaData, Table, create_engine, text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from datetime import datetime
import json
import re

logger = logging.getLogger(__name__)

class SchemaChangeDetector:
    """Detect schema changes when scraping new data"""
    
    def __init__(self, engine, metadata: MetaData):
        self.engine = engine
        self.metadata = metadata
        self.inspector = inspect(engine)
        
    def detect_changes_from_scraped_data(self, 
                                       table_name: str, 
                                       scraped_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Compare scraped data structure with existing table schema
        
        Args:
            table_name: Name of the target table
            scraped_data: List of scraped data dictionaries
            
        Returns:
            List of schema change dictionaries
        """
        if not scraped_data:
            return []
        
        changes = []
        
        # Get current table schema
        existing_columns = self._get_table_columns(table_name)
        
        # Analyze scraped data to determine required columns
        required_columns = self._analyze_data_structure(scraped_data)
        
        # Find new columns
        for col_name, col_info in required_columns.items():
            if col_name not in existing_columns:
                changes.append({
                    'operation': 'add',
                    'table_name': table_name,
                    'column_name': col_name,
                    'new_definition': col_info,
                    'reason': 'Found in scraped data but not in existing schema'
                })
        
        # Find columns that might need type changes
        for col_name, col_info in required_columns.items():
            if col_name in existing_columns:
                existing_type = existing_columns[col_name]['type']
                required_type = col_info['type']
                
                if self._should_upgrade_column_type(existing_type, required_type):
                    changes.append({
                        'operation': 'modify',
                        'table_name': table_name,
                        'column_name': col_name,
                        'old_definition': existing_columns[col_name],
                        'new_definition': col_info,
                        'reason': f'Type upgrade needed: {existing_type} -> {required_type}'
                    })
        
        return changes
    
    def _get_table_columns(self, table_name: str) -> Dict[str, Dict[str, Any]]:
        """Get existing table column information"""
        try:
            columns = {}
            for column in self.inspector.get_columns(table_name):
                columns[column['name']] = {
                    'type': str(column['type']),
                    'nullable': column['nullable'],
                    'default': column.get('default')
                }
            return columns
        except Exception as e:
            logger.warning(f"Could not inspect table {table_name}: {e}")
            return {}
    
    def _analyze_data_structure(self, data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Analyze scraped data to determine column types and constraints"""
        columns = {}
        
        # Sample data to determine types
        sample_size = min(100, len(data))
        
        for record in data[:sample_size]:
            for key, value in record.items():
                if key not in columns:
                    columns[key] = {
                        'type': None,
                        'nullable': False,
                        'max_length': 0
                    }
                
                col_info = columns[key]
                
                # Update nullable status
                if value is None:
                    col_info['nullable'] = True
                    continue
                
                # Determine type
                inferred_type = self._infer_column_type(value)
                
                # Update type with most permissive option
                current_type = col_info['type']
                if current_type is None:
                    col_info['type'] = inferred_type
                elif current_type != inferred_type:
                    col_info['type'] = self._get_most_permissive_type(current_type, inferred_type)
                
                # Update max length for strings
                if isinstance(value, str):
                    col_info['max_length'] = max(col_info['max_length'], len(value))
        
        # Finalize column definitions
        for col_name, col_info in columns.items():
            if col_info['type'] == 'string':
                # Choose appropriate string length
                max_len = col_info['max_length']
                if max_len <= 50:
                    col_info['sql_type'] = 'VARCHAR(100)'  # Buffer for growth
                elif max_len <= 255:
                    col_info['sql_type'] = 'VARCHAR(500)'
                else:
                    col_info['sql_type'] = 'TEXT'
            elif col_info['type'] == 'integer':
                col_info['sql_type'] = 'INTEGER'
            elif col_info['type'] == 'float':
                col_info['sql_type'] = 'FLOAT'
            elif col_info['type'] == 'boolean':
                col_info['sql_type'] = 'BOOLEAN'
            elif col_info['type'] == 'date':
                col_info['sql_type'] = 'DATE'
            elif col_info['type'] == 'datetime':
                col_info['sql_type'] = 'TIMESTAMP'
            else:
                col_info['sql_type'] = 'TEXT'  # Default fallback
        
        return columns
    
    def _infer_column_type(self, value: Any) -> str:
        """Infer column type from a sample value"""
        if isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            # Check if it looks like a date
            if self._looks_like_date(value):
                return 'date'
            elif self._looks_like_datetime(value):
                return 'datetime'
            else:
                return 'string'
        else:
            return 'string'
    
    def _looks_like_date(self, value: str) -> bool:
        """Check if string looks like a date"""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY
            r'^\d{1,2}/\d{1,2}/\d{4}$',  # M/D/YYYY
        ]
        return any(re.match(pattern, value) for pattern in date_patterns)
    
    def _looks_like_datetime(self, value: str) -> bool:
        """Check if string looks like a datetime"""
        datetime_patterns = [
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO format
        ]
        return any(re.match(pattern, value) for pattern in datetime_patterns)
    
    def _get_most_permissive_type(self, type1: str, type2: str) -> str:
        """Get the most permissive type between two types"""
        type_hierarchy = {
            'boolean': 0,
            'integer': 1,
            'float': 2,
            'date': 3,
            'datetime': 4,
            'string': 5
        }
        
        rank1 = type_hierarchy.get(type1, 5)
        rank2 = type_hierarchy.get(type2, 5)
        
        return type1 if rank1 > rank2 else type2
    
    def _should_upgrade_column_type(self, existing_type: str, required_type: str) -> bool:
        """Determine if a column type should be upgraded"""
        upgrades = {
            'INTEGER': ['FLOAT', 'TEXT'],
            'VARCHAR': ['TEXT'],
            'FLOAT': ['TEXT'],
            'DATE': ['TIMESTAMP', 'TEXT'],
            'BOOLEAN': ['TEXT']
        }
        
        existing_upper = existing_type.upper()
        required_upper = required_type.upper()
        
        return required_upper in upgrades.get(existing_upper, [])

class SchemaMigrationManager:
    """Manage database schema migrations"""
    
    def __init__(self, engine, session_maker):
        self.engine = engine
        self.session_maker = session_maker
        
    def apply_schema_changes(self, changes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Apply a list of schema changes
        
        Args:
            changes: List of change dictionaries
            
        Returns:
            Results dictionary with success/failure info
        """
        results = {
            'applied': [],
            'failed': [],
            'skipped': []
        }
        
        session = self.session_maker()
        
        try:
            for change in changes:
                try:
                    if change['operation'] == 'add':
                        self._add_column(change, session)
                        results['applied'].append(change)
                    elif change['operation'] == 'modify':
                        self._modify_column(change, session)
                        results['applied'].append(change)
                    elif change['operation'] == 'remove':
                        # Only remove if explicitly confirmed
                        logger.warning(f"Column removal requested but not auto-applied: {change}")
                        results['skipped'].append(change)
                    else:
                        logger.warning(f"Unknown operation: {change['operation']}")
                        results['skipped'].append(change)
                        
                except Exception as e:
                    logger.error(f"Failed to apply change {change}: {e}")
                    change['error'] = str(e)
                    results['failed'].append(change)
            
            # Record successful changes
            self._record_schema_changes(results['applied'], session)
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Schema migration failed: {e}")
            raise
        finally:
            session.close()
        
        return results
    
    def _add_column(self, change: Dict[str, Any], session: Session):
        """Add a new column to a table"""
        table_name = change['table_name']
        column_name = change['column_name']
        col_def = change['new_definition']
        
        sql_type = col_def['sql_type']
        nullable = "NULL" if col_def['nullable'] else "NOT NULL DEFAULT ''"
        
        # Handle different SQL dialects
        if 'postgresql' in str(self.engine.dialect):
            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type} {nullable}"
        elif 'sqlite' in str(self.engine.dialect):
            # SQLite doesn't support NOT NULL on added columns without default
            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type}"
        else:
            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type} {nullable}"
        
        logger.info(f"Adding column: {alter_sql}")
        session.execute(text(alter_sql))
    
    def _modify_column(self, change: Dict[str, Any], session: Session):
        """Modify an existing column"""
        table_name = change['table_name']
        column_name = change['column_name']
        new_def = change['new_definition']
        
        sql_type = new_def['sql_type']
        
        # Handle different SQL dialects
        if 'postgresql' in str(self.engine.dialect):
            alter_sql = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {sql_type}"
        elif 'sqlite' in str(self.engine.dialect):
            logger.warning(f"SQLite doesn't support column modification. Skipping {column_name}")
            return
        else:
            alter_sql = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} {sql_type}"
        
        logger.info(f"Modifying column: {alter_sql}")
        session.execute(text(alter_sql))
    
    def _record_schema_changes(self, changes: List[Dict[str, Any]], session: Session):
        """Record schema changes in the tracking table"""
        try:
            # Import here to avoid circular dependency
            from database_schema import SchemaChange
            
            for change in changes:
                schema_change = SchemaChange(
                    table_name=change['table_name'],
                    column_name=change['column_name'],
                    operation=change['operation'],
                    old_definition=change.get('old_definition'),
                    new_definition=change.get('new_definition'),
                    migration_applied=True,
                    applied_at=datetime.now()
                )
                session.add(schema_change)
                
        except Exception as e:
            logger.warning(f"Could not record schema changes: {e}")

class SchemaFeedbackSystem:
    """Provide feedback when schema changes are detected"""
    
    def __init__(self, 
                 pause_on_changes: bool = True,
                 notification_callback: Optional[Callable] = None):
        self.pause_on_changes = pause_on_changes
        self.notification_callback = notification_callback
        
    def handle_schema_changes(self, 
                            changes: List[Dict[str, Any]], 
                            scraper_name: str) -> bool:
        """
        Handle detected schema changes
        
        Args:
            changes: List of detected changes
            scraper_name: Name of the scraper that detected changes
            
        Returns:
            Boolean indicating whether to continue scraping
        """
        if not changes:
            return True
        
        logger.warning(f"Schema changes detected in {scraper_name}:")
        for change in changes:
            logger.warning(f"  {change['operation'].upper()}: {change['table_name']}.{change['column_name']}")
            logger.warning(f"    Reason: {change.get('reason', 'Unknown')}")
        
        # Send notification if callback provided
        if self.notification_callback:
            try:
                self.notification_callback(changes, scraper_name)
            except Exception as e:
                logger.error(f"Notification callback failed: {e}")
        
        # Pause scraping if configured
        if self.pause_on_changes:
            logger.critical(f"Scraping paused due to schema changes in {scraper_name}")
            logger.critical("Please review the changes and update the schema manually")
            logger.critical("Or configure auto-migration if you want changes applied automatically")
            return False
        
        return True

class DynamicColumnSystem:
    """Main interface for dynamic column management"""
    
    def __init__(self, 
                 engine,
                 session_maker,
                 auto_migrate: bool = False,
                 pause_on_changes: bool = True):
        self.engine = engine
        self.session_maker = session_maker
        self.auto_migrate = auto_migrate
        
        # Initialize components
        metadata = MetaData()
        self.detector = SchemaChangeDetector(engine, metadata)
        self.migration_manager = SchemaMigrationManager(engine, session_maker)
        self.feedback_system = SchemaFeedbackSystem(
            pause_on_changes=pause_on_changes,
            notification_callback=self._default_notification
        )
    
    def process_scraped_data(self, 
                           table_name: str, 
                           scraped_data: List[Dict[str, Any]],
                           scraper_name: str) -> Dict[str, Any]:
        """
        Process scraped data and handle any schema changes
        
        Args:
            table_name: Target table name
            scraped_data: Scraped data to process
            scraper_name: Name of the scraper
            
        Returns:
            Processing results dictionary
        """
        # Detect changes
        changes = self.detector.detect_changes_from_scraped_data(table_name, scraped_data)
        
        results = {
            'changes_detected': len(changes),
            'changes': changes,
            'migration_applied': False,
            'continue_scraping': True
        }
        
        if changes:
            # Handle feedback
            continue_scraping = self.feedback_system.handle_schema_changes(changes, scraper_name)
            results['continue_scraping'] = continue_scraping
            
            # Apply migrations if configured
            if self.auto_migrate and continue_scraping:
                migration_results = self.migration_manager.apply_schema_changes(changes)
                results['migration_results'] = migration_results
                results['migration_applied'] = len(migration_results['applied']) > 0
                
                if migration_results['failed']:
                    logger.error(f"Some migrations failed: {migration_results['failed']}")
                    results['continue_scraping'] = False
        
        return results
    
    def _default_notification(self, changes: List[Dict[str, Any]], scraper_name: str):
        """Default notification callback"""
        logger.info(f"NOTIFICATION: Schema changes detected in {scraper_name}")
        for change in changes:
            logger.info(f"  - {change['operation'].upper()} {change['column_name']} in {change['table_name']}")

# Column Handler Registry for dynamic handlers
class ColumnHandlerRegistry:
    """Registry for managing custom column handlers"""
    
    def __init__(self):
        self.handlers: Dict[str, Callable] = {}
        self._setup_default_handlers()
    
    def _setup_default_handlers(self):
        """Setup default column handlers"""
        
        def handle_player_name_and_id(cell_text: str, cell_html: str) -> Dict[str, Any]:
            """Handle player name with potential link to player page"""
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(cell_html, 'html.parser')
            link = soup.find('a')
            
            if link:
                name = link.get_text(strip=True)
                href = str(link.get('href', '') or '')
                player_id_match = re.search(r'/players/[a-z]/([^/]+)\.html', href)
                player_id = player_id_match.group(1) if player_id_match else None
                
                return {
                    'player_name': name,
                    'player_br_id': player_id,
                    'player_url': href
                }
            else:
                return {'player_name': cell_text.strip()}
        
        def handle_team_abbreviation(cell_text: str, cell_html: str) -> Dict[str, Any]:
            """Handle team abbreviation with potential link"""
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(cell_html, 'html.parser')
            link = soup.find('a')
            
            if link:
                team_name = link.get_text(strip=True)
                href = str(link.get('href', '') or '')
                team_abbrev_match = re.search(r'/teams/([A-Z]+)/', href)
                team_abbrev = team_abbrev_match.group(1) if team_abbrev_match else team_name
                
                return {
                    'team_name': team_name,
                    'team_abbreviation': team_abbrev,
                    'team_url': href
                }
            else:
                return {'team_abbreviation': cell_text.strip()}
        
        def handle_numeric_with_percentage(cell_text: str, cell_html: str) -> Dict[str, Any]:
            """Handle numeric values that might be percentages"""
            text = cell_text.strip()
            if not text or text in ['-', 'N/A']:
                return {'value': None}
            
            try:
                if '%' in text:
                    return {'value': float(text.replace('%', '')) / 100.0}
                else:
                    return {'value': float(text)}
            except ValueError:
                return {'value': None, 'raw_text': text}
        
        # Register default handlers
        self.register('player_name_and_id', handle_player_name_and_id)
        self.register('team_abbreviation', handle_team_abbreviation)
        self.register('numeric_with_percentage', handle_numeric_with_percentage)
    
    def register(self, name: str, handler: Callable):
        """Register a custom column handler"""
        self.handlers[name] = handler
        logger.info(f"Registered column handler: {name}")
    
    def get(self, name: str) -> Optional[Callable]:
        """Get a column handler by name"""
        return self.handlers.get(name)
    
    def list_handlers(self) -> List[str]:
        """List all available handlers"""
        return list(self.handlers.keys())

if __name__ == "__main__":
    # Example usage
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    # Create test database
    engine = create_engine("sqlite:///test_dynamic.db")
    SessionMaker = sessionmaker(bind=engine)
    
    # Create dynamic column system
    dynamic_system = DynamicColumnSystem(
        engine=engine,
        session_maker=SessionMaker,
        auto_migrate=True,
        pause_on_changes=False
    )
    
    # Test with sample data
    sample_data = [
        {'player_name': 'LeBron James', 'team': 'LAL', 'points': 25.5, 'new_stat': 'test'},
        {'player_name': 'Stephen Curry', 'team': 'GSW', 'points': 30.2, 'another_new_stat': 42}
    ]
    
    results = dynamic_system.process_scraped_data(
        table_name='test_table',
        scraped_data=sample_data,
        scraper_name='TestScraper'
    )
    
    print("Processing results:", results)