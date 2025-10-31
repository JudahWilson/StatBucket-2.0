"""
Migration System

Python function application system for back-updating saved fields when new column handlers are added.
Supports batch processing, rollback capabilities, and comprehensive migration tracking.
"""

import logging
import traceback
import json
from typing import Dict, List, Any, Optional, Callable, Union, Iterator
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import inspect
from abc import ABC, abstractmethod
import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

class MigrationStatus(Enum):
    """Migration execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    PARTIAL = "partial"

class BatchStatus(Enum):
    """Batch processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class MigrationFunction:
    """Represents a migration function"""
    name: str
    description: str
    function: Callable[[Dict[str, Any]], Dict[str, Any]]
    target_columns: List[str]  # Columns this function will populate
    source_columns: List[str]  # Columns this function needs as input
    version: str
    created_at: datetime = field(default_factory=datetime.now)
    function_hash: str = field(default="")
    
    def __post_init__(self):
        """Calculate function hash for versioning"""
        if not self.function_hash:
            func_source = inspect.getsource(self.function)
            self.function_hash = hashlib.md5(func_source.encode()).hexdigest()

@dataclass
class BatchResult:
    """Results from processing a batch of records"""
    batch_id: str
    status: BatchStatus
    records_processed: int = 0
    records_failed: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    failed_record_ids: List[Any] = field(default_factory=list)

@dataclass
class MigrationResult:
    """Results from a complete migration execution"""
    migration_id: str
    migration_name: str
    status: MigrationStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    total_records: int = 0
    records_processed: int = 0
    records_failed: int = 0
    batches_completed: int = 0
    batches_failed: int = 0
    rollback_available: bool = False
    error_message: Optional[str] = None
    batch_results: List[BatchResult] = field(default_factory=list)
    
    def add_batch_result(self, batch_result: BatchResult):
        """Add a batch result and update totals"""
        self.batch_results.append(batch_result)
        self.records_processed += batch_result.records_processed
        self.records_failed += batch_result.records_failed
        
        if batch_result.status == BatchStatus.COMPLETED:
            self.batches_completed += 1
        elif batch_result.status == BatchStatus.FAILED:
            self.batches_failed += 1

class MigrationExecutor:
    """Executes migration functions on database records"""
    
    def __init__(self, 
                 session_maker: Callable,
                 batch_size: int = 1000,
                 max_workers: int = 4,
                 enable_rollback: bool = True):
        self.session_maker = session_maker
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.enable_rollback = enable_rollback
        self.active_migrations: Dict[str, MigrationResult] = {}
    
    def execute_migration(self,
                         migration_func: MigrationFunction,
                         table_name: str,
                         filter_conditions: Optional[Dict[str, Any]] = None,
                         dry_run: bool = False,
                         parallel: bool = True) -> MigrationResult:
        """
        Execute a migration function on database records
        
        Args:
            migration_func: The migration function to apply
            table_name: Target table name
            filter_conditions: Optional filters for which records to migrate
            dry_run: If True, don't actually save changes
            parallel: Whether to process batches in parallel
        """
        migration_id = str(uuid.uuid4())
        
        result = MigrationResult(
            migration_id=migration_id,
            migration_name=migration_func.name,
            status=MigrationStatus.RUNNING,
            start_time=datetime.now(),
            rollback_available=self.enable_rollback and not dry_run
        )
        
        self.active_migrations[migration_id] = result
        
        logger.info(f"Starting migration: {migration_func.name} on {table_name}")
        
        try:
            # Get total record count
            session = self.session_maker()
            try:
                result.total_records = self._get_record_count(
                    session, table_name, filter_conditions
                )
                logger.info(f"Total records to migrate: {result.total_records}")
                
                if result.total_records == 0:
                    result.status = MigrationStatus.COMPLETED
                    result.end_time = datetime.now()
                    return result
                
                # Create backup table if rollback is enabled
                backup_table_name = None
                if self.enable_rollback and not dry_run:
                    backup_table_name = f"{table_name}_backup_{migration_id[:8]}"
                    self._create_backup_table(session, table_name, backup_table_name)
                    logger.info(f"Created backup table: {backup_table_name}")
                
                session.commit()
                
                # Process in batches
                batch_generator = self._generate_batches(
                    table_name, filter_conditions, result.total_records
                )
                
                if parallel and self.max_workers > 1:
                    self._process_batches_parallel(
                        batch_generator, migration_func, table_name, 
                        dry_run, result
                    )
                else:
                    self._process_batches_sequential(
                        batch_generator, migration_func, table_name, 
                        dry_run, result
                    )
                
                # Determine final status
                if result.records_failed == 0:
                    result.status = MigrationStatus.COMPLETED
                elif result.records_processed > 0:
                    result.status = MigrationStatus.PARTIAL
                else:
                    result.status = MigrationStatus.FAILED
                
                # Log migration to database
                if not dry_run:
                    self._log_migration(session, migration_func, result, backup_table_name)
                    session.commit()
                
            finally:
                session.close()
                
        except Exception as e:
            result.status = MigrationStatus.FAILED
            result.error_message = str(e)
            logger.error(f"Migration failed: {e}")
            logger.error(traceback.format_exc())
        
        result.end_time = datetime.now()
        
        logger.info(f"Migration completed: {result}")
        
        return result
    
    def _get_record_count(self, 
                         session: Any, 
                         table_name: str, 
                         filter_conditions: Optional[Dict[str, Any]]) -> int:
        """Get total count of records to migrate"""
        from sqlalchemy import text
        
        query = f"SELECT COUNT(*) FROM {table_name}"
        params = {}
        
        if filter_conditions:
            conditions = []
            for column, value in filter_conditions.items():
                if isinstance(value, list):
                    placeholders = ','.join([f':param_{i}' for i in range(len(value))])
                    conditions.append(f"{column} IN ({placeholders})")
                    for i, v in enumerate(value):
                        params[f'param_{i}'] = v
                else:
                    conditions.append(f"{column} = :param_{column}")
                    params[f'param_{column}'] = value
            
            query += f" WHERE {' AND '.join(conditions)}"
        
        result = session.execute(text(query), params)
        return result.scalar()
    
    def _generate_batches(self, 
                         table_name: str, 
                         filter_conditions: Optional[Dict[str, Any]],
                         total_records: int) -> Iterator[Dict[str, Any]]:
        """Generate batch parameters for processing"""
        num_batches = (total_records + self.batch_size - 1) // self.batch_size
        
        for batch_num in range(num_batches):
            offset = batch_num * self.batch_size
            yield {
                'batch_id': f"batch_{batch_num:04d}",
                'table_name': table_name,
                'filter_conditions': filter_conditions,
                'offset': offset,
                'limit': self.batch_size,
                'batch_num': batch_num,
                'total_batches': num_batches
            }
    
    def _process_batches_sequential(self, 
                                  batch_generator: Iterator[Dict[str, Any]],
                                  migration_func: MigrationFunction,
                                  table_name: str,
                                  dry_run: bool,
                                  result: MigrationResult):
        """Process batches sequentially"""
        for batch_params in batch_generator:
            batch_result = self._process_batch(
                batch_params, migration_func, table_name, dry_run
            )
            result.add_batch_result(batch_result)
            
            logger.info(f"Completed batch {batch_params['batch_num']+1}/"
                       f"{batch_params['total_batches']}: "
                       f"{batch_result.records_processed} processed, "
                       f"{batch_result.records_failed} failed")
    
    def _process_batches_parallel(self, 
                                 batch_generator: Iterator[Dict[str, Any]],
                                 migration_func: MigrationFunction,
                                 table_name: str,
                                 dry_run: bool,
                                 result: MigrationResult):
        """Process batches in parallel"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all batches
            future_to_batch = {
                executor.submit(
                    self._process_batch, 
                    batch_params, 
                    migration_func, 
                    table_name, 
                    dry_run
                ): batch_params
                for batch_params in batch_generator
            }
            
            # Process completed batches
            for future in as_completed(future_to_batch):
                batch_params = future_to_batch[future]
                try:
                    batch_result = future.result()
                    result.add_batch_result(batch_result)
                    
                    logger.info(f"Completed batch {batch_params['batch_num']+1}/"
                               f"{batch_params['total_batches']}: "
                               f"{batch_result.records_processed} processed, "
                               f"{batch_result.records_failed} failed")
                    
                except Exception as e:
                    logger.error(f"Batch {batch_params['batch_id']} failed: {e}")
                    failed_batch = BatchResult(
                        batch_id=batch_params['batch_id'],
                        status=BatchStatus.FAILED,
                        error_message=str(e)
                    )
                    result.add_batch_result(failed_batch)
    
    def _process_batch(self, 
                      batch_params: Dict[str, Any],
                      migration_func: MigrationFunction,
                      table_name: str,
                      dry_run: bool) -> BatchResult:
        """Process a single batch of records"""
        batch_result = BatchResult(
            batch_id=batch_params['batch_id'],
            status=BatchStatus.PROCESSING,
            start_time=datetime.now()
        )
        
        session = self.session_maker()
        
        try:
            # Fetch batch records
            records = self._fetch_batch_records(session, batch_params)
            
            for record in records:
                try:
                    # Convert record to dict
                    record_dict = self._record_to_dict(record)
                    
                    # Apply migration function
                    updated_dict = migration_func.function(record_dict)
                    
                    # Update record if not dry run
                    if not dry_run:
                        self._update_record(session, record, updated_dict, migration_func)
                    
                    batch_result.records_processed += 1
                    
                except Exception as e:
                    logger.error(f"Failed to migrate record {getattr(record, 'id', 'unknown')}: {e}")
                    batch_result.records_failed += 1
                    batch_result.failed_record_ids.append(getattr(record, 'id', None))
            
            if not dry_run:
                session.commit()
            
            batch_result.status = BatchStatus.COMPLETED
            
        except Exception as e:
            session.rollback()
            batch_result.status = BatchStatus.FAILED
            batch_result.error_message = str(e)
            logger.error(f"Batch {batch_params['batch_id']} failed: {e}")
        
        finally:
            session.close()
            batch_result.end_time = datetime.now()
        
        return batch_result
    
    def _fetch_batch_records(self, session: Any, batch_params: Dict[str, Any]) -> List[Any]:
        """Fetch records for a batch"""
        from sqlalchemy import text
        
        table_name = batch_params['table_name']
        filter_conditions = batch_params.get('filter_conditions')
        offset = batch_params['offset']
        limit = batch_params['limit']
        
        query = f"SELECT * FROM {table_name}"
        params = {}
        
        if filter_conditions:
            conditions = []
            for column, value in filter_conditions.items():
                if isinstance(value, list):
                    placeholders = ','.join([f':param_{i}' for i in range(len(value))])
                    conditions.append(f"{column} IN ({placeholders})")
                    for i, v in enumerate(value):
                        params[f'param_{i}'] = v
                else:
                    conditions.append(f"{column} = :param_{column}")
                    params[f'param_{column}'] = value
            
            query += f" WHERE {' AND '.join(conditions)}"
        
        query += f" LIMIT {limit} OFFSET {offset}"
        
        result = session.execute(text(query), params)
        return result.fetchall()
    
    def _record_to_dict(self, record: Any) -> Dict[str, Any]:
        """Convert database record to dictionary"""
        if hasattr(record, '_asdict'):
            return record._asdict()
        elif hasattr(record, '__dict__'):
            return {k: v for k, v in record.__dict__.items() if not k.startswith('_')}
        else:
            # Handle Row objects from SQLAlchemy
            return dict(record._mapping)
    
    def _update_record(self, 
                      session: Any,
                      record: Any, 
                      updated_dict: Dict[str, Any],
                      migration_func: MigrationFunction):
        """Update a database record with migrated values"""
        from sqlalchemy import text
        
        # Only update the target columns
        updates = []
        params = {}
        
        for column in migration_func.target_columns:
            if column in updated_dict:
                updates.append(f"{column} = :new_{column}")
                params[f'new_{column}'] = updated_dict[column]
        
        if updates and hasattr(record, 'id'):
            # Assume all tables have an 'id' primary key
            table_name = record.__table__.name
            update_query = f"UPDATE {table_name} SET {', '.join(updates)} WHERE id = :record_id"
            params['record_id'] = record.id
            
            session.execute(text(update_query), params)
    
    def _create_backup_table(self, session: Any, table_name: str, backup_table_name: str):
        """Create backup table for rollback capability"""
        from sqlalchemy import text
        
        # Create backup table as copy of original
        backup_query = f"CREATE TABLE {backup_table_name} AS SELECT * FROM {table_name}"
        session.execute(text(backup_query))
    
    def _log_migration(self, 
                      session: Any,
                      migration_func: MigrationFunction,
                      result: MigrationResult,
                      backup_table_name: Optional[str]):
        """Log migration execution to database"""
        try:
            from database_schema import DataMigration
            
            migration_log = DataMigration(
                migration_id=result.migration_id,
                migration_name=migration_func.name,
                description=migration_func.description,
                function_hash=migration_func.function_hash,
                target_columns=migration_func.target_columns,
                source_columns=migration_func.source_columns,
                status=result.status.value,
                records_processed=result.records_processed,
                records_failed=result.records_failed,
                backup_table_name=backup_table_name,
                executed_at=result.start_time,
                completed_at=result.end_time
            )
            
            session.add(migration_log)
            
        except ImportError:
            logger.warning("DataMigration table not available, migration not logged")
    
    def rollback_migration(self, migration_id: str) -> bool:
        """Rollback a migration using backup table"""
        session = self.session_maker()
        
        try:
            from database_schema import DataMigration
            from sqlalchemy import text
            
            # Find migration record
            migration = session.query(DataMigration).filter(
                DataMigration.migration_id == migration_id
            ).first()
            
            if not migration or not migration.backup_table_name:
                logger.error(f"No rollback available for migration {migration_id}")
                return False
            
            # Get original table name from backup table name
            backup_name = migration.backup_table_name
            original_name = backup_name.replace(f"_backup_{migration_id[:8]}", "")
            
            # Restore from backup
            logger.info(f"Rolling back migration {migration_id}")
            
            # Drop current table and rename backup
            session.execute(text(f"DROP TABLE {original_name}"))
            session.execute(text(f"ALTER TABLE {backup_name} RENAME TO {original_name}"))
            
            # Update migration status
            migration.status = MigrationStatus.ROLLED_BACK.value
            migration.rolled_back_at = datetime.now()
            
            session.commit()
            
            logger.info(f"Successfully rolled back migration {migration_id}")
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to rollback migration {migration_id}: {e}")
            return False
        
        finally:
            session.close()

class MigrationManager:
    """High-level migration management"""
    
    def __init__(self, 
                 session_maker: Callable,
                 column_handler_system: Any = None):
        self.session_maker = session_maker
        self.column_handler_system = column_handler_system
        self.executor = MigrationExecutor(session_maker)
        self.registered_functions: Dict[str, MigrationFunction] = {}
    
    def register_migration_function(self, migration_func: MigrationFunction):
        """Register a migration function"""
        self.registered_functions[migration_func.name] = migration_func
        logger.info(f"Registered migration function: {migration_func.name}")
    
    def create_column_handler_migration(self, 
                                      handler_name: str,
                                      table_name: str,
                                      description: str = "") -> Optional[MigrationFunction]:
        """Create migration function from a column handler"""
        if not self.column_handler_system:
            logger.error("Column handler system not available")
            return None
        
        handler = self.column_handler_system.get_handler(handler_name)
        if not handler:
            logger.error(f"Handler not found: {handler_name}")
            return None
        
        def migration_function(record: Dict[str, Any]) -> Dict[str, Any]:
            """Apply column handler to record"""
            try:
                # Create a mock BeautifulSoup object if needed
                from bs4 import BeautifulSoup
                
                # If we have HTML in the record, parse it
                html_content = record.get('raw_html', record.get('html', ''))
                if html_content:
                    soup = BeautifulSoup(html_content, 'html.parser')
                    return handler.process_columns(soup, record.copy())
                else:
                    # Apply handler to existing data
                    return handler.process_columns(None, record.copy())
                    
            except Exception as e:
                logger.error(f"Migration function failed for {handler_name}: {e}")
                return record
        
        migration_func = MigrationFunction(
            name=f"apply_handler_{handler_name}_{table_name}",
            description=description or f"Apply {handler_name} handler to {table_name}",
            function=migration_function,
            target_columns=handler.output_columns,
            source_columns=handler.input_columns,
            version="1.0"
        )
        
        return migration_func
    
    def apply_handler_to_table(self, 
                              handler_name: str,
                              table_name: str,
                              filter_conditions: Optional[Dict[str, Any]] = None,
                              dry_run: bool = False) -> MigrationResult:
        """Apply a column handler to existing table data"""
        
        # Create migration function
        migration_func = self.create_column_handler_migration(
            handler_name, table_name,
            f"Back-apply {handler_name} to existing {table_name} data"
        )
        
        if not migration_func:
            raise ValueError(f"Could not create migration for handler {handler_name}")
        
        # Execute migration
        return self.executor.execute_migration(
            migration_func, table_name, filter_conditions, dry_run
        )
    
    def get_migration_history(self, 
                             table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get migration history"""
        session = self.session_maker()
        
        try:
            from database_schema import DataMigration
            
            query = session.query(DataMigration)
            
            if table_name:
                # This would need to be enhanced to track table names in migrations
                pass
            
            migrations = query.order_by(DataMigration.executed_at.desc()).all()
            
            return [
                {
                    'migration_id': m.migration_id,
                    'migration_name': m.migration_name,
                    'description': m.description,
                    'status': m.status,
                    'records_processed': m.records_processed,
                    'records_failed': m.records_failed,
                    'executed_at': m.executed_at,
                    'completed_at': m.completed_at,
                    'can_rollback': m.backup_table_name is not None
                }
                for m in migrations
            ]
            
        except ImportError:
            logger.warning("DataMigration table not available")
            return []
        
        finally:
            session.close()

# Example migration functions for NBA data
def extract_player_id_from_url(record: Dict[str, Any]) -> Dict[str, Any]:
    """Extract player ID from basketball-reference URL"""
    import re
    
    updated = record.copy()
    
    url = record.get('player_url', record.get('url', ''))
    if url:
        # Extract player ID from URL like /players/j/jamesle01.html
        match = re.search(r'/players/[a-z]/([^.]+)\.html', url)
        if match:
            updated['player_id'] = match.group(1)
    
    return updated

def parse_game_date(record: Dict[str, Any]) -> Dict[str, Any]:
    """Parse game date from various formats"""
    from datetime import datetime
    
    updated = record.copy()
    
    date_str = record.get('date', record.get('game_date', ''))
    if date_str and isinstance(date_str, str):
        try:
            # Try different date formats
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%b %d, %Y']:
                try:
                    parsed_date = datetime.strptime(date_str.strip(), fmt)
                    updated['parsed_date'] = parsed_date.date()
                    updated['season_year'] = parsed_date.year if parsed_date.month >= 10 else parsed_date.year - 1
                    break
                except ValueError:
                    continue
        except Exception as e:
            logger.warning(f"Could not parse date '{date_str}': {e}")
    
    return updated

def calculate_age_on_date(record: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate player age on a specific date"""
    from datetime import datetime
    
    updated = record.copy()
    
    birth_date = record.get('birth_date')
    game_date = record.get('game_date', record.get('parsed_date'))
    
    if birth_date and game_date:
        try:
            if isinstance(birth_date, str):
                birth_date = datetime.strptime(birth_date, '%Y-%m-%d').date()
            if isinstance(game_date, str):
                game_date = datetime.strptime(game_date, '%Y-%m-%d').date()
            
            age_days = (game_date - birth_date).days
            updated['age_on_date'] = round(age_days / 365.25, 2)
            
        except Exception as e:
            logger.warning(f"Could not calculate age: {e}")
    
    return updated

if __name__ == "__main__":
    # Example usage
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    engine = create_engine("sqlite:///nba_migration_test.db")
    SessionMaker = sessionmaker(bind=engine)
    
    # Create migration manager
    manager = MigrationManager(SessionMaker)
    
    # Register example migration functions
    player_id_migration = MigrationFunction(
        name="extract_player_id",
        description="Extract player ID from basketball-reference URLs",
        function=extract_player_id_from_url,
        target_columns=["player_id"],
        source_columns=["player_url", "url"],
        version="1.0"
    )
    
    date_migration = MigrationFunction(
        name="parse_dates",
        description="Parse game dates and calculate season year",
        function=parse_game_date,
        target_columns=["parsed_date", "season_year"],
        source_columns=["date", "game_date"],
        version="1.0"
    )
    
    manager.register_migration_function(player_id_migration)
    manager.register_migration_function(date_migration)
    
    print(f"Registered {len(manager.registered_functions)} migration functions")
    print("Available migrations:", list(manager.registered_functions.keys()))