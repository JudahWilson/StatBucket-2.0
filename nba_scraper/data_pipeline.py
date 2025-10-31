"""
Data Pipeline Architecture

Provides separate functions per dataset with HTML download/extraction separation,
intermediate test tables, and optional direct-to-destination saving.
"""

import logging
import traceback
from typing import Dict, List, Any, Optional, Callable, Union
from datetime import datetime, date
from dataclasses import dataclass, field
from enum import Enum
import json
import uuid
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class SaveMode(Enum):
    """Data saving modes"""
    INTERMEDIATE_ONLY = "intermediate_only"
    DESTINATION_ONLY = "destination_only" 
    BOTH = "both"

class PipelineStatus(Enum):
    """Pipeline execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"

@dataclass
class DatasetConfig:
    """Configuration for a dataset scraping pipeline"""
    name: str
    description: str
    base_urls: List[str]
    url_generator: Optional[Callable[..., List[str]]] = None
    extractor_class: Optional[type] = None
    table_mappings: Dict[str, str] = field(default_factory=dict)  # intermediate -> destination table
    column_handlers: Dict[str, str] = field(default_factory=dict)  # column -> handler name
    dependencies: List[str] = field(default_factory=list)  # Other datasets this depends on
    schedule: Optional[str] = None  # Cron-like schedule
    enabled: bool = True

@dataclass
class PipelineResult:
    """Results from a pipeline execution"""
    dataset_name: str
    status: PipelineStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    urls_processed: int = 0
    urls_failed: int = 0
    records_extracted: int = 0
    records_saved: int = 0
    error_message: Optional[str] = None
    failed_urls: List[Dict[str, str]] = field(default_factory=list)
    schema_changes: List[Dict[str, Any]] = field(default_factory=list)
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))

class DataExtractorBase(ABC):
    """Base class for data extractors"""
    
    @abstractmethod
    def extract_from_html(self, html: str, url: str, **kwargs) -> List[Dict[str, Any]]:
        """Extract structured data from HTML"""
        pass
    
    @abstractmethod
    def get_table_name(self) -> str:
        """Get the target table name for this extractor"""
        pass

class DataSaverBase(ABC):
    """Base class for data savers"""
    
    @abstractmethod
    def save_to_intermediate(self, 
                           data: List[Dict[str, Any]], 
                           metadata: Dict[str, Any],
                           session: Any) -> int:
        """Save data to intermediate table"""
        pass
    
    @abstractmethod
    def save_to_destination(self, 
                          data: List[Dict[str, Any]], 
                          metadata: Dict[str, Any],
                          session: Any) -> int:
        """Save data to destination tables"""
        pass

class DatasetPipeline:
    """Individual dataset scraping pipeline"""
    
    def __init__(self, 
                 config: DatasetConfig,
                 downloader: Any,  # HTMLDownloader
                 extractor: DataExtractorBase,
                 saver: DataSaverBase,
                 dynamic_schema: Any,  # DynamicColumnSystem
                 session_maker: Callable):
        self.config = config
        self.downloader = downloader
        self.extractor = extractor
        self.saver = saver
        self.dynamic_schema = dynamic_schema
        self.session_maker = session_maker
        self.logger = logging.getLogger(f"{__name__}.{config.name}")
    
    def execute(self, 
                save_mode: SaveMode = SaveMode.INTERMEDIATE_ONLY,
                use_cache: bool = True,
                limit: Optional[int] = None,
                **kwargs) -> PipelineResult:
        """
        Execute the complete pipeline
        
        Args:
            save_mode: Where to save the data
            use_cache: Whether to use cached HTML
            limit: Maximum number of URLs to process (for testing)
            **kwargs: Additional arguments for URL generation
        """
        result = PipelineResult(
            dataset_name=self.config.name,
            status=PipelineStatus.RUNNING,
            start_time=datetime.now()
        )
        
        self.logger.info(f"Starting pipeline execution: {self.config.name}")
        
        try:
            # Generate URLs to scrape
            urls = self._generate_urls(**kwargs)
            if limit:
                urls = urls[:limit]
            
            self.logger.info(f"Generated {len(urls)} URLs to process")
            
            # Process each URL
            session = self.session_maker()
            
            try:
                for i, url in enumerate(urls, 1):
                    self.logger.info(f"Processing URL {i}/{len(urls)}: {url}")
                    
                    try:
                        # Download HTML
                        html = self.downloader.download(url, use_cache=use_cache)
                        
                        # Extract data
                        extracted_data = self.extractor.extract_from_html(html, url, **kwargs)
                        
                        if not extracted_data:
                            self.logger.warning(f"No data extracted from {url}")
                            continue
                        
                        result.records_extracted += len(extracted_data)
                        
                        # Handle schema changes
                        if self.dynamic_schema:
                            schema_result = self.dynamic_schema.process_scraped_data(
                                table_name=self.extractor.get_table_name(),
                                scraped_data=extracted_data,
                                scraper_name=self.config.name
                            )
                            
                            result.schema_changes.extend(schema_result.get('changes', []))
                            
                            # Stop if schema changes require attention
                            if not schema_result.get('continue_scraping', True):
                                result.status = PipelineStatus.PAUSED
                                result.error_message = "Pipeline paused due to schema changes"
                                break
                        
                        # Save data
                        metadata = {
                            'source_url': url,
                            'extraction_time': datetime.now(),
                            'dataset_name': self.config.name,
                            'execution_id': result.execution_id
                        }
                        
                        records_saved = 0
                        
                        if save_mode in [SaveMode.INTERMEDIATE_ONLY, SaveMode.BOTH]:
                            records_saved += self.saver.save_to_intermediate(
                                extracted_data, metadata, session
                            )
                        
                        if save_mode in [SaveMode.DESTINATION_ONLY, SaveMode.BOTH]:
                            records_saved += self.saver.save_to_destination(
                                extracted_data, metadata, session
                            )
                        
                        result.records_saved += records_saved
                        result.urls_processed += 1
                        
                        # Commit after each URL
                        session.commit()
                        
                    except Exception as e:
                        self.logger.error(f"Failed to process {url}: {e}")
                        result.failed_urls.append({
                            'url': url,
                            'error': str(e),
                            'traceback': traceback.format_exc()
                        })
                        result.urls_failed += 1
                        session.rollback()
                        continue
                
                # Final commit
                session.commit()
                
                if result.status == PipelineStatus.RUNNING:
                    result.status = PipelineStatus.COMPLETED
                    
            except Exception as e:
                session.rollback()
                raise
            finally:
                session.close()
                
        except Exception as e:
            result.status = PipelineStatus.FAILED
            result.error_message = str(e)
            self.logger.error(f"Pipeline execution failed: {e}")
            self.logger.error(traceback.format_exc())
        
        result.end_time = datetime.now()
        self.logger.info(f"Pipeline execution completed: {result}")
        
        return result
    
    def _generate_urls(self, **kwargs) -> List[str]:
        """Generate URLs to scrape"""
        if self.config.url_generator:
            return self.config.url_generator(**kwargs)
        else:
            return self.config.base_urls.copy()

class PipelineManager:
    """Manages multiple dataset pipelines"""
    
    def __init__(self, 
                 downloader: Any,
                 dynamic_schema: Any,
                 session_maker: Callable):
        self.downloader = downloader
        self.dynamic_schema = dynamic_schema
        self.session_maker = session_maker
        self.pipelines: Dict[str, DatasetPipeline] = {}
        self.execution_history: List[PipelineResult] = []
    
    def register_pipeline(self, pipeline: DatasetPipeline):
        """Register a dataset pipeline"""
        self.pipelines[pipeline.config.name] = pipeline
        logger.info(f"Registered pipeline: {pipeline.config.name}")
    
    def execute_pipeline(self, 
                        dataset_name: str,
                        save_mode: SaveMode = SaveMode.INTERMEDIATE_ONLY,
                        **kwargs) -> PipelineResult:
        """Execute a specific pipeline"""
        if dataset_name not in self.pipelines:
            raise ValueError(f"Pipeline not found: {dataset_name}")
        
        pipeline = self.pipelines[dataset_name]
        result = pipeline.execute(save_mode=save_mode, **kwargs)
        
        self.execution_history.append(result)
        return result
    
    def execute_all_pipelines(self, 
                            save_mode: SaveMode = SaveMode.INTERMEDIATE_ONLY,
                            respect_dependencies: bool = True,
                            **kwargs) -> Dict[str, PipelineResult]:
        """Execute all registered pipelines"""
        results = {}
        
        if respect_dependencies:
            # Execute in dependency order
            execution_order = self._resolve_dependencies()
        else:
            execution_order = list(self.pipelines.keys())
        
        for dataset_name in execution_order:
            if dataset_name in self.pipelines and self.pipelines[dataset_name].config.enabled:
                logger.info(f"Executing pipeline: {dataset_name}")
                result = self.execute_pipeline(dataset_name, save_mode, **kwargs)
                results[dataset_name] = result
                
                # Stop if pipeline failed and dependencies are respected
                if respect_dependencies and result.status == PipelineStatus.FAILED:
                    logger.error(f"Pipeline {dataset_name} failed, stopping execution")
                    break
        
        return results
    
    def _resolve_dependencies(self) -> List[str]:
        """Resolve pipeline dependencies to determine execution order"""
        # Simple topological sort
        visited = set()
        temp_visited = set()
        result = []
        
        def visit(name: str):
            if name in temp_visited:
                raise ValueError(f"Circular dependency detected involving {name}")
            if name in visited:
                return
            
            temp_visited.add(name)
            
            if name in self.pipelines:
                for dep in self.pipelines[name].config.dependencies:
                    visit(dep)
            
            temp_visited.remove(name)
            visited.add(name)
            result.append(name)
        
        for pipeline_name in self.pipelines:
            if pipeline_name not in visited:
                visit(pipeline_name)
        
        return result
    
    def get_pipeline_status(self, dataset_name: str) -> Optional[PipelineResult]:
        """Get the latest execution result for a pipeline"""
        for result in reversed(self.execution_history):
            if result.dataset_name == dataset_name:
                return result
        return None
    
    def list_pipelines(self) -> List[str]:
        """List all registered pipeline names"""
        return list(self.pipelines.keys())

# Helper classes for common NBA datasets
class NBADataExtractor(DataExtractorBase):
    """Base NBA data extractor"""
    
    def __init__(self, table_name: str, data_extractor: Any):
        self.table_name = table_name
        self.data_extractor = data_extractor
    
    def get_table_name(self) -> str:
        return self.table_name
    
    def extract_from_html(self, html: str, url: str, **kwargs) -> List[Dict[str, Any]]:
        """Extract data using the data extractor"""
        from bs4 import BeautifulSoup
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find the main data table (implementation depends on specific page type)
        table_id = kwargs.get('table_id')
        table_class = kwargs.get('table_class')
        
        data = self.data_extractor.extract_table_data(
            soup, table_id=table_id, table_class=table_class
        )
        
        # Add metadata
        for record in data:
            record['_source_url'] = url
            record['_extraction_time'] = datetime.now().isoformat()
        
        return data

class NBADataSaver(DataSaverBase):
    """NBA-specific data saver"""
    
    def save_to_intermediate(self, 
                           data: List[Dict[str, Any]], 
                           metadata: Dict[str, Any],
                           session: Any) -> int:
        """Save to intermediate table"""
        try:
            from database_schema import IntermediateData, ScrapingJob
            
            # Create scraping job record
            job = ScrapingJob(
                job_id=metadata['execution_id'],
                dataset_name=metadata['dataset_name'],
                url=metadata['source_url'],
                status='completed',
                completed_at=metadata['extraction_time'],
                records_scraped=len(data)
            )
            session.merge(job)
            
            # Save data records
            for record in data:
                intermediate = IntermediateData(
                    job_id=metadata['execution_id'],
                    table_name=metadata['dataset_name'],
                    source_url=metadata['source_url'],
                    raw_data=record,
                    created_at=metadata['extraction_time']
                )
                session.add(intermediate)
            
            session.flush()
            return len(data)
            
        except Exception as e:
            logger.error(f"Failed to save to intermediate table: {e}")
            raise
    
    def save_to_destination(self, 
                          data: List[Dict[str, Any]], 
                          metadata: Dict[str, Any],
                          session: Any) -> int:
        """Save to destination tables"""
        # This would be implemented based on specific table schemas
        # For now, just log that we would save to destination
        logger.info(f"Would save {len(data)} records to destination tables")
        return len(data)

# URL generators for common NBA data patterns
def generate_season_urls(base_url: str, seasons: List[int]) -> List[str]:
    """Generate URLs for multiple seasons"""
    urls = []
    for season in seasons:
        url = base_url.replace('{season}', str(season))
        urls.append(url)
    return urls

def generate_team_season_urls(base_url: str, 
                            teams: List[str], 
                            seasons: List[int]) -> List[str]:
    """Generate URLs for team-season combinations"""
    urls = []
    for team in teams:
        for season in seasons:
            url = base_url.replace('{team}', team).replace('{season}', str(season))
            urls.append(url)
    return urls

def generate_player_season_urls(base_url: str,
                              player_ids: List[str],
                              seasons: List[int]) -> List[str]:
    """Generate URLs for player-season combinations"""
    urls = []
    for player_id in player_ids:
        for season in seasons:
            url = base_url.replace('{player_id}', player_id).replace('{season}', str(season))
            urls.append(url)
    return urls

# Factory function to create common NBA pipelines
def create_nba_pipeline(name: str,
                       description: str,
                       base_urls: List[str],
                       table_name: str,
                       downloader: Any,
                       data_extractor: Any,
                       dynamic_schema: Any,
                       session_maker: Callable,
                       url_generator: Optional[Callable] = None,
                       column_handlers: Optional[Dict[str, str]] = None,
                       dependencies: Optional[List[str]] = None) -> DatasetPipeline:
    """Factory function to create NBA dataset pipelines"""
    
    config = DatasetConfig(
        name=name,
        description=description,
        base_urls=base_urls,
        url_generator=url_generator,
        column_handlers=column_handlers or {},
        dependencies=dependencies or []
    )
    
    extractor = NBADataExtractor(table_name, data_extractor)
    saver = NBADataSaver()
    
    return DatasetPipeline(
        config=config,
        downloader=downloader,
        extractor=extractor,
        saver=saver,
        dynamic_schema=dynamic_schema,
        session_maker=session_maker
    )

if __name__ == "__main__":
    # Example usage
    from scraping_framework import HTMLDownloader, DataExtractor, RateLimiter
    from dynamic_schema import DynamicColumnSystem
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    # Setup components
    engine = create_engine("sqlite:///nba_pipeline_test.db")
    SessionMaker = sessionmaker(bind=engine)
    
    downloader = HTMLDownloader(rate_limiter=RateLimiter(requests_per_second=0.5))
    data_extractor = DataExtractor()
    dynamic_schema = DynamicColumnSystem(engine, SessionMaker, auto_migrate=False)
    
    # Create pipeline manager
    manager = PipelineManager(downloader, dynamic_schema, SessionMaker)
    
    # Create sample pipeline
    pipeline = create_nba_pipeline(
        name="team_stats_2024",
        description="Team statistics for 2024 season",
        base_urls=["https://www.basketball-reference.com/leagues/NBA_2024.html"],
        table_name="team_season_stats",
        downloader=downloader,
        data_extractor=data_extractor,
        dynamic_schema=dynamic_schema,
        session_maker=SessionMaker
    )
    
    manager.register_pipeline(pipeline)
    
    # Execute pipeline
    result = manager.execute_pipeline("team_stats_2024", save_mode=SaveMode.INTERMEDIATE_ONLY, limit=1)
    print(f"Pipeline result: {result}")
    
    print(f"Available pipelines: {manager.list_pipelines()}")