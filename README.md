# NBA Basketball Reference Scraper

A comprehensive system for scraping NBA data from basketball-reference.com with dynamic schema evolution and relationship management.

## Installation

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
# Install dependencies
uv sync

# Run the CLI
uv run python -m nba_scraper
```

## Commands

- To see all tertiary commands, run:

```bash
uv run list-scripts
```

## Features

- **Dynamic Schema**: Automatic column detection and database schema evolution
- **Rate Limiting**: Respectful scraping with configurable rate limits
- **Data Relationships**: Comprehensive database schema linking teams, players, games, seasons
- **Migration System**: Apply Python functions to update existing data
- **Modular Architecture**: Separate modules for different aspects of scraping

## Modules

- `database_schema.py` - Database models and relationships
- `scraping_framework.py` - HTML downloading and rate limiting
- `dynamic_schema.py` - Schema evolution and migration tracking
- `column_handlers.py` - Regex-based data processing
- `data_pipeline.py` - Pipeline architecture with batch processing
- `migration_system.py` - Migration management with rollback support
- `nba_scrapers_fixed.py` - NBA-specific data extractors

## Usage

```python
from nba_scraper import NBAScraper, HTMLDownloader, RateLimiter

# Create scraper with rate limiting
downloader = HTMLDownloader(rate_limiter=RateLimiter(requests_per_second=0.5))
scraper = NBAScraper(downloader, session_maker)

# Scrape season data
data = scraper.scrape_season_data(2024)
```

## Development

```bash
# Add development dependencies
uv add --dev pytest black ruff

# Run tests
uv run pytest

# Format code
uv run black nba_scraper/

# Lint code
uv run ruff check nba_scraper/
```
