"""NBA Basketball Reference Scraper - Simple package initialization"""

__version__ = "0.1.0"

# Simple imports for the main components
try:
    from .database_schema import Base, Team, Player, Season
    from .scraping_framework import HTMLDownloader, RateLimiter
    from .nba_scrapers_fixed import NBAScraper
except ImportError:
    # Handle case where modules aren't moved yet
    pass

__all__ = [
    "Base",
    "Team", 
    "Player",
    "Season",
    "HTMLDownloader",
    "RateLimiter", 
    "NBAScraper",
]