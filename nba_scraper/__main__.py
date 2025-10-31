"""Simple CLI for the NBA scraper"""

def main():
    """Main CLI entry point"""
    print("NBA Basketball Reference Scraper v0.1.0")
    print("=" * 40)
    
    try:
        from nba_scraper import NBAScraper
        print("✓ Package loaded successfully")
        
        # Show available functionality
        print("\nAvailable modules:")
        print("  - database_schema.py: Database models and relationships")
        print("  - scraping_framework.py: HTML downloading and rate limiting")
        print("  - dynamic_schema.py: Schema evolution and change tracking")
        print("  - column_handlers.py: Data processing handlers")
        print("  - data_pipeline.py: Pipeline architecture")
        print("  - nba_scrapers_fixed.py: NBA-specific scrapers")
        
        print("\nUsage:")
        print("  uv run python -m nba_scraper")
        print("  or import nba_scraper in your Python code")
        
    except ImportError as e:
        print(f"✗ Import error: {e}")
        print("Make sure all dependencies are installed with: uv sync")

if __name__ == "__main__":
    main()