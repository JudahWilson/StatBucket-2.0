from playwright.sync_api import sync_playwright
from scraping.Players import Players

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)  # Set to True for server environments
    page = browser.new_page()
    
    players_scraper = Players(
        webpage=page
    )
    players_scraper.run()