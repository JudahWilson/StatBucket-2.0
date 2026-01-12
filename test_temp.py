"""Test script to access basketball-reference.com using Playwright."""

from playwright.sync_api import sync_playwright
from statbucket.scraping.Players import Players

def test_basketball_reference(slug):
    """Test accessing basketball-reference.com homepage."""
    with sync_playwright() as p:
        # Launch browser in HEADLESS mode (perfect for servers)
        browser = p.chromium.launch(headless=True)  # Set to True for server environments
        page = browser.new_page()
        
        # Navigate to basketball reference
        response = page.goto(f'https://www.basketball-reference.com/{slug}')
        
        print(f"Status code: {response.status}")
        print(f"URL: {response.url}")
        print(f"OK: {response.ok}")
        
        if response.ok:
            title = page.title()
            print(f"Page title: {title}")
            
            # Try to get some content to verify we're not blocked
            content = page.content()
            with open('test_basketball_reference.html', 'w', encoding='utf-8') as f:
                f.write(content)
            
            if "basketball" in content.lower():
                print("SUCCESS: Page loaded successfully in headless mode!")
            else:
                print("WARNING: Page loaded but content may be blocked")
        else:
            print(f"FAILED: Status {response.status}")
        
        browser.close()

if __name__ == "__main__":
    test_basketball_reference("players/a/abdulka01.html")