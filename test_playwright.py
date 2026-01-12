#!/usr/bin/env python3
"""Test script to access basketball-reference.com using Playwright."""

from playwright.sync_api import sync_playwright

def test_basketball_reference():
    """Test accessing basketball-reference.com homepage."""
    with sync_playwright() as p:
        # Launch browser in HEADLESS mode (perfect for servers)
        browser = p.chromium.launch(headless=True)  # Set to True for server environments
        page = browser.new_page()
        
        # Navigate to basketball reference
        response = page.goto('https://www.basketball-reference.com/')
        
        print(f"Status code: {response.status}")
        print(f"URL: {response.url}")
        print(f"OK: {response.ok}")
        
        if response.ok:
            title = page.title()
            print(f"Page title: {title}")
            
            # Try to get some content to verify we're not blocked
            content = page.content()
            if "basketball" in content.lower():
                print("SUCCESS: Page loaded successfully in headless mode!")
            else:
                print("WARNING: Page loaded but content may be blocked")
        else:
            print(f"FAILED: Status {response.status}")
        
        browser.close()

if __name__ == "__main__":
    test_basketball_reference()