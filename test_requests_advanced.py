#!/usr/bin/env python3
"""Test script to access basketball-reference.com using advanced requests techniques."""

import requests
import time
from urllib.parse import urljoin

def test_basketball_reference_requests():
    """Test accessing basketball-reference.com homepage with advanced requests."""
    
    # Create a session to maintain cookies
    session = requests.Session()
    
    # Set comprehensive headers to mimic a real browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
    }
    
    session.headers.update(headers)
    
    try:
        print("Attempting to access basketball-reference.com...")
        
        # First try the main page
        url = 'https://www.basketball-reference.com/'
        response = session.get(url, timeout=10)
        
        print(f"Status code: {response.status_code}")
        print(f"URL: {response.url}")
        print(f"Headers received: {dict(response.headers)}")
        
        if response.status_code == 200:
            print("SUCCESS: Got 200 response!")
            # Check if we got actual content
            content = response.text
            if 'basketball' in content.lower() and len(content) > 1000:
                print("SUCCESS: Page content looks legitimate!")
                print(f"Content length: {len(content)}")
                # Look for specific basketball reference elements
                if 'basketball-reference.com' in content:
                    print("SUCCESS: Confirmed basketball-reference content!")
                else:
                    print("WARNING: Content may be from a redirect or block page")
            else:
                print(f"WARNING: Content seems suspicious. Length: {len(content)}")
                print("First 500 characters:")
                print(content[:500])
        else:
            print(f"FAILED: HTTP {response.status_code}")
            print(f"Response text: {response.text[:500]}")
            
    except requests.exceptions.RequestException as e:
        print(f"ERROR: {e}")

def test_with_referrer():
    """Test with a Google referrer to make it look more natural."""
    session = requests.Session()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.google.com/',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    session.headers.update(headers)
    
    print("\nTesting with Google referrer...")
    try:
        response = session.get('https://www.basketball-reference.com/', timeout=10)
        print(f"Status with referrer: {response.status_code}")
        if response.status_code == 200:
            print("SUCCESS with referrer approach!")
        
    except requests.exceptions.RequestException as e:
        print(f"ERROR with referrer: {e}")

if __name__ == "__main__":
    test_basketball_reference_requests()
    test_with_referrer()