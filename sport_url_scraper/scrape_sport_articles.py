#!/usr/bin/env python3
"""
Script to scrape title and content from all sport article URLs
Reads URLs from all_sport_urls.json and fetches article data for each URL.
"""

import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import time


class BBCArticleScraper:
    """Scraper for BBC articles"""
    
    def __init__(self):
        self.base_url = "https://www.bbc.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a webpage"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None
    
    def extract_title(self, soup: BeautifulSoup) -> str:
        """Extract article title"""
        # Multiple possible selectors for BBC titles
        title_selectors = [
            'h1.ssrcss-15xko80-StyledHeading',
            'h1.story-body__h1',
            'h1[data-testid="headline"]',
            'h1',
            'meta[property="og:title"]',
            'meta[name="twitter:title"]'
        ]
        
        for selector in title_selectors:
            elem = soup.select_one(selector)
            if elem:
                if elem.get('content'):
                    return elem.get('content').strip()
                text = elem.get_text().strip()
                if text:
                    return text
        
        return "Unknown Title"
    
    def extract_content(self, soup: BeautifulSoup) -> str:
        """Extract article content as a single string"""
        content_paragraphs = []
        
        # BBC uses various class names for article content
        content_selectors = [
            'div[data-component="text-block"]',
            '.story-body__inner p',
            '.article-body p',
            'article p',
            '.ssrcss-pv1rh6-Paragraph eq5iqo00'
        ]
        
        for selector in content_selectors:
            paragraphs = soup.select(selector)
            if paragraphs:
                for p in paragraphs:
                    text = p.get_text().strip()
                    if text and len(text) > 20:  # Filter out very short text
                        content_paragraphs.append(text)
                if content_paragraphs:  # If we found content with this selector, use it
                    break
        
        # Join all paragraphs with newlines
        return '\n\n'.join(content_paragraphs)
    
    def scrape_article(self, article_url: str) -> Optional[Dict]:
        """Scrape a single article from its URL"""
        soup = self.get_page(article_url)
        
        if not soup:
            return None
        
        title = self.extract_title(soup)
        content = self.extract_content(soup)
        
        if not content or len(content.strip()) < 50:  # Skip articles with very little content
            return None
        
        return {
            'title': title,
            'content': content
        }


def extract_label_from_url(url: str) -> str:
    """Extract detailed category (label) from URL
    
    Example: 
    https://www.bbc.com/sport/american-football/articles/... -> american-football
    https://www.bbc.com/sport/golf/articles/... -> golf
    """
    path = urlparse(url).path
    parts = [p for p in path.split('/') if p]
    
    # Look for pattern: /sport/{category}/articles/...
    if len(parts) >= 3 and parts[0] == 'sport':
        return parts[1]  # The category name
    
    return 'unknown'


def scrape_all_sport_articles(input_file: str, output_file: str, delay: float = 0.3):
    """
    Scrape all sport articles from URLs in input_file.
    
    Args:
        input_file: Path to all_sport_urls.json
        output_file: Path to output JSON file
        delay: Delay between requests in seconds (default: 0.3)
    """
    # Read the input JSON file
    print(f"Reading URLs from: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    urls = data.get('urls', [])
    total_urls = len(urls)
    
    print(f"Found {total_urls} URLs to scrape")
    print(f"Output file: {output_file}")
    print(f"Delay between requests: {delay}s")
    print("="*80)
    
    scraper = BBCArticleScraper()
    articles = []
    successful = 0
    failed = 0
    
    # Process each URL
    for idx, url in enumerate(urls, 1):
        if idx % 10 == 0 or idx == 1:
            print(f"[{idx}/{total_urls}] Processing... (Success: {successful}, Failed: {failed})")
        
        # Extract label from URL
        label = extract_label_from_url(url)
        
        # Scrape article
        try:
            article_data = scraper.scrape_article(url)
            
            if article_data:
                # Create article object with required fields
                article = {
                    'title': article_data['title'],
                    'content': article_data['content'],
                    'url': url,
                    'category': 'sport',
                    'label': label
                }
                articles.append(article)
                successful += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  Error scraping {url}: {e}")
            failed += 1
            continue
        
        # Rate limiting
        if idx < total_urls:  # Don't sleep after last URL
            time.sleep(delay)
    
    # Create output structure
    output_data = {
        'scraped_at': datetime.now().isoformat(),
        'total_urls': total_urls,
        'successful': successful,
        'failed': failed,
        'articles': articles
    }
    
    # Write to output file
    print(f"\nWriting {len(articles)} articles to: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print("="*80)
    print("SCRAPING COMPLETE")
    print("="*80)
    print(f"Total URLs processed: {total_urls}")
    print(f"Successfully scraped: {successful}")
    print(f"Failed: {failed}")
    print(f"Articles saved: {len(articles)}")
    print(f"Saved to: {output_file}")
    print("="*80)
    
    return output_data


if __name__ == "__main__":
    # Define file paths
    script_dir = Path(__file__).parent
    input_file = script_dir / "all_sport_urls.json"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = script_dir / f"sport_articles_scraped_{timestamp}.json"
    
    # Scrape articles
    scrape_all_sport_articles(str(input_file), str(output_file), delay=0.3)

