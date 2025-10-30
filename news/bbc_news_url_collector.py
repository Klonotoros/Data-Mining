"""
BBC News URL Collector
Only collects article URLs from BBC News - no content scraping
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import time
import os
import sys
import glob
from datetime import datetime
from typing import Set
from pathlib import Path

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()


class BBCNewsURLCollector:
    """Collects article URLs from BBC News"""
    
    def __init__(self):
        self.base_url = "https://www.bbc.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def get_page(self, url: str) -> BeautifulSoup:
        """Fetch and parse a webpage"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None
    
    def is_news_article_url(self, url: str) -> bool:
        """Check if URL is a news article URL"""
        # Accept /news/articles/ pattern and other news article patterns
        if '/news/articles/' not in url and '/news/' not in url:
            return False
        
        url_lower = url.lower()
        
        # Exclude category pages, live pages, etc.
        exclude_patterns = [
            '/news/events/',
            '/news/live/',
            '/news/video/',
            '/news/audio/',
            '/news/pictures/',
            '/news/in-pictures/',
            '/topics/',
            '/live/',
            '/watch/', 
            '/listen/',
            '/news/$',  # Just the /news/ page itself
            '/news/uk',
            '/news/world',
            '/news/election',
        ]
        
        # Must not match exclude patterns
        if any(pattern in url_lower for pattern in exclude_patterns):
            return False
        
        # Must match article patterns
        url_parts = [p for p in url.split('/') if p]
        
        # Pattern 1: /news/articles/... (definite articles) - PRIMARY PATTERN
        if '/news/articles/' in url_lower:
            return True
        
        # Pattern 2: /news/YYYY/MM/DD/... (dated articles)
        if len(url_parts) >= 5 and url_parts[1] == 'news':
            # Check if there's a date-like pattern (YYYY/MM/DD)
            if len(url_parts) >= 4 and url_parts[2].isdigit() and len(url_parts[2]) == 4:  # Year
                return True
        
        # Pattern 3: /news/category/numeric-id or slug (article IDs)
        # Be more careful here - only accept if it looks like an article
        if len(url_parts) >= 3 and url_parts[1] == 'news':
            last_part = url_parts[-1]
            # Accept if it's a numeric ID or looks like an article slug
            if last_part.isdigit() or (len(last_part) > 8 and any(c.isdigit() for c in last_part)):
                # Exclude common non-article patterns
                excluded_news_slugs = ['news', 'uk', 'world', 'sport', 'business', 'culture',
                                     'arts', 'travel', 'earth', 'science', 'technology']
                if last_part not in excluded_news_slugs:
                    return True
        
        return False
    
    def url_belongs_to_news(self, url: str) -> bool:
        """Check if a URL belongs to news section"""
        url_lower = url.lower()
        
        # Must contain /news/ or /news/articles/
        if '/news/' not in url_lower:
            return False
        
        # Exclude other sections that might have "news" in the path
        if any(section in url_lower for section in ['/sport/', '/business/', '/culture/', '/arts/']):
            return False
        
        return True
    
    def collect_urls_from_news(self, max_urls: int = 10000, save_on_interrupt: bool = False, timestamp: str = None) -> Set[str]:
        """Collect article URLs from BBC News"""
        news_url = "https://www.bbc.com/news"
        news_articles_url = "https://www.bbc.com/news/articles"
        
        article_urls = set()
        # Start with both the main news page and articles page
        pages_to_check = [news_url, news_articles_url]
        pages_checked = set([news_url, news_articles_url])
        
        print(f"    Collecting URLs from: {news_url}")
        print(f"    Also checking: {news_articles_url}")
        print(f"    Target: {max_urls} unique article URLs")
        print(f"    Category: news")
        
        page_num = 0
        # Track URL count stability: if it doesn't change for 3 consecutive 10-page intervals, stop
        last_url_count_at_interval = None  # Will be set after first interval
        consecutive_unchanged_intervals = 0
        
        try:
            while pages_to_check:
                # Stop if we've found enough URLs
                if len(article_urls) >= max_urls:
                    print(f"    [OK] Reached {max_urls} URLs limit. Stopping search.")
                    break
                
                current_page = pages_to_check.pop(0)
                page_num += 1
                
                if page_num % 10 == 0:
                    current_url_count = len(article_urls)
                    print(f"      [Page {page_num}] Found {current_url_count} URLs so far...")
                    
                    # Check if URL count has changed since last 10-page interval
                    if last_url_count_at_interval is not None:
                        # We have a previous interval to compare with
                        if current_url_count == last_url_count_at_interval:
                            consecutive_unchanged_intervals += 1
                            if consecutive_unchanged_intervals >= 3:
                                print(f"    [OK] URL count unchanged for 3 consecutive intervals (30 pages). Stopping search.")
                                print(f"    [OK] Final count: {current_url_count} URLs after checking {page_num} pages")
                                break
                        else:
                            # Reset counter if URL count changed
                            consecutive_unchanged_intervals = 0
                            last_url_count_at_interval = current_url_count
                    else:
                        # First interval (page 10) - just store the count, don't check yet
                        last_url_count_at_interval = current_url_count
                
                soup = self.get_page(current_page)
                if not soup:
                    continue
                
                # Find article links
                links = soup.find_all('a', href=True)
                
                for link in links:
                    # Stop if we've reached the limit
                    if len(article_urls) >= max_urls:
                        break
                    
                    href = link.get('href')
                    if not href:
                        continue
                    
                    # Make absolute URL
                    if href.startswith('http'):
                        full_url = href
                    else:
                        full_url = urljoin(self.base_url, href)
                    
                    # Clean URL
                    clean_url = full_url.split('#')[0].split('?')[0]
                    
                    # Check if it's a news article URL
                    if self.is_news_article_url(clean_url) and self.url_belongs_to_news(clean_url):
                        article_urls.add(clean_url)
                        
                        # Stop immediately if we hit the limit after adding a URL
                        if len(article_urls) >= max_urls:
                            print(f"    [OK] Reached {max_urls} URLs. Stopping search.")
                            break
                    
                    # Check if it's a pagination link or related news page
                    # Accept URLs that start with news URL or are pagination links
                    if (clean_url.startswith(news_url) or '/news/' in clean_url) and clean_url not in pages_checked:
                        # Look for pagination indicators
                        if any(indicator in clean_url.lower() for indicator in ['/page/', '/p/', '?page=', '?p=', '/archive', '/articles']):
                            if clean_url not in pages_checked:
                                pages_to_check.append(clean_url)
                                pages_checked.add(clean_url)
                        # Check for deeper paths that might have articles (but not too deep)
                        elif not any(exclude in clean_url.lower() for exclude in ['/live/', '/video/', '/audio/', '/pictures/']):
                            # Only follow paths that look like they might lead to articles
                            if '/articles' in clean_url.lower() or any(year in clean_url for year in ['/202', '/201']):
                                pages_to_check.append(clean_url)
                                pages_checked.add(clean_url)
                    
                    # Look for date-based archive links (only within news section)
                    if clean_url.startswith(news_url) and clean_url not in pages_checked:
                        if any(pattern in clean_url.lower() for pattern in ['/archive', '/articles', '/202', '/201']):
                            if clean_url not in pages_checked:
                                pages_to_check.append(clean_url)
                                pages_checked.add(clean_url)
                
                # Look for "Load more", "Next page", "Archive" links
                next_link_keywords = ['more', 'next', 'archive', 'older', 'previous', 'earlier']
                for next_link in soup.find_all('a', href=True):
                    link_text = next_link.get_text().lower() if next_link.get_text() else ''
                    href = next_link.get('href')
                    
                    if href and any(keyword in link_text for keyword in next_link_keywords):
                        if href.startswith('http'):
                            next_url = href
                        else:
                            next_url = urljoin(self.base_url, href)
                        clean_next_url = next_url.split('#')[0].split('?')[0]
                        
                        # Only add pagination links within the news section
                        if ('/news/' in clean_next_url or clean_next_url.startswith(news_url)) and clean_next_url not in pages_checked:
                            pages_to_check.append(clean_next_url)
                            pages_checked.add(clean_next_url)
                
                # Look for date-based navigation links (year/month archives)
                for date_link in soup.find_all('a', href=True):
                    href = date_link.get('href')
                    if href:
                        if href.startswith('http'):
                            date_url = href
                        else:
                            date_url = urljoin(self.base_url, href)
                        clean_date_url = date_url.split('#')[0].split('?')[0]
                        
                        # Check if URL contains date patterns (only within news section)
                        if '/news/' in clean_date_url:
                            if any(year in clean_date_url for year in ['/202', '/201', '/2020', '/2021', '/2022', '/2023', '/2024', '/2025']):
                                if clean_date_url not in pages_checked:
                                    pages_to_check.append(clean_date_url)
                                    pages_checked.add(clean_date_url)
                
                # Check if we've reached the limit before continuing to next page
                if len(article_urls) >= max_urls:
                    break
                
                time.sleep(0.1)  # Small delay between page checks
            
            print(f"    [OK] Complete! Found {len(article_urls)} unique article URLs after checking {page_num} pages\n")
        except KeyboardInterrupt:
            print(f"\n    [WARNING] Interrupted! Saving {len(article_urls)} URLs collected so far...")
            print(f"    [OK] Partial collection: {len(article_urls)} unique article URLs after checking {page_num} pages\n")
            
            # Save partial data if requested
            if save_on_interrupt and len(article_urls) > 0 and timestamp:
                category_data = {
                    'category': 'news',
                    'category_url': news_url,
                    'url_count': len(article_urls),
                    'urls': sorted(list(article_urls))
                }
                try:
                    checkpoint_filename = save_news_checkpoint(category_data, timestamp)
                    print(f"    [OK] Partial checkpoint saved: {checkpoint_filename}")
                except Exception as e:
                    print(f"    [WARNING] Failed to save partial checkpoint: {e}")
            
            # Re-raise to allow main function to handle saving
            raise
        
        return article_urls


def save_news_checkpoint(news_data: dict, timestamp: str, output_dir: Path = None):
    """Save a checkpoint JSON file for news URLs"""
    # Use script directory if not specified
    if output_dir is None:
        output_dir = SCRIPT_DIR
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    filename = output_dir / f'bbc_news_{news_data["url_count"]}_urls_{timestamp}.json'
    
    checkpoint_data = {
        'scraped_at': datetime.now().isoformat(),
        'category': news_data['category'],
        'category_url': news_data['category_url'],
        'url_count': news_data['url_count'],
        'urls': news_data['urls']
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
    
    return str(filename)


def collect_news_urls(max_urls: int = 10000):
    """Collect article URLs from BBC News"""
    
    collector = BBCNewsURLCollector()
    
    # Create timestamp for all files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("="*80)
    print("BBC NEWS URL COLLECTOR")
    print("="*80)
    print(f"Collecting URLs from: https://www.bbc.com/news")
    print(f"Focus: /news/articles/ pattern")
    print(f"Target: {max_urls} URLs")
    print(f"Mode: URL collection only (no content scraping)")
    print("="*80)
    print()
    
    try:
        print("[1/1] Category: news")
        
        news_urls = collector.collect_urls_from_news(
            max_urls=max_urls,
            save_on_interrupt=True,
            timestamp=timestamp
        )
        
        news_data = {
            'category': 'news',
            'category_url': 'https://www.bbc.com/news',
            'url_count': len(news_urls),
            'urls': sorted(list(news_urls))
        }
        
        # Save checkpoint for news
        checkpoint_filename = save_news_checkpoint(news_data, timestamp)
        print(f"    [OK] Checkpoint saved: {checkpoint_filename}")
        print(f"    Total URLs collected: {len(news_urls)}")
        print()
        
        # Save combined file
        output_dir = SCRIPT_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_data = {
            'scraped_at': datetime.now().isoformat(),
            'total_unique_urls': len(news_urls),
            'max_urls': max_urls,
            'news': news_data
        }
        
        combined_filename = output_dir / f'bbc_news_all_urls_{timestamp}.json'
        with open(combined_filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print("="*80)
        print("COLLECTION COMPLETE")
        print("="*80)
        print(f"Total unique URLs collected: {len(news_urls)}")
        print(f"\nSaved to: {combined_filename}")
        print("="*80)
        
        return news_data
        
    except KeyboardInterrupt:
        print("\n" + "="*80)
        print("[WARNING] INTERRUPTED BY USER (CTRL+C)")
        print("="*80)
        print("Saving progress...")
        
        # Try to load partial checkpoint data if it was saved
        output_dir = SCRIPT_DIR
        checkpoint_pattern = f'bbc_news_*_{timestamp}.json'
        checkpoint_files = glob.glob(str(output_dir / checkpoint_pattern))
        
        # Try to load any partial checkpoints
        news_data = None
        for checkpoint_file in checkpoint_files:
            try:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint_data = json.load(f)
                    news_data = {
                        'category': checkpoint_data.get('category', 'news'),
                        'category_url': checkpoint_data.get('category_url', 'https://www.bbc.com/news'),
                        'url_count': checkpoint_data.get('url_count', 0),
                        'urls': checkpoint_data.get('urls', [])
                    }
                    break
            except Exception:
                pass
        
        if news_data:
            # Save combined file with all collected data
            output_dir = SCRIPT_DIR
            output_data = {
                'scraped_at': datetime.now().isoformat(),
                'interrupted': True,
                'total_unique_urls': news_data['url_count'],
                'max_urls': max_urls,
                'news': news_data
            }
            
            combined_filename = output_dir / f'bbc_news_interrupted_{timestamp}.json'
            with open(combined_filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            print(f"[OK] Progress saved to: {combined_filename}")
            print(f"[OK] {news_data['url_count']} URLs collected")
        
        print("="*80)
        print("\nAll individual checkpoints were saved during collection.")
        print("You can resume collection later if needed.\n")
        
        # Re-raise to exit
        raise


if __name__ == "__main__":
    # Collect URLs from BBC News
    # Target: 10,000 URLs
    try:
        results = collect_news_urls(max_urls=10000)
    except KeyboardInterrupt:
        # Already handled in collect_news_urls, just exit cleanly
        print("\nExiting...")
        sys.exit(0)

