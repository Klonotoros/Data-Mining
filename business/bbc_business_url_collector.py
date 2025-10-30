"""
BBC Business URL Collector
Only collects article URLs from BBC Business - no content scraping
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


class BBCBusinessURLCollector:
    """Collects article URLs from BBC Business"""
    
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
    
    def is_bbc_article_url(self, url: str) -> bool:
        """Check if URL is any BBC article URL (regardless of category prefix)
        Articles found on business pages are categorized as business articles"""
        url_lower = url.lower()
        
        # Must be a bbc.com URL
        if 'bbc.com' not in url_lower and 'bbc.co.uk' not in url_lower:
            return False
        
        # Exclude non-article patterns
        exclude_patterns = [
            '/events/',
            '/live/',
            '/video/',
            '/audio/',
            '/topics/',
            '/watch/', 
            '/listen/',
            '/maestro/',
            '/shop/',
            '/weather/',
            '/newsletters/',
            '/help/',
            '/terms/',
            '/privacy/',
            '/cookies/',
            '/accessibility/',
            '/contact/',
        ]
        
        # Must not match exclude patterns
        if any(pattern in url_lower for pattern in exclude_patterns):
            return False
        
        # Must match article patterns
        url_parts = [p for p in url.split('/') if p]
        
        # Pattern 1: /articles/... (any category - news, business, sport, culture, etc.)
        if '/articles/' in url_lower:
            # Extract article ID pattern (e.g., c5yp2y8rdpro)
            article_part = url_lower.split('/articles/')[-1].split('/')[0].split('?')[0]
            # BBC article IDs are usually alphanumeric, 10-15 chars
            if len(article_part) >= 10 and len(article_part) <= 20 and article_part.replace('-', '').replace('_', '').isalnum():
                return True
        
        # Pattern 2: /YYYY/MM/DD/... or /category/YYYY/MM/DD/... (dated articles)
        # Look for year pattern (4 digits)
        for i, part in enumerate(url_parts):
            if part.isdigit() and len(part) == 4 and int(part) >= 2000 and int(part) <= 2030:
                # Found a year, check if followed by month/day or article slug
                if i + 1 < len(url_parts):
                    return True
        
        # Pattern 3: Numeric article IDs (old style)
        # Check last part - if it's purely numeric and reasonable length, might be article ID
        if url_parts:
            last_part = url_parts[-1]
            if last_part.isdigit() and len(last_part) >= 7 and len(last_part) <= 10:
                # Could be an article ID, but be cautious
                return True
        
        return False
    
    def collect_urls_from_business(self, max_urls: int = 10000, save_on_interrupt: bool = False, timestamp: str = None) -> Set[str]:
        """
        Collect article URLs from BBC Business
        
        IMPORTANT: This collector categorizes articles based on WHERE they are found,
        not their URL structure. Articles displayed on business pages (https://www.bbc.com/business)
        are saved as "business" articles, even if they have URLs like:
        - /news/articles/... (news URL prefix)
        - /business/articles/... (business URL prefix)
        - Any other BBC article URL
        
        This approach correctly groups articles by their actual category context.
        """
        business_url = "https://www.bbc.com/business"
        
        article_urls = set()
        pages_to_check = [business_url]
        pages_checked = set([business_url])
        
        print(f"    Collecting URLs from: {business_url}")
        print(f"    Target: {max_urls} unique article URLs")
        print(f"    Category: business (all articles found on business pages, regardless of URL prefix)")
        
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
                    
                    # Check if it's any BBC article URL
                    # IMPORTANT: We collect ALL articles found on business pages as "business" articles,
                    # regardless of their URL prefix (could be /news/articles/, /business/articles/, etc.)
                    # This correctly groups articles by their source category, not URL structure.
                    if self.is_bbc_article_url(clean_url):
                        article_urls.add(clean_url)
                        
                        # Stop immediately if we hit the limit after adding a URL
                        if len(article_urls) >= max_urls:
                            print(f"    [OK] Reached {max_urls} URLs. Stopping search.")
                            break
                    
                    # Check if it's a pagination link or related business page
                    if clean_url.startswith(business_url) and clean_url not in pages_checked:
                        # Look for pagination indicators
                        if any(indicator in clean_url.lower() for indicator in ['/page/', '/p/', '?page=', '?p=', '/archive']):
                            pages_to_check.append(clean_url)
                            pages_checked.add(clean_url)
                        # Check for deeper paths that might have articles
                        elif not any(exclude in clean_url.lower() for exclude in ['/live/', '/scores', '/fixtures', '/results']):
                            path_depth = len([p for p in clean_url.replace(business_url, '').split('/') if p])
                            if path_depth > 0:
                                pages_to_check.append(clean_url)
                                pages_checked.add(clean_url)
                    
                    # Look for date-based archive links (only within business section)
                    if clean_url.startswith(business_url) and clean_url not in pages_checked:
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
                        
                        # Only add pagination links within the business section
                        if clean_next_url.startswith(business_url) and clean_next_url not in pages_checked:
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
                        
                        # Check if URL contains date patterns (only within business section)
                        if clean_date_url.startswith(business_url):
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
                    'category': 'business',
                    'category_url': business_url,
                    'url_count': len(article_urls),
                    'urls': sorted(list(article_urls))
                }
                try:
                    checkpoint_filename = save_business_checkpoint(category_data, timestamp)
                    print(f"    [OK] Partial checkpoint saved: {checkpoint_filename}")
                except Exception as e:
                    print(f"    [WARNING] Failed to save partial checkpoint: {e}")
            
            # Re-raise to allow main function to handle saving
            raise
        
        return article_urls


def save_business_checkpoint(business_data: dict, timestamp: str, output_dir: Path = None):
    """Save a checkpoint JSON file for business URLs"""
    # Use script directory if not specified
    if output_dir is None:
        output_dir = SCRIPT_DIR
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    filename = output_dir / f'bbc_business_{business_data["url_count"]}_urls_{timestamp}.json'
    
    checkpoint_data = {
        'scraped_at': datetime.now().isoformat(),
        'category': business_data['category'],
        'category_url': business_data['category_url'],
        'url_count': business_data['url_count'],
        'urls': business_data['urls']
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
    
    return str(filename)


def collect_business_urls(max_urls: int = 10000):
    """Collect article URLs from BBC Business"""
    
    collector = BBCBusinessURLCollector()
    
    # Create timestamp for all files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("="*80)
    print("BBC BUSINESS URL COLLECTOR")
    print("="*80)
    print(f"Collecting URLs from: https://www.bbc.com/business")
    print(f"Target: {max_urls} URLs")
    print(f"Mode: URL collection only (no content scraping)")
    print("="*80)
    print()
    
    try:
        print("[1/1] Category: business")
        
        business_urls = collector.collect_urls_from_business(
            max_urls=max_urls,
            save_on_interrupt=True,
            timestamp=timestamp
        )
        
        business_data = {
            'category': 'business',
            'category_url': 'https://www.bbc.com/business',
            'url_count': len(business_urls),
            'urls': sorted(list(business_urls))
        }
        
        # Save checkpoint for business
        checkpoint_filename = save_business_checkpoint(business_data, timestamp)
        print(f"    [OK] Checkpoint saved: {checkpoint_filename}")
        print(f"    Total URLs collected: {len(business_urls)}")
        print()
        
        # Save combined file
        output_dir = SCRIPT_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_data = {
            'scraped_at': datetime.now().isoformat(),
            'total_unique_urls': len(business_urls),
            'max_urls': max_urls,
            'business': business_data
        }
        
        combined_filename = output_dir / f'bbc_business_all_urls_{timestamp}.json'
        with open(combined_filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print("="*80)
        print("COLLECTION COMPLETE")
        print("="*80)
        print(f"Total unique URLs collected: {len(business_urls)}")
        print(f"\nSaved to: {combined_filename}")
        print("="*80)
        
        return business_data
        
    except KeyboardInterrupt:
        print("\n" + "="*80)
        print("[WARNING] INTERRUPTED BY USER (CTRL+C)")
        print("="*80)
        print("Saving progress...")
        
        # Try to load partial checkpoint data if it was saved
        output_dir = SCRIPT_DIR
        checkpoint_pattern = f'bbc_business_*_{timestamp}.json'
        checkpoint_files = glob.glob(str(output_dir / checkpoint_pattern))
        
        # Try to load any partial checkpoints
        business_data = None
        for checkpoint_file in checkpoint_files:
            try:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint_data = json.load(f)
                    business_data = {
                        'category': checkpoint_data.get('category', 'business'),
                        'category_url': checkpoint_data.get('category_url', 'https://www.bbc.com/business'),
                        'url_count': checkpoint_data.get('url_count', 0),
                        'urls': checkpoint_data.get('urls', [])
                    }
                    break
            except Exception:
                pass
        
        if business_data:
            # Save combined file with all collected data
            output_dir = SCRIPT_DIR
            output_data = {
                'scraped_at': datetime.now().isoformat(),
                'interrupted': True,
                'total_unique_urls': business_data['url_count'],
                'max_urls': max_urls,
                'business': business_data
            }
            
            combined_filename = output_dir / f'bbc_business_interrupted_{timestamp}.json'
            with open(combined_filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            print(f"[OK] Progress saved to: {combined_filename}")
            print(f"[OK] {business_data['url_count']} URLs collected")
        
        print("="*80)
        print("\nAll individual checkpoints were saved during collection.")
        print("You can resume collection later if needed.\n")
        
        # Re-raise to exit
        raise


if __name__ == "__main__":
    # Collect URLs from BBC Business
    # Target: 10,000 URLs
    try:
        results = collect_business_urls(max_urls=10000)
    except KeyboardInterrupt:
        # Already handled in collect_business_urls, just exit cleanly
        print("\nExiting...")
        sys.exit(0)

