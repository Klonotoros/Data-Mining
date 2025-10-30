"""
BBC Sport URL Collector
Only collects article URLs from BBC Sport categories - no content scraping
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


class BBCSportURLCollector:
    """Collects article URLs from BBC Sport categories"""
    
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
    
    def is_sport_article_url(self, url: str) -> bool:
        """Check if URL is a sport article URL"""
        if '/sport/' not in url:
            return False
        
        url_lower = url.lower()
        
        # Exclude category pages, live pages, fixtures, etc.
        exclude_patterns = [
            '/sport/events/',
            '/sport/live/',
            '/sport/fixtures',
            '/sport/results',
            '/sport/standings',
            '/sport/tables',
            '/sport/scores',
            '/sport/video/',
            '/sport/audio/',
            '/sport/gossip',
            '/sport/highlights',
            '/topics/',
            '/live/',
            '/watch/', 
            '/listen/',
            '/sport/$',
        ]
        
        # Must not match exclude patterns
        if any(pattern in url_lower for pattern in exclude_patterns):
            return False
        
        # Must match article patterns
        url_parts = [p for p in url.split('/') if p]
        
        # Pattern 1: /sport/category/articles/... (definite articles)
        if '/articles/' in url_lower:
            return True
        
        # Pattern 2: /sport/category/YYYY/MM/DD/... (dated articles)
        if len(url_parts) >= 6 and url_parts[1] == 'sport':
            # Check if there's a date-like pattern (YYYY/MM/DD)
            if url_parts[3].isdigit() and len(url_parts[3]) == 4:  # Year
                return True
        
        # Pattern 3: /sport/category/numeric-id (article IDs)
        if len(url_parts) >= 4 and url_parts[1] == 'sport':
            # If last part is numeric or has a slug-like structure, likely an article
            last_part = url_parts[-1]
            if last_part.isdigit() or (len(last_part) > 5 and '-' in last_part):
                return True
        
        return False
    
    def url_belongs_to_category(self, url: str, category_url: str) -> bool:
        """Check if a URL belongs to a specific category"""
        # Extract category from category_url
        # e.g., 'https://www.bbc.com/sport/athletics' -> target_category = 'athletics'
        url_lower = url.lower()
        category_url_lower = category_url.lower()
        
        # Find '/sport/category' pattern in the category URL
        # e.g., from 'https://www.bbc.com/sport/athletics' extract 'athletics'
        sport_index = category_url_lower.find('/sport/')
        if sport_index == -1:
            return False
        
        # Extract the category name (everything after '/sport/')
        category_part = category_url_lower[sport_index + len('/sport/'):]
        # Remove trailing slash and query params if any
        target_category = category_part.split('/')[0].split('?')[0].strip()
        
        if not target_category:
            return False
        
        # Check if URL contains '/sport/{target_category}' pattern
        # Must match exactly: '/sport/athletics/' or '/sport/athletics' (end of URL)
        required_pattern = f'/sport/{target_category}'
        
        # Check if the pattern appears in the URL
        pattern_index = url_lower.find(required_pattern)
        if pattern_index == -1:
            return False
        
        # Ensure it's not followed by a different category name
        # e.g., avoid matching '/sport/athletics-in-something-else'
        after_pattern = url_lower[pattern_index + len(required_pattern):]
        if after_pattern and not after_pattern.startswith('/') and not after_pattern.startswith('?'):
            # Might be a different category (e.g., 'athletics-football')
            return False
        
        return True
    
    def collect_urls_from_category(self, category_url: str, max_urls: int = 1000, save_on_interrupt: bool = False, timestamp: str = None) -> Set[str]:
        """Collect article URLs from a sport category - unlimited pages until max_urls reached"""
        # Extract the category name for display
        category_url_lower = category_url.lower()
        sport_index = category_url_lower.find('/sport/')
        if sport_index != -1:
            category_part = category_url_lower[sport_index + len('/sport/'):]
            target_category = category_part.split('/')[0].split('?')[0].strip()
        else:
            target_category = 'unknown'
        
        article_urls = set()
        pages_to_check = [category_url]
        pages_checked = set([category_url])
        
        print(f"    Collecting URLs from: {category_url}")
        print(f"    Target: {max_urls} unique article URLs")
        print(f"    Category filter: {target_category}")
        
        page_num = 0
        # Track URL count stability: if it doesn't change for 3 consecutive 10-page intervals, stop
        last_url_count_at_interval = None  # Will be set after first interval
        consecutive_unchanged_intervals = 0
        
        try:
            while pages_to_check:
                # Stop if we've found enough URLs for this category
                if len(article_urls) >= max_urls:
                    print(f"    ✓ Reached {max_urls} URLs limit. Stopping search for this category.")
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
                                print(f"    ✓ URL count unchanged for 3 consecutive intervals (30 pages). Stopping search.")
                                print(f"    ✓ Final count: {current_url_count} URLs after checking {page_num} pages")
                                break
                            # Keep same last_url_count_at_interval for next comparison
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
                    
                    # Check if it's an article URL AND belongs to this specific category
                    if self.is_sport_article_url(clean_url) and self.url_belongs_to_category(clean_url, category_url):
                        article_urls.add(clean_url)
                        
                        # Stop immediately if we hit the limit after adding a URL
                        if len(article_urls) >= max_urls:
                            print(f"    ✓ Reached {max_urls} URLs. Stopping search.")
                            break
                    
                    # Check if it's a pagination link or related category page
                    if clean_url.startswith(category_url) and clean_url not in pages_checked:
                        # Look for pagination indicators
                        if any(indicator in clean_url.lower() for indicator in ['/page/', '/p/', '?page=', '?p=', '/archive']):
                            pages_to_check.append(clean_url)
                            pages_checked.add(clean_url)
                        # Check for deeper paths that might have articles
                        elif not any(exclude in clean_url.lower() for exclude in ['/live/', '/scores', '/fixtures', '/results']):
                            path_depth = len([p for p in clean_url.replace(category_url, '').split('/') if p])
                            if path_depth > 0:
                                pages_to_check.append(clean_url)
                                pages_checked.add(clean_url)
                    
                    # Look for date-based archive links (only within same category)
                    if clean_url.startswith(category_url) and clean_url not in pages_checked:
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
                        
                        # Only add pagination links within the same category
                        if clean_next_url.startswith(category_url) and clean_next_url not in pages_checked:
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
                        
                        # Check if URL contains date patterns (only within same category)
                        if clean_date_url.startswith(category_url):
                            if any(year in clean_date_url for year in ['/202', '/201', '/2020', '/2021', '/2022', '/2023', '/2024', '/2025']):
                                if clean_date_url not in pages_checked:
                                    pages_to_check.append(clean_date_url)
                                    pages_checked.add(clean_date_url)
                
                # Check if we've reached the limit before continuing to next page
                if len(article_urls) >= max_urls:
                    break
                
                time.sleep(0.1)  # Small delay between page checks
            
            print(f"    ✓ Complete! Found {len(article_urls)} unique article URLs after checking {page_num} pages\n")
        except KeyboardInterrupt:
            print(f"\n    ⚠ Interrupted! Saving {len(article_urls)} URLs collected so far...")
            print(f"    ✓ Partial collection: {len(article_urls)} unique article URLs after checking {page_num} pages\n")
            
            # Save partial category data if requested
            if save_on_interrupt and len(article_urls) > 0 and timestamp:
                category_data = {
                    'category': target_category,
                    'category_url': category_url,
                    'url_count': len(article_urls),
                    'urls': sorted(list(article_urls))
                }
                try:
                    checkpoint_filename = save_category_checkpoint(category_data, timestamp)
                    print(f"    ✓ Partial checkpoint saved: {checkpoint_filename}")
                except Exception as e:
                    print(f"    [WARNING] Failed to save partial checkpoint: {e}")
            
            # Re-raise to allow main function to handle saving
            raise
        
        return article_urls


def extract_detailed_category(url: str) -> str:
    """Extract detailed category from URL"""
    parts = url.split('/')
    if len(parts) >= 4 and parts[3] == 'sport':
        if len(parts) > 4:
            return parts[4]
    return 'unknown'


def save_category_checkpoint(category_data: dict, timestamp: str, output_dir: Path = None):
    """Save a checkpoint JSON file for a single category"""
    # Use script directory if not specified
    if output_dir is None:
        output_dir = SCRIPT_DIR
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    filename = output_dir / f'bbc_sport_{category_data["category"]}_{category_data["url_count"]}_urls_{timestamp}.json'
    
    checkpoint_data = {
        'scraped_at': datetime.now().isoformat(),
        'category': category_data['category'],
        'category_url': category_data['category_url'],
        'url_count': category_data['url_count'],
        'urls': category_data['urls']
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
    
    return str(filename)


def save_combined_results(all_urls_by_category: dict, total_urls: int, 
                         timestamp: str, max_urls_per_category: int, 
                         categories_processed: int, interrupted: bool = False):
    """Save combined file with all collected URLs"""
    output_dir = SCRIPT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    
    prefix = 'bbc_sport_interrupted' if interrupted else 'bbc_sport_all_urls'
    combined_filename = output_dir / f'{prefix}_{timestamp}.json'
    
    output_data = {
        'scraped_at': datetime.now().isoformat(),
        'interrupted': interrupted,
        'total_unique_urls': total_urls,
        'max_urls_per_category': max_urls_per_category,
        'categories_processed': categories_processed,
        'urls_by_category': all_urls_by_category
    }
    
    with open(combined_filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    return str(combined_filename)


def collect_all_sport_urls(max_urls_per_category: int = 1000):
    """Collect article URLs from all BBC Sport categories"""
    
    collector = BBCSportURLCollector()
    all_urls_by_category = {}
    
    # Create timestamp for all files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # All BBC Sport categories
    sport_categories = [
        # 'https://www.bbc.com/sport/american-football',
        # 'https://www.bbc.com/sport/archery',
        # 'https://www.bbc.com/sport/athletics',
        # 'https://www.bbc.com/sport/badminton',
         #'https://www.bbc.com/sport/baseball',
         #'https://www.bbc.com/sport/basketball',
        # 'https://www.bbc.com/sport/bowls',
        # 'https://www.bbc.com/sport/boxing',
       # 'https://www.bbc.com/sport/canoeing',
       #  'https://www.bbc.com/sport/commonwealth-games',
         #'https://www.bbc.com/sport/cricket',
        # 'https://www.bbc.com/sport/curling',
        # 'https://www.bbc.com/sport/cycling',
         #'https://www.bbc.com/sport/darts',
        # 'https://www.bbc.com/sport/disability-sport',
         #'https://www.bbc.com/sport/diving',
         #'https://www.bbc.com/sport/equestrian',
        # 'https://www.bbc.com/sport/fencing', 
     'https://www.bbc.com/sport/football',
       #  'https://www.bbc.com/sport/formula1',
      #   'https://www.bbc.com/sport/gaelic-games',
        # 'https://www.bbc.com/sport/golf',
        # 'https://www.bbc.com/sport/gymnastics',
        #    'https://www.bbc.com/sport/handball',
        #    'https://www.bbc.com/sport/hockey',
        # 'https://www.bbc.com/sport/horse-racing',
        # 'https://www.bbc.com/sport/ice-hockey', 
        # 'https://www.bbc.com/sport/judo',
        # 'https://www.bbc.com/sport/karate',
        # 'https://www.bbc.com/sport/mixed-martial-arts',
        # 'https://www.bbc.com/sport/modern-pentathlon',
        # 'https://www.bbc.com/sport/motorsport',
        # 'https://www.bbc.com/sport/netball',
        # 'https://www.bbc.com/sport/olympics',
        # 'https://www.bbc.com/sport/rowing',
        # 'https://www.bbc.com/sport/rugby-league',
        # 'https://www.bbc.com/sport/rugby-union',
        # 'https://www.bbc.com/sport/sailing',
        # 'https://www.bbc.com/sport/shooting',
        # 'https://www.bbc.com/sport/snooker',
        # 'https://www.bbc.com/sport/sport-climbing',
        # 'https://www.bbc.com/sport/squash',
        # 'https://www.bbc.com/sport/swimming',
        # 'https://www.bbc.com/sport/table-tennis',
        # 'https://www.bbc.com/sport/taekwondo',
        # 'https://www.bbc.com/sport/tennis',
        # 'https://www.bbc.com/sport/triathlon',
        # 'https://www.bbc.com/sport/volleyball',
        # 'https://www.bbc.com/sport/water-polo',
        # 'https://www.bbc.com/sport/weightlifting',
        # 'https://www.bbc.com/sport/winter-sports',
        # 'https://www.bbc.com/sport/wrestling',
    ]
    
    print("="*80)
    print("BBC SPORT URL COLLECTOR")
    print("="*80)
    print(f"Collecting URLs from {len(sport_categories)} sport categories")
    print(f"Target: {max_urls_per_category} URLs per category")
    print(f"Mode: URL collection only (no content scraping)")
    print("="*80)
    print()
    
    total_urls = 0
    categories_processed = 0
    
    try:
        for category_url in sport_categories:
            categories_processed += 1
            detailed_category = extract_detailed_category(category_url)
            
            print(f"[{categories_processed}/{len(sport_categories)}] Category: {detailed_category}")
            
            try:
                category_urls = collector.collect_urls_from_category(
                    category_url, 
                    max_urls=max_urls_per_category,
                    save_on_interrupt=True,
                    timestamp=timestamp
                )
                category_data = {
                    'category': detailed_category,
                    'category_url': category_url,
                    'url_count': len(category_urls),
                    'urls': sorted(list(category_urls))
                }
                all_urls_by_category[detailed_category] = category_data
                total_urls += len(category_urls)
                
                # Save checkpoint for this category immediately
                checkpoint_filename = save_category_checkpoint(category_data, timestamp)
                print(f"    ✓ Checkpoint saved: {checkpoint_filename}")
                print(f"    Total URLs collected so far: {total_urls}")
                print()
                
            except KeyboardInterrupt:
                # Partial URLs were already saved by collect_urls_from_category if any were collected
                # Try to reload from the checkpoint file to include in combined save
                try:
                    # The checkpoint was already saved, but we don't have the URLs in memory
                    # Mark this category as partially processed
                    if detailed_category not in all_urls_by_category:
                        # Create a placeholder - actual data is in the checkpoint file
                        category_data = {
                            'category': detailed_category,
                            'category_url': category_url,
                            'url_count': 0,  # Will be accurate in checkpoint file
                            'urls': []  # Will be accurate in checkpoint file
                        }
                        all_urls_by_category[detailed_category] = category_data
                except Exception:
                    pass
                
                # Re-raise to trigger main save
                raise
                
            except Exception as e:
                print(f"    [ERROR] Failed to collect URLs from {detailed_category}: {e}\n")
                category_data = {
                    'category': detailed_category,
                    'category_url': category_url,
                    'url_count': 0,
                    'urls': []
                }
                all_urls_by_category[detailed_category] = category_data
                
                # Still save checkpoint even if empty (to track failed categories)
                checkpoint_filename = save_category_checkpoint(category_data, timestamp)
                print(f"    ✓ Checkpoint saved (empty): {checkpoint_filename}")
                continue
            
            time.sleep(0.2)  # Rate limiting between categories
            
    except KeyboardInterrupt:
        print("\n" + "="*80)
        print("⚠ INTERRUPTED BY USER (CTRL+C)")
        print("="*80)
        print(f"Saving progress: {total_urls} URLs from {categories_processed} categories...")
        
        # Try to load partial checkpoint data for the current category if it was saved
        # (collect_urls_from_category saves partial data when interrupted)
        # Look for the most recent checkpoint file for any category that might have been interrupted
        output_dir = SCRIPT_DIR
        checkpoint_pattern = f'bbc_sport_*_{timestamp}.json'
        checkpoint_files = glob.glob(str(output_dir / checkpoint_pattern))
        
        # Try to load any partial checkpoints and merge them into all_urls_by_category
        for checkpoint_file in checkpoint_files:
            try:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint_data = json.load(f)
                    category_name = checkpoint_data.get('category', '')
                    if category_name:
                        # Update or add category data from checkpoint
                        # This handles both new categories and replacing placeholders
                        existing_count = all_urls_by_category.get(category_name, {}).get('url_count', 0)
                        checkpoint_count = checkpoint_data.get('url_count', 0)
                        
                        # Only update if checkpoint has data and is more recent/better
                        if checkpoint_count > 0 and checkpoint_count >= existing_count:
                            if category_name not in all_urls_by_category:
                                total_urls += checkpoint_count
                            elif all_urls_by_category[category_name]['url_count'] < checkpoint_count:
                                # Update existing with better data
                                total_urls += (checkpoint_count - all_urls_by_category[category_name]['url_count'])
                            
                            all_urls_by_category[category_name] = {
                                'category': checkpoint_data['category'],
                                'category_url': checkpoint_data['category_url'],
                                'url_count': checkpoint_data['url_count'],
                                'urls': checkpoint_data['urls']
                            }
            except Exception:
                pass  # Skip if we can't read the checkpoint
        
        # Save combined file with all collected data (including partial checkpoints)
        combined_filename = save_combined_results(
            all_urls_by_category, 
            total_urls, 
            timestamp, 
            max_urls_per_category,
            categories_processed,
            interrupted=True
        )
        
        print(f"✓ Progress saved to: {combined_filename}")
        print("="*80)
        print("\nAll individual category checkpoints were saved during collection.")
        print("Partial data from interrupted categories is included in the combined file.")
        print("You can resume collection later if needed.\n")
        
        # Re-raise to exit
        raise
    
    # Save combined file with all URLs (all checkpoints combined)
    combined_filename = save_combined_results(
        all_urls_by_category,
        total_urls,
        timestamp,
        max_urls_per_category,
        len(sport_categories),
        interrupted=False
    )
    
    print("="*80)
    print("COLLECTION COMPLETE")
    print("="*80)
    print(f"Total unique URLs collected: {total_urls}")
    print(f"Categories processed: {categories_processed}/{len(sport_categories)}")
    print(f"\nSaved to: {combined_filename}")
    print("="*80)
    
    # Print summary by category
    print("\n" + "="*80)
    print("CATEGORY BREAKDOWN:")
    print("="*80)
    print(f"{'Category':<30} {'URLs':>10}")
    print("-"*80)
    
    for category_data in sorted(all_urls_by_category.values(), key=lambda x: x['url_count'], reverse=True):
        print(f"{category_data['category']:<30} {category_data['url_count']:>10}")
    
    print("="*80)
    
    return all_urls_by_category


if __name__ == "__main__":
    # Collect URLs from all sport categories
    # Each category will collect up to 1000 URLs (unlimited pages)
    try:
        results = collect_all_sport_urls(max_urls_per_category=10000)
    except KeyboardInterrupt:
        # Already handled in collect_all_sport_urls, just exit cleanly
        print("\nExiting...")
        sys.exit(0)

