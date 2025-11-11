
























































"""
BBC News Category Labeler
Labels article URLs with categories extracted from page metadata
Works with URLs collected by bbc_news_url_collector.py

Usage:
    # Normal mode: Label URLs from input file
    python label_news_categories.py input_file.json [-o output_file.json] [-d delay] [-s start_index] [-l limit]
    
    # Label left.json mode: Label URLs from left.json and save to output file
    python label_news_categories.py -m [--left-file left.json] [-o output_file.json] [-d delay] [-s start_index] [-l limit]
    
    Examples:
        # Normal mode - Process all URLs (default) - saves checkpoint every 1000 URLs
        python label_news_categories.py bbc_news_10000_urls_20251030_013236.json
        
        # Normal mode - Process all URLs with custom checkpoint interval (every 500 URLs)
        python label_news_categories.py bbc_news_10000_urls_20251030_013236.json -c 500
        
        # Normal mode - Process limited number of URLs (no checkpoints)
        python label_news_categories.py bbc_news_10000_urls_20251030_013236.json -l 500
        
        # Normal mode - Label with custom output file and delay
        python label_news_categories.py bbc_news_10000_urls_20251030_013236.json -o labeled_articles.json -d 0.3
        
        # Normal mode - Resume from index 5000 (if interrupted)
        python label_news_categories.py bbc_news_10000_urls_20251030_013236.json -s 5000
        
        # Label left.json mode - Label URLs from left.json (saves to left_labeled_TIMESTAMP.json)
        python label_news_categories.py -m
        
        # Label left.json mode - With custom output file
        python label_news_categories.py -m -o left_labeled.json

The script extracts categories using multiple methods:
1. Meta tags (article:section, og:section)
2. JSON-LD structured data
3. Breadcrumbs navigation
4. URL pattern analysis (fallback)

Categories detected: uk, world, business, technology, science, health, 
entertainment, sport, politics, education, climate, culture, travel
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import sys

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()


class BBCNewsCategoryLabeler:
    """Labels BBC News article URLs with categories from page metadata"""
    
    def __init__(self, delay: float = 0.2):
        self.base_url = "https://www.bbc.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.delay = delay
    
    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a webpage"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            print(f"    [ERROR] Error fetching {url}: {e}")
            return None
    
    def extract_category_from_meta(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract category from meta tags"""
        # Try article:section first (most reliable)
        category_meta = soup.find('meta', property='article:section')
        if category_meta and category_meta.get('content'):
            return category_meta.get('content').strip()
        
        # Try og:section
        og_section = soup.find('meta', property='og:section')
        if og_section and og_section.get('content'):
            return og_section.get('content').strip()
        
        # Try news_keywords
        news_keywords = soup.find('meta', attrs={'name': 'news_keywords'})
        if news_keywords and news_keywords.get('content'):
            # Sometimes contains category info
            keywords = news_keywords.get('content').strip()
            # Check if it starts with a category-like word
            common_categories = ['uk', 'world', 'business', 'technology', 'science', 
                               'health', 'entertainment', 'sport', 'politics', 'education']
            for cat in common_categories:
                if keywords.lower().startswith(cat):
                    return cat
        
        return None
    
    def extract_category_from_breadcrumbs(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract category from breadcrumbs navigation"""
        # Try various breadcrumb selectors
        breadcrumb_selectors = [
            'nav[aria-label="Breadcrumb"] a',
            '.breadcrumbs a',
            'ol[data-component="breadcrumbs"] a',
            '[data-component="breadcrumb"] a',
            '.ssrcss-1mrs5ns-BreadcrumbsWrapper a'
        ]
        
        for selector in breadcrumb_selectors:
            breadcrumbs = soup.select(selector)
            if breadcrumbs:
                # Look for category in breadcrumb links
                for crumb in breadcrumbs:
                    text = crumb.get_text().strip().lower()
                    href = crumb.get('href', '')
                    
                    # Common BBC News categories
                    category_map = {
                        'uk': 'uk',
                        'world': 'world',
                        'business': 'business',
                        'technology': 'technology',
                        'science': 'science',
                        'health': 'health',
                        'entertainment': 'entertainment',
                        'sport': 'sport',
                        'politics': 'politics',
                        'education': 'education',
                        'climate': 'climate',
                        'culture': 'culture',
                        'travel': 'travel'
                    }
                    
                    # Check text
                    for key, value in category_map.items():
                        if key in text:
                            return value
                    
                    # Check href
                    if '/news/' in href:
                        parts = [p for p in href.split('/') if p]
                        if 'news' in parts:
                            idx = parts.index('news')
                            if idx + 1 < len(parts):
                                potential_cat = parts[idx + 1]
                                if potential_cat in category_map.values():
                                    return potential_cat
        
        return None
    
    def extract_category_from_json_ld(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract category from JSON-LD structured data"""
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                
                # Handle both single objects and arrays
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            category = self._extract_from_json_ld_item(item)
                            if category:
                                return category
                elif isinstance(data, dict):
                    category = self._extract_from_json_ld_item(data)
                    if category:
                        return category
            except (json.JSONDecodeError, AttributeError):
                continue
        
        return None
    
    def _extract_from_json_ld_item(self, item: dict) -> Optional[str]:
        """Extract category from a single JSON-LD item"""
        # Check articleSection
        if 'articleSection' in item:
            return item['articleSection'].strip().lower()
        
        # Check in keywords
        if 'keywords' in item:
            keywords = item['keywords']
            if isinstance(keywords, str):
                keywords = [k.strip() for k in keywords.split(',')]
            
            # Common categories
            category_map = {
                'uk': 'uk', 'united kingdom': 'uk', 'britain': 'uk',
                'world': 'world', 'international': 'world',
                'business': 'business', 'economy': 'business',
                'technology': 'technology', 'tech': 'technology',
                'science': 'science',
                'health': 'health', 'medical': 'health',
                'entertainment': 'entertainment',
                'sport': 'sport', 'sports': 'sport',
                'politics': 'politics', 'political': 'politics',
                'education': 'education',
                'climate': 'climate', 'environment': 'climate',
                'culture': 'culture',
                'travel': 'travel'
            }
            
            for keyword in keywords:
                keyword_lower = keyword.lower().strip()
                if keyword_lower in category_map:
                    return category_map[keyword_lower]
        
        return None
    
    def extract_category_from_url_pattern(self, url: str) -> Optional[str]:
        """Try to extract category from URL pattern as fallback"""
        url_lower = url.lower()
        path = urlparse(url).path
        parts = [p for p in path.split('/') if p]
        
        # Common BBC News URL patterns
        if 'news' in parts:
            idx = parts.index('news')
            if idx + 1 < len(parts):
                potential_cat = parts[idx + 1]
                
                # Exclude common non-category parts
                exclude = ['articles', 'live', 'video', 'audio', 'pictures', 
                          'topics', 'events', 'archive', '202', '201']
                
                if potential_cat not in exclude and not potential_cat.isdigit():
                    # Map to standard categories
                    category_map = {
                        'uk': 'uk',
                        'world': 'world',
                        'business': 'business',
                        'technology': 'technology',
                        'science': 'science',
                        'health': 'health',
                        'entertainment': 'entertainment',
                        'sport': 'sport',
                        'politics': 'politics',
                        'education': 'education',
                        'climate': 'climate',
                        'culture': 'culture',
                        'travel': 'travel'
                    }
                    
                    if potential_cat in category_map:
                        return category_map[potential_cat]
                    elif len(potential_cat) > 2:  # Might be a valid category
                        return potential_cat
        
        return None
    
    def extract_category(self, url: str) -> Optional[str]:
        """Extract category from article page using multiple methods"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        # Try multiple extraction methods in order of reliability
        category = self.extract_category_from_meta(soup)
        if category:
            return category
        
        category = self.extract_category_from_json_ld(soup)
        if category:
            return category
        
        category = self.extract_category_from_breadcrumbs(soup)
        if category:
            return category
        
        # Fallback to URL pattern
        category = self.extract_category_from_url_pattern(url)
        if category:
            return category
        
        return 'unknown'
    
    def label_urls(self, urls: List[str], start_index: int = 0, limit: int = None, 
                   checkpoint_callback: callable = None, checkpoint_interval: int = 1000) -> List[Dict]:
        """Label a list of URLs with categories
        
        Args:
            urls: List of URLs to label
            start_index: Index to start from
            limit: Maximum number of URLs to process (None for all)
            checkpoint_callback: Function to call for checkpoints (checkpoint_callback(articles, index))
            checkpoint_interval: Save checkpoint every N articles
        """
        labeled_articles = []
        total = len(urls)
        
        # Apply limit if specified
        if limit is not None:
            urls_to_process = urls[start_index:start_index + limit]
            actual_total = min(limit, total - start_index)
        else:
            urls_to_process = urls[start_index:]
            actual_total = total - start_index
        
        if limit is None:
            print(f"Labeling all URLs (total: {total})...")
        else:
            print(f"Labeling {actual_total} URLs (from {total} total)...")
        if start_index > 0:
            print(f"Starting from index {start_index}")
        if limit is not None:
            print(f"Limit: {limit} URLs")
        if checkpoint_callback:
            print(f"Checkpoint interval: every {checkpoint_interval} URLs")
        print()
        
        for i, url in enumerate(urls_to_process, start=start_index):
            current_num = i - start_index + 1
            if limit is None:
                # Show current number and total, but make it clear it's not a limit
                print(f"Processing URL {current_num} (of {total} total): {url[:80]}...")
            else:
                print(f"[{current_num}/{actual_total}] Processing: {url[:80]}...")
            
            category = self.extract_category(url)
            
            article_data = {
                'url': url,
                'category': category if category else 'unknown',
                'labeled_at': datetime.now().isoformat()
            }
            
            labeled_articles.append(article_data)
            
            if category:
                print(f"    [OK] Category: {category}")
            else:
                print(f"    [WARNING] Could not determine category")
            
            # Save checkpoint every N articles
            if checkpoint_callback and len(labeled_articles) > 0:
                if len(labeled_articles) % checkpoint_interval == 0:
                    print(f"\n    [CHECKPOINT] Saving checkpoint at {len(labeled_articles)} articles...")
                    checkpoint_callback(labeled_articles, i)
                    print(f"    [OK] Checkpoint saved\n")
            
            # Rate limiting
            if i < start_index + len(urls_to_process) - 1:  # Don't delay after last item
                time.sleep(self.delay)
        
        return labeled_articles


def load_urls_from_file(input_file: str) -> List[str]:
    """Load URLs from JSON file"""
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Handle different JSON structures
    if isinstance(data, dict):
        # Check for 'urls' key
        if 'urls' in data:
            return data['urls']
        # Check for 'news' -> 'urls'
        elif 'news' in data and isinstance(data['news'], dict) and 'urls' in data['news']:
            return data['news']['urls']
        # Check for articles array
        elif 'articles' in data and isinstance(data['articles'], list):
            return [article.get('url') for article in data['articles'] if article.get('url')]
    elif isinstance(data, list):
        # List of URLs or list of objects with 'url' key
        if data and isinstance(data[0], str):
            return data
        elif data and isinstance(data[0], dict):
            return [item.get('url') for item in data if item.get('url')]
    
    raise ValueError(f"Could not parse URLs from {input_file}")


def save_labeled_urls(labeled_articles: List[Dict], output_file: str, input_file: str = None, 
                      is_checkpoint: bool = False):
    """Save labeled URLs to JSON file
    
    Args:
        labeled_articles: List of labeled articles
        output_file: Output file path
        input_file: Input file path (for reference)
        is_checkpoint: If True, this is a checkpoint save (not final)
    """
    output_data = {
        'labeled_at': datetime.now().isoformat(),
        'total_urls': len(labeled_articles),
        'input_file': input_file,
        'is_checkpoint': is_checkpoint,
        'articles': labeled_articles
    }
    
    # Count categories
    category_counts = {}
    for article in labeled_articles:
        cat = article.get('category', 'unknown')
        category_counts[cat] = category_counts.get(cat, 0) + 1
    
    output_data['category_counts'] = category_counts
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    return output_data


def load_existing_labeled_urls(existing_file: str) -> Dict:
    """Load existing labeled URLs from JSON file
    
    Returns:
        Dictionary with existing labeled data, or None if file doesn't exist
    """
    if not Path(existing_file).exists():
        return None
    
    try:
        with open(existing_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"[WARNING] Could not load existing labeled file: {e}")
        return None


def merge_labeled_urls(new_articles: List[Dict], existing_file: str, output_file: str = None) -> Dict:
    """Merge new labeled articles with existing labeled URLs, avoiding duplicates
    
    Args:
        new_articles: List of newly labeled articles
        existing_file: Path to existing labeled URLs file
        output_file: Output file path (defaults to existing_file)
    
    Returns:
        Merged data dictionary
    """
    if output_file is None:
        output_file = existing_file
    
    # Load existing data
    existing_data = load_existing_labeled_urls(existing_file)
    
    if existing_data is None:
        # No existing file, create new one
        print(f"[INFO] No existing labeled file found. Creating new file: {output_file}")
        existing_articles = []
        existing_urls = set()
    else:
        existing_articles = existing_data.get('articles', [])
        existing_urls = {article.get('url') for article in existing_articles if article.get('url')}
        print(f"[INFO] Loaded {len(existing_articles)} existing labeled URLs")
    
    # Filter out duplicates from new articles
    new_unique_articles = []
    new_urls = set()
    duplicates = 0
    
    for article in new_articles:
        url = article.get('url')
        if url and url not in existing_urls and url not in new_urls:
            new_unique_articles.append(article)
            new_urls.add(url)
        elif url in existing_urls:
            duplicates += 1
    
    print(f"[INFO] Found {duplicates} duplicate URLs (already in existing file)")
    print(f"[INFO] Adding {len(new_unique_articles)} new labeled URLs")
    
    # Merge articles
    merged_articles = existing_articles + new_unique_articles
    
    # Create merged output data
    merged_data = {
        'labeled_at': datetime.now().isoformat(),
        'total_urls': len(merged_articles),
        'input_file': existing_data.get('input_file') if existing_data else None,
        'is_checkpoint': False,
        'articles': merged_articles
    }
    
    # Recalculate category counts
    category_counts = {}
    for article in merged_articles:
        cat = article.get('category', 'unknown')
        category_counts[cat] = category_counts.get(cat, 0) + 1
    
    merged_data['category_counts'] = category_counts
    
    # Save merged data
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    print(f"[OK] Merged data saved to: {output_file}")
    print(f"[OK] Total URLs in merged file: {len(merged_articles)}")
    
    return merged_data


def label_left_json(left_file: str = "left.json", 
                    output_file: str = None,
                    delay: float = 0.2,
                    start_index: int = 0,
                    limit: int = None,
                    checkpoint_interval: int = 1000):
    """Label URLs from left.json and save to output file
    
    Args:
        left_file: Path to left.json file with URLs to label
        output_file: Path to output file (default: left_labeled_TIMESTAMP.json)
        delay: Delay between requests
        start_index: Start from this index
        limit: Limit number of URLs to process
        checkpoint_interval: Checkpoint interval
    """
    left_path = Path(left_file)
    if not left_path.is_absolute():
        left_path = SCRIPT_DIR / left_path
    
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = SCRIPT_DIR / f"left_labeled_{timestamp}.json"
    else:
        output_path = Path(output_file)
        if not output_path.is_absolute():
            output_file = SCRIPT_DIR / output_path
    
    print("="*80)
    print("BBC NEWS CATEGORY LABELER - LABEL LEFT.JSON")
    print("="*80)
    print(f"Input file: {left_path}")
    print(f"Output file: {output_file}")
    print(f"Delay between requests: {delay}s")
    print(f"Limit: {limit} URLs" if limit is not None else "Limit: All URLs (unlimited)")
    print(f"Checkpoint interval: every {checkpoint_interval} URLs")
    print("="*80)
    print()
    
    # Load URLs from left.json
    print("Loading URLs from left.json...")
    try:
        urls = load_urls_from_file(str(left_path))
        print(f"[OK] Loaded {len(urls)} URLs from left.json")
    except Exception as e:
        print(f"[ERROR] Failed to load URLs from left.json: {e}")
        return None
    
    if not urls:
        print("[ERROR] No URLs found in left.json")
        return None
    
    print()
    
    # Label URLs
    labeler = BBCNewsCategoryLabeler(delay=delay)
    
    # Checkpoint callback function
    checkpoint_count = [0]
    labeled_articles = []
    
    def save_checkpoint(articles: List[Dict], current_index: int):
        """Save checkpoint file"""
        nonlocal checkpoint_count
        checkpoint_count[0] += 1
        checkpoint_file = SCRIPT_DIR / f"left_checkpoint_{checkpoint_count[0]}_{len(articles)}_urls.json"
        save_labeled_urls(articles, str(checkpoint_file), str(left_path), is_checkpoint=True)
        print(f"    [OK] Checkpoint saved: {checkpoint_file}")
    
    try:
        # Use checkpoints if processing more than checkpoint_interval URLs
        use_checkpoints = (limit is None) or (limit is not None and limit > checkpoint_interval)
        
        labeled_articles = labeler.label_urls(
            urls, 
            start_index=start_index, 
            limit=limit,
            checkpoint_callback=save_checkpoint if use_checkpoints else None,
            checkpoint_interval=checkpoint_interval
        )
    except KeyboardInterrupt:
        print("\n[WARNING] Interrupted by user. Saving partial results...")
        if labeled_articles and len(labeled_articles) > 0:
            # Save partial results
            output_data = save_labeled_urls(labeled_articles, str(output_file), str(left_path), is_checkpoint=False)
            print(f"[OK] Partial results saved: {len(labeled_articles)} URLs labeled")
            print(f"[OK] Output file: {output_file}")
            return output_data
        else:
            print("[WARNING] No articles were processed before interruption.")
            raise
    
    # Save results
    print()
    print("Saving labeled URLs...")
    output_data = save_labeled_urls(labeled_articles, str(output_file), str(left_path), is_checkpoint=False)
    
    print()
    print("="*80)
    print("LABELING COMPLETE")
    print("="*80)
    print(f"Total URLs labeled: {len(labeled_articles)}")
    print(f"Output file: {output_file}")
    if checkpoint_count[0] > 0:
        print(f"Checkpoints saved: {checkpoint_count[0]}")
    print()
    print("Category breakdown:")
    print("-" * 80)
    for category, count in sorted(output_data['category_counts'].items(), key=lambda x: x[1], reverse=True):
        print(f"  {category:<20} {count:>10}")
    print("="*80)
    
    return output_data


def label_news_urls(input_file: str, output_file: str = None, delay: float = 0.2, 
                     start_index: int = 0, limit: int = None, checkpoint_interval: int = 1000):
    """Main function to label URLs from input file
    
    Default behavior: Process all URLs, save checkpoint every 1000 articles.
    Use limit to restrict number of URLs processed.
    """
    
    if output_file is None:
        # Generate output filename from input filename
        input_path = Path(input_file)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = input_path.parent / f"{input_path.stem}_labeled_{timestamp}.json"
    
    # Generate checkpoint filename pattern
    input_path = Path(input_file)
    checkpoint_base = input_path.parent / f"{input_path.stem}_checkpoint"
    
    print("="*80)
    print("BBC NEWS CATEGORY LABELER")
    print("="*80)
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"Delay between requests: {delay}s")
    print(f"Limit: {limit} URLs" if limit is not None else "Limit: All URLs (unlimited)")
    print(f"Checkpoint interval: every {checkpoint_interval} URLs")
    print("="*80)
    print()
    
    # Load URLs
    print("Loading URLs from input file...")
    try:
        urls = load_urls_from_file(input_file)
        print(f"[OK] Loaded {len(urls)} URLs")
    except Exception as e:
        print(f"[ERROR] Failed to load URLs: {e}")
        return None
    
    if not urls:
        print("[ERROR] No URLs found in input file")
        return None
    
    print()
    
    # Label URLs
    labeler = BBCNewsCategoryLabeler(delay=delay)
    
    # Checkpoint callback function
    checkpoint_count = [0]  # Use list to allow modification in nested function
    labeled_articles = []  # Initialize to avoid UnboundLocalError
    
    def save_checkpoint(articles: List[Dict], current_index: int):
        """Save checkpoint file"""
        nonlocal checkpoint_count
        checkpoint_count[0] += 1
        checkpoint_file = f"{checkpoint_base}_{checkpoint_count[0]}_{len(articles)}_urls.json"
        save_labeled_urls(articles, str(checkpoint_file), input_file, is_checkpoint=True)
        print(f"    [OK] Checkpoint saved: {checkpoint_file}")
    
    try:
        # Use checkpoints if processing more than checkpoint_interval URLs
        use_checkpoints = (limit is None) or (limit is not None and limit > checkpoint_interval)
        
        labeled_articles = labeler.label_urls(
            urls, 
            start_index=start_index, 
            limit=limit,
            checkpoint_callback=save_checkpoint if use_checkpoints else None,
            checkpoint_interval=checkpoint_interval
        )
    except KeyboardInterrupt:
        print("\n[WARNING] Interrupted by user. Saving partial results...")
        # Save what we have so far
        if labeled_articles and len(labeled_articles) > 0:
            output_data = save_labeled_urls(labeled_articles, output_file, input_file, is_checkpoint=False)
            print(f"[OK] Partial results saved: {len(labeled_articles)} URLs labeled")
            print(f"[OK] Output file: {output_file}")
            return output_data
        else:
            print("[WARNING] No articles were processed before interruption.")
            raise
    
    # Save final results
    print()
    print("Saving final results...")
    output_data = save_labeled_urls(labeled_articles, output_file, input_file, is_checkpoint=False)
    
    print("="*80)
    print("LABELING COMPLETE")
    print("="*80)
    print(f"Total URLs labeled: {len(labeled_articles)}")
    print(f"Output file: {output_file}")
    if checkpoint_count[0] > 0:
        print(f"Checkpoints saved: {checkpoint_count[0]}")
    print()
    print("Category breakdown:")
    print("-" * 80)
    for category, count in sorted(output_data['category_counts'].items(), key=lambda x: x[1], reverse=True):
        print(f"  {category:<20} {count:>10}")
    print("="*80)
    
    return output_data


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Label BBC News URLs with categories')
    parser.add_argument('input_file', nargs='?', help='Input JSON file with URLs (not needed in merge mode)')
    parser.add_argument('-m', '--merge', action='store_true', help='Label URLs from left.json and save to output file')
    parser.add_argument('-o', '--output', help='Output JSON file (default: auto-generated)')
    parser.add_argument('-d', '--delay', type=float, default=0.2, help='Delay between requests in seconds (default: 0.2)')
    parser.add_argument('-s', '--start', type=int, default=0, help='Start from this index (default: 0)')
    parser.add_argument('-l', '--limit', type=int, default=None, help='Limit number of URLs to process (default: unlimited/all)')
    parser.add_argument('-c', '--checkpoint', type=int, default=1000, help='Checkpoint interval - save every N URLs (default: 1000)')
    parser.add_argument('--left-file', default='left.json', help='Path to left.json file (use with -m flag, default: left.json)')
    
    args = parser.parse_args()
    
    # Convert -1 to None (process all) for backward compatibility
    limit = None if args.limit == -1 else args.limit
    
    try:
        if args.merge:
            # Label left.json mode: label URLs from left.json and save to output file
            label_left_json(
                left_file=args.left_file,
                output_file=args.output,
                delay=args.delay,
                start_index=args.start,
                limit=limit,
                checkpoint_interval=args.checkpoint
            )
        else:
            # Normal mode: label from input file
            if not args.input_file:
                parser.error("input_file is required when not using merge mode (-m)")
            
            label_news_urls(
                input_file=args.input_file,
                output_file=args.output,
                delay=args.delay,
                start_index=args.start,
                limit=limit,
                checkpoint_interval=args.checkpoint
            )
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)

