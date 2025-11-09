"""
BBC News Category Labeler
Labels article URLs with categories extracted from page metadata
Works with URLs collected by bbc_news_url_collector.py

Usage:
    python label_news_categories.py input_file.json [-o output_file.json] [-d delay] [-s start_index] [-l limit]
    
    Examples:
        # Label all URLs from collected file
        python label_news_categories.py bbc_news_10000_urls_20251030_013236.json
        
        # Test with first 100 URLs
        python label_news_categories.py bbc_news_10000_urls_20251030_013236.json -l 100
        
        # Label with custom output file and delay
        python label_news_categories.py bbc_news_10000_urls_20251030_013236.json -o labeled_articles.json -d 0.3
        
        # Resume from index 5000 (if interrupted)
        python label_news_categories.py bbc_news_10000_urls_20251030_013236.json -s 5000

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
    
    def label_urls(self, urls: List[str], start_index: int = 0, limit: int = None) -> List[Dict]:
        """Label a list of URLs with categories"""
        labeled_articles = []
        total = len(urls)
        
        # Apply limit if specified
        if limit is not None:
            urls_to_process = urls[start_index:start_index + limit]
            actual_total = min(limit, total - start_index)
        else:
            urls_to_process = urls[start_index:]
            actual_total = total - start_index
        
        print(f"Labeling {actual_total} URLs (from {total} total)...")
        if start_index > 0:
            print(f"Starting from index {start_index}")
        if limit is not None:
            print(f"Limit: {limit} URLs")
        print()
        
        for i, url in enumerate(urls_to_process, start=start_index):
            print(f"[{i-start_index+1}/{actual_total}] Processing: {url[:80]}...")
            
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


def save_labeled_urls(labeled_articles: List[Dict], output_file: str, input_file: str = None):
    """Save labeled URLs to JSON file"""
    output_data = {
        'labeled_at': datetime.now().isoformat(),
        'total_urls': len(labeled_articles),
        'input_file': input_file,
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


def label_news_urls(input_file: str, output_file: str = None, delay: float = 0.2, start_index: int = 0, limit: int = None):
    """Main function to label URLs from input file"""
    
    if output_file is None:
        # Generate output filename from input filename
        input_path = Path(input_file)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = input_path.parent / f"{input_path.stem}_labeled_{timestamp}.json"
    
    print("="*80)
    print("BBC NEWS CATEGORY LABELER")
    print("="*80)
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"Delay between requests: {delay}s")
    if limit is not None:
        print(f"Limit: {limit} URLs")
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
    
    try:
        labeled_articles = labeler.label_urls(urls, start_index=start_index, limit=limit)
    except KeyboardInterrupt:
        print("\n[WARNING] Interrupted by user. Saving partial results...")
        # Save what we have so far
        if labeled_articles:
            output_data = save_labeled_urls(labeled_articles, output_file, input_file)
            print(f"[OK] Partial results saved: {len(labeled_articles)} URLs labeled")
            print(f"[OK] Output file: {output_file}")
            return output_data
        raise
    
    # Save results
    print()
    print("Saving labeled URLs...")
    output_data = save_labeled_urls(labeled_articles, output_file, input_file)
    
    print("="*80)
    print("LABELING COMPLETE")
    print("="*80)
    print(f"Total URLs labeled: {len(labeled_articles)}")
    print(f"Output file: {output_file}")
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
    parser.add_argument('input_file', help='Input JSON file with URLs')
    parser.add_argument('-o', '--output', help='Output JSON file (default: auto-generated)')
    parser.add_argument('-d', '--delay', type=float, default=0.2, help='Delay between requests in seconds (default: 0.2)')
    parser.add_argument('-s', '--start', type=int, default=0, help='Start from this index (default: 0)')
    parser.add_argument('-l', '--limit', type=int, default=None, help='Limit number of URLs to process (default: all)')
    
    args = parser.parse_args()
    
    try:
        label_news_urls(
            input_file=args.input_file,
            output_file=args.output,
            delay=args.delay,
            start_index=args.start,
            limit=args.limit
        )
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)

