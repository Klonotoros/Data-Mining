"""
Relabel Unknown Articles Using Keyword Analysis
Analyzes existing labeled articles to build category keyword profiles,
then uses those profiles to classify unknown articles.

Usage:
    python relabel_unknown_articles.py [url_labeled.json] [-d delay] [-s start_index] [-l limit]
"""

import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Set
from pathlib import Path
from collections import Counter, defaultdict
import re
import sys

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()


class KeywordBasedLabeler:
    """Uses keyword analysis to label unknown articles"""
    
    def __init__(self, delay: float = 0.2):
        self.base_url = "https://www.bbc.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.delay = delay
        self.category_keywords = defaultdict(Counter)
        self.category_profiles = {}
    
    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a webpage"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            print(f"    [ERROR] Error fetching {url}: {e}")
            return None
    
    def extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article title"""
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
        return None
    
    def extract_tags(self, soup: BeautifulSoup) -> List[str]:
        """Extract article tags"""
        tags = []
        tag_selectors = [
            'meta[property="article:tag"]',
            'meta[name="keywords"]',
            '.story-body__keywords',
            'div[data-component="tag-list"] a',
            '.ssrcss-tq36fq-List eqd0sw14 a',
            'a.ssrcss-c5gviz-Tag'
        ]
        
        for selector in tag_selectors:
            elements = soup.select(selector)
            for elem in elements:
                if elem.get('content'):
                    tag = elem.get('content')
                elif elem.get_text():
                    tag = elem.get_text().strip()
                else:
                    continue
                
                if tag and tag not in tags:
                    tags.append(tag)
        
        return tags
    
    def extract_keywords_from_text(self, text: str) -> Set[str]:
        """Extract meaningful keywords from text"""
        if not text:
            return set()
        
        # Convert to lowercase and split
        words = re.findall(r'\b[a-z]{3,}\b', text.lower())
        
        # Common stop words to filter out
        stop_words = {
            'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'her',
            'was', 'one', 'our', 'out', 'day', 'get', 'has', 'him', 'his', 'how',
            'its', 'may', 'new', 'now', 'old', 'see', 'two', 'way', 'who', 'boy',
            'did', 'has', 'let', 'put', 'say', 'she', 'too', 'use', 'bbc', 'news',
            'said', 'will', 'this', 'that', 'with', 'from', 'have', 'been', 'more',
            'than', 'their', 'would', 'there', 'about', 'which', 'could', 'other'
        }
        
        # Filter stop words and get unique words
        keywords = {w for w in words if w not in stop_words and len(w) > 3}
        return keywords
    
    def build_category_profiles(self, articles: List[Dict], min_keywords: int = 5):
        """Build keyword profiles for each category from labeled articles"""
        print("Building category keyword profiles from labeled articles...")
        
        # Collect keywords for each category
        for article in articles:
            category = article.get('category')
            if not category or category == 'unknown':
                continue
            
            # Try to get title from article data or fetch it
            title = article.get('title')
            tags = article.get('tags', [])
            
            # If title not in article, we'll fetch it later for unknown articles
            if title:
                keywords = self.extract_keywords_from_text(title)
                # Add tags as keywords too
                for tag in tags:
                    if tag:
                        tag_keywords = self.extract_keywords_from_text(tag)
                        keywords.update(tag_keywords)
                
                # Add keywords to category profile
                for keyword in keywords:
                    self.category_keywords[category][keyword] += 1
        
        # Build profiles: keep top keywords per category
        for category, keyword_counts in self.category_keywords.items():
            # Get top keywords (appearing in at least min_keywords articles)
            top_keywords = {
                kw: count 
                for kw, count in keyword_counts.most_common(100)
                if count >= min_keywords
            }
            self.category_profiles[category] = top_keywords
            print(f"  {category}: {len(top_keywords)} keywords")
        
        print(f"[OK] Built profiles for {len(self.category_profiles)} categories")
        return self.category_profiles
    
    def classify_by_keywords(self, title: str, tags: List[str] = None) -> Optional[tuple]:
        """Classify article based on title and tags keywords
        
        Returns:
            Tuple of (category, confidence_score) or None
        """
        if not title:
            return None
        
        title_keywords = self.extract_keywords_from_text(title)
        tag_keywords = set()
        if tags:
            for tag in tags:
                tag_keywords.update(self.extract_keywords_from_text(tag))
        
        all_keywords = title_keywords | tag_keywords
        
        if not all_keywords:
            return None
        
        # Score each category
        category_scores = {}
        for category, profile_keywords in self.category_profiles.items():
            if not profile_keywords:
                continue
            
            # Count matching keywords
            matches = sum(1 for kw in all_keywords if kw in profile_keywords)
            if matches > 0:
                # Calculate confidence: matches / total keywords in profile
                confidence = matches / len(profile_keywords)
                # Weight by how common the keywords are
                weighted_score = sum(
                    profile_keywords.get(kw, 0) 
                    for kw in all_keywords 
                    if kw in profile_keywords
                )
                category_scores[category] = (matches, confidence, weighted_score)
        
        if not category_scores:
            return None
        
        # Get best match (highest weighted score)
        best_category = max(category_scores.items(), key=lambda x: x[1][2])
        category, (matches, confidence, weighted_score) = best_category
        
        # Only return if we have reasonable confidence
        if matches >= 2 and confidence > 0.1:  # At least 2 keyword matches
            return (category, confidence)
        
        return None
    
    def fetch_and_classify(self, url: str) -> Optional[tuple]:
        """Fetch article and classify it"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        title = self.extract_title(soup)
        tags = self.extract_tags(soup)
        
        if not title:
            return None
        
        return self.classify_by_keywords(title, tags)


def load_labeled_urls(input_file: str) -> Dict:
    """Load labeled URLs from JSON file"""
    with open(input_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_labeled_urls(data: Dict, output_file: str):
    """Save labeled URLs to JSON file"""
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def relabel_unknown_articles(input_file: str = "url_labeled.json",
                             delay: float = 0.2,
                             start_index: int = 0,
                             limit: int = None,
                             checkpoint_interval: int = 500,
                             min_keywords: int = 5):
    """Relabel unknown articles using keyword analysis
    
    Args:
        input_file: Path to url_labeled.json file
        delay: Delay between requests
        start_index: Start from this index in unknown articles list
        limit: Limit number of articles to process
        checkpoint_interval: Save checkpoint every N articles
        min_keywords: Minimum keyword occurrences to include in profile
    """
    input_path = Path(input_file)
    if not input_path.is_absolute():
        input_path = SCRIPT_DIR / input_path
    
    print("="*80)
    print("RELABEL UNKNOWN ARTICLES - KEYWORD ANALYSIS")
    print("="*80)
    print(f"Input file: {input_path}")
    print(f"Delay between requests: {delay}s")
    print(f"Checkpoint interval: every {checkpoint_interval} articles")
    print("="*80)
    print()
    
    # Load the file
    print("Loading url_labeled.json...")
    try:
        data = load_labeled_urls(str(input_path))
        print(f"[OK] File loaded successfully")
    except Exception as e:
        print(f"[ERROR] Failed to load file: {e}")
        return None
    
    articles = data.get('articles', [])
    if not articles:
        print("[ERROR] No articles found in file")
        return None
    
    print(f"[INFO] Total articles: {len(articles)}")
    
    # Separate known and unknown articles
    known_articles = [a for a in articles if a.get('category') and a.get('category') != 'unknown']
    unknown_articles = [
        (i, a) for i, a in enumerate(articles) 
        if a.get('category') == 'unknown'
    ]
    
    print(f"[INFO] Known articles: {len(known_articles)}")
    print(f"[INFO] Unknown articles: {len(unknown_articles)}")
    print()
    
    if not known_articles:
        print("[ERROR] No known articles found. Need labeled articles to build profiles.")
        return None
    
    if not unknown_articles:
        print("[INFO] No unknown articles to relabel.")
        return data
    
    # Initialize labeler and build profiles
    labeler = KeywordBasedLabeler(delay=delay)
    
    # First, try to extract titles from known articles if available
    # Otherwise, we'll build profiles from what we have
    print("Building keyword profiles...")
    labeler.build_category_profiles(known_articles, min_keywords=min_keywords)
    print()
    
    # Apply start_index and limit
    if start_index > 0:
        unknown_articles = unknown_articles[start_index:]
        print(f"[INFO] Starting from index {start_index}")
    
    if limit is not None:
        unknown_articles = unknown_articles[:limit]
        print(f"[INFO] Processing {len(unknown_articles)} articles (limited)")
    else:
        print(f"[INFO] Processing all {len(unknown_articles)} unknown articles")
    
    print()
    
    # Process unknown articles
    processed_count = 0
    relabeled_count = 0
    category_changes = Counter()
    checkpoint_count = [0]
    
    def save_checkpoint():
        """Save checkpoint"""
        nonlocal checkpoint_count
        checkpoint_count[0] += 1
        backup_path = input_path.with_suffix('.json.backup')
        save_labeled_urls(data, str(backup_path))
        print(f"    [CHECKPOINT] Saved backup: {backup_path}")
    
    try:
        for idx, (article_idx, article) in enumerate(unknown_articles, start=start_index):
            url = article.get('url')
            
            if not url:
                continue
            
            current_num = idx - start_index + 1
            total_to_process = len(unknown_articles)
            
            print(f"[{current_num}/{total_to_process}] Processing: {url[:80]}...")
            
            # Classify article
            result = labeler.fetch_and_classify(url)
            
            if result:
                new_category, confidence = result
                old_category = article.get('category', 'unknown')
                article['category'] = new_category
                article['relabeled_at'] = datetime.now().isoformat()
                article['relabel_confidence'] = round(confidence, 3)
                article['relabel_method'] = 'keyword_analysis'
                
                category_changes[new_category] += 1
                relabeled_count += 1
                print(f"    [OK] Relabeled: {old_category} -> {new_category} (confidence: {confidence:.2f})")
            else:
                print(f"    [SKIP] Could not classify (keeping as unknown)")
            
            processed_count += 1
            
            # Save checkpoint
            if processed_count % checkpoint_interval == 0:
                print(f"\n    [CHECKPOINT] Saving checkpoint at {processed_count} articles...")
                save_checkpoint()
                print(f"    [OK] Checkpoint saved\n")
            
            # Rate limiting
            if idx < len(unknown_articles) - 1:  # Don't delay after last item
                time.sleep(delay)
    
    except KeyboardInterrupt:
        print("\n[WARNING] Interrupted by user. Saving partial results...")
        save_checkpoint()
        print(f"[OK] Partial results saved: {processed_count} articles processed, {relabeled_count} relabeled")
        return data
    
    # Save final results
    print()
    print("Saving final results...")
    save_labeled_urls(data, str(input_path))
    print(f"[OK] File updated: {input_path}")
    
    # Update metadata
    data['relabeled_at'] = datetime.now().isoformat()
    data['relabel_stats'] = {
        'processed': processed_count,
        'relabeled': relabeled_count,
        'category_changes': dict(category_changes)
    }
    
    # Save again with metadata
    save_labeled_urls(data, str(input_path))
    
    print()
    print("="*80)
    print("RELABELING COMPLETE")
    print("="*80)
    print(f"Articles processed: {processed_count}")
    print(f"Articles relabeled: {relabeled_count}")
    print(f"Success rate: {relabeled_count/processed_count*100:.1f}%" if processed_count > 0 else "N/A")
    print()
    print("Category distribution (new labels):")
    print("-" * 80)
    for category, count in sorted(category_changes.items(), key=lambda x: x[1], reverse=True):
        print(f"  {category:<30} {count:>10}")
    print("="*80)
    
    return data


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Relabel unknown articles using keyword analysis')
    parser.add_argument('input_file', nargs='?', default='url_labeled.json', help='Input JSON file (default: url_labeled.json)')
    parser.add_argument('-d', '--delay', type=float, default=0.2, help='Delay between requests in seconds (default: 0.2)')
    parser.add_argument('-s', '--start', type=int, default=0, help='Start from this index (default: 0)')
    parser.add_argument('-l', '--limit', type=int, default=None, help='Limit number of articles to process (default: unlimited)')
    parser.add_argument('-c', '--checkpoint', type=int, default=500, help='Checkpoint interval - save every N articles (default: 500)')
    parser.add_argument('-k', '--min-keywords', type=int, default=5, help='Minimum keyword occurrences for profile (default: 5)')
    
    args = parser.parse_args()
    
    # Convert -1 to None (process all) for backward compatibility
    limit = None if args.limit == -1 else args.limit
    
    try:
        relabel_unknown_articles(
            input_file=args.input_file,
            delay=args.delay,
            start_index=args.start,
            limit=limit,
            checkpoint_interval=args.checkpoint,
            min_keywords=args.min_keywords
        )
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

