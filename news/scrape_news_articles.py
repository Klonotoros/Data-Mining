#!/usr/bin/env python3
"""
Script to scrape text content from news articles in url_labeled.json
Creates array of objects with "text" and "label" fields.

Usage:
    python scrape_news_articles.py [url_labeled.json] [-o output_file.json] [-d delay] [-s start_index] [-l limit] [-c checkpoint]
"""

import json
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import time
import sys

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()


class BBCNewsArticleScraper:
    """Scraper for BBC News articles"""
    
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
    
    def extract_content(self, soup: BeautifulSoup) -> str:
        """Extract article content as a single string"""
        content_paragraphs = []
        
        # BBC uses various class names for article content
        content_selectors = [
            'div[data-component="text-block"]',
            '.story-body__inner p',
            '.article-body p',
            'article p',
            '.ssrcss-pv1rh6-Paragraph eq5iqo00',
            'p.ssrcss-1q0x1qg-Paragraph'
        ]
        
        for selector in content_selectors:
            paragraphs = soup.select(selector)
            if paragraphs:
                for p in paragraphs:
                    text = p.get_text().strip()
                    # Filter out very short text and common non-content elements
                    if text and len(text) > 20:
                        # Skip common non-content patterns
                        if not any(skip in text.lower() for skip in [
                            'cookie', 'javascript', 'enable', 'disable',
                            'subscribe', 'newsletter', 'follow us', 'share this'
                        ]):
                            content_paragraphs.append(text)
                if content_paragraphs:  # If we found content with this selector, use it
                    break
        
        # Join all paragraphs with newlines
        return '\n\n'.join(content_paragraphs)
    
    def scrape_article_text(self, article_url: str) -> Optional[str]:
        """Scrape article content from URL
        
        Returns:
            Article content text or None if failed
        """
        soup = self.get_page(article_url)
        
        if not soup:
            return None
        
        content = self.extract_content(soup)
        
        if not content or len(content.strip()) < 50:  # Skip articles with very little content
            return None
        
        return content


def scrape_news_articles(input_file: str = "url_labeled.json",
                        output_file: str = None,
                        delay: float = 0.2,
                        start_index: int = 0,
                        limit: int = None,
                        checkpoint_interval: int = 1000):
    """
    Scrape all news articles from url_labeled.json and create array with "text" and "label" fields.
    
    Args:
        input_file: Path to url_labeled.json
        output_file: Path to output JSON file (default: auto-generated)
        delay: Delay between requests in seconds (default: 0.2)
        start_index: Start from this index (default: 0)
        limit: Limit number of articles to process (default: unlimited)
        checkpoint_interval: Save checkpoint every N articles (default: 1000)
    """
    input_path = Path(input_file)
    if not input_path.is_absolute():
        input_path = SCRIPT_DIR / input_path
    
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = SCRIPT_DIR / f"news_articles_{timestamp}.json"
    else:
        output_path = Path(output_file)
        if not output_path.is_absolute():
            output_file = SCRIPT_DIR / output_path
    
    print("="*80)
    print("SCRAPE NEWS ARTICLES")
    print("="*80)
    print(f"Input file: {input_path}")
    print(f"Output file: {output_file}")
    print(f"Delay between requests: {delay}s")
    print(f"Start index: {start_index}")
    print(f"Limit: {limit} articles" if limit is not None else "Limit: All articles")
    print(f"Checkpoint interval: every {checkpoint_interval} articles")
    print("="*80)
    print()
    
    # Read the input JSON file
    print("Reading articles from url_labeled.json...")
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load file: {e}")
        return None
    
    articles = data.get('articles', [])
    total_articles = len(articles)
    
    print(f"[OK] Found {total_articles} articles")
    print()
    
    # Apply start_index and limit
    if start_index > 0:
        articles = articles[start_index:]
        print(f"[INFO] Starting from index {start_index}")
    
    if limit is not None:
        articles = articles[:limit]
        print(f"[INFO] Processing {len(articles)} articles (limited)")
    else:
        print(f"[INFO] Processing all {len(articles)} articles")
    
    print()
    
    scraper = BBCNewsArticleScraper(delay=delay)
    result_articles = []  # Array of objects with "text" and "label"
    successful = 0
    failed = 0
    checkpoint_count = [0]
    
    def save_checkpoint():
        """Save checkpoint file"""
        nonlocal checkpoint_count
        checkpoint_count[0] += 1
        checkpoint_file = SCRIPT_DIR / f"news_articles_checkpoint_{checkpoint_count[0]}_{len(result_articles)}.json"
        
        # Save as array
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(result_articles, f, indent=2, ensure_ascii=False)
        
        print(f"    [CHECKPOINT] Saved: {checkpoint_file}")
    
    # Process each article
    try:
        for idx, article in enumerate(articles, start=start_index):
            url = article.get('url')
            label = article.get('category', 'unknown')  # Use category as label (keep 'unknown' if unknown)
            
            if not url:
                failed += 1
                continue
            
            # Progress update
            current_num = idx - start_index + 1
            total_to_process = len(articles)
            
            if current_num % 10 == 0 or current_num == 1:
                print(f"[{current_num}/{total_to_process}] Processing... (Success: {successful}, Failed: {failed})")
                print(f"    URL: {url[:80]}...")
            
            # Scrape article text
            try:
                text = scraper.scrape_article_text(url)
                
                if text:
                    # Create object with "text" and "label" fields
                    article_obj = {
                        'text': text,
                        'label': label  # category becomes label (unknown stays unknown)
                    }
                    result_articles.append(article_obj)
                    successful += 1
                    print(f"    [OK] Scraped: {label}")
                else:
                    failed += 1
                    print(f"    [FAILED] Could not scrape content")
            except Exception as e:
                print(f"    [ERROR] Error scraping {url}: {e}")
                failed += 1
                continue
            
            # Save checkpoint
            if len(result_articles) > 0 and len(result_articles) % checkpoint_interval == 0:
                print(f"\n    [CHECKPOINT] Saving checkpoint at {len(result_articles)} articles...")
                save_checkpoint()
                print()
            
            # Rate limiting
            if idx < start_index + len(articles) - 1:  # Don't sleep after last URL
                time.sleep(delay)
    
    except KeyboardInterrupt:
        print("\n[WARNING] Interrupted by user. Saving partial results...")
        if result_articles:
            save_checkpoint()
        print(f"[OK] Partial results saved: {len(result_articles)} articles scraped")
        return None
    
    # Write to output file as array
    print()
    print(f"Writing {len(result_articles)} articles to: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result_articles, f, indent=2, ensure_ascii=False)
    
    print()
    print("="*80)
    print("SCRAPING COMPLETE")
    print("="*80)
    print(f"Total articles in input: {total_articles}")
    print(f"Articles processed: {len(articles)}")
    print(f"Successfully scraped: {successful}")
    print(f"Failed: {failed}")
    print(f"Articles saved: {len(result_articles)}")
    print(f"Saved to: {output_file}")
    if checkpoint_count[0] > 0:
        print(f"Checkpoints saved: {checkpoint_count[0]}")
    
    # Show label distribution
    label_counts = {}
    for article in result_articles:
        label = article.get('label', 'unknown')
        label_counts[label] = label_counts.get(label, 0) + 1
    
    print()
    print("Label distribution:")
    print("-" * 80)
    for label, count in sorted(label_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {label:<30} {count:>10}")
    print("="*80)
    
    return result_articles


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape news articles from url_labeled.json')
    parser.add_argument('input_file', nargs='?', default='url_labeled.json', help='Input JSON file (default: url_labeled.json)')
    parser.add_argument('-o', '--output', help='Output JSON file (default: auto-generated)')
    parser.add_argument('-d', '--delay', type=float, default=0.2, help='Delay between requests in seconds (default: 0.2)')
    parser.add_argument('-s', '--start', type=int, default=0, help='Start from this index (default: 0)')
    parser.add_argument('-l', '--limit', type=int, default=None, help='Limit number of articles to process (default: unlimited)')
    parser.add_argument('-c', '--checkpoint', type=int, default=1000, help='Checkpoint interval - save every N articles (default: 1000)')
    
    args = parser.parse_args()
    
    # Convert -1 to None (process all) for backward compatibility
    limit = None if args.limit == -1 else args.limit
    
    try:
        scrape_news_articles(
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
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

