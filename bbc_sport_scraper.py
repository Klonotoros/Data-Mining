"""
BBC Sport Specific Scraper
Scrapes articles from all A-Z Sports categories
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import time
from datetime import datetime
from typing import Dict, List, Optional


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
    
    def extract_category_from_url(self, url: str) -> str:
        """Extract category from article URL (e.g., /news/uk, /sport/football)"""
        path = urlparse(url).path
        parts = [p for p in path.split('/') if p]
        if parts and parts[0] in ['news', 'sport', 'business', 'culture', 'travel', 'earth', 'innovation']:
            return parts[0]
        return "unknown"
    
    def extract_tags(self, soup: BeautifulSoup) -> List[str]:
        """Extract article tags"""
        tags = []
        
        # Try multiple selectors that BBC might use for tags
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
                    tag = elem.string
                
                if tag and tag not in tags:
                    tags.append(tag)
        
        return tags
    
    def extract_category(self, soup: BeautifulSoup, url: str) -> str:
        """Extract article category"""
        category = self.extract_category_from_url(url)
        
        # Try to get more specific category from metadata
        category_meta = soup.find('meta', property='article:section')
        if category_meta and category_meta.get('content'):
            return category_meta.get('content')
        
        return category
    
    def extract_topic(self, soup: BeautifulSoup) -> str:
        """Extract article topic/title"""
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
        
        return "Unknown Topic"
    
    def extract_content(self, soup: BeautifulSoup) -> List[str]:
        """Extract article content/paragraphs"""
        content = []
        
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
                        content.append(text)
        
        return content
    
    def scrape_article(self, article_url: str) -> Dict:
        """Scrape a single article from its URL"""
        print(f"Scraping: {article_url}")
        soup = self.get_page(article_url)
        
        if not soup:
            return None
        
        article_data = {
            'url': article_url,
            'topic': self.extract_topic(soup),
            'category': self.extract_category(soup, article_url),
            'tags': self.extract_tags(soup),
            'content': self.extract_content(soup),
            'content_length': len(' '.join(self.extract_content(soup)))
        }
        
        return article_data



def extract_detailed_category(url):
    """Extract detailed category from URL"""
    # Example: https://www.bbc.com/sport/football -> football
    parts = url.split('/')
    if len(parts) >= 4 and parts[3] == 'sport':
        if len(parts) > 4:
            return parts[4]
    return 'unknown'


def is_sport_article_url(url: str) -> bool:
    """Check if URL is a sport article URL"""
    if '/sport/' not in url:
        return False
    
    # BBC Sport article patterns:
    # - /sport/football/articles/abc123
    # - /sport/football/12345678
    # - /sport/category/articles/...
    # - /sport/category/YYYY/MM/DD/...
    
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


def find_all_article_urls(scraper: BBCArticleScraper, category_url: str) -> set:
    """Find all article URLs from a category page, including pagination - Stops at 1000 URLs per category"""
    article_urls = set()
    pages_to_check = [category_url]
    pages_checked = set([category_url])
    max_urls_per_category = 100
    
    print(f"    Discovering article URLs (max {max_urls_per_category} per category)...", end=" ")
    
    page_num = 0
    while pages_to_check:
        # Stop if we've found enough URLs for this category
        if len(article_urls) >= max_urls_per_category:
            print(f"\n    Reached {max_urls_per_category} URLs limit for this category. Stopping search.")
            break
        
        current_page = pages_to_check.pop(0)
        page_num += 1
        
        if page_num % 10 == 0:
            print(f"\n      Checking page {page_num}... (found {len(article_urls)} URLs so far)", end=" ")
        
        soup = scraper.get_page(current_page)
        if not soup:
            continue
        
        # Find article links
        links = soup.find_all('a', href=True)
        
        for link in links:
            # Stop if we've reached the limit
            if len(article_urls) >= max_urls_per_category:
                break
            
            href = link.get('href')
            if not href:
                continue
            
            # Make absolute URL
            if href.startswith('http'):
                full_url = href
            else:
                full_url = urljoin(scraper.base_url, href)
            
            # Clean URL
            clean_url = full_url.split('#')[0].split('?')[0]
            
            # Check if it's an article URL
            if is_sport_article_url(clean_url):
                article_urls.add(clean_url)
                
                # Stop immediately if we hit the limit after adding a URL
                if len(article_urls) >= max_urls_per_category:
                    print(f"\n    Reached {max_urls_per_category} URLs limit for this category. Stopping search.")
                    break
            
            # Check if it's a pagination link or related category page (same category base)
            if clean_url.startswith(category_url) and clean_url not in pages_checked:
                # Look for pagination indicators or any links within this category
                if any(indicator in clean_url.lower() for indicator in ['/page/', '/p/', '?page=', '?p=', '/archive']):
                    pages_to_check.append(clean_url)
                    pages_checked.add(clean_url)
                # Also check for any links that might lead to more articles (not excluded patterns)
                elif not any(exclude in clean_url.lower() for exclude in ['/live/', '/scores', '/fixtures', '/results']):
                    # If it's a deeper path in the same category, it might have articles
                    path_depth = len([p for p in clean_url.replace(category_url, '').split('/') if p])
                    if path_depth > 0:  # More than just the category URL
                        pages_to_check.append(clean_url)
                        pages_checked.add(clean_url)
            
            # Also check for links to other sport subcategories that might have articles
            # This helps discover archive pages, date-based navigation, etc.
            if '/sport/' in clean_url and clean_url.startswith(scraper.base_url) and clean_url not in pages_checked:
                # Check if it looks like it could lead to more articles
                if any(pattern in clean_url.lower() for pattern in ['/archive', '/articles', '/202', '/201']):
                    if clean_url not in pages_checked:
                        pages_to_check.append(clean_url)
                        pages_checked.add(clean_url)
        
        # Look for "Load more", "Next page", "More articles", "Archive" links
        next_link_keywords = ['more', 'next', 'archive', 'older', 'previous', 'earlier']
        next_links = soup.find_all('a', href=True)
        for next_link in next_links:
            link_text = next_link.get_text().lower() if next_link.get_text() else ''
            href = next_link.get('href')
            
            if href and any(keyword in link_text for keyword in next_link_keywords):
                if href.startswith('http'):
                    next_url = href
                else:
                    next_url = urljoin(scraper.base_url, href)
                clean_next_url = next_url.split('#')[0].split('?')[0]
                
                # Add if it's related to sport and not already checked
                if '/sport/' in clean_next_url and clean_next_url not in pages_checked:
                    pages_to_check.append(clean_next_url)
                    pages_checked.add(clean_next_url)
        
        # Also look for date-based navigation links (month/year archives)
        date_links = soup.find_all('a', href=True)
        for date_link in date_links:
            href = date_link.get('href')
            if href:
                if href.startswith('http'):
                    date_url = href
                else:
                    date_url = urljoin(scraper.base_url, href)
                clean_date_url = date_url.split('#')[0].split('?')[0]
                
                # Check if URL contains date patterns (YYYY/MM format)
                if '/sport/' in clean_date_url and any(year in clean_date_url for year in ['/202', '/201', '/2020', '/2021', '/2022', '/2023', '/2024', '/2025']):
                    if clean_date_url not in pages_checked:
                        pages_to_check.append(clean_date_url)
                        pages_checked.add(clean_date_url)
        
        # Check if we've reached the limit before continuing to next page
        if len(article_urls) >= max_urls_per_category:
            break
        
        time.sleep(0.1)  # Small delay between page checks
    
    print(f"\n    Complete! Found {len(article_urls)} URLs after checking {page_num} pages")
    return article_urls


def scrape_sport_articles():
    """Scrape as many sport articles as possible from all A-Z Sports categories"""
    
    scraper = BBCArticleScraper()
    results = []
    scraped_urls = set()  # To avoid duplicates across all categories
    
    print("="*80)
    print("BBC SPORT SCRAPER - Unlimited Articles")
    print("="*80)
    print("Mode: Collect ALL possible sport article URLs, then scrape them")
    print("="*80)
    main_sections = [
        'https://www.bbc.com/sport/american-football',
        'https://www.bbc.com/sport/archery',
        'https://www.bbc.com/sport/athletics',
        'https://www.bbc.com/sport/badminton',
        'https://www.bbc.com/sport/baseball',
        'https://www.bbc.com/sport/basketball',
        'https://www.bbc.com/sport/bowls',
        'https://www.bbc.com/sport/boxing',
        'https://www.bbc.com/sport/canoeing',
        'https://www.bbc.com/sport/commonwealth-games',
        'https://www.bbc.com/sport/cricket',
        'https://www.bbc.com/sport/curling',
        'https://www.bbc.com/sport/cycling',
        'https://www.bbc.com/sport/darts',
        'https://www.bbc.com/sport/disability-sport',
        'https://www.bbc.com/sport/diving',
        'https://www.bbc.com/sport/equestrian',
        'https://www.bbc.com/sport/fencing',
        'https://www.bbc.com/sport/football',
        'https://www.bbc.com/sport/formula1',
        'https://www.bbc.com/sport/gaelic-games',
        'https://www.bbc.com/sport/golf',
        'https://www.bbc.com/sport/gymnastics',
        'https://www.bbc.com/sport/handball',
        'https://www.bbc.com/sport/hockey',
        'https://www.bbc.com/sport/horse-racing',
        'https://www.bbc.com/sport/ice-hockey',
        'https://www.bbc.com/sport/judo',
        'https://www.bbc.com/sport/karate',
        'https://www.bbc.com/sport/mixed-martial-arts',
        'https://www.bbc.com/sport/modern-pentathlon',
        'https://www.bbc.com/sport/motorsport',
        'https://www.bbc.com/sport/netball',
        'https://www.bbc.com/sport/olympics',
        'https://www.bbc.com/sport/rowing',
        'https://www.bbc.com/sport/rugby-league',
        'https://www.bbc.com/sport/rugby-union',
        'https://www.bbc.com/sport/sailing',
        'https://www.bbc.com/sport/shooting',
        'https://www.bbc.com/sport/snooker',
        'https://www.bbc.com/sport/sport-climbing',
        'https://www.bbc.com/sport/squash',
        'https://www.bbc.com/sport/swimming',
        'https://www.bbc.com/sport/table-tennis',
        'https://www.bbc.com/sport/taekwondo',
        'https://www.bbc.com/sport/tennis',
        'https://www.bbc.com/sport/triathlon',
        'https://www.bbc.com/sport/volleyball',
        'https://www.bbc.com/sport/water-polo',
        'https://www.bbc.com/sport/weightlifting',
        'https://www.bbc.com/sport/winter-sports',
        'https://www.bbc.com/sport/wrestling',
    ]
    
    # PHASE 1: Collect ALL article URLs from all categories
    print("\n" + "="*80)
    print("PHASE 1: Discovering article URLs")
    print("="*80)
    
    all_article_urls = set()
    sections_checked = 0
    
    for section_url in main_sections:
        detailed_category = extract_detailed_category(section_url)
        sections_checked += 1
        print(f"\n[{sections_checked}/{len(main_sections)}] {detailed_category}: {section_url}")
        
        try:
            category_urls = find_all_article_urls(scraper, section_url)
            all_article_urls.update(category_urls)
            print(f"    Total URLs found so far: {len(all_article_urls)}")
        except Exception as e:
            print(f"  [ERROR] {e}")
            continue
        
        time.sleep(0.2)  # Rate limiting between categories
    
    print(f"\n{'='*80}")
    print(f"PHASE 1 COMPLETE: Found {len(all_article_urls)} unique article URLs")
    print(f"{'='*80}\n")
    
    # PHASE 2: Scrape all collected URLs
    print("="*80)
    print("PHASE 2: Scraping articles")
    print("="*80)
    
    total_urls = len(all_article_urls)
    scraped_count = 0
    failed_count = 0
    
    for idx, article_url in enumerate(sorted(all_article_urls), 1):
        if article_url in scraped_urls:
            continue
        
        # Extract category for this URL
        detailed_category = extract_detailed_category(article_url)
        
        if idx % 10 == 0 or idx == 1:
            print(f"[{idx}/{total_urls}] Scraping article...")
        
        try:
            article_data = scraper.scrape_article(article_url)
            
            if article_data and len(article_data.get('content', [])) > 0:
                result = {
                    'url': article_url,
                    'main_category': 'sport',
                    'detailed_category': detailed_category,
                    'topic': article_data.get('topic'),
                    'content': article_data.get('content')
                }
                results.append(result)
                scraped_urls.add(article_url)
                scraped_count += 1
            else:
                failed_count += 1
        except Exception as e:
            print(f"  [ERROR] Failed to scrape {article_url}: {e}")
            failed_count += 1
            continue
        
        time.sleep(0.3)  # Rate limiting
    
    print(f"\n{'='*80}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*80}")
    print(f"Total URLs discovered: {total_urls}")
    print(f"Successfully scraped: {scraped_count}")
    print(f"Failed to scrape: {failed_count}")
    print(f"Total articles saved: {len(results)}")
    
    # Group articles by detailed_category
    articles_by_category = {}
    for article in results:
        cat = article.get('detailed_category', 'unknown')
        if cat not in articles_by_category:
            articles_by_category[cat] = []
        articles_by_category[cat].append(article)
    
    # Save separate JSON file for each category
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    saved_files = []
    
    for category, category_articles in articles_by_category.items():
        filename = f'bbc_sport_{category}_{len(category_articles)}_articles_{timestamp}.json'
        
        output = {
            'scraped_at': datetime.now().isoformat(),
            'total_articles': len(category_articles),
            'category': category,
            'articles': category_articles
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        saved_files.append(filename)
        print(f"Saved: {filename} ({len(category_articles)} articles)")
    
    # Summary by detailed category
    categories = {}
    for article in results:
        cat = article.get('detailed_category', 'unknown')
        categories[cat] = categories.get(cat, 0) + 1
    
    print(f"\n{'='*80}")
    print(f"DETAILED CATEGORY BREAKDOWN:")
    print(f"{'='*80}")
    for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
        print(f"  {cat}: {count} articles")
    
    print(f"\n{'='*80}")
    print(f"Total files created: {len(saved_files)}")
    print(f"{'='*80}\n")
    
    # Generate report file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f'bbc_sport_scraping_report_{timestamp}.txt'
    
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("BBC SPORT SCRAPING REPORT\n")
        f.write("="*80 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Total Categories: {len(main_sections)}\n")
        f.write(f"Total URLs Discovered: {total_urls}\n")
        f.write(f"Successfully Scraped: {scraped_count}\n")
        f.write(f"Failed to Scrape: {failed_count}\n")
        f.write(f"Total Articles Saved: {len(results)}\n")
        f.write("="*80 + "\n\n")
        
        f.write("CATEGORY BREAKDOWN:\n")
        f.write("-"*80 + "\n")
        f.write(f"{'Category':<30} {'Articles':>10} {'Filename':>35}\n")
        f.write("-"*80 + "\n")
        
        for category, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            filename = f"bbc_sport_{category}_{count}_articles_{timestamp[:8]}.json"
            f.write(f"{category:<30} {count:>10} {filename:>35}\n")
        
        # Add categories with 0 articles
        scraped_categories = set(categories.keys())
        all_categories = [extract_detailed_category(url) for url in main_sections]
        missing_categories = [cat for cat in all_categories if cat not in scraped_categories]
        
        if missing_categories:
            f.write("\n" + "="*80 + "\n")
            f.write("CATEGORIES WITH 0 ARTICLES:\n")
            f.write("-"*80 + "\n")
            f.write(f"{'Category':<30} {'Articles':>10}\n")
            f.write("-"*80 + "\n")
            for cat in sorted(missing_categories):
                f.write(f"{cat:<30} {0:>10}\n")
        
        f.write("\n" + "="*80 + "\n")
        f.write("SUMMARY STATISTICS:\n")
        f.write("-"*80 + "\n")
        f.write(f"Categories with articles: {len(scraped_categories)}\n")
        f.write(f"Categories with 0 articles: {len(missing_categories)}\n")
        if total_urls > 0:
            f.write(f"Success rate: {scraped_count}/{total_urls} URLs ({scraped_count/total_urls*100:.1f}%)\n")
        else:
            f.write("Success rate: N/A\n")
        f.write("="*80 + "\n")
    
    print(f"\n[REPORT] Generated: {report_filename}")
    print("="*80 + "\n")
    
    return results


if __name__ == "__main__":
    results = scrape_sport_articles()

