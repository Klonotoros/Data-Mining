"""
Merge BBC News URL JSON files, remove duplicates, and label with categories
"""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Set, List, Dict, Optional

# Import the labeler from the label_news_categories module
from label_news_categories import BBCNewsCategoryLabeler

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()


def normalize_url(url: str) -> str:
    """
    Normalize URL to handle http/https and www variations for duplicate detection
    """
    # Convert to lowercase and remove trailing slashes
    url = url.lower().rstrip('/')
    # Normalize http to https
    if url.startswith('http://'):
        url = url.replace('http://', 'https://', 1)
    # Normalize www variations
    url = url.replace('www.bbc.co.uk', 'bbc.co.uk')
    url = url.replace('www.bbc.com', 'bbc.com')
    return url


def load_json_file(file_path: Path) -> dict:
    """Load JSON file and return its contents"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {file_path}: {e}")
        raise


def merge_and_label_urls(file1_path: Path, file2_path: Path, output_path: Path = None, 
                          delay: float = 0.2, label_urls: bool = True) -> dict:
    """
    Merge two JSON files containing URLs, remove duplicates, and label with categories
    
    Args:
        file1_path: Path to first JSON file
        file2_path: Path to second JSON file
        output_path: Optional path for output file. If None, generates timestamped filename
        delay: Delay between requests when labeling (default: 0.2 seconds)
        label_urls: Whether to label URLs with categories (default: True)
    
    Returns:
        Dictionary containing merged and labeled URLs
    """
    print("="*80)
    print("BBC NEWS URL MERGER AND LABELER")
    print("="*80)
    print(f"Loading {file1_path}...")
    data1 = load_json_file(file1_path)
    
    print(f"Loading {file2_path}...")
    data2 = load_json_file(file2_path)
    
    # Extract URLs from both files
    urls1 = data1.get('urls', [])
    urls2 = data2.get('urls', [])
    
    print(f"File 1 contains {len(urls1)} URLs")
    print(f"File 2 contains {len(urls2)} URLs")
    
    # Use a dictionary to track unique URLs (normalized as key, original as value)
    # This preserves the original URL format from the first occurrence
    unique_urls_dict = {}
    seen_normalized: Set[str] = set()
    
    # Process URLs from first file
    for url in urls1:
        normalized = normalize_url(url)
        if normalized not in seen_normalized:
            unique_urls_dict[normalized] = url
            seen_normalized.add(normalized)
    
    # Process URLs from second file
    duplicates_count = 0
    for url in urls2:
        normalized = normalize_url(url)
        if normalized not in seen_normalized:
            unique_urls_dict[normalized] = url
            seen_normalized.add(normalized)
        else:
            duplicates_count += 1
    
    # Convert dictionary values to list (preserving original URL format)
    merged_urls = list(unique_urls_dict.values())
    
    print(f"\nMerged results:")
    print(f"  Total unique URLs: {len(merged_urls)}")
    print(f"  Duplicates removed: {duplicates_count}")
    
    # Label URLs with categories
    labeled_articles = []
    category_counts = {}
    
    if label_urls:
        print("\n" + "="*80)
        print("LABELING URLs WITH CATEGORIES")
        print("="*80)
        print(f"Processing {len(merged_urls)} URLs...")
        print(f"Delay between requests: {delay}s")
        print()
        
        labeler = BBCNewsCategoryLabeler(delay=delay)
        
        for i, url in enumerate(merged_urls, 1):
            print(f"[{i}/{len(merged_urls)}] Processing: {url[:80]}...")
            
            category = labeler.extract_category(url)
            if not category:
                category = 'unknown'
            
            article_data = {
                'url': url,
                'category': category,
                'labeled_at': datetime.now().isoformat()
            }
            
            labeled_articles.append(article_data)
            
            # Update category counts
            category_counts[category] = category_counts.get(category, 0) + 1
            
            if category and category != 'unknown':
                print(f"    [OK] Category: {category}")
            else:
                print(f"    [WARNING] Category: {category}")
            
            # Rate limiting (don't delay after last item)
            if i < len(merged_urls):
                time.sleep(delay)
        
        print("\n" + "="*80)
        print("LABELING COMPLETE")
        print("="*80)
    else:
        # If not labeling, create simple URL objects
        for url in merged_urls:
            labeled_articles.append({
                'url': url,
                'category': None,
                'labeled_at': None
            })
    
    # Create merged data structure
    merged_data = {
        "merged_at": datetime.now().isoformat(),
        "category": data1.get('category', 'news'),
        "category_url": data1.get('category_url', 'https://www.bbc.com/news'),
        "total_urls": len(merged_urls),
        "duplicates_removed": duplicates_count,
        "source_files": [
            str(file1_path.name),
            str(file2_path.name)
        ],
        "articles": labeled_articles
    }
    
    # Add category summary if URLs were labeled
    if label_urls and category_counts:
        merged_data["category_counts"] = category_counts
        merged_data["category_summary"] = {
            category: count 
            for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
        }
    
    # Generate output filename if not provided
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if label_urls:
            output_path = SCRIPT_DIR / f"bbc_news_merged_labeled_{len(merged_urls)}_urls_{timestamp}.json"
        else:
            output_path = SCRIPT_DIR / f"bbc_news_merged_{len(merged_urls)}_urls_{timestamp}.json"
    
    # Save merged data
    print(f"\nSaving merged and labeled data to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    print(f"Successfully saved {len(merged_urls)} unique URLs to {output_path}")
    
    # Print category summary
    if label_urls and category_counts:
        print("\n" + "="*80)
        print("CATEGORY SUMMARY")
        print("="*80)
        for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(merged_urls)) * 100
            print(f"  {category:<20} {count:>10} ({percentage:>5.1f}%)")
        print("="*80)
    
    return merged_data


def label_existing_merged_file(input_file: Path, output_path: Path = None, delay: float = 0.2) -> dict:
    """
    Label URLs from an already merged JSON file
    
    Args:
        input_file: Path to merged JSON file with URLs
        output_path: Optional path for output file. If None, generates timestamped filename
        delay: Delay between requests when labeling (default: 0.2 seconds)
    
    Returns:
        Dictionary containing labeled URLs
    """
    print("="*80)
    print("BBC NEWS URL LABELER")
    print("="*80)
    print(f"Loading {input_file}...")
    data = load_json_file(input_file)
    
    # Extract URLs - handle both 'urls' array and 'articles' array formats
    if 'urls' in data:
        urls = data.get('urls', [])
    elif 'articles' in data:
        # If already has articles, extract URLs from them
        articles = data.get('articles', [])
        if articles and isinstance(articles[0], dict) and 'url' in articles[0]:
            urls = [article.get('url') for article in articles if article.get('url')]
        else:
            urls = []
    else:
        raise ValueError("Could not find 'urls' or 'articles' in input file")
    
    print(f"Found {len(urls)} URLs to label")
    
    # Label URLs with categories
    labeled_articles = []
    category_counts = {}
    
    print("\n" + "="*80)
    print("LABELING URLs WITH CATEGORIES")
    print("="*80)
    print(f"Processing {len(urls)} URLs...")
    print(f"Delay between requests: {delay}s")
    print()
    
    labeler = BBCNewsCategoryLabeler(delay=delay)
    
    for i, url in enumerate(urls, 1):
        print(f"[{i}/{len(urls)}] Processing: {url[:80]}...")
        
        category = labeler.extract_category(url)
        if not category:
            category = 'unknown'
        
        article_data = {
            'url': url,
            'category': category,
            'labeled_at': datetime.now().isoformat()
        }
        
        labeled_articles.append(article_data)
        
        # Update category counts
        category_counts[category] = category_counts.get(category, 0) + 1
        
        if category and category != 'unknown':
            print(f"    [OK] Category: {category}")
        else:
            print(f"    [WARNING] Category: {category}")
        
        # Rate limiting (don't delay after last item)
        if i < len(urls):
            time.sleep(delay)
    
    print("\n" + "="*80)
    print("LABELING COMPLETE")
    print("="*80)
    
    # Create output data structure
    output_data = {
        "labeled_at": datetime.now().isoformat(),
        "source_file": str(input_file.name),
        "total_urls": len(urls),
        "category_counts": category_counts,
        "category_summary": {
            category: count 
            for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
        },
        "articles": labeled_articles
    }
    
    # Preserve original metadata if present
    if 'scraped_at' in data:
        output_data['original_scraped_at'] = data['scraped_at']
    if 'merged_at' in data:
        output_data['original_merged_at'] = data['merged_at']
    if 'category' in data:
        output_data['category'] = data['category']
    if 'category_url' in data:
        output_data['category_url'] = data['category_url']
    if 'source_files' in data:
        output_data['source_files'] = data['source_files']
    
    # Generate output filename if not provided
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        input_stem = Path(input_file).stem
        output_path = SCRIPT_DIR / f"{input_stem}_labeled_{timestamp}.json"
    
    # Save labeled data
    print(f"\nSaving labeled data to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"Successfully saved {len(urls)} labeled URLs to {output_path}")
    
    # Print category summary
    print("\n" + "="*80)
    print("CATEGORY SUMMARY")
    print("="*80)
    for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(urls)) * 100
        print(f"  {category:<20} {count:>10} ({percentage:>5.1f}%)")
    print("="*80)
    
    return output_data


def main():
    """Main function to merge and label JSON files or label an existing merged file"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Merge BBC News URL JSON files and label with categories, or label an existing merged file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Merge and label two files (default behavior)
  python merge_news_urls.py
  
  # Label an already merged file
  python merge_news_urls.py --label-only bbc_news_merged_25461_urls_20251109_221538.json
  
  # Merge two specific files with custom delay
  python merge_news_urls.py --file1 file1.json --file2 file2.json -d 0.3
  
  # Merge without labeling
  python merge_news_urls.py --no-label
        """
    )
    parser.add_argument('--file1', help='First JSON file to merge (default: checkpoint file)')
    parser.add_argument('--file2', help='Second JSON file to merge (default: 10000_urls file)')
    parser.add_argument('--label-only', help='Label an already merged file (provide path to merged file)')
    parser.add_argument('-o', '--output', help='Output JSON file (default: auto-generated)')
    parser.add_argument('-d', '--delay', type=float, default=0.2, help='Delay between requests when labeling (default: 0.2)')
    parser.add_argument('--no-label', action='store_true', help='Skip labeling URLs (only merge)')
    
    args = parser.parse_args()
    
    # If --label-only is specified, label the existing merged file
    if args.label_only:
        input_file = Path(args.label_only)
        if not input_file.exists():
            print(f"Error: File not found: {input_file}")
            return
        
        try:
            output_path = Path(args.output) if args.output else None
            label_existing_merged_file(input_file, output_path=output_path, delay=args.delay)
        except KeyboardInterrupt:
            print("\n[WARNING] Interrupted by user.")
            print("Partial results may have been saved.")
        except Exception as e:
            print(f"Error during labeling: {e}")
            raise
        return
    
    # Otherwise, merge two files
    # Define default file paths
    if args.file1:
        file1_path = Path(args.file1)
    else:
        file1_path = SCRIPT_DIR / "urls" / "bbc_news_checkpoint_144_20000_urls_20251109_025237.json"
    
    if args.file2:
        file2_path = Path(args.file2)
    else:
        file2_path = SCRIPT_DIR / "bbc_news_10000_urls_20251030_013236.json"
    
    # Check if files exist
    if not file1_path.exists():
        print(f"Error: File not found: {file1_path}")
        return
    
    if not file2_path.exists():
        print(f"Error: File not found: {file2_path}")
        return
    
    # Merge and label the files
    try:
        output_path = Path(args.output) if args.output else None
        merge_and_label_urls(
            file1_path, 
            file2_path, 
            output_path=output_path,
            delay=args.delay,
            label_urls=not args.no_label
        )
    except KeyboardInterrupt:
        print("\n[WARNING] Interrupted by user.")
        print("Partial results may have been saved.")
    except Exception as e:
        print(f"Error during merge: {e}")
        raise


if __name__ == "__main__":
    main()

