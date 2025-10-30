"""
Merge all BBC Sport category JSON files into one total_sports.json file
and generate a report about categories and URL counts.
"""

import json
import glob
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()


def load_all_sport_json_files(directory: Path):
    """Load all BBC Sport category JSON files"""
    # Pattern to match category files (exclude combined files)
    pattern = str(directory / 'bbc_sport_*_*_urls_*.json')
    json_files = glob.glob(pattern)
    
    # Exclude combined/all/interrupted files
    json_files = [f for f in json_files if 'all_urls' not in f and 'interrupted' not in f]
    
    categories_data = []
    errors = []
    
    for json_file in sorted(json_files):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                categories_data.append(data)
        except Exception as e:
            errors.append((json_file, str(e)))
    
    return categories_data, errors


def merge_categories(categories_data: list) -> dict:
    """Merge all category data into one structure"""
    merged_data = {
        'merged_at': datetime.now().isoformat(),
        'total_unique_urls': 0,
        'total_categories': len(categories_data),
        'categories_by_name': {}
    }
    
    all_urls = set()
    categories_by_name = {}
    
    for category_data in categories_data:
        category_name = category_data.get('category', 'unknown')
        category_url = category_data.get('category_url', '')
        url_count = category_data.get('url_count', 0)
        urls = category_data.get('urls', [])
        
        # Store category info
        categories_by_name[category_name] = {
            'category': category_name,
            'category_url': category_url,
            'url_count': url_count,
            'scraped_at': category_data.get('scraped_at', ''),
            'urls': urls
        }
        
        # Collect all unique URLs
        for url in urls:
            all_urls.add(url)
    
    merged_data['total_unique_urls'] = len(all_urls)
    merged_data['categories_by_name'] = categories_by_name
    merged_data['all_urls'] = sorted(list(all_urls))
    
    return merged_data


def generate_report(categories_data: list, output_file: Path):
    """Generate a text report about categories and URL counts"""
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("BBC SPORT URL COLLECTION REPORT")
    report_lines.append("=" * 80)
    report_lines.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")
    
    # Calculate totals
    total_categories = len(categories_data)
    total_urls = sum(cat.get('url_count', 0) for cat in categories_data)
    unique_urls = set()
    for cat in categories_data:
        for url in cat.get('urls', []):
            unique_urls.add(url)
    
    report_lines.append(f"SUMMARY:")
    report_lines.append(f"  Total categories processed: {total_categories}")
    report_lines.append(f"  Total URLs collected: {total_urls}")
    report_lines.append(f"  Unique URLs (deduplicated): {len(unique_urls)}")
    report_lines.append("")
    report_lines.append("=" * 80)
    report_lines.append("CATEGORY BREAKDOWN:")
    report_lines.append("=" * 80)
    report_lines.append(f"{'Category':<35} {'URLs':>10} {'Status':<15}")
    report_lines.append("-" * 80)
    
    # Sort by URL count (descending)
    sorted_categories = sorted(categories_data, key=lambda x: x.get('url_count', 0), reverse=True)
    
    for cat in sorted_categories:
        category_name = cat.get('category', 'unknown')
        url_count = cat.get('url_count', 0)
        status = "[OK] Active" if url_count > 0 else "[EMPTY]"
        report_lines.append(f"{category_name:<35} {url_count:>10} {status:<15}")
    
    report_lines.append("")
    report_lines.append("=" * 80)
    report_lines.append("TOP 10 CATEGORIES BY URL COUNT:")
    report_lines.append("=" * 80)
    
    for i, cat in enumerate(sorted_categories[:10], 1):
        category_name = cat.get('category', 'unknown')
        url_count = cat.get('url_count', 0)
        category_url = cat.get('category_url', '')
        report_lines.append(f"{i:2}. {category_name:<30} {url_count:>6} URLs")
        report_lines.append(f"    {category_url}")
    
    report_lines.append("")
    report_lines.append("=" * 80)
    report_lines.append("CATEGORIES WITH FEW OR NO URLs (< 20 URLs):")
    report_lines.append("=" * 80)
    
    small_categories = [cat for cat in sorted_categories if cat.get('url_count', 0) < 20]
    for cat in small_categories:
        category_name = cat.get('category', 'unknown')
        url_count = cat.get('url_count', 0)
        report_lines.append(f"  {category_name:<35} {url_count:>3} URLs")
    
    report_lines.append("")
    report_lines.append("=" * 80)
    
    report_text = "\n".join(report_lines)
    
    # Save report to file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report_text)
    
    # Also print to console
    print(report_text)
    
    return report_text


def main():
    """Main function to merge JSON files and generate report"""
    print("=" * 80)
    print("BBC SPORT JSON MERGER")
    print("=" * 80)
    print(f"Searching for JSON files in: {SCRIPT_DIR}")
    print()
    
    # Load all category JSON files
    categories_data, errors = load_all_sport_json_files(SCRIPT_DIR)
    
    if errors:
        print(f"Warning: {len(errors)} file(s) had errors:")
        for filename, error in errors:
            print(f"  - {filename}: {error}")
        print()
    
    if not categories_data:
        print("No category JSON files found!")
        return
    
    print(f"Found {len(categories_data)} category files")
    print()
    
    # Merge all categories
    print("Merging categories...")
    merged_data = merge_categories(categories_data)
    
    # Save merged JSON
    output_json = SCRIPT_DIR / 'total_sports.json'
    print(f"Saving merged data to: {output_json}")
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    print(f"[OK] Saved {merged_data['total_unique_urls']} unique URLs from {merged_data['total_categories']} categories")
    print()
    
    # Generate report
    print("Generating report...")
    report_file = SCRIPT_DIR / 'sports_collection_report.txt'
    generate_report(categories_data, report_file)
    
    print()
    print("=" * 80)
    print("COMPLETE!")
    print("=" * 80)
    print(f"[OK] Merged JSON saved to: {output_json}")
    print(f"[OK] Report saved to: {report_file}")
    print("=" * 80)


if __name__ == "__main__":
    main()

