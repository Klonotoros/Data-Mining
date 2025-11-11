"""
Recount Categories in url_labeled.json
Recalculates category_counts and total_urls based on the articles in the file.

Usage:
    python recount_categories.py [url_labeled.json]
"""

import json
from pathlib import Path
from datetime import datetime

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()


def recount_categories(input_file: str = "url_labeled.json"):
    """Recount categories in url_labeled.json
    
    Args:
        input_file: Path to url_labeled.json file
    """
    input_path = Path(input_file)
    if not input_path.is_absolute():
        input_path = SCRIPT_DIR / input_path
    
    print("="*80)
    print("RECOUNT CATEGORIES")
    print("="*80)
    print(f"Input file: {input_path}")
    print("="*80)
    print()
    
    # Load the file
    print("Loading url_labeled.json...")
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"[OK] File loaded successfully")
    except FileNotFoundError:
        print(f"[ERROR] File not found: {input_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON: {e}")
        return None
    except Exception as e:
        print(f"[ERROR] Failed to load file: {e}")
        return None
    
    # Get articles
    articles = data.get('articles', [])
    if not articles:
        print("[ERROR] No articles found in file")
        return None
    
    print(f"[INFO] Found {len(articles)} articles")
    print()
    
    # Save original data for backup before modifying
    original_data = json.loads(json.dumps(data))  # Deep copy
    
    # Count categories
    print("Recounting categories...")
    category_counts = {}
    for article in articles:
        category = article.get('category', 'unknown')
        category_counts[category] = category_counts.get(category, 0) + 1
    
    # Update the data
    data['category_counts'] = category_counts
    data['total_urls'] = len(articles)
    data['recounted_at'] = datetime.now().isoformat()
    
    print(f"[OK] Counted {len(category_counts)} unique categories")
    print()
    
    # Show category breakdown
    print("Category breakdown:")
    print("-" * 80)
    for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {category:<30} {count:>10}")
    print("-" * 80)
    print(f"  {'TOTAL':<30} {len(articles):>10}")
    print()
    
    # Save the file
    print("Saving updated file...")
    try:
        # Create backup
        backup_path = input_path.with_suffix('.json.backup')
        with open(backup_path, 'w', encoding='utf-8') as f:
            json.dump(original_data, f, indent=2, ensure_ascii=False)
        print(f"[OK] Backup created: {backup_path}")
        
        # Save updated file
        with open(input_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"[OK] File updated: {input_path}")
    except Exception as e:
        print(f"[ERROR] Failed to save file: {e}")
        return None
    
    print()
    print("="*80)
    print("RECOUNT COMPLETE")
    print("="*80)
    print(f"Total articles: {len(articles)}")
    print(f"Unique categories: {len(category_counts)}")
    print("="*80)
    
    return data


if __name__ == "__main__":
    import sys
    
    input_file = sys.argv[1] if len(sys.argv) > 1 else "url_labeled.json"
    
    try:
        recount_categories(input_file)
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        sys.exit(1)

