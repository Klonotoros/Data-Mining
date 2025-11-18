import json
from collections import Counter
from datetime import datetime
import shutil
import os

def merge_labeled_articles(good_categories_file, unknown_labeled_file, output_file, min_confidence=0):
    """
    Merge labeled unknown articles into good_categories.json
    
    Args:
        good_categories_file: Path to existing good_categories.json
        unknown_labeled_file: Path to unknown_labeled.json with predictions
        output_file: Path to save merged result
        min_confidence: Minimum confidence threshold (0-100) to include articles
    """
    print("="*80)
    print("MERGING LABELED ARTICLES")
    print("="*80)
    
    # Create backup of original file
    if os.path.exists(good_categories_file):
        backup_file = f"{good_categories_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"\nCreating backup: {backup_file}")
        shutil.copy2(good_categories_file, backup_file)
        print(f"[OK] Backup created")
    
    # Load existing good_categories.json
    print(f"\nLoading {good_categories_file}...")
    with open(good_categories_file, 'r', encoding='utf-8') as f:
        existing_articles = json.load(f)
    
    print(f"Loaded {len(existing_articles)} existing articles")
    
    # Count existing categories
    existing_categories = Counter([article['label'] for article in existing_articles])
    print(f"\nExisting categories:")
    for cat, count in existing_categories.most_common():
        print(f"  {cat}: {count} articles")
    
    # Load unknown_labeled.json
    print(f"\nLoading {unknown_labeled_file}...")
    with open(unknown_labeled_file, 'r', encoding='utf-8') as f:
        labeled_articles = json.load(f)
    
    print(f"Loaded {len(labeled_articles)} labeled articles")
    
    # Filter by confidence threshold and extract text/label
    print(f"\nFiltering articles with confidence >= {min_confidence}%...")
    new_articles = []
    filtered_out = 0
    
    for article in labeled_articles:
        confidence = article.get('confidence', 0)
        
        if confidence >= min_confidence:
            # Extract only text and label (same format as good_categories.json)
            new_articles.append({
                'text': article['text'],
                'label': article['label']
            })
        else:
            filtered_out += 1
    
    print(f"[OK] {len(new_articles)} articles passed confidence threshold")
    if filtered_out > 0:
        print(f"  Filtered out {filtered_out} articles below {min_confidence}% confidence")
    
    # Count new categories
    new_categories = Counter([article['label'] for article in new_articles])
    print(f"\nNew articles by category:")
    for cat, count in new_categories.most_common():
        print(f"  {cat}: {count} articles")
    
    # Merge articles
    print(f"\nMerging articles...")
    merged_articles = existing_articles + new_articles
    
    print(f"[OK] Total merged articles: {len(merged_articles)}")
    print(f"  Original: {len(existing_articles)}")
    print(f"  Added: {len(new_articles)}")
    
    # Count final categories
    final_categories = Counter([article['label'] for article in merged_articles])
    print(f"\nFinal category distribution:")
    for cat, count in final_categories.most_common():
        print(f"  {cat}: {count} articles")
    
    # Save merged result
    print(f"\nSaving merged articles to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(merged_articles, f, ensure_ascii=False, indent=2)
    
    print(f"[OK] Saved {len(merged_articles)} articles to {output_file}")
    
    # Create summary
    summary = {
        'merge_date': datetime.now().isoformat(),
        'original_count': len(existing_articles),
        'added_count': len(new_articles),
        'total_count': len(merged_articles),
        'min_confidence_threshold': min_confidence,
        'filtered_out_count': filtered_out,
        'category_distribution': dict(final_categories),
        'category_changes': {
            cat: {
                'before': existing_categories.get(cat, 0),
                'added': new_categories.get(cat, 0),
                'after': final_categories.get(cat, 0)
            }
            for cat in final_categories.keys()
        }
    }
    
    summary_file = output_file.replace('.json', '_merge_summary.json')
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    
    print(f"[OK] Saved merge summary to {summary_file}")
    
    return merged_articles, summary

if __name__ == "__main__":
    # File paths
    good_categories_file = "good_categories.json"
    unknown_labeled_file = "unknown_labeled.json"
    output_file = "good_categories.json"  # Overwrite original
    
    # Confidence threshold (0 = include all, 50 = only >= 50%, etc.)
    # You can adjust this to filter out low-confidence predictions
    min_confidence = 0  # Set to 0 to include all, or higher (e.g., 80) to filter
    
    print(f"Configuration:")
    print(f"  Input: {good_categories_file}")
    print(f"  Source: {unknown_labeled_file}")
    print(f"  Output: {output_file}")
    print(f"  Min confidence: {min_confidence}%")
    
    if min_confidence > 0:
        print(f"\nNote: Only articles with confidence >= {min_confidence}% will be included")
    else:
        print(f"\nNote: All articles will be included (no confidence filter)")
    
    response = input("\nProceed with merge? (yes/no): ").strip().lower()
    if response in ['yes', 'y']:
        merged_articles, summary = merge_labeled_articles(
            good_categories_file,
            unknown_labeled_file,
            output_file,
            min_confidence=min_confidence
        )
        
        print("\n" + "="*80)
        print("MERGE COMPLETE!")
        print("="*80)
        print(f"\nTotal articles in {output_file}: {len(merged_articles)}")
    else:
        print("\nMerge cancelled.")

