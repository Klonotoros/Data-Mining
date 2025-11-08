#!/usr/bin/env python3
"""
Script to count the number of articles for each label (sport category)
in sport_articles_labeled.json
"""

import json
from pathlib import Path
from collections import Counter
from datetime import datetime


def count_articles_by_label(input_file: str):
    """
    Count articles by label and display statistics.
    
    Args:
        input_file: Path to sport_articles_labeled.json
    """
    # Read the input JSON file
    print(f"Reading articles from: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    articles = data.get('articles', [])
    total_articles = len(articles)
    
    print(f"Total articles found: {total_articles}")
    print("="*80)
    
    # Count articles by label
    label_counts = Counter()
    for article in articles:
        label = article.get('label', 'unknown')
        label_counts[label] += 1
    
    # Sort by count (descending)
    sorted_counts = sorted(label_counts.items(), key=lambda x: x[1], reverse=True)
    
    # Display results
    print("\nArticles by Label:")
    print("="*80)
    print(f"{'Label':<30} {'Count':>10}")
    print("-"*80)
    
    for label, count in sorted_counts:
        print(f"{label:<30} {count:>10}")
    
    print("-"*80)
    print(f"{'TOTAL':<30} {total_articles:>10}")
    print("="*80)
    
    # Create output dictionary
    output_data = {
        'generated_at': datetime.now().isoformat(),
        'total_articles': total_articles,
        'total_labels': len(label_counts),
        'articles_by_label': dict(sorted_counts)
    }
    
    # Save to JSON file
    script_dir = Path(__file__).parent
    output_file = script_dir / "articles_by_label_count.json"
    
    print(f"\nSaving counts to: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print("="*80)
    print("COMPLETE")
    print("="*80)
    
    return output_data


if __name__ == "__main__":
    # Define file path
    script_dir = Path(__file__).parent
    input_file = script_dir / "sport_articles_labeled.json"
    
    # Count articles by label
    count_articles_by_label(str(input_file))

