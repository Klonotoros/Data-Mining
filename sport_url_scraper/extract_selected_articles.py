#!/usr/bin/env python3
"""
Script to extract selected articles from sport_articles_labeled.json
and save them in a simplified format with only "text" and "label" fields.
"""

import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict


def extract_selected_articles(input_file: str, output_file: str):
    """
    Extract articles from specified labels with specific counts.
    
    Args:
        input_file: Path to sport_articles_labeled.json
        output_file: Path to output JSON file
    """
    # Define the labels and their target counts
    target_counts = {
        'football': 400,
        'rugby-union': 400,
        'cricket': 400,
        'formula1': 400,
        'boxing': 384,
        'rugby-league': 334,
        'tennis': 266,
        'american-football': 249,  
        'mixed-martial-arts': 209  
    }
    
    # Read the input JSON file
    print(f"Reading articles from: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    articles = data.get('articles', [])
    print(f"Total articles: {len(articles)}")
    print("="*80)
    
    # Group articles by label
    articles_by_label = defaultdict(list)
    for article in articles:
        label = article.get('label', '')
        if label in target_counts:
            articles_by_label[label].append(article)
    
    # Extract the specified number of articles from each label
    selected_articles = []
    extraction_summary = {}
    
    print("\nExtracting articles:")
    print("="*80)
    print(f"{'Label':<25} {'Available':>12} {'Requested':>12} {'Extracted':>12}")
    print("-"*80)
    
    for label, target_count in sorted(target_counts.items()):
        available = len(articles_by_label[label])
        count_to_extract = min(target_count, available)
        
        # Take the first N articles for this label
        selected = articles_by_label[label][:count_to_extract]
        
        # Convert to simplified format: only "text" and "label"
        for article in selected:
            simplified_article = {
                'text': article.get('content', ''),
                'label': label
            }
            selected_articles.append(simplified_article)
        
        extraction_summary[label] = {
            'available': available,
            'requested': target_count,
            'extracted': count_to_extract
        }
        
        print(f"{label:<25} {available:>12} {target_count:>12} {count_to_extract:>12}")
    
    print("-"*80)
    print(f"{'TOTAL':<25} {'':>12} {'':>12} {len(selected_articles):>12}")
    print("="*80)
    
    # Create output structure
    output_data = selected_articles
    
    # Write to output file
    print(f"\nWriting {len(selected_articles)} articles to: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print("="*80)
    print("EXTRACTION COMPLETE")
    print("="*80)
    print(f"Total articles extracted: {len(selected_articles)}")
    print(f"Saved to: {output_file}")
    print("="*80)
    
    # Show any warnings
    warnings = []
    for label, summary in extraction_summary.items():
        if summary['available'] < summary['requested']:
            warnings.append(f"  {label}: Only {summary['available']} available (requested {summary['requested']})")
    
    if warnings:
        print("\nWARNINGS:")
        for warning in warnings:
            print(warning)
    
    return output_data


if __name__ == "__main__":
    # Define file paths
    script_dir = Path(__file__).parent
    input_file = script_dir / "sport_articles_labeled.json"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = script_dir / f"selected_articles_{timestamp}.json"
    
    # Extract articles
    extract_selected_articles(str(input_file), str(output_file))

