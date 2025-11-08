#!/usr/bin/env python3
"""
Script to extract all article URLs from total_sports.json
and combine them into a single JSON file with category: "sport"
"""

import json
from pathlib import Path
from datetime import datetime


def extract_all_sport_urls(input_file: str, output_file: str):
    """
    Extract all URLs from all sport categories and combine into one JSON.
    
    Args:
        input_file: Path to total_sports.json
        output_file: Path to output JSON file
    """
    # Read the input JSON file
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Collect all URLs from all categories
    all_urls = []
    
    if 'categories_by_name' in data:
        for category_name, category_data in data['categories_by_name'].items():
            if 'urls' in category_data:
                all_urls.extend(category_data['urls'])
    
    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in all_urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)
    
    # Create output structure
    output_data = {
        "category": "sport",
        "url_count": len(unique_urls),
        "extracted_at": datetime.now().isoformat(),
        "urls": unique_urls
    }
    
    # Write to output file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"Extracted {len(unique_urls)} unique URLs")
    print(f"Saved to: {output_file}")
    
    return output_data


if __name__ == "__main__":
    # Define file paths
    script_dir = Path(__file__).parent
    input_file = script_dir / "total_sports.json"
    output_file = script_dir / "all_sport_urls.json"
    
    # Extract URLs
    extract_all_sport_urls(str(input_file), str(output_file))

