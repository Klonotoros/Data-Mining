import json
import os
from collections import defaultdict

# Hardcoded file path - relative to script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(script_dir, "good_categories.json")

# Categories to keep (excluding Health)
target_categories = ["Politics", "Business", "World", "Culture", "Technology"]
articles_per_category = 200

print(f"Reading from {input_file}...")
try:
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"Total articles before filtering: {len(data)}")
    
    # Group articles by category
    articles_by_category = defaultdict(list)
    for article in data:
        label = article.get("label", "Unknown")
        articles_by_category[label].append(article)
    
    # Display current distribution
    print("\n" + "="*50)
    print("Current Category Distribution:")
    print("="*50)
    for category in sorted(articles_by_category.keys()):
        count = len(articles_by_category[category])
        print(f"  {category:15s}: {count:5d} articles")
    print("="*50)
    
    # Filter and limit articles
    filtered_data = []
    removed_categories = []
    
    for category in target_categories:
        if category in articles_by_category:
            articles = articles_by_category[category][:articles_per_category]
            filtered_data.extend(articles)
            original_count = len(articles_by_category[category])
            print(f"\n{category}: Keeping {len(articles)} out of {original_count} articles")
        else:
            print(f"\n{category}: WARNING - No articles found!")
    
    # Check for categories that will be removed
    for category in articles_by_category.keys():
        if category not in target_categories:
            removed_categories.append(category)
            print(f"\n{category}: REMOVING {len(articles_by_category[category])} articles")
    
    # Display final distribution
    print("\n" + "="*50)
    print("Final Category Distribution:")
    print("="*50)
    final_counts = {}
    for article in filtered_data:
        label = article.get("label", "Unknown")
        final_counts[label] = final_counts.get(label, 0) + 1
    
    for category in sorted(final_counts.keys()):
        count = final_counts[category]
        print(f"  {category:15s}: {count:5d} articles")
    print("="*50)
    print(f"Total articles: {len(filtered_data)}")
    
    # Create backup before overwriting
    backup_file = input_file + ".backup"
    print(f"\nCreating backup: {backup_file}")
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    # Write filtered data
    print(f"Writing filtered data to {input_file}...")
    with open(input_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_data, f, ensure_ascii=False, indent=2)
    
    print(f"\nDone! Filtered {len(data)} articles down to {len(filtered_data)} articles")
    print(f"Backup saved to: {backup_file}")
    
except FileNotFoundError:
    print(f"Error: File '{input_file}' not found!")
except json.JSONDecodeError as e:
    print(f"Error: Invalid JSON in '{input_file}': {e}")
except Exception as e:
    print(f"Error: {e}")

