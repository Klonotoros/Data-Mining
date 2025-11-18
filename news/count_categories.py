import json
import os
from collections import Counter

# Hardcoded file path - relative to script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(script_dir, "good_categories.json")

print(f"Reading from {input_file}...")
try:
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"Total articles: {len(data)}")
    
    # Count articles per category
    category_counts = Counter()
    for article in data:
        label = article.get("label", "Unknown")
        category_counts[label] += 1
    
    # Display results
    print("\n" + "="*50)
    print("Category Distribution:")
    print("="*50)
    
    # Sort by count (descending) then by category name
    sorted_categories = sorted(category_counts.items(), key=lambda x: (-x[1], x[0]))
    
    for category, count in sorted_categories:
        percentage = (count / len(data)) * 100
        print(f"  {category:15s}: {count:5d} articles ({percentage:5.2f}%)")
    
    print("="*50)
    print(f"Total categories: {len(category_counts)}")
    print(f"Total articles: {len(data)}")
    
except FileNotFoundError:
    print(f"Error: File '{input_file}' not found!")
except json.JSONDecodeError as e:
    print(f"Error: Invalid JSON in '{input_file}': {e}")
except Exception as e:
    print(f"Error: {e}")

