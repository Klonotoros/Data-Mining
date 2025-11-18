import json
import os

# Define the good categories
good_categories = ["Business", "Politics", "Culture", "Health", "Technology", "World"]

# Hardcoded file paths - relative to script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(script_dir, "newsLabeled.json")
output_file = os.path.join(script_dir, "good_categories.json")

print(f"Reading from {input_file}...")
with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f"Total articles: {len(data)}")

# Filter articles with good categories
filtered_data = [article for article in data if article.get("label") in good_categories]

print(f"Filtered articles: {len(filtered_data)}")

# Count articles per category
category_counts = {}
for article in filtered_data:
    label = article.get("label")
    category_counts[label] = category_counts.get(label, 0) + 1

print("\nCategory distribution:")
for category, count in sorted(category_counts.items()):
    print(f"  {category}: {count}")

# Write to output file
print(f"\nWriting to {output_file}...")
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(filtered_data, f, ensure_ascii=False, indent=2)

print(f"Done! Extracted {len(filtered_data)} articles to {output_file}")

