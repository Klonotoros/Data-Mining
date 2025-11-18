import json
from collections import defaultdict

def extract_1000_per_category(input_file, output_file, categories, count_per_category=1000):
    """
    Extract specified number of articles from each category.
    
    Args:
        input_file: Path to input JSON file
        output_file: Path to output JSON file
        categories: List of category labels to extract
        count_per_category: Number of articles to extract per category
    """
    # Read the input JSON file
    print(f"Reading {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        articles = json.load(f)
    
    print(f"Total articles loaded: {len(articles)}")
    
    # Group articles by label (case-insensitive matching)
    articles_by_label = defaultdict(list)
    label_mapping = {}  # Store original label case
    
    for article in articles:
        label = article.get('label', '')
        if label:
            # Store original case
            label_lower = label.lower()
            if label_lower not in label_mapping:
                label_mapping[label_lower] = label
            articles_by_label[label_lower].append(article)
    
    # Extract articles for each requested category
    extracted_articles = []
    categories_lower = [cat.lower() for cat in categories]
    
    for category in categories:
        category_lower = category.lower()
        
        if category_lower not in articles_by_label:
            print(f"Warning: Category '{category}' not found in the data.")
            print(f"Available categories: {list(set(label_mapping.values()))}")
            continue
        
        available = len(articles_by_label[category_lower])
        to_extract = min(count_per_category, available)
        
        # Take first 'to_extract' articles from this category
        extracted = articles_by_label[category_lower][:to_extract]
        extracted_articles.extend(extracted)
        
        print(f"Category '{label_mapping[category_lower]}': Extracted {to_extract} articles (available: {available})")
    
    # Write to output file
    print(f"\nWriting {len(extracted_articles)} articles to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(extracted_articles, f, ensure_ascii=False, indent=2)
    
    print(f"Successfully created {output_file}")
    
    # Print summary
    print("\nSummary:")
    summary = defaultdict(int)
    for article in extracted_articles:
        summary[article.get('label', 'Unknown')] += 1
    
    for label, count in sorted(summary.items()):
        print(f"  {label}: {count} articles")

if __name__ == "__main__":
    input_file = "good_categories.json"
    output_file = "labeled_1000.json"
    
    # Categories to extract (case-insensitive)
    categories = ["business", "politics", "culture", "health", "technology", "World"]
    
    extract_1000_per_category(input_file, output_file, categories, count_per_category=1000)

