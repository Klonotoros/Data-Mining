import json
from datetime import datetime

def extract_unknown_articles(input_file, output_file):
    """
    Extract articles with label 'unknown' from newsLabeled.json
    and save only their 'text' field to unknown.json
    """
    print(f"Reading {input_file}...")
    
    # Read the input JSON file
    with open(input_file, 'r', encoding='utf-8') as f:
        articles = json.load(f)
    
    print(f"Total articles: {len(articles)}")
    
    # Filter articles with label 'unknown' and extract only 'text' field
    unknown_articles = []
    for article in articles:
        if article.get('label') == 'unknown':
            unknown_articles.append({
                'text': article.get('text', '')
            })
    
    print(f"Found {len(unknown_articles)} articles with label 'unknown'")
    
    # Write to output file
    print(f"Writing to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(unknown_articles, f, ensure_ascii=False, indent=2)
    
    print(f"Successfully saved {len(unknown_articles)} unknown articles to {output_file}")

if __name__ == "__main__":
    input_file = "newsLabeled.json"
    output_file = "unknown.json"
    
    extract_unknown_articles(input_file, output_file)

