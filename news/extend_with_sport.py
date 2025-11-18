import json
import os

def extend_labeled_with_sport():
    """
    Extend labeled_1000.json with 1000 sport articles from sport_articles_labeled.json.
    Extracts articles with label "football" and converts them to format with "text" and "sport" label.
    """
    # Paths
    sport_file = "../sport_url_scraper/sport_articles_labeled.json"
    labeled_file = "labeled_1000.json"
    output_file = "labeled_1000_with_sport.json"
    
    # Read sport articles
    print(f"Reading {sport_file}...")
    with open(sport_file, 'r', encoding='utf-8') as f:
        sport_data = json.load(f)
    
    # Extract articles with label "football"
    articles = sport_data.get('articles', [])
    football_articles = [article for article in articles if article.get('label') == 'football']
    
    print(f"Found {len(football_articles)} articles with label 'football'")
    
    # Take first 1000 football articles
    selected_football = football_articles[:1000]
    print(f"Selected {len(selected_football)} football articles")
    
    # Convert to the format used in labeled_1000.json
    # Use "content" field as "text" and change label to "sport"
    sport_articles_formatted = []
    for article in selected_football:
        content = article.get('content', '')
        if content:  # Only add if content exists
            sport_articles_formatted.append({
                "text": content,
                "label": "sport"
            })
    
    print(f"Formatted {len(sport_articles_formatted)} sport articles")
    
    # Read existing labeled_1000.json
    print(f"\nReading {labeled_file}...")
    with open(labeled_file, 'r', encoding='utf-8') as f:
        existing_articles = json.load(f)
    
    print(f"Found {len(existing_articles)} existing articles")
    
    # Combine the articles
    combined_articles = existing_articles + sport_articles_formatted
    
    print(f"\nTotal articles: {len(combined_articles)}")
    
    # Write to output file
    print(f"Writing to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(combined_articles, f, ensure_ascii=False, indent=2)
    
    print(f"Successfully created {output_file}")
    
    # Print summary
    print("\nSummary:")
    summary = {}
    for article in combined_articles:
        label = article.get('label', 'Unknown')
        summary[label] = summary.get(label, 0) + 1
    
    for label, count in sorted(summary.items()):
        print(f"  {label}: {count} articles")

if __name__ == "__main__":
    extend_labeled_with_sport()

