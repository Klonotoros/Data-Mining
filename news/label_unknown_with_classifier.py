import json
from collections import Counter
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import Pipeline
import numpy as np

def train_classifier(training_file):
    """
    Train the Naive Bayes classifier using the same setup as news_classifier.ipynb
    """
    print("="*80)
    print("TRAINING CLASSIFIER")
    print("="*80)
    
    # Load training data
    print(f"\nLoading training data from {training_file}...")
    with open(training_file, 'r', encoding='utf-8') as f:
        training_data = json.load(f)
    
    print(f"Loaded {len(training_data)} training articles")
    
    # Prepare texts and labels
    texts = []
    labels = []
    
    for article in training_data:
        texts.append(article['text'])
        labels.append(article['label'])
    
    # Show category distribution
    category_counts = Counter(labels)
    print(f"\nCategories in training data: {len(category_counts)}")
    print("Top 10 categories:")
    for cat, count in category_counts.most_common(10):
        print(f"  {cat}: {count} articles")
    
    # Split data (same as notebook)
    X_train, X_test, y_train, y_test = train_test_split(
        texts, labels, test_size=0.2, random_state=42, stratify=labels
    )
    
    print(f"\nTraining set: {len(X_train)} articles")
    print(f"Test set: {len(X_test)} articles")
    
    # Create classifier pipeline (same as notebook)
    classifier = Pipeline([
        ('vectorizer', CountVectorizer(
            lowercase=True,
            token_pattern=r'\b[a-z]+\b',
            min_df=2,
            max_features=10000
        )),
        ('classifier', MultinomialNB(alpha=1.0))
    ])
    
    # Train classifier
    print("\nTraining classifier...")
    classifier.fit(X_train, y_train)
    print("[OK] Training complete!")
    print(f"Vocabulary size: {len(classifier.named_steps['vectorizer'].vocabulary_)}")
    
    return classifier

def label_unknown_articles(classifier, unknown_file, output_file):
    """
    Label unknown articles and save with confidence scores
    """
    print("\n" + "="*80)
    print("LABELING UNKNOWN ARTICLES")
    print("="*80)
    
    # Load unknown articles
    print(f"\nLoading unknown articles from {unknown_file}...")
    with open(unknown_file, 'r', encoding='utf-8') as f:
        unknown_articles = json.load(f)
    
    print(f"Loaded {len(unknown_articles)} unknown articles")
    
    # Extract texts
    texts = [article['text'] for article in unknown_articles]
    
    # Predict labels and get probabilities
    print("\nPredicting labels with confidence scores...")
    predictions = classifier.predict(texts)
    probabilities = classifier.predict_proba(texts)
    
    # Get class names (categories)
    class_names = classifier.classes_
    
    # Create labeled articles with confidence scores
    labeled_articles = []
    confidence_stats = []
    
    for i, (text, pred_label, probs) in enumerate(zip(texts, predictions, probabilities)):
        # Find the index of predicted label
        pred_idx = np.where(class_names == pred_label)[0][0]
        confidence = probs[pred_idx] * 100  # Convert to percentage
        
        # Get top 3 predictions with their probabilities
        top_indices = np.argsort(probs)[-3:][::-1]  # Top 3, descending
        top_predictions = [
            {
                'label': class_names[idx],
                'confidence': float(probs[idx] * 100)
            }
            for idx in top_indices
        ]
        
        labeled_articles.append({
            'text': text,
            'label': pred_label,
            'confidence': round(confidence, 2),  # Confidence as percentage
            'top_predictions': top_predictions  # Top 3 predictions
        })
        
        confidence_stats.append(confidence)
    
    # Print statistics
    print(f"\n[OK] Labeled {len(labeled_articles)} articles")
    print(f"\nConfidence Statistics:")
    print(f"  Mean confidence: {np.mean(confidence_stats):.2f}%")
    print(f"  Median confidence: {np.median(confidence_stats):.2f}%")
    print(f"  Min confidence: {np.min(confidence_stats):.2f}%")
    print(f"  Max confidence: {np.max(confidence_stats):.2f}%")
    
    # Count predictions by label
    pred_counts = Counter([article['label'] for article in labeled_articles])
    print(f"\nPredicted label distribution:")
    for label, count in pred_counts.most_common(10):
        print(f"  {label}: {count} articles")
    
    # Save results
    print(f"\nSaving labeled articles to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(labeled_articles, f, ensure_ascii=False, indent=2)
    
    print(f"[OK] Saved {len(labeled_articles)} labeled articles to {output_file}")
    
    # Save summary statistics
    summary = {
        'total_articles': len(labeled_articles),
        'confidence_stats': {
            'mean': float(np.mean(confidence_stats)),
            'median': float(np.median(confidence_stats)),
            'min': float(np.min(confidence_stats)),
            'max': float(np.max(confidence_stats)),
            'std': float(np.std(confidence_stats))
        },
        'label_distribution': dict(pred_counts)
    }
    
    summary_file = output_file.replace('.json', '_summary.json')
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    
    print(f"[OK] Saved summary statistics to {summary_file}")
    
    return labeled_articles

if __name__ == "__main__":
    # File paths
    training_file = "good_categories.json"
    unknown_file = "unknown.json"
    output_file = "unknown_labeled.json"
    
    # Train classifier
    classifier = train_classifier(training_file)
    
    # Label unknown articles
    labeled_articles = label_unknown_articles(classifier, unknown_file, output_file)
    
    print("\n" + "="*80)
    print("COMPLETE!")
    print("="*80)
    print(f"\nResults saved to: {output_file}")
    print(f"Summary saved to: {output_file.replace('.json', '_summary.json')}")

