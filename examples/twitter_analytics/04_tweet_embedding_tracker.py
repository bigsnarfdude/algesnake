"""
Example 04: Tweet Embedding Frequency Tracker

Real-world use case: Track which websites embed tweets and how often
Inspired by: Twitter's actual use of Algebird for reverse feed analytics

Twitter's Original Problem:
- Tweets can be embedded on millions of websites
- Need to track: "Which sites embed this popular tweet?"
- Need to know: "How many times has TechCrunch embedded our tweets?"
- Can't store exact counts for every (tweet_id, url) pair

Solution:
- CountMinSketch provides frequency estimation with bounded error
- MapMonoid allows composing tweet_id -> {url: count} mappings
- Never underestimates (conservative update)

Reference: Michael Noll's "Of Algebirds, Monoids, Monads" blog post
This was a real Twitter use case mentioned in Sam Ritchie's talk.
"""

from algesnake.approximate import CountMinSketch
from algesnake.approximate.countminsketch import heavy_hitters
from algesnake import MapMonoid, Add
import random
from collections import defaultdict


class TweetEmbeddingTracker:
    """
    Track which websites embed tweets (Twitter's actual use case).

    This was used by Twitter to analyze the reverse feed:
    - Find which news sites embed popular tweets
    - Identify influential domains
    - Measure reach beyond Twitter platform
    """

    def __init__(self, epsilon=0.001, delta=0.01):
        """
        Initialize embedding tracker.

        Args:
            epsilon: Accuracy parameter (1% error)
            delta: Confidence parameter (99% confidence)
        """
        # Global frequency counter for all embeddings
        self.global_frequencies = CountMinSketch.from_error_rate(
            epsilon=epsilon,
            delta=delta
        )

        # Per-tweet tracking
        self.tweet_embeddings = {}  # tweet_id -> CountMinSketch

        # Per-domain tracking
        self.domain_embeddings = CountMinSketch.from_error_rate(
            epsilon=epsilon,
            delta=delta
        )

        # Stats
        self.total_embeddings = Add(0)

        # For comparison: exact counts
        self.exact_embeddings = defaultdict(lambda: defaultdict(int))
        self.exact_domain_counts = defaultdict(int)

    def track_embedding(self, tweet_id, embedding_url):
        """
        Track when a tweet is embedded on a website.

        This would be called when Twitter detects an embed
        (via JavaScript callback or server logs).
        """
        self.total_embeddings += Add(1)

        # Create key for global tracking
        key = f"{tweet_id}:{embedding_url}"
        self.global_frequencies.add(key)

        # Track per-tweet
        if tweet_id not in self.tweet_embeddings:
            self.tweet_embeddings[tweet_id] = CountMinSketch.from_error_rate(
                epsilon=0.001, delta=0.01
            )
        self.tweet_embeddings[tweet_id].add(embedding_url)

        # Track per-domain
        domain = self._extract_domain(embedding_url)
        self.domain_embeddings.add(domain)

        # Exact tracking (for comparison)
        self.exact_embeddings[tweet_id][embedding_url] += 1
        self.exact_domain_counts[domain] += 1

    def get_embedding_count(self, tweet_id, url):
        """Get how many times a specific URL embedded this tweet."""
        key = f"{tweet_id}:{url}"
        approx = self.global_frequencies.estimate(key)
        exact = self.exact_embeddings[tweet_id][url]

        return {
            'tweet_id': tweet_id,
            'url': url,
            'approx_count': approx,
            'exact_count': exact,
            'error': abs(approx - exact),
            'error_pct': (abs(approx - exact) / exact * 100) if exact > 0 else 0
        }

    def get_top_embedding_sites(self, tweet_id, threshold=10):
        """
        Find which sites embed this tweet most frequently.

        Uses heavy_hitters algorithm from CountMinSketch.
        """
        if tweet_id not in self.tweet_embeddings:
            return []

        # Get all URLs that embedded this tweet
        all_urls = list(self.exact_embeddings[tweet_id].keys())

        # Find heavy hitters
        cms = self.tweet_embeddings[tweet_id]
        hitters = heavy_hitters(cms, all_urls, threshold)

        return sorted(hitters, key=lambda x: x[1], reverse=True)

    def get_domain_influence(self, domain):
        """Get how many total tweets this domain has embedded."""
        approx = self.domain_embeddings.estimate(domain)
        exact = self.exact_domain_counts[domain]

        return {
            'domain': domain,
            'approx_embeds': approx,
            'exact_embeds': exact,
            'error': abs(approx - exact)
        }

    def get_most_influential_domains(self, n=10):
        """Find the N most influential domains (most tweet embeds)."""
        domains = list(self.exact_domain_counts.keys())
        domain_counts = []

        for domain in domains:
            count = self.domain_embeddings.estimate(domain)
            domain_counts.append((domain, count))

        return sorted(domain_counts, key=lambda x: x[1], reverse=True)[:n]

    def _extract_domain(self, url):
        """Extract domain from URL."""
        # Simple domain extraction
        if '://' in url:
            domain = url.split('://')[1].split('/')[0]
        else:
            domain = url.split('/')[0]
        return domain


def simulate_tweet_embeddings():
    """
    Simulate Twitter's tweet embedding tracking.

    Scenario: Track how news sites and blogs embed popular tweets.
    """
    tracker = TweetEmbeddingTracker()

    # Popular domains that embed tweets
    news_sites = [
        'techcrunch.com',
        'theverge.com',
        'arstechnica.com',
        'wired.com',
        'mashable.com',
        'engadget.com',
        'cnet.com',
        'reuters.com',
        'bloomberg.com',
        'wsj.com'
    ]

    blog_sites = [
        'medium.com',
        'dev.to',
        'hackernoon.com',
        'substack.com',
    ]

    # Popular tweets (e.g., from Elon Musk, tech influencers)
    popular_tweets = [
        'tweet_elon_12345',
        'tweet_tech_67890',
        'tweet_news_11111',
        'tweet_viral_22222',
    ]

    # Less popular tweets
    normal_tweets = [f'tweet_{i}' for i in range(1000)]

    print("Simulating tweet embeddings...")

    # Simulate 100K embed events
    for _ in range(100000):
        # Popular tweets get embedded more
        if random.random() < 0.7:
            tweet_id = random.choice(popular_tweets)
        else:
            tweet_id = random.choice(normal_tweets)

        # News sites embed more than blogs
        if random.random() < 0.8:
            domain = random.choice(news_sites)
        else:
            domain = random.choice(blog_sites)

        url = f"https://{domain}/article/{random.randint(1000, 9999)}"
        tracker.track_embedding(tweet_id, url)

    return tracker, popular_tweets


def analyze_embeddings(tracker, popular_tweets):
    """Analyze embedding patterns."""
    print("\n" + "="*70)
    print("TWEET EMBEDDING ANALYSIS")
    print("="*70)

    # Analyze most popular tweet
    popular_tweet = popular_tweets[0]
    print(f"\nAnalyzing {popular_tweet}:")
    print("-" * 70)

    top_sites = tracker.get_top_embedding_sites(popular_tweet, threshold=50)
    print(f"\nTop 10 sites that embedded this tweet:")
    print(f"{'Rank':<6} {'URL':<40} {'Count':<10}")
    print("-" * 70)

    for i, (url, count) in enumerate(top_sites[:10], 1):
        domain = tracker._extract_domain(url)
        exact = tracker.exact_embeddings[popular_tweet][url]
        print(f"{i:<6} {domain:<40} {count:<10} (exact: {exact})")

    # Most influential domains
    print("\n" + "="*70)
    print("MOST INFLUENTIAL DOMAINS")
    print("="*70)

    influential = tracker.get_most_influential_domains(10)
    print(f"\n{'Rank':<6} {'Domain':<30} {'Embeds':<15}")
    print("-" * 70)

    for i, (domain, count) in enumerate(influential, 1):
        exact = tracker.exact_domain_counts[domain]
        print(f"{i:<6} {domain:<30} {int(count):<15,} (exact: {exact:,})")

    # Specific embedding stats
    print("\n" + "="*70)
    print("SPECIFIC EMBEDDING QUERIES")
    print("="*70)

    test_cases = [
        (popular_tweets[0], 'https://techcrunch.com/article/1234'),
        (popular_tweets[1], 'https://theverge.com/article/5678'),
    ]

    for tweet_id, url in test_cases:
        stats = tracker.get_embedding_count(tweet_id, url)
        print(f"\nTweet {tweet_id} on {tracker._extract_domain(url)}:")
        print(f"  Approx count: {stats['approx_count']:,}")
        print(f"  Exact count: {stats['exact_count']:,}")
        print(f"  Error: {stats['error']} ({stats['error_pct']:.2f}%)")


def distributed_embedding_tracking():
    """
    Demonstrate distributed embedding tracking across data centers.
    """
    print("\n" + "="*70)
    print("DISTRIBUTED EMBEDDING TRACKING")
    print("="*70)

    # Three data centers track embeddings
    dc1_tracker = TweetEmbeddingTracker()
    dc2_tracker = TweetEmbeddingTracker()
    dc3_tracker = TweetEmbeddingTracker()

    # Each DC sees different embedding events
    for _ in range(10000):
        tweet_id = f"tweet_{random.randint(1, 100)}"
        url = f"https://site{random.randint(1, 20)}.com/article/{random.randint(1, 100)}"
        dc1_tracker.track_embedding(tweet_id, url)

    for _ in range(8000):
        tweet_id = f"tweet_{random.randint(1, 100)}"
        url = f"https://site{random.randint(1, 20)}.com/article/{random.randint(1, 100)}"
        dc2_tracker.track_embedding(tweet_id, url)

    for _ in range(12000):
        tweet_id = f"tweet_{random.randint(1, 100)}"
        url = f"https://site{random.randint(1, 20)}.com/article/{random.randint(1, 100)}"
        dc3_tracker.track_embedding(tweet_id, url)

    print(f"\nDC1: {dc1_tracker.total_embeddings.value:,} embeddings")
    print(f"DC2: {dc2_tracker.total_embeddings.value:,} embeddings")
    print(f"DC3: {dc3_tracker.total_embeddings.value:,} embeddings")

    # MERGE CountMinSketches (monoid operation!)
    global_cms = (dc1_tracker.global_frequencies +
                  dc2_tracker.global_frequencies +
                  dc3_tracker.global_frequencies)

    total = dc1_tracker.total_embeddings + dc2_tracker.total_embeddings + dc3_tracker.total_embeddings

    print(f"\nGlobal total: {total.value:,} embeddings")
    print("✓ Merged CountMinSketch from 3 data centers using monoid + operation!")


if __name__ == "__main__":
    print("="*70)
    print("Twitter Tweet Embedding Frequency Tracker")
    print("Based on Twitter's Actual Algebird Use Case")
    print("="*70)
    print("\nUse case: Track which websites embed tweets (reverse feed analytics)")
    print("Reference: Sam Ritchie's talk on Algebird at Twitter")

    # Run simulation
    tracker, popular_tweets = simulate_tweet_embeddings()

    print(f"\nTotal embeddings tracked: {tracker.total_embeddings.value:,}")
    print(f"Unique tweets embedded: {len(tracker.tweet_embeddings):,}")

    # Analyze results
    analyze_embeddings(tracker, popular_tweets)

    # Distributed example
    distributed_embedding_tracking()

    print("\n" + "="*70)
    print("KEY TAKEAWAYS")
    print("="*70)
    print("✓ CountMinSketch tracks (tweet, URL) frequencies with bounded memory")
    print("✓ Heavy hitters algorithm finds most influential embedding sites")
    print("✓ Never underestimates counts (conservative update)")
    print("✓ Monoid operations enable merging across data centers")
    print("✓ This was Twitter's actual use case for analyzing tweet reach beyond platform")
    print("="*70)
