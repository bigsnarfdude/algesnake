"""
Example 02: Trending Hashtags (TopK + CountMinSketch)

Real-world use case: Track trending hashtags on Twitter
Inspired by: Twitter's Algebird usage for heavy hitters detection

Twitter's Original Problem:
- Millions of hashtags used every minute
- Need to find top trending hashtags in real-time
- Can't store exact counts for every hashtag (too much memory)

Solution:
- CountMinSketch for frequency estimation (conservative, never underestimates)
- TopK (SpaceSaver) for tracking top trending hashtags
- Memory: O(k) instead of O(n) where n = total unique hashtags

Reference: Twitter Algebird CountMinSketch + TopK implementation
"""

from algesnake.approximate import CountMinSketch, TopK
from algesnake import Add
import random
import re


class TrendingHashtagTracker:
    """
    Track trending hashtags like Twitter's "Trending Topics" feature.

    Uses CountMinSketch for frequency estimation and TopK for
    maintaining the top trending hashtags with bounded memory.
    """

    def __init__(self, top_k=10, epsilon=0.001, delta=0.01):
        """
        Initialize trending tracker.

        Args:
            top_k: Number of top hashtags to track (e.g., 10 for top 10)
            epsilon: CMS accuracy parameter (smaller = more accurate)
            delta: CMS confidence parameter (smaller = higher confidence)
        """
        # CountMinSketch for frequency estimation
        # Conservative update: never underestimates
        self.cms = CountMinSketch.from_error_rate(epsilon=epsilon, delta=delta)

        # TopK for tracking heavy hitters
        self.topk = TopK(k=top_k)

        # Stats
        self.total_tweets = Add(0)
        self.total_hashtags = Add(0)

        # For comparison: exact counting (in production, this wouldn't exist)
        self.exact_counts = {}

    def process_tweet(self, tweet_text):
        """
        Process a single tweet and extract hashtags.

        Args:
            tweet_text: The tweet content
        """
        self.total_tweets += Add(1)

        # Extract hashtags (anything starting with #)
        hashtags = re.findall(r'#(\w+)', tweet_text.lower())

        for hashtag in hashtags:
            self.total_hashtags += Add(1)

            # Add to approximate structures
            self.cms.add(hashtag)
            self.topk.add(hashtag)

            # Add to exact (for comparison)
            self.exact_counts[hashtag] = self.exact_counts.get(hashtag, 0) + 1

    def process_batch(self, tweets):
        """Process a batch of tweets."""
        for tweet in tweets:
            self.process_tweet(tweet['text'])

    def get_trending(self, n=10):
        """
        Get top N trending hashtags.

        Returns:
            List of (hashtag, count) tuples
        """
        return self.topk.top(n)

    def get_frequency(self, hashtag):
        """
        Get estimated frequency of a hashtag.

        Returns conservative estimate (never underestimates).
        """
        approx = self.cms.estimate(hashtag.lower())
        exact = self.exact_counts.get(hashtag.lower(), 0)
        return {
            'hashtag': hashtag,
            'approx_count': approx,
            'exact_count': exact,
            'overestimate': approx - exact
        }

    def print_trending_report(self, n=10):
        """Print a trending hashtags report."""
        print(f"\n{'='*70}")
        print(f"TOP {n} TRENDING HASHTAGS")
        print(f"{'='*70}")

        trending = self.get_trending(n)

        print(f"{'Rank':<6} {'Hashtag':<20} {'Count':<15} {'Exact':<15}")
        print("-" * 70)

        for i, (hashtag, count) in enumerate(trending, 1):
            exact = self.exact_counts.get(hashtag, 0)
            print(f"{i:<6} #{hashtag:<19} {count:<15,} {exact:<15,}")

        print("-" * 70)
        print(f"Total tweets processed: {self.total_tweets.value:,}")
        print(f"Total hashtags: {self.total_hashtags.value:,}")
        print(f"Unique hashtags: {len(self.exact_counts):,}")
        print(f"Memory: TopK stores only {n} items, CMS uses fixed memory")
        print(f"vs Exact: Would need to store all {len(self.exact_counts):,} hashtags")


def simulate_twitter_trending():
    """
    Simulate Twitter's trending hashtag detection.

    We'll simulate various trending topics with different popularity levels.
    """
    # Popular hashtags with their base probability
    trending_topics = [
        ('#python', 0.15),           # Very popular
        ('#machinelearning', 0.12),  # Very popular
        ('#ai', 0.10),               # Popular
        ('#javascript', 0.08),       # Popular
        ('#coding', 0.07),           # Moderate
        ('#datascience', 0.06),      # Moderate
        ('#webdev', 0.05),           # Moderate
        ('#programming', 0.04),      # Less common
        ('#scala', 0.03),            # Less common
        ('#algebird', 0.02),         # Niche
    ]

    # Long tail: Many hashtags used rarely
    long_tail = [f'#topic{i}' for i in range(100)]

    tweets = []

    # Generate 100K tweets
    for _ in range(100000):
        # Pick hashtags based on probability
        hashtags = []

        # Add trending hashtags
        for hashtag, prob in trending_topics:
            if random.random() < prob:
                hashtags.append(hashtag)

        # Add some long-tail hashtags
        if random.random() < 0.3:
            hashtags.append(random.choice(long_tail))

        # Create tweet text
        if hashtags:
            tweet_text = f"Check out {' '.join(hashtags)} - this is amazing!"
        else:
            tweet_text = "Just a regular tweet without hashtags"

        tweets.append({'text': tweet_text})

    return tweets


def distributed_trending_example():
    """
    Demonstrate distributed trending hashtag detection.

    Simulates multiple servers tracking different regions,
    then merging to get global trending topics.
    """
    print("\n" + "="*70)
    print("DISTRIBUTED TRENDING DETECTION (Multi-Region)")
    print("="*70)

    # Simulate 3 regions with different trending topics
    regions = {
        'US': TrendingHashtagTracker(top_k=10),
        'EU': TrendingHashtagTracker(top_k=10),
        'ASIA': TrendingHashtagTracker(top_k=10),
    }

    # US tweets: #python and #javascript trending
    us_tweets = [
        {'text': f'Learning #python today! {random.choice(["#coding", "#ai", ""])}'}
        for _ in range(5000)
    ]
    us_tweets += [
        {'text': f'#javascript is awesome! {random.choice(["#webdev", "#coding", ""])}'}
        for _ in range(3000)
    ]

    # EU tweets: #scala and #algebird trending
    eu_tweets = [
        {'text': f'#scala is great for big data! {random.choice(["#programming", "#ai", ""])}'}
        for _ in range(4000)
    ]
    eu_tweets += [
        {'text': f'Using #algebird for analytics! {random.choice(["#scala", "#bigdata", ""])}'}
        for _ in range(2000)
    ]

    # Asia tweets: #ai and #machinelearning trending
    asia_tweets = [
        {'text': f'#ai will change the world! {random.choice(["#machinelearning", "#python", ""])}'}
        for _ in range(6000)
    ]
    asia_tweets += [
        {'text': f'#machinelearning course! {random.choice(["#datascience", "#ai", ""])}'}
        for _ in range(4000)
    ]

    # Process in each region
    regions['US'].process_batch(us_tweets)
    regions['EU'].process_batch(eu_tweets)
    regions['ASIA'].process_batch(asia_tweets)

    # Print regional trends
    for region_name, tracker in regions.items():
        print(f"\n{region_name} Regional Trends:")
        trending = tracker.get_trending(5)
        for i, (hashtag, count) in enumerate(trending, 1):
            print(f"  {i}. #{hashtag}: {count:,} mentions")

    # MERGE across regions (monoid operation!)
    print("\n" + "="*70)
    print("GLOBAL TRENDING (Merged from all regions)")
    print("="*70)

    global_cms = regions['US'].cms + regions['EU'].cms + regions['ASIA'].cms
    global_topk = regions['US'].topk + regions['EU'].topk + regions['ASIA'].topk

    print("\nTop 10 Global Trending Hashtags:")
    print(f"{'Rank':<6} {'Hashtag':<20} {'Count':<15}")
    print("-" * 50)

    for i, (hashtag, count) in enumerate(global_topk.top(10), 1):
        print(f"{i:<6} #{hashtag:<19} {count:<15,}")

    print("\n✓ Merged CountMinSketch + TopK from 3 regions using monoid operations!")


if __name__ == "__main__":
    print("="*70)
    print("Twitter Trending Hashtags Tracker")
    print("Based on Twitter's Algebird CountMinSketch + TopK Implementation")
    print("="*70)

    # Create tracker
    tracker = TrendingHashtagTracker(top_k=10, epsilon=0.001, delta=0.01)

    # Simulate Twitter stream
    print("\nProcessing 100,000 tweets...")
    tweets = simulate_twitter_trending()
    tracker.process_batch(tweets)

    # Show trending report
    tracker.print_trending_report(10)

    # Check specific hashtag frequency
    print("\n" + "="*70)
    print("FREQUENCY ESTIMATION (CountMinSketch)")
    print("="*70)

    for hashtag in ['python', 'machinelearning', 'algebird', 'scala']:
        freq = tracker.get_frequency(hashtag)
        print(f"\n#{freq['hashtag']}:")
        print(f"  Approximate count: {freq['approx_count']:,}")
        print(f"  Exact count: {freq['exact_count']:,}")
        print(f"  Overestimate: {freq['overestimate']} (CMS never underestimates)")

    # Distributed example
    distributed_trending_example()

    print("\n" + "="*70)
    print("KEY TAKEAWAYS")
    print("="*70)
    print("✓ TopK tracks top trending items with O(k) memory")
    print("✓ CountMinSketch provides frequency estimates with bounded error")
    print("✓ CMS never underestimates (conservative update)")
    print("✓ Monoid operations enable merging across regions/servers")
    print("✓ This is how Twitter finds trending topics at global scale")
    print("="*70)
