"""
Example 09: URL Shortener Analytics (t.co)

Real-world use case: Track URL click-through rates for Twitter's t.co shortener
Inspired by: Twitter's URL shortening service analytics

Problem:
- Twitter shortens all URLs to t.co links
- Need to track: click-through rates, popular domains, unique clickers
- Billions of URLs shortened daily

Solution:
- CountMinSketch for URL click frequencies
- HyperLogLog for unique clickers per URL
- TopK for most clicked URLs
"""

from algesnake.approximate import CountMinSketch, HyperLogLog, TopK
from algesnake import Add, MapMonoid
import random
import hashlib


class URLShortenerAnalytics:
    """
    Analytics for Twitter's t.co URL shortening service.
    """
    
    def __init__(self):
        # Click frequency per URL
        self.url_clicks = CountMinSketch.from_error_rate(epsilon=0.001, delta=0.01)
        
        # Unique clickers per URL (using dict of HLLs)
        self.unique_clickers = {}
        
        # Top clicked URLs
        self.top_urls = TopK(k=100)
        
        # Total stats
        self.total_clicks = Add(0)
        self.total_urls_shortened = Add(0)
    
    def shorten_url(self, original_url):
        """Shorten a URL (simulate t.co)."""
        # Create short URL hash
        hash_obj = hashlib.md5(original_url.encode())
        short_code = hash_obj.hexdigest()[:8]
        return f"https://t.co/{short_code}"
    
    def record_url_created(self, original_url):
        """Record that a URL was shortened."""
        self.total_urls_shortened += Add(1)
        short_url = self.shorten_url(original_url)
        
        # Initialize HLL for this URL
        if short_url not in self.unique_clickers:
            self.unique_clickers[short_url] = HyperLogLog(precision=12)
        
        return short_url
    
    def record_click(self, short_url, user_id):
        """Record a click on a shortened URL."""
        self.total_clicks += Add(1)
        
        # Track click frequency
        self.url_clicks.add(short_url)
        
        # Track unique clickers
        if short_url not in self.unique_clickers:
            self.unique_clickers[short_url] = HyperLogLog(precision=12)
        self.unique_clickers[short_url].add(user_id)
        
        # Track for TopK
        self.top_urls.add(short_url)
    
    def get_url_stats(self, short_url):
        """Get statistics for a specific URL."""
        clicks = self.url_clicks.estimate(short_url)
        unique = int(self.unique_clickers[short_url].cardinality()) if short_url in self.unique_clickers else 0
        
        return {
            'short_url': short_url,
            'total_clicks': clicks,
            'unique_clickers': unique,
            'click_through_multiplier': clicks / unique if unique > 0 else 0
        }
    
    def get_top_urls(self, n=10):
        """Get top N most clicked URLs."""
        return self.top_urls.top(n)
    
    def merge(self, other):
        """Merge URL analytics (MONOID OPERATION!)."""
        merged = URLShortenerAnalytics()
        merged.url_clicks = self.url_clicks + other.url_clicks
        merged.top_urls = self.top_urls + other.top_urls
        merged.total_clicks = self.total_clicks + other.total_clicks
        merged.total_urls_shortened = self.total_urls_shortened + other.total_urls_shortened
        
        # Merge HLLs for unique clickers
        all_urls = set(list(self.unique_clickers.keys()) + list(other.unique_clickers.keys()))
        for url in all_urls:
            hll1 = self.unique_clickers.get(url, HyperLogLog(precision=12))
            hll2 = other.unique_clickers.get(url, HyperLogLog(precision=12))
            merged.unique_clickers[url] = hll1 + hll2
        
        return merged
    
    def __add__(self, other):
        return self.merge(other)


def simulate_url_shortening():
    """Simulate Twitter URL shortening and clicks."""
    analytics = URLShortenerAnalytics()
    
    # Popular URLs that get shared often
    popular_urls = [
        "https://techcrunch.com/breaking-news",
        "https://github.com/trending",
        "https://youtube.com/viral-video",
        "https://news.ycombinator.com/top",
        "https://medium.com/popular-article",
    ]
    
    # Less popular URLs
    normal_urls = [f"https://example{i}.com/article" for i in range(100)]
    
    # Shorten URLs
    shortened_urls = {}
    
    # Popular URLs get shortened often
    for url in popular_urls:
        short = analytics.record_url_created(url)
        shortened_urls[url] = short
    
    # Normal URLs
    for url in random.sample(normal_urls, 20):
        short = analytics.record_url_created(url)
        shortened_urls[url] = short
    
    # Simulate clicks
    for _ in range(50000):
        # Popular URLs get more clicks
        if random.random() < 0.7:
            url = random.choice(popular_urls)
        else:
            url = random.choice(list(shortened_urls.keys()))
        
        short_url = shortened_urls[url]
        user_id = f"user_{random.randint(1, 10000)}"
        
        analytics.record_click(short_url, user_id)
    
    return analytics, shortened_urls


def distributed_url_analytics():
    """Analyze URL clicks across distributed servers."""
    print("\n" + "="*70)
    print("DISTRIBUTED URL ANALYTICS (Multi-Region)")
    print("="*70)
    
    # 3 regional servers
    us_analytics = URLShortenerAnalytics()
    eu_analytics = URLShortenerAnalytics()
    asia_analytics = URLShortenerAnalytics()
    
    # Create some URLs
    test_url = "https://breaking-news.com/story"
    short_url_us = us_analytics.record_url_created(test_url)
    short_url_eu = eu_analytics.record_url_created(test_url)
    short_url_asia = asia_analytics.record_url_created(test_url)
    
    # Same short URL across regions (deterministic hashing)
    assert short_url_us == short_url_eu == short_url_asia
    short_url = short_url_us
    
    # Each region sees different clicks
    for _ in range(5000):
        us_analytics.record_click(short_url, f"us_user_{random.randint(1, 3000)}")
    
    for _ in range(7000):
        eu_analytics.record_click(short_url, f"eu_user_{random.randint(1, 4000)}")
    
    for _ in range(6000):
        asia_analytics.record_click(short_url, f"asia_user_{random.randint(1, 3500)}")
    
    print(f"\nRegional clicks:")
    print(f"  US:   {us_analytics.total_clicks.value:,} clicks")
    print(f"  EU:   {eu_analytics.total_clicks.value:,} clicks")
    print(f"  ASIA: {asia_analytics.total_clicks.value:,} clicks")
    
    # MERGE across regions (MONOID OPERATION!)
    global_analytics = us_analytics + eu_analytics + asia_analytics
    
    global_stats = global_analytics.get_url_stats(short_url)
    
    print(f"\nGlobal statistics for {short_url}:")
    print(f"  Total clicks: {global_stats['total_clicks']:,}")
    print(f"  Unique clickers: {global_stats['unique_clickers']:,}")
    print(f"  CTR multiplier: {global_stats['click_through_multiplier']:.2f}x")
    
    print("\n✓ Merged URL analytics from 3 regions using monoid operations!")


if __name__ == "__main__":
    print("="*70)
    print("Twitter URL Shortener Analytics (t.co)")
    print("Track clicks and engagement for shortened URLs")
    print("="*70)
    
    # Run simulation
    print("\nSimulating URL shortening and clicks...")
    analytics, shortened_urls = simulate_url_shortening()
    
    # Overall stats
    print("\n" + "="*70)
    print("OVERALL STATISTICS")
    print("="*70)
    print(f"Total URLs shortened: {analytics.total_urls_shortened.value:,}")
    print(f"Total clicks: {analytics.total_clicks.value:,}")
    print(f"Avg clicks per URL: {analytics.total_clicks.value / analytics.total_urls_shortened.value:.1f}")
    
    # Top URLs
    print("\n" + "="*70)
    print("TOP 10 MOST CLICKED URLs")
    print("="*70)
    
    top_urls = analytics.get_top_urls(10)
    print(f"{'Rank':<6} {'Short URL':<30} {'Clicks':<10}")
    print("-" * 70)
    
    for i, (short_url, clicks) in enumerate(top_urls, 1):
        print(f"{i:<6} {short_url:<30} {clicks:<10,}")
    
    # Detailed stats for top URL
    if top_urls:
        top_url = top_urls[0][0]
        stats = analytics.get_url_stats(top_url)
        
        print("\n" + "="*70)
        print(f"DETAILED STATS: {top_url}")
        print("="*70)
        print(f"Total clicks: {stats['total_clicks']:,}")
        print(f"Unique clickers: {stats['unique_clickers']:,}")
        print(f"Avg clicks per user: {stats['click_through_multiplier']:.2f}x")
    
    # Distributed example
    distributed_url_analytics()
    
    print("\n" + "="*70)
    print("KEY TAKEAWAYS")
    print("="*70)
    print("✓ CountMinSketch tracks click frequencies per URL")
    print("✓ HyperLogLog counts unique clickers efficiently")
    print("✓ TopK maintains most popular URLs with O(k) memory")
    print("✓ All structures merge via monoid operations")
    print("✓ This is how Twitter's t.co analytics works at scale")
    print("="*70)
