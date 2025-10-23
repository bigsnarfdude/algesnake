"""
Example 08: Engagement Rate Percentiles (TDigest)

Real-world use case: Track engagement rate distributions for tweets
Inspired by: Using T-Digest for percentile-based metrics

Problem:
- Need to know: "What's the p95 engagement rate for our tweets?"
- Helps identify: viral content (p99), average performance (p50), poor performers (p10)
- Can't store all engagement rates (billions of tweets)

Solution:
- TDigest provides accurate percentile estimation
- Memory: O(compression) instead of O(n)
- Accurate to 0.1-1% for tail percentiles (p95, p99, p999)
"""

from algesnake.approximate import TDigest
from algesnake import Add, Max, Min
import random


class EngagementAnalyzer:
    """
    Analyze tweet engagement rate distributions using TDigest.
    
    Engagement rate = (likes + retweets + replies) / impressions * 100
    """
    
    def __init__(self, compression=100):
        # T-Digest for engagement rate percentiles
        self.engagement_rates = TDigest(compression=compression)
        
        # Track extremes
        self.best_engagement = Max(0.0)
        self.worst_engagement = Min(100.0)
        
        # Counters
        self.total_tweets = Add(0)
        self.viral_tweets = Add(0)  # p95+
        self.poor_tweets = Add(0)   # p10-
        
    def record_tweet(self, impressions, likes, retweets, replies):
        """Record engagement for a tweet."""
        if impressions == 0:
            return
        
        engagement = (likes + retweets + replies) / impressions * 100
        
        self.engagement_rates.add(engagement)
        self.total_tweets += Add(1)
        
        self.best_engagement += Max(engagement)
        self.worst_engagement += Min(engagement)
    
    def get_percentiles(self):
        """Get engagement rate percentiles."""
        if self.engagement_rates.count == 0:
            return None
        
        return {
            'p10': self.engagement_rates.percentile(10),
            'p25': self.engagement_rates.percentile(25),
            'p50': self.engagement_rates.percentile(50),
            'p75': self.engagement_rates.percentile(75),
            'p90': self.engagement_rates.percentile(90),
            'p95': self.engagement_rates.percentile(95),
            'p99': self.engagement_rates.percentile(99),
            'p999': self.engagement_rates.percentile(99.9),
            'best': self.best_engagement.value,
            'worst': self.worst_engagement.value,
        }
    
    def classify_tweet(self, engagement_rate):
        """Classify tweet performance."""
        percentiles = self.get_percentiles()
        if not percentiles:
            return "unknown"
        
        if engagement_rate >= percentiles['p95']:
            return "viral"
        elif engagement_rate >= percentiles['p75']:
            return "great"
        elif engagement_rate >= percentiles['p50']:
            return "good"
        elif engagement_rate >= percentiles['p25']:
            return "below_average"
        else:
            return "poor"
    
    def merge(self, other):
        """Merge engagement analyzers (MONOID OPERATION!)."""
        merged = EngagementAnalyzer(compression=100)
        merged.engagement_rates = self.engagement_rates + other.engagement_rates
        merged.best_engagement = self.best_engagement + other.best_engagement
        merged.worst_engagement = self.worst_engagement + other.worst_engagement
        merged.total_tweets = self.total_tweets + other.total_tweets
        return merged
    
    def __add__(self, other):
        return self.merge(other)


def simulate_tweet_engagement():
    """Simulate various tweet engagement patterns."""
    analyzer = EngagementAnalyzer()
    
    # Simulate 10K tweets with varying engagement
    for _ in range(10000):
        impressions = random.randint(100, 10000)
        
        # Most tweets have low engagement
        if random.random() < 0.8:  # 80% normal tweets
            engagement_factor = random.uniform(0.001, 0.05)  # 0.1% - 5%
        elif random.random() < 0.15:  # 15% good tweets
            engagement_factor = random.uniform(0.05, 0.15)    # 5% - 15%
        else:  # 5% viral tweets
            engagement_factor = random.uniform(0.15, 0.50)    # 15% - 50%
        
        total_engagement = int(impressions * engagement_factor)
        
        # Distribute engagement across actions
        likes = int(total_engagement * 0.7)
        retweets = int(total_engagement * 0.2)
        replies = int(total_engagement * 0.1)
        
        analyzer.record_tweet(impressions, likes, retweets, replies)
    
    return analyzer


def distributed_engagement_analysis():
    """Analyze engagement across multiple time windows."""
    print("\n" + "="*70)
    print("DISTRIBUTED ENGAGEMENT ANALYSIS (Hourly → Daily)")
    print("="*70)
    
    # 24 hours of tweet activity
    hourly_analyzers = []
    
    for hour in range(24):
        analyzer = EngagementAnalyzer()
        
        # Varying activity by hour
        num_tweets = random.randint(100, 500)
        
        for _ in range(num_tweets):
            impressions = random.randint(100, 5000)
            engagement_factor = random.uniform(0.001, 0.20)
            total_eng = int(impressions * engagement_factor)
            
            analyzer.record_tweet(
                impressions,
                likes=int(total_eng * 0.7),
                retweets=int(total_eng * 0.2),
                replies=int(total_eng * 0.1)
            )
        
        hourly_analyzers.append(analyzer)
    
    # Merge all hours into daily (MONOID OPERATION!)
    daily_analyzer = hourly_analyzers[0]
    for analyzer in hourly_analyzers[1:]:
        daily_analyzer = daily_analyzer + analyzer
    
    print(f"\nTotal tweets analyzed: {daily_analyzer.total_tweets.value:,}")
    
    percentiles = daily_analyzer.get_percentiles()
    print("\nEngagement Rate Distribution:")
    print("-" * 70)
    print(f"  p10 (10th percentile): {percentiles['p10']:.3f}%")
    print(f"  p25 (1st quartile):    {percentiles['p25']:.3f}%")
    print(f"  p50 (median):          {percentiles['p50']:.3f}%")
    print(f"  p75 (3rd quartile):    {percentiles['p75']:.3f}%")
    print(f"  p90 (90th percentile): {percentiles['p90']:.3f}%")
    print(f"  p95:                   {percentiles['p95']:.3f}%")
    print(f"  p99:                   {percentiles['p99']:.3f}%")
    print(f"  p999:                  {percentiles['p999']:.3f}%")
    print(f"\n  Best:  {percentiles['best']:.3f}%")
    print(f"  Worst: {percentiles['worst']:.3f}%")
    
    print("\n✓ Merged 24 hourly T-Digests using monoid operations!")


if __name__ == "__main__":
    print("="*70)
    print("Twitter Engagement Rate Percentiles")
    print("Using T-Digest for Distribution Analysis")
    print("="*70)
    
    # Run simulation
    print("\nAnalyzing 10,000 tweets...")
    analyzer = simulate_tweet_engagement()
    
    # Get percentiles
    percentiles = analyzer.get_percentiles()
    
    print("\n" + "="*70)
    print("ENGAGEMENT RATE DISTRIBUTION")
    print("="*70)
    print(f"\nTotal tweets analyzed: {analyzer.total_tweets.value:,}")
    print("\nPercentiles:")
    print(f"  p50 (median):  {percentiles['p50']:.3f}%")
    print(f"  p75:           {percentiles['p75']:.3f}%")
    print(f"  p90:           {percentiles['p90']:.3f}%")
    print(f"  p95:           {percentiles['p95']:.3f}%")
    print(f"  p99:           {percentiles['p99']:.3f}%")
    print(f"  p999:          {percentiles['p999']:.3f}%")
    
    print(f"\nExtremes:")
    print(f"  Best:  {percentiles['best']:.3f}%")
    print(f"  Worst: {percentiles['worst']:.3f}%")
    
    # Example classification
    print("\n" + "="*70)
    print("TWEET PERFORMANCE CLASSIFICATION")
    print("="*70)
    
    test_rates = [0.5, 2.0, 5.0, 10.0, 20.0]
    for rate in test_rates:
        classification = analyzer.classify_tweet(rate)
        print(f"  {rate:>5.1f}% engagement: {classification}")
    
    # Distributed analysis
    distributed_engagement_analysis()
    
    print("\n" + "="*70)
    print("KEY TAKEAWAYS")
    print("="*70)
    print("✓ T-Digest provides accurate percentile estimation")
    print("✓ Memory efficient: O(compression) vs O(n)")
    print("✓ Accurate to 0.1-1% for tail percentiles")
    print("✓ Merges via monoid operations")
    print("✓ Perfect for SLA monitoring and performance analysis")
    print("="*70)
