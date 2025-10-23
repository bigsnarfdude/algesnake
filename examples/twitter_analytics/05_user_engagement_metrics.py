"""
Example 05: User Engagement Metrics

Real-world use case: Aggregate user engagement metrics across distributed systems
Inspired by: Twitter's use of basic monoids for analytics aggregation

Twitter's Original Problem:
- Track engagement metrics for millions of users
- Metrics: likes, retweets, replies, followers, etc.
- Need to aggregate across time windows and servers
- Must handle partial failures (some servers down)

Solution:
- Basic monoids (Add, Max, Min) for numeric aggregations
- MapMonoid for per-user metric dictionaries
- Associative operations allow flexible aggregation order
"""

from algesnake import Add, Max, Min, MapMonoid
from algesnake.approximate import HyperLogLog
import random


class UserEngagementMetrics:
    """
    Track user engagement metrics using monoids.

    Each metric is a monoid, allowing easy aggregation across
    time windows, users, or distributed servers.
    """

    def __init__(self, user_id):
        self.user_id = user_id

        # Basic count metrics (Add monoid)
        self.tweets = Add(0)
        self.likes_received = Add(0)
        self.retweets_received = Add(0)
        self.replies_received = Add(0)

        # Max/Min metrics
        self.max_likes_single_tweet = Max(0)
        self.min_likes_single_tweet = Min(float('inf'))

        # Unique counters (HyperLogLog)
        self.unique_retweeters = HyperLogLog(precision=12)
        self.unique_repliers = HyperLogLog(precision=12)

    def record_tweet(self, likes=0, retweets=0, replies=0, retweeters=None, repliers=None):
        """Record engagement for a single tweet."""
        self.tweets += Add(1)
        self.likes_received += Add(likes)
        self.retweets_received += Add(retweets)
        self.replies_received += Add(replies)

        # Track max/min likes
        self.max_likes_single_tweet += Max(likes)
        if likes > 0:
            self.min_likes_single_tweet += Min(likes)

        # Track unique users who engaged
        if retweeters:
            for user in retweeters:
                self.unique_retweeters.add(user)

        if repliers:
            for user in repliers:
                self.unique_repliers.add(user)

    def merge(self, other):
        """
        Merge metrics from another time period or server (MONOID OPERATION!).

        This is the key: we can merge metrics from:
        - Different time windows (hour 1 + hour 2 = day 1)
        - Different servers (server A + server B = global)
        - Partial computations (batch 1 + batch 2 = total)
        """
        merged = UserEngagementMetrics(self.user_id)

        # All monoid additions!
        merged.tweets = self.tweets + other.tweets
        merged.likes_received = self.likes_received + other.likes_received
        merged.retweets_received = self.retweets_received + other.retweets_received
        merged.replies_received = self.replies_received + other.replies_received

        merged.max_likes_single_tweet = self.max_likes_single_tweet + other.max_likes_single_tweet
        merged.min_likes_single_tweet = self.min_likes_single_tweet + other.min_likes_single_tweet

        merged.unique_retweeters = self.unique_retweeters + other.unique_retweeters
        merged.unique_repliers = self.unique_repliers + other.unique_repliers

        return merged

    def __add__(self, other):
        """Enable using + operator for merging."""
        return self.merge(other)

    def get_report(self):
        """Generate engagement report."""
        return {
            'user_id': self.user_id,
            'tweets': self.tweets.value,
            'total_engagement': {
                'likes': self.likes_received.value,
                'retweets': self.retweets_received.value,
                'replies': self.replies_received.value,
                'total': (self.likes_received + self.retweets_received + self.replies_received).value
            },
            'per_tweet_avg': {
                'likes': self.likes_received.value / self.tweets.value if self.tweets.value > 0 else 0,
                'retweets': self.retweets_received.value / self.tweets.value if self.tweets.value > 0 else 0,
            },
            'best_tweet': {
                'max_likes': self.max_likes_single_tweet.value,
                'min_likes': self.min_likes_single_tweet.value if self.min_likes_single_tweet.value != float('inf') else 0
            },
            'unique_users': {
                'retweeters': int(self.unique_retweeters.cardinality()),
                'repliers': int(self.unique_repliers.cardinality())
            }
        }


def simulate_user_activity():
    """Simulate a user's Twitter activity over time."""
    metrics = UserEngagementMetrics("@elonmusk")

    # Simulate 100 tweets with varying engagement
    for i in range(100):
        # Some tweets go viral
        if random.random() < 0.05:  # 5% viral tweets
            likes = random.randint(10000, 100000)
            retweets = random.randint(1000, 10000)
            replies = random.randint(500, 5000)
        elif random.random() < 0.2:  # 20% popular tweets
            likes = random.randint(1000, 10000)
            retweets = random.randint(100, 1000)
            replies = random.randint(50, 500)
        else:  # Normal tweets
            likes = random.randint(10, 1000)
            retweets = random.randint(1, 100)
            replies = random.randint(0, 50)

        # Generate unique users
        retweeters = [f"user_{random.randint(1, 50000)}" for _ in range(min(retweets, 100))]
        repliers = [f"user_{random.randint(1, 50000)}" for _ in range(min(replies, 100))]

        metrics.record_tweet(likes, retweets, replies, retweeters, repliers)

    return metrics


def time_based_aggregation_example():
    """
    Demonstrate aggregating metrics across time windows.

    This shows how monoids enable flexible time-based rollups.
    """
    print("\n" + "="*70)
    print("TIME-BASED AGGREGATION (Hour -> Day -> Week)")
    print("="*70)

    user_id = "@techinfluencer"

    # Simulate 7 days, 24 hours each
    daily_metrics = []

    for day in range(7):
        hourly_metrics = []

        # 24 hours in a day
        for hour in range(24):
            hour_metrics = UserEngagementMetrics(user_id)

            # Simulate tweets during this hour
            num_tweets = random.randint(0, 5)
            for _ in range(num_tweets):
                likes = random.randint(10, 1000)
                retweets = random.randint(1, 100)
                replies = random.randint(0, 50)
                hour_metrics.record_tweet(likes, retweets, replies)

            hourly_metrics.append(hour_metrics)

        # Aggregate hours into day (MONOID OPERATION!)
        day_metrics = hourly_metrics[0]
        for hour_metric in hourly_metrics[1:]:
            day_metrics = day_metrics + hour_metric

        daily_metrics.append(day_metrics)
        print(f"Day {day + 1}: {day_metrics.tweets.value} tweets, "
              f"{day_metrics.likes_received.value:,} likes")

    # Aggregate days into week (MONOID OPERATION!)
    week_metrics = daily_metrics[0]
    for day_metric in daily_metrics[1:]:
        week_metrics = week_metrics + day_metric

    print("\n" + "-"*70)
    print(f"Weekly Total: {week_metrics.tweets.value} tweets, "
          f"{week_metrics.likes_received.value:,} likes, "
          f"{week_metrics.retweets_received.value:,} retweets")


def distributed_aggregation_example():
    """
    Demonstrate aggregating metrics across distributed servers.
    """
    print("\n" + "="*70)
    print("DISTRIBUTED AGGREGATION (Multi-Server)")
    print("="*70)

    user_id = "@distributed_user"

    # Simulate 3 servers processing different events for the same user
    server1 = UserEngagementMetrics(user_id)
    server2 = UserEngagementMetrics(user_id)
    server3 = UserEngagementMetrics(user_id)

    # Each server sees different tweets
    print("\nProcessing on 3 distributed servers...")

    for _ in range(30):
        server1.record_tweet(
            likes=random.randint(10, 100),
            retweets=random.randint(1, 20),
            replies=random.randint(0, 10)
        )

    for _ in range(40):
        server2.record_tweet(
            likes=random.randint(10, 100),
            retweets=random.randint(1, 20),
            replies=random.randint(0, 10)
        )

    for _ in range(35):
        server3.record_tweet(
            likes=random.randint(10, 100),
            retweets=random.randint(1, 20),
            replies=random.randint(0, 10)
        )

    print(f"Server 1: {server1.tweets.value} tweets")
    print(f"Server 2: {server2.tweets.value} tweets")
    print(f"Server 3: {server3.tweets.value} tweets")

    # MERGE across servers (MONOID OPERATION!)
    global_metrics = server1 + server2 + server3

    print(f"\nGlobal (merged): {global_metrics.tweets.value} tweets")
    print("✓ Merged metrics from 3 servers using monoid + operation!")


if __name__ == "__main__":
    print("="*70)
    print("Twitter User Engagement Metrics")
    print("Based on Twitter's Algebird Monoid Aggregations")
    print("="*70)

    # Simulate user activity
    print("\nSimulating user activity...")
    metrics = simulate_user_activity()

    # Print report
    report = metrics.get_report()
    print("\n" + "="*70)
    print(f"ENGAGEMENT REPORT: {report['user_id']}")
    print("="*70)
    print(f"\nTotal Tweets: {report['tweets']:,}")
    print(f"\nTotal Engagement:")
    print(f"  Likes: {report['total_engagement']['likes']:,}")
    print(f"  Retweets: {report['total_engagement']['retweets']:,}")
    print(f"  Replies: {report['total_engagement']['replies']:,}")
    print(f"  Total: {report['total_engagement']['total']:,}")
    print(f"\nPer-Tweet Average:")
    print(f"  Likes: {report['per_tweet_avg']['likes']:.1f}")
    print(f"  Retweets: {report['per_tweet_avg']['retweets']:.1f}")
    print(f"\nBest Tweet:")
    print(f"  Max likes: {report['best_tweet']['max_likes']:,}")
    print(f"\nUnique Users:")
    print(f"  Retweeters: {report['unique_users']['retweeters']:,}")
    print(f"  Repliers: {report['unique_users']['repliers']:,}")

    # Time-based aggregation
    time_based_aggregation_example()

    # Distributed aggregation
    distributed_aggregation_example()

    print("\n" + "="*70)
    print("KEY TAKEAWAYS")
    print("="*70)
    print("✓ Monoids enable flexible aggregation patterns")
    print("✓ Same code works for time-based and server-based aggregation")
    print("✓ Associativity allows arbitrary grouping: (a+b)+c = a+(b+c)")
    print("✓ Add, Max, Min provide basic metric building blocks")
    print("✓ Composition: combine multiple monoids for complex metrics")
    print("="*70)
