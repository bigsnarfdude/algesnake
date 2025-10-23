"""
Example 01: Unique Users in Streaming Data

Real-world use case: Count unique Twitter users across billions of events
Inspired by: Twitter's Spark Streaming + Algebird HyperLogLog usage

Twitter's Original Problem:
- Millions of tweets per minute
- Need to count unique users in real-time
- Can't store all user IDs in memory (billions of users)

Solution:
- HyperLogLog provides ~2% accuracy with only 16KB memory
- Monoid operations allow merging across distributed servers
- Works in both batch and streaming modes

Reference: https://gist.github.com/MLnick/4761966
"""

from algesnake.approximate import HyperLogLog
from algesnake import Add
import random


class TwitterStreamAnalyzer:
    """
    Analyze Twitter streams like Twitter does with Algebird.

    Based on Twitter's actual Spark Streaming implementation using
    HyperLogLog for counting unique users across distributed streams.
    """

    def __init__(self, precision=14):
        """
        Initialize stream analyzer.

        Args:
            precision: HyperLogLog precision (10-18)
                      Higher = more accurate but more memory
                      Twitter uses precision=12-14 in production
        """
        # Global aggregation across all batches
        self.global_hll = HyperLogLog(precision=precision)
        self.total_tweets = Add(0)
        self.total_batches = Add(0)

        # Exact tracking for error measurement
        self.exact_users = set()

    def process_batch(self, tweets):
        """
        Process a micro-batch of tweets (like Spark Streaming).

        In production, this would be called every 1-5 seconds with
        a batch of tweets collected during that window.
        """
        # Create batch-level aggregations
        batch_hll = HyperLogLog(precision=14)
        batch_tweets = Add(0)
        batch_users = set()

        # Process each tweet in the batch
        for tweet in tweets:
            user_id = tweet['user_id']

            # Add to approximate counter (HyperLogLog)
            batch_hll.add(user_id)
            batch_tweets += Add(1)

            # Add to exact counter (for error measurement)
            batch_users.add(user_id)

        # Merge batch into global state (MONOID OPERATION!)
        # This is the key: we can merge HLLs from different time windows
        # or different servers, and the result is mathematically sound
        self.global_hll = self.global_hll + batch_hll
        self.total_tweets = self.total_tweets + batch_tweets
        self.total_batches += Add(1)
        self.exact_users.update(batch_users)

        return self._generate_report(batch_hll, batch_users, batch_tweets)

    def _generate_report(self, batch_hll, batch_users, batch_tweets):
        """Generate analytics report for this batch."""
        batch_unique = batch_hll.cardinality()
        global_unique = self.global_hll.cardinality()
        exact_count = len(self.exact_users)

        # Calculate error rate (how close are we to exact?)
        if exact_count > 0:
            error = ((global_unique / exact_count) - 1) * 100
        else:
            error = 0.0

        return {
            'batch': {
                'tweets': batch_tweets.value,
                'approx_unique_users': int(batch_unique),
                'exact_unique_users': len(batch_users),
            },
            'global': {
                'total_batches': self.total_batches.value,
                'total_tweets': self.total_tweets.value,
                'approx_unique_users': int(global_unique),
                'exact_unique_users': exact_count,
                'error_rate': f"{error:.5f}%",
                'memory_savings': self._calculate_memory_savings()
            }
        }

    def _calculate_memory_savings(self):
        """Calculate memory savings vs exact counting."""
        # Exact: 8 bytes per user ID (assuming 64-bit integers)
        exact_memory_mb = (len(self.exact_users) * 8) / (1024 * 1024)

        # HyperLogLog: 2^precision bytes (precision=14 -> 16KB)
        hll_memory_kb = (2 ** 14) / 1024

        if exact_memory_mb > 0:
            savings = (1 - (hll_memory_kb / 1024 / exact_memory_mb)) * 100
        else:
            savings = 0

        return {
            'exact_memory_mb': f"{exact_memory_mb:.2f}",
            'hll_memory_kb': f"{hll_memory_kb:.2f}",
            'savings_percent': f"{savings:.2f}%"
        }


def simulate_twitter_stream():
    """
    Simulate a Twitter firehose stream.

    In production, Twitter processes millions of tweets per minute.
    We'll simulate smaller batches to demonstrate the concept.
    """
    # Simulate 100K total unique users
    total_unique_users = 100000

    # Generate batches (simulating 10 micro-batches)
    batches = []
    for batch_num in range(10):
        batch = []

        # Each batch has 10K tweets
        # Users follow Zipf distribution (some users tweet a lot)
        for _ in range(10000):
            # Heavy users (top 20%) tweet more frequently
            if random.random() < 0.8:
                user_id = f"user_{random.randint(0, int(total_unique_users * 0.2))}"
            else:
                user_id = f"user_{random.randint(0, total_unique_users)}"

            batch.append({
                'user_id': user_id,
                'text': 'Some tweet text',
                'timestamp': batch_num
            })

        batches.append(batch)

    return batches


def distributed_processing_example():
    """
    Demonstrate distributed processing (like Spark).

    This simulates processing data across multiple servers,
    then merging results using monoid operations.
    """
    print("\n" + "="*70)
    print("DISTRIBUTED PROCESSING EXAMPLE (Multi-Server)")
    print("="*70)

    # Simulate 3 servers processing different partitions of data
    server1_data = [f"user_{i}" for i in range(50000)]
    server2_data = [f"user_{i}" for i in range(25000, 75000)]  # Overlap!
    server3_data = [f"user_{i}" for i in range(50000, 100000)]

    # Each server processes its partition independently
    server1_hll = HyperLogLog(precision=14)
    for user in server1_data:
        server1_hll.add(user)

    server2_hll = HyperLogLog(precision=14)
    for user in server2_data:
        server2_hll.add(user)

    server3_hll = HyperLogLog(precision=14)
    for user in server3_data:
        server3_hll.add(user)

    # MERGE across servers (this is the power of monoids!)
    global_hll = server1_hll + server2_hll + server3_hll

    # Calculate exact for comparison
    exact_users = set(server1_data + server2_data + server3_data)

    print(f"Server 1: {server1_hll.cardinality():.0f} unique users")
    print(f"Server 2: {server2_hll.cardinality():.0f} unique users")
    print(f"Server 3: {server3_hll.cardinality():.0f} unique users")
    print(f"\nMerged (approx): {global_hll.cardinality():.0f} unique users")
    print(f"Exact count: {len(exact_users):,} unique users")
    print(f"Error: {((global_hll.cardinality() / len(exact_users)) - 1) * 100:.3f}%")
    print(f"\nMemory per server: 16 KB (HyperLogLog)")
    print(f"Memory for exact: {len(exact_users) * 8 / 1024 / 1024:.2f} MB")
    print(f"Savings: {(1 - (16/1024) / (len(exact_users) * 8 / 1024 / 1024)) * 100:.2f}%")


if __name__ == "__main__":
    print("="*70)
    print("Twitter Unique Users Streaming Analytics")
    print("Based on Twitter's Algebird + Spark Streaming Implementation")
    print("="*70)

    # Create analyzer (like Twitter's production system)
    analyzer = TwitterStreamAnalyzer(precision=14)

    # Simulate Twitter firehose
    print("\nProcessing Twitter stream batches...\n")
    batches = simulate_twitter_stream()

    for i, batch in enumerate(batches, 1):
        report = analyzer.process_batch(batch)

        if i % 3 == 0:  # Print every 3rd batch
            print(f"Batch {i}:")
            print(f"  Batch tweets: {report['batch']['tweets']:,}")
            print(f"  Batch unique (approx): {report['batch']['approx_unique_users']:,}")
            print(f"  Batch unique (exact): {report['batch']['exact_unique_users']:,}")
            print(f"\n  Global tweets: {report['global']['total_tweets']:,}")
            print(f"  Global unique (approx): {report['global']['approx_unique_users']:,}")
            print(f"  Global unique (exact): {report['global']['exact_unique_users']:,}")
            print(f"  Error rate: {report['global']['error_rate']}")
            print(f"  Memory savings: {report['global']['memory_savings']['savings_percent']}")
            print()

    # Final report
    print("="*70)
    print("FINAL RESULTS")
    print("="*70)
    final_report = analyzer.process_batch([])  # Empty batch to get final stats
    print(f"Total batches processed: {final_report['global']['total_batches']}")
    print(f"Total tweets: {final_report['global']['total_tweets']:,}")
    print(f"Unique users (approx): {final_report['global']['approx_unique_users']:,}")
    print(f"Unique users (exact): {final_report['global']['exact_unique_users']:,}")
    print(f"Error rate: {final_report['global']['error_rate']}")
    print(f"\nMemory comparison:")
    print(f"  HyperLogLog: {final_report['global']['memory_savings']['hll_memory_kb']} KB")
    print(f"  Exact set: {final_report['global']['memory_savings']['exact_memory_mb']} MB")
    print(f"  Savings: {final_report['global']['memory_savings']['savings_percent']}")

    # Distributed processing example
    distributed_processing_example()

    print("\n" + "="*70)
    print("KEY TAKEAWAYS")
    print("="*70)
    print("✓ HyperLogLog provides ~2% accuracy with 99.9%+ memory savings")
    print("✓ Monoid properties enable distributed processing (associative merging)")
    print("✓ Works seamlessly in batch, streaming, and hybrid modes")
    print("✓ This is how Twitter counts unique users at billion-scale")
    print("="*70)
