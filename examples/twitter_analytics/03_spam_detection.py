"""
Example 03: Spam Detection with Bloom Filters

Real-world use case: Detect spam accounts and prevent duplicate content
Inspired by: Twitter's use of Bloom filters for spam detection and deduplication

Twitter's Original Problem:
- Billions of tweets, need to detect spam in real-time
- Known spam accounts database is huge (millions of accounts)
- Need fast lookup: "Have we seen this spammer before?"
- Can't afford false negatives (missing spam) but can tolerate some false positives

Solution:
- Bloom Filter provides O(1) membership testing
- ~1% false positive rate with 99% memory savings
- Never has false negatives (if it says "not spam", it's definitely not in known spam list)

Real-world uses:
- Medium uses Bloom filters to recommend articles users haven't read
- Chrome uses them to check URLs against malicious site database
- Cassandra/HBase use them to reduce disk IO
"""

from algesnake.approximate import BloomFilter
from algesnake import Add, SetMonoid
import random
import hashlib


class SpamDetectionSystem:
    """
    Spam detection system using Bloom filters.

    Based on Twitter's approach to detecting known spammers
    and duplicate/near-duplicate content.
    """

    def __init__(self, expected_spammers=1000000, false_positive_rate=0.01):
        """
        Initialize spam detection system.

        Args:
            expected_spammers: Expected number of spam accounts
            false_positive_rate: Acceptable false positive rate (e.g., 0.01 = 1%)
        """
        # Bloom filter for known spam accounts
        self.spam_accounts = BloomFilter(
            capacity=expected_spammers,
            error_rate=false_positive_rate
        )

        # Bloom filter for seen content (detect duplicates)
        self.seen_content = BloomFilter(
            capacity=expected_spammers * 10,  # More content than users
            error_rate=0.001  # Lower false positive for content
        )

        # Stats
        self.spam_checks = Add(0)
        self.spam_detected = Add(0)
        self.duplicate_content = Add(0)

        # For comparison: exact sets (in production, these wouldn't exist)
        self.exact_spam_accounts = set()
        self.exact_seen_content = set()

    def add_known_spammer(self, user_id):
        """
        Add a known spammer to the database.

        This would be called when moderators identify spam accounts.
        """
        self.spam_accounts.add(user_id)
        self.exact_spam_accounts.add(user_id)  # For comparison

    def is_spam_account(self, user_id):
        """
        Check if a user is a known spammer.

        Returns:
            True if likely spam (can have false positives)
            False if definitely not spam (no false negatives)
        """
        self.spam_checks += Add(1)

        in_bloom = user_id in self.spam_accounts
        in_exact = user_id in self.exact_spam_accounts

        if in_bloom:
            self.spam_detected += Add(1)

        return {
            'user_id': user_id,
            'is_spam_bloom': in_bloom,
            'is_spam_exact': in_exact,
            'false_positive': in_bloom and not in_exact
        }

    def has_seen_content(self, tweet_text):
        """
        Check if we've seen this content before (duplicate detection).

        Uses content hash for deduplication.
        """
        # Hash the content
        content_hash = hashlib.md5(tweet_text.encode()).hexdigest()

        in_bloom = content_hash in self.seen_content
        in_exact = content_hash in self.exact_seen_content

        if not in_bloom:
            # New content, add it
            self.seen_content.add(content_hash)
            self.exact_seen_content.add(content_hash)
        else:
            self.duplicate_content += Add(1)

        return {
            'content_hash': content_hash[:16] + '...',
            'is_duplicate_bloom': in_bloom,
            'is_duplicate_exact': in_exact,
            'false_positive': in_bloom and not in_exact
        }

    def process_tweet(self, user_id, tweet_text):
        """
        Process a tweet: check for spam and duplicates.
        """
        spam_check = self.is_spam_account(user_id)
        duplicate_check = self.has_seen_content(tweet_text)

        return {
            'spam': spam_check,
            'duplicate': duplicate_check,
            'should_block': spam_check['is_spam_bloom'] or duplicate_check['is_duplicate_bloom']
        }

    def get_stats(self):
        """Get detection statistics."""
        return {
            'spam_checks': self.spam_checks.value,
            'spam_detected': self.spam_detected.value,
            'duplicates_detected': self.duplicate_content.value,
            'known_spammers': len(self.exact_spam_accounts),
            'unique_content': len(self.exact_seen_content)
        }

    def calculate_memory_savings(self):
        """Calculate memory savings vs exact approach."""
        # Exact approach: store all user IDs and content hashes
        # Assuming 64-bit user IDs and 128-bit content hashes
        exact_users_mb = (len(self.exact_spam_accounts) * 8) / (1024 * 1024)
        exact_content_mb = (len(self.exact_seen_content) * 16) / (1024 * 1024)

        # Bloom filter memory (from capacity and error rate)
        # Formula: m = -(n * ln(p)) / (ln(2)^2) where n=capacity, p=error_rate
        import math
        n_users = len(self.exact_spam_accounts)
        n_content = len(self.exact_seen_content)

        # Approximate Bloom filter sizes
        bloom_users_mb = 1.44 * n_users * abs(math.log(0.01)) / (1024 * 1024 * 8) if n_users > 0 else 0
        bloom_content_mb = 1.44 * n_content * abs(math.log(0.001)) / (1024 * 1024 * 8) if n_content > 0 else 0

        total_exact = exact_users_mb + exact_content_mb
        total_bloom = bloom_users_mb + bloom_content_mb

        return {
            'exact_mb': total_exact,
            'bloom_mb': total_bloom,
            'savings_percent': ((total_exact - total_bloom) / total_exact * 100) if total_exact > 0 else 0
        }


def simulate_twitter_spam_detection():
    """
    Simulate Twitter's spam detection system.
    """
    # Create spam detection system
    detector = SpamDetectionSystem(
        expected_spammers=100000,
        false_positive_rate=0.01
    )

    # Add known spammers (simulate moderator actions)
    print("Loading known spammer database...")
    for i in range(10000):
        spammer_id = f"spam_user_{i}"
        detector.add_known_spammer(spammer_id)

    print(f"Loaded {detector.get_stats()['known_spammers']:,} known spammers into Bloom filter\n")

    # Simulate tweet stream
    print("="*70)
    print("PROCESSING TWEET STREAM")
    print("="*70)

    results = {
        'total_tweets': 0,
        'spam_blocked': 0,
        'duplicates_blocked': 0,
        'false_positives': 0,
        'passed': 0
    }

    # Process 50K tweets
    for i in range(50000):
        # 10% spam accounts, 5% duplicate content
        if random.random() < 0.1:
            # Spam tweet
            user_id = f"spam_user_{random.randint(0, 10000)}"
            tweet_text = "BUY NOW! CLICK HERE! " + str(random.randint(0, 100))
        elif random.random() < 0.05:
            # Duplicate content
            user_id = f"user_{random.randint(0, 100000)}"
            tweet_text = "This is a repeated tweet"  # Same content
        else:
            # Normal tweet
            user_id = f"user_{random.randint(0, 100000)}"
            tweet_text = f"Normal tweet {i} about interesting things"

        result = detector.process_tweet(user_id, tweet_text)
        results['total_tweets'] += 1

        if result['should_block']:
            if result['spam']['is_spam_bloom']:
                results['spam_blocked'] += 1
                if result['spam']['false_positive']:
                    results['false_positives'] += 1
            if result['duplicate']['is_duplicate_bloom']:
                results['duplicates_blocked'] += 1
        else:
            results['passed'] += 1

    return detector, results


def distributed_spam_detection():
    """
    Demonstrate distributed spam detection across multiple servers.
    """
    print("\n" + "="*70)
    print("DISTRIBUTED SPAM DETECTION (Multi-Server)")
    print("="*70)

    # Create 3 regional spam detectors
    us_detector = SpamDetectionSystem(expected_spammers=50000, false_positive_rate=0.01)
    eu_detector = SpamDetectionSystem(expected_spammers=50000, false_positive_rate=0.01)
    asia_detector = SpamDetectionSystem(expected_spammers=50000, false_positive_rate=0.01)

    # Each region discovers different spammers
    print("\nEach region loading local spammer database...")
    for i in range(2000):
        us_detector.add_known_spammer(f"us_spam_{i}")
    for i in range(1500):
        eu_detector.add_known_spammer(f"eu_spam_{i}")
    for i in range(1000):
        asia_detector.add_known_spammer(f"asia_spam_{i}")

    # Some overlap (global spammers)
    for i in range(500):
        global_spammer = f"global_spam_{i}"
        us_detector.add_known_spammer(global_spammer)
        eu_detector.add_known_spammer(global_spammer)
        asia_detector.add_known_spammer(global_spammer)

    print(f"US: {us_detector.get_stats()['known_spammers']:,} spammers")
    print(f"EU: {eu_detector.get_stats()['known_spammers']:,} spammers")
    print(f"ASIA: {asia_detector.get_stats()['known_spammers']:,} spammers")

    # MERGE Bloom filters (monoid operation!)
    print("\nMerging spam databases from all regions...")
    global_spam_filter = us_detector.spam_accounts + eu_detector.spam_accounts + asia_detector.spam_accounts

    # Count unique spammers
    all_spammers = (us_detector.exact_spam_accounts |
                   eu_detector.exact_spam_accounts |
                   asia_detector.exact_spam_accounts)

    print(f"\nGlobal spam database: {len(all_spammers):,} unique spammers")
    print("✓ Merged 3 Bloom filters using monoid + operation!")

    # Test against merged filter
    test_users = [
        ("us_spam_100", True),
        ("eu_spam_100", True),
        ("asia_spam_100", True),
        ("global_spam_100", True),
        ("normal_user_12345", False),
    ]

    print("\nTesting merged filter:")
    for user_id, should_be_spam in test_users:
        in_filter = user_id in global_spam_filter
        symbol = "✓" if in_filter == should_be_spam else "✗"
        print(f"  {symbol} {user_id}: {'SPAM' if in_filter else 'CLEAN'}")


if __name__ == "__main__":
    print("="*70)
    print("Twitter Spam Detection with Bloom Filters")
    print("Based on Twitter's Algebird Bloom Filter Implementation")
    print("="*70)

    # Run spam detection simulation
    detector, results = simulate_twitter_spam_detection()

    # Print results
    print(f"\nResults:")
    print(f"  Total tweets: {results['total_tweets']:,}")
    print(f"  Spam blocked: {results['spam_blocked']:,}")
    print(f"  Duplicates blocked: {results['duplicates_blocked']:,}")
    print(f"  False positives: {results['false_positives']:,}")
    print(f"  Passed: {results['passed']:,}")

    # Memory savings
    savings = detector.calculate_memory_savings()
    print(f"\nMemory Comparison:")
    print(f"  Exact approach: {savings['exact_mb']:.2f} MB")
    print(f"  Bloom filter: {savings['bloom_mb']:.2f} MB")
    print(f"  Savings: {savings['savings_percent']:.1f}%")

    # Distributed example
    distributed_spam_detection()

    print("\n" + "="*70)
    print("KEY TAKEAWAYS")
    print("="*70)
    print("✓ Bloom filters provide fast O(1) membership testing")
    print("✓ Never has false negatives (if not in filter, definitely not spam)")
    print("✓ Configurable false positive rate (1% typical)")
    print("✓ 90%+ memory savings vs exact approach")
    print("✓ Monoid operations enable merging filters from multiple sources")
    print("✓ Used by Medium, Chrome, Cassandra, and Twitter for real-world spam detection")
    print("="*70)
