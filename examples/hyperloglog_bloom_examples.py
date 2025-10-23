"""Approximation Algorithms - Examples

This file demonstrates HyperLogLog and Bloom Filter usage for:
- Counting unique elements with minimal memory
- Testing set membership probabilistically
- Distributed aggregation scenarios
"""

from algesnake.approximate import HyperLogLog, BloomFilter
from algesnake.approximate.hyperloglog import estimate_cardinality, merge_hlls
from algesnake.approximate.bloom import create_bloom_filter, merge_bloom_filters


def example_hyperloglog_basics():
    """Basic HyperLogLog usage."""
    print("=" * 60)
    print("Example 1: HyperLogLog Basics")
    print("=" * 60)
    
    # Create HLL with precision 14 (~1.6% error, 16 KB memory)
    hll = HyperLogLog(precision=14)
    
    # Add items
    for i in range(10000):
        hll.add(f"user_{i}")
    
    print(f"Added 10,000 unique users")
    print(f"Estimated cardinality: {hll.cardinality()}")
    print(f"Error: {abs(hll.cardinality() - 10000) / 10000 * 100:.2f}%")
    print()


def example_hyperloglog_streaming():
    """Streaming cardinality estimation."""
    print("=" * 60)
    print("Example 2: Streaming Unique User Count")
    print("=" * 60)
    
    hll = HyperLogLog(precision=12)
    
    # Simulate streaming events
    events = [
        ("user_1", "login"),
        ("user_2", "login"),
        ("user_1", "click"),  # Duplicate user
        ("user_3", "purchase"),
        ("user_2", "logout"),  # Duplicate user
        ("user_4", "login"),
    ]
    
    for user_id, event in events:
        hll.add(user_id)
        print(f"Event: {event:10} | User: {user_id:10} | Unique users ≈ {hll.cardinality()}")
    
    print(f"\nFinal unique users: {hll.cardinality()} (actual: 4)")
    print()


def example_hyperloglog_distributed():
    """Distributed unique user counting across partitions."""
    print("=" * 60)
    print("Example 3: Distributed Unique User Counting")
    print("=" * 60)
    
    # Simulate 3 server partitions
    partition1 = HyperLogLog(precision=14)
    partition2 = HyperLogLog(precision=14)
    partition3 = HyperLogLog(precision=14)
    
    # Partition 1: users 0-999
    for i in range(1000):
        partition1.add(f"user_{i}")
    
    # Partition 2: users 500-1499 (50% overlap)
    for i in range(500, 1500):
        partition2.add(f"user_{i}")
    
    # Partition 3: users 1000-1999 (50% overlap)
    for i in range(1000, 2000):
        partition3.add(f"user_{i}")
    
    # Merge partitions using monoid operation
    total = partition1 + partition2 + partition3
    
    print(f"Partition 1: ~{partition1.cardinality()} users")
    print(f"Partition 2: ~{partition2.cardinality()} users")
    print(f"Partition 3: ~{partition3.cardinality()} users")
    print(f"Total unique users: ~{total.cardinality()} (actual: 2000)")
    print(f"Error: {abs(total.cardinality() - 2000) / 2000 * 100:.2f}%")
    print()


def example_hyperloglog_sum():
    """Using sum() builtin with HyperLogLog."""
    print("=" * 60)
    print("Example 4: Using sum() for Aggregation")
    print("=" * 60)
    
    # Multiple HLLs from different data sources
    hlls = []
    
    for source_id in range(5):
        hll = HyperLogLog(precision=12)
        for i in range(200):
            # Each source has overlapping user ranges
            user_id = source_id * 150 + i
            hll.add(f"user_{user_id}")
        hlls.append(hll)
    
    # Aggregate using sum()
    total = sum(hlls)
    
    print(f"Merged {len(hlls)} data sources")
    print(f"Total unique users: ~{total.cardinality()}")
    print(f"(Expected ~750 with 50% overlap per source)")
    print()


def example_bloom_filter_basics():
    """Basic Bloom Filter usage."""
    print("=" * 60)
    print("Example 5: Bloom Filter Basics")
    print("=" * 60)
    
    # Create Bloom Filter for 1000 items with 1% false positive rate
    bf = BloomFilter(capacity=1000, error_rate=0.01)
    
    # Add spam emails
    spam_emails = [
        "spam1@bad.com",
        "spam2@bad.com",
        "phishing@scam.org",
        "malware@virus.net",
    ]
    
    for email in spam_emails:
        bf.add(email)
    
    # Test membership
    print("Testing known spam:")
    for email in spam_emails:
        result = "BLOCKED" if email in bf else "ALLOWED"
        print(f"  {email:25} -> {result}")
    
    print("\nTesting legitimate emails:")
    legit_emails = ["user@example.com", "admin@company.org"]
    for email in legit_emails:
        result = "BLOCKED" if email in bf else "ALLOWED"
        print(f"  {email:25} -> {result}")
    
    print(f"\nBloom Filter saturation: {bf.saturation():.2%}")
    print(f"Expected false positive rate: {bf.expected_fpr():.2%}")
    print()


def example_bloom_filter_web_crawler():
    """Web crawler duplicate URL detection."""
    print("=" * 60)
    print("Example 6: Web Crawler Duplicate Detection")
    print("=" * 60)
    
    # Bloom Filter for 1 million URLs with 0.1% FPR
    visited = BloomFilter(capacity=1000000, error_rate=0.001)
    
    # Simulate crawling
    urls_to_crawl = [
        "https://example.com/page1",
        "https://example.com/page2",
        "https://example.com/page1",  # Duplicate
        "https://example.com/page3",
        "https://example.com/page2",  # Duplicate
        "https://example.com/page4",
    ]
    
    crawled_count = 0
    
    for url in urls_to_crawl:
        if url not in visited:
            # New URL, crawl it
            print(f"CRAWLING: {url}")
            visited.add(url)
            crawled_count += 1
        else:
            print(f"SKIPPING: {url} (already visited)")
    
    print(f"\nCrawled {crawled_count} unique URLs")
    print(f"Bloom Filter memory: ~{visited.size // 8 / 1024:.2f} KB")
    print()


def example_bloom_filter_distributed():
    """Distributed spam blocklist using Bloom Filters."""
    print("=" * 60)
    print("Example 7: Distributed Spam Blocklist")
    print("=" * 60)
    
    # Three mail servers maintain separate blocklists
    server1 = BloomFilter(capacity=5000, error_rate=0.01)
    server2 = BloomFilter(capacity=5000, error_rate=0.01)
    server3 = BloomFilter(capacity=5000, error_rate=0.01)
    
    # Each server adds spam IPs from their region
    for i in range(100):
        server1.add(f"192.168.1.{i}")
    
    for i in range(100, 200):
        server2.add(f"192.168.1.{i}")
    
    for i in range(200, 300):
        server3.add(f"192.168.1.{i}")
    
    # Merge blocklists using monoid operation
    global_blocklist = server1 + server2 + server3
    
    # Test against merged blocklist
    test_ips = [
        "192.168.1.50",   # In server1
        "192.168.1.150",  # In server2
        "192.168.1.250",  # In server3
        "192.168.1.999",  # Not in any blocklist
    ]
    
    print("Testing IPs against global blocklist:")
    for ip in test_ips:
        status = "BLOCKED" if ip in global_blocklist else "ALLOWED"
        print(f"  {ip:20} -> {status}")
    
    print(f"\nGlobal blocklist saturation: {global_blocklist.saturation():.2%}")
    print()


def example_combined_analytics():
    """Combining HLL and Bloom Filter for analytics."""
    print("=" * 60)
    print("Example 8: Combined Analytics Pipeline")
    print("=" * 60)
    
    # Track unique visitors (HLL) and malicious IPs (Bloom Filter)
    unique_visitors = HyperLogLog(precision=14)
    malicious_ips = BloomFilter(capacity=10000, error_rate=0.01)
    
    # Simulate web traffic
    traffic = [
        ("192.168.1.1", "user_1", "GET /home"),
        ("192.168.1.2", "user_2", "GET /products"),
        ("192.168.1.1", "user_1", "GET /cart"),      # Repeat visitor
        ("10.0.0.1", "bot_1", "GET /admin"),         # Malicious
        ("10.0.0.2", "bot_2", "GET /login"),         # Malicious
        ("192.168.1.3", "user_3", "POST /checkout"),
        ("10.0.0.1", "bot_1", "GET /admin"),         # Repeat attacker
    ]
    
    blocked_requests = 0
    
    for ip, user_id, request in traffic:
        # Check if IP is malicious
        if ip.startswith("10.0.0"):  # Malicious subnet
            if ip not in malicious_ips:
                malicious_ips.add(ip)
                print(f"BLOCKED: {ip:15} {request:20} (added to blocklist)")
            else:
                print(f"BLOCKED: {ip:15} {request:20} (known malicious)")
            blocked_requests += 1
        else:
            # Track legitimate visitor
            unique_visitors.add(user_id)
            print(f"ALLOWED: {ip:15} {request:20}")
    
    print(f"\nUnique visitors: ~{unique_visitors.cardinality()}")
    print(f"Blocked requests: {blocked_requests}")
    print(f"Malicious IPs tracked: {malicious_ips.count}")
    print()


def example_memory_comparison():
    """Memory efficiency comparison."""
    print("=" * 60)
    print("Example 9: Memory Efficiency Comparison")
    print("=" * 60)
    
    # Scenario: Track 1 million unique user IDs
    
    # Traditional approach (storing in set)
    import sys
    traditional_set = set()
    for i in range(100000):
        traditional_set.add(f"user_{i}")
    
    set_memory_kb = sys.getsizeof(traditional_set) / 1024
    
    # HyperLogLog approach
    hll = HyperLogLog(precision=14)
    for i in range(100000):
        hll.add(f"user_{i}")
    
    hll_memory_kb = (hll.m * 8) / 1024  # 8 bits per register
    
    print("Tracking 100,000 unique users:")
    print(f"\nTraditional set:")
    print(f"  Memory: {set_memory_kb:.2f} KB")
    print(f"  Exact count: {len(traditional_set)}")
    
    print(f"\nHyperLogLog (precision=14):")
    print(f"  Memory: {hll_memory_kb:.2f} KB")
    print(f"  Estimated count: {hll.cardinality()}")
    print(f"  Error: {abs(hll.cardinality() - 100000) / 100000 * 100:.2f}%")
    print(f"  Memory savings: {(1 - hll_memory_kb / set_memory_kb) * 100:.1f}%")
    print()


def example_convenience_functions():
    """Using convenience functions."""
    print("=" * 60)
    print("Example 10: Convenience Functions")
    print("=" * 60)
    
    # estimate_cardinality - quick cardinality estimation
    items = [f"product_{i}" for i in range(1000)]
    items.extend([f"product_{i}" for i in range(500)])  # Add duplicates
    
    unique_count = estimate_cardinality(items, precision=12)
    print(f"Estimated unique products: {unique_count} (actual: 1000)")
    
    # create_bloom_filter - quick filter creation
    spam_list = ["spam1@bad.com", "spam2@bad.com", "phishing@scam.org"]
    spam_filter = create_bloom_filter(spam_list, error_rate=0.01)
    
    print(f"Created spam filter with {spam_filter.count} items")
    print(f"'spam1@bad.com' in filter: {spam_filter.contains('spam1@bad.com')}")
    print(f"'legit@good.com' in filter: {spam_filter.contains('legit@good.com')}")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("ALGESNAKE PHASE 3: APPROXIMATION ALGORITHMS")
    print("HyperLogLog & Bloom Filter Examples")
    print("=" * 60 + "\n")
    
    # HyperLogLog examples
    example_hyperloglog_basics()
    example_hyperloglog_streaming()
    example_hyperloglog_distributed()
    example_hyperloglog_sum()
    
    # Bloom Filter examples
    example_bloom_filter_basics()
    example_bloom_filter_web_crawler()
    example_bloom_filter_distributed()
    
    # Combined examples
    example_combined_analytics()
    example_memory_comparison()
    example_convenience_functions()
    
    print("=" * 60)
    print("All examples completed successfully!")
    print("=" * 60)
