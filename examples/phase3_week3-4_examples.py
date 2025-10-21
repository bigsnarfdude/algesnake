"""Phase 3 Week 3-4: CountMinSketch & TopK - Examples

This file demonstrates CountMinSketch and TopK usage for:
- Frequency estimation with minimal memory
- Finding heavy hitters and trending items
- Real-world analytics scenarios
"""

from algesnake.approximate import CountMinSketch, TopK
from algesnake.approximate.countminsketch import count_frequencies, heavy_hitters
from algesnake.approximate.topk import find_top_k, streaming_top_k
import random


def example_cms_basics():
    """Basic CountMinSketch usage."""
    print("=" * 60)
    print("Example 1: CountMinSketch Basics")
    print("=" * 60)
    
    # Create CMS with width=1000, depth=5
    cms = CountMinSketch(width=1000, depth=5)
    
    # Add word frequencies
    text = "the quick brown fox jumps over the lazy dog the fox was quick".split()
    
    for word in text:
        cms.add(word)
    
    # Query frequencies
    print(f"Added {len(text)} words")
    print(f"\nWord frequencies (estimates >= actual):")
    for word in ["the", "quick", "fox", "brown"]:
        print(f"  '{word}': {cms.estimate(word)}")
    
    print(f"\nTotal count tracked: {cms.total_count}")
    print()


def example_cms_from_error_rate():
    """Creating CMS from desired error bounds."""
    print("=" * 60)
    print("Example 2: CMS with Error Rate Configuration")
    print("=" * 60)
    
    # Create CMS with 1% error, 99% confidence
    cms = CountMinSketch.from_error_rate(epsilon=0.01, delta=0.01)
    
    print(f"Created CMS with:")
    print(f"  Width: {cms.width} (for 1% error)")
    print(f"  Depth: {cms.depth} (for 99% confidence)")
    print(f"  Memory: ~{(cms.width * cms.depth * 8) / 1024:.2f} KB")
    
    # Add 10,000 items
    for i in range(10000):
        cms.add(f"item_{i % 100}")  # 100 unique items, 100 occurrences each
    
    # Check accuracy
    actual = 100
    estimated = cms.estimate("item_0")
    error = abs(estimated - actual) / actual
    
    print(f"\nAccuracy test:")
    print(f"  Actual frequency: {actual}")
    print(f"  Estimated frequency: {estimated}")
    print(f"  Error: {error * 100:.2f}%")
    print()


def example_cms_word_counting():
    """Word frequency counting in large text."""
    print("=" * 60)
    print("Example 3: Word Frequency Counting")
    print("=" * 60)
    
    # Simulate large corpus
    common_words = ["the", "a", "an", "is", "was", "in", "on", "at"]
    rare_words = ["algorithm", "datastructure", "optimization"]
    
    words = []
    for _ in range(10000):
        if random.random() < 0.8:
            words.append(random.choice(common_words))
        else:
            words.append(random.choice(rare_words))
    
    # Count with CMS
    cms = count_frequencies(words, width=1000, depth=5)
    
    print(f"Processed {len(words)} words")
    print(f"\nCommon words:")
    for word in common_words[:4]:
        print(f"  '{word}': ~{cms.estimate(word)} occurrences")
    
    print(f"\nRare words:")
    for word in rare_words[:3]:
        print(f"  '{word}': ~{cms.estimate(word)} occurrences")
    print()


def example_cms_distributed():
    """Distributed log analysis with CMS."""
    print("=" * 60)
    print("Example 4: Distributed Log Analysis")
    print("=" * 60)
    
    # Simulate 3 servers with logs
    server1 = CountMinSketch(width=1000, depth=5)
    server2 = CountMinSketch(width=1000, depth=5)
    server3 = CountMinSketch(width=1000, depth=5)
    
    # Server 1: errors
    for _ in range(100):
        server1.add("ERROR: Connection timeout")
    for _ in range(50):
        server1.add("ERROR: Out of memory")
    
    # Server 2: errors
    for _ in range(75):
        server2.add("ERROR: Connection timeout")
    for _ in range(30):
        server2.add("ERROR: File not found")
    
    # Server 3: errors
    for _ in range(50):
        server3.add("ERROR: Connection timeout")
    for _ in range(20):
        server3.add("ERROR: Permission denied")
    
    # Merge using monoid operation
    global_cms = server1 + server2 + server3
    
    print("Per-server error counts:")
    print(f"  Server 1: ~{server1.total_count} total errors")
    print(f"  Server 2: ~{server2.total_count} total errors")
    print(f"  Server 3: ~{server3.total_count} total errors")
    
    print(f"\nGlobal error frequencies:")
    errors = [
        "ERROR: Connection timeout",
        "ERROR: Out of memory",
        "ERROR: File not found",
        "ERROR: Permission denied"
    ]
    
    for error in errors:
        freq = global_cms.estimate(error)
        print(f"  {error}: ~{freq}")
    
    print()


def example_cms_heavy_hitters():
    """Finding heavy hitters in data streams."""
    print("=" * 60)
    print("Example 5: Finding Heavy Hitters")
    print("=" * 60)
    
    cms = CountMinSketch(width=2000, depth=7)
    
    # Simulate IP address requests
    # Some IPs have high traffic (DDoS-like)
    ips = {
        "10.0.0.1": 1000,  # Suspicious
        "10.0.0.2": 800,   # Suspicious
        "192.168.1.1": 50,
        "192.168.1.2": 40,
        "192.168.1.3": 30,
    }
    
    for ip, count in ips.items():
        cms.add(ip, count=count)
    
    # Find heavy hitters (threshold = 100 requests)
    hitters = heavy_hitters(cms, list(ips.keys()), threshold=100)
    
    print(f"Total IPs tracked: {len(ips)}")
    print(f"\nHeavy hitters (>100 requests):")
    for ip, freq in hitters:
        print(f"  {ip}: ~{freq} requests ⚠️")
    
    print(f"\nNormal traffic:")
    for ip in ["192.168.1.1", "192.168.1.2"]:
        print(f"  {ip}: ~{cms.estimate(ip)} requests")
    print()


def example_topk_basics():
    """Basic TopK usage."""
    print("=" * 60)
    print("Example 6: TopK Basics")
    print("=" * 60)
    
    # Track top 5 items
    topk = TopK(k=5)
    
    # Add items
    items = ["apple"]*10 + ["banana"]*7 + ["cherry"]*5 + ["date"]*3 + ["elderberry"]*2 + ["fig"]*1
    
    for item in items:
        topk.add(item)
    
    print(f"Added {len(items)} items ({len(set(items))} unique)")
    
    print(f"\nTop 5 items:")
    for i, (item, freq) in enumerate(topk.top(), 1):
        print(f"  {i}. {item}: {freq}")
    
    print()


def example_topk_trending():
    """Tracking trending hashtags."""
    print("=" * 60)
    print("Example 7: Trending Hashtags")
    print("=" * 60)
    
    topk = TopK(k=3)
    
    # Simulate tweet stream
    tweets = [
        "#python", "#ai", "#machinelearning",
        "#python", "#datascience", "#python",
        "#ai", "#python", "#coding",
        "#python", "#ai", "#machinelearning",
        "#python", "#datascience", "#ai"
    ]
    
    print("Processing tweet stream...")
    for hashtag in tweets:
        topk.add(hashtag)
    
    print(f"\nTop 3 trending hashtags:")
    for rank, (tag, count) in enumerate(topk.top(), 1):
        bar = "█" * (count * 2)
        print(f"  {rank}. {tag:20} {bar} ({count})")
    
    print()


def example_topk_streaming():
    """Streaming data processing with TopK."""
    print("=" * 60)
    print("Example 8: Streaming Analytics")
    print("=" * 60)
    
    topk = TopK(k=5)
    
    # Simulate streaming batches
    batches = [
        ["page_a", "page_b", "page_a", "page_c"],
        ["page_a", "page_d", "page_b", "page_a"],
        ["page_c", "page_a", "page_e", "page_b"],
        ["page_a", "page_f", "page_c", "page_a"]
    ]
    
    print("Processing streaming batches:")
    for i, batch in enumerate(batches, 1):
        for page in batch:
            topk.add(page)
        
        print(f"\n  After batch {i}:")
        for page, views in topk.top(n=3):
            print(f"    {page}: {views} views")
    
    print()


def example_topk_distributed():
    """Distributed TopK aggregation."""
    print("=" * 60)
    print("Example 9: Distributed Page View Tracking")
    print("=" * 60)
    
    # Three web servers tracking page views
    server1 = TopK(k=5)
    server2 = TopK(k=5)
    server3 = TopK(k=5)
    
    # Server 1 traffic
    for _ in range(100):
        server1.add("/home")
    for _ in range(50):
        server1.add("/products")
    
    # Server 2 traffic
    for _ in range(75):
        server2.add("/home")
    for _ in range(60):
        server2.add("/about")
    
    # Server 3 traffic
    for _ in range(50):
        server3.add("/products")
    for _ in range(40):
        server3.add("/contact")
    
    # Merge using monoid operation
    global_topk = server1 + server2 + server3
    
    print("Per-server top pages:")
    print(f"\n  Server 1:")
    for page, views in server1.top(n=2):
        print(f"    {page}: {views} views")
    
    print(f"\n  Server 2:")
    for page, views in server2.top(n=2):
        print(f"    {page}: {views} views")
    
    print(f"\n  Server 3:")
    for page, views in server3.top(n=2):
        print(f"    {page}: {views} views")
    
    print(f"\n  Global top pages:")
    for rank, (page, views) in enumerate(global_topk.top(), 1):
        print(f"    {rank}. {page}: {views} total views")
    
    print()


def example_topk_error_analysis():
    """Error log analysis with TopK."""
    print("=" * 60)
    print("Example 10: Error Log Analysis")
    print("=" * 60)
    
    topk = TopK(k=5)
    
    # Simulate error logs
    errors = {
        "ConnectionTimeout": 150,
        "NullPointerException": 80,
        "OutOfMemory": 120,
        "FileNotFound": 30,
        "PermissionDenied": 20,
        "InvalidInput": 10,
        "DatabaseError": 5
    }
    
    for error, count in errors.items():
        topk.add(error, count=count)
    
    print(f"Total errors tracked: {sum(errors.values())}")
    print(f"\nTop 5 errors by frequency:")
    
    for rank, (error, freq) in enumerate(topk.top(), 1):
        percentage = (freq / sum(errors.values())) * 100
        print(f"  {rank}. {error:25} {freq:4} ({percentage:.1f}%)")
    
    print(f"\nFocus areas for fixing:")
    for error, freq in topk.top(n=3):
        print(f"  → {error}")
    
    print()


def example_combined_cms_topk():
    """Combining CMS and TopK for analytics."""
    print("=" * 60)
    print("Example 11: Combined CMS + TopK Analytics")
    print("=" * 60)
    
    # Use CMS for frequency estimation
    cms = CountMinSketch(width=1000, depth=5)
    
    # Use TopK to track top items
    topk = TopK(k=10)
    
    # Simulate user activity
    users = [f"user_{i}" for i in range(100)]
    activities = []
    
    # Generate activity stream
    for _ in range(1000):
        user = random.choice(users)
        activities.append(user)
        cms.add(user)
        topk.add(user)
    
    print(f"Processed {len(activities)} activities")
    print(f"Unique users: {len(users)}")
    
    print(f"\nTop 5 most active users:")
    for rank, (user, activity_count) in enumerate(topk.top(n=5), 1):
        print(f"  {rank}. {user}: ~{activity_count} activities")
    
    # Verify with CMS
    print(f"\nCMS verification for top user:")
    top_user = topk.top(n=1)[0][0]
    print(f"  {top_user}: CMS estimate = {cms.estimate(top_user)}")
    
    print()


def example_memory_comparison():
    """Memory efficiency comparison."""
    print("=" * 60)
    print("Example 12: Memory Efficiency")
    print("=" * 60)
    
    # Scenario: Track 10,000 word frequencies
    
    # Traditional approach: dictionary
    traditional_dict = {}
    for i in range(10000):
        word = f"word_{i % 1000}"  # 1000 unique words
        traditional_dict[word] = traditional_dict.get(word, 0) + 1
    
    import sys
    dict_memory_kb = sys.getsizeof(traditional_dict) / 1024
    
    # CMS approach
    cms = CountMinSketch(width=1000, depth=5)
    for i in range(10000):
        cms.add(f"word_{i % 1000}")
    
    cms_memory_kb = (cms.width * cms.depth * 8) / 1024  # 8 bytes per counter
    
    # TopK approach
    topk = TopK(k=100)
    for i in range(10000):
        topk.add(f"word_{i % 1000}")
    
    topk_memory_kb = len(topk.frequencies) * 16 / 1024  # Rough estimate
    
    print("Tracking 10,000 items (1000 unique):")
    
    print(f"\nTraditional dictionary:")
    print(f"  Memory: {dict_memory_kb:.2f} KB")
    print(f"  Exact counts: ✓")
    
    print(f"\nCountMinSketch:")
    print(f"  Memory: {cms_memory_kb:.2f} KB")
    print(f"  Exact counts: ✗ (estimates)")
    print(f"  Memory savings: {(1 - cms_memory_kb / dict_memory_kb) * 100:.1f}%")
    
    print(f"\nTopK (k=100):")
    print(f"  Memory: {topk_memory_kb:.2f} KB")
    print(f"  Tracks: Top 100 items only")
    print(f"  Memory savings: {(1 - topk_memory_kb / dict_memory_kb) * 100:.1f}%")
    
    print()


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("ALGESNAKE PHASE 3 WEEK 3-4")
    print("CountMinSketch & TopK Examples")
    print("=" * 60 + "\n")
    
    # CountMinSketch examples
    example_cms_basics()
    example_cms_from_error_rate()
    example_cms_word_counting()
    example_cms_distributed()
    example_cms_heavy_hitters()
    
    # TopK examples
    example_topk_basics()
    example_topk_trending()
    example_topk_streaming()
    example_topk_distributed()
    example_topk_error_analysis()
    
    # Combined examples
    example_combined_cms_topk()
    example_memory_comparison()
    
    print("=" * 60)
    print("All examples completed successfully!")
    print("=" * 60)
