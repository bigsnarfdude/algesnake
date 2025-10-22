"""
Integration tests for approximate algorithms and probabilistic data structures.

Tests combining multiple approximate structures and real-world scenarios.
"""

import pytest
from algesnake.approximate.hyperloglog import HyperLogLog
from algesnake.approximate.bloom import BloomFilter
from algesnake.approximate.countminsketch import CountMinSketch
from algesnake.approximate.topk import TopK
from algesnake.approximate.tdigest import TDigest


class TestDistributedCardinality:
    """Test HyperLogLog for distributed cardinality estimation."""

    def test_unique_users_across_datacenters(self):
        """Estimate unique users across multiple datacenters."""
        # Datacenter 1: Users in region 1
        dc1 = HyperLogLog(precision=12)
        for user_id in range(1000):
            dc1.add(f"user_{user_id}")

        # Datacenter 2: Users in region 2 (some overlap)
        dc2 = HyperLogLog(precision=12)
        for user_id in range(500, 1500):
            dc2.add(f"user_{user_id}")

        # Datacenter 3: Users in region 3 (some overlap)
        dc3 = HyperLogLog(precision=12)
        for user_id in range(1000, 2000):
            dc3.add(f"user_{user_id}")

        # Combine across datacenters (monoid merge)
        global_hll = dc1 + dc2 + dc3

        # True unique count: 0-1999 = 2000 users
        # HyperLogLog should be within ~2% error
        estimated = global_hll.cardinality()
        true_count = 2000
        error_rate = abs(estimated - true_count) / true_count

        assert error_rate < 0.05  # Within 5% error

    def test_streaming_cardinality(self):
        """Test incremental cardinality estimation in streaming scenario."""
        hll = HyperLogLog(precision=14)

        # Stream 1: First batch of events
        for i in range(5000):
            hll.add(f"event_{i}")

        checkpoint1 = hll.cardinality()
        assert 4500 < checkpoint1 < 5500  # ~5000 with error

        # Stream 2: More events (some duplicates)
        for i in range(3000, 8000):
            hll.add(f"event_{i}")

        checkpoint2 = hll.cardinality()
        # True unique: 8000 (0-7999)
        assert 7500 < checkpoint2 < 8500

    def test_hll_merge_associativity(self):
        """Test that HLL merges are associative (monoid property)."""
        hll1 = HyperLogLog(precision=10)
        hll2 = HyperLogLog(precision=10)
        hll3 = HyperLogLog(precision=10)

        for i in range(100):
            hll1.add(f"a_{i}")
        for i in range(100):
            hll2.add(f"b_{i}")
        for i in range(100):
            hll3.add(f"c_{i}")

        # Different merge orders
        left = (hll1 + hll2) + hll3
        right = hll1 + (hll2 + hll3)

        # Should get same cardinality estimate
        assert abs(left.cardinality() - right.cardinality()) < 5


class TestBloomFilterIntegration:
    """Test Bloom Filter integration scenarios."""

    def test_deduplication_pipeline(self):
        """Test using Bloom Filter for deduplication in a pipeline."""
        bf = BloomFilter(capacity=10000, error_rate=0.01)

        # Simulate event stream with duplicates
        events_seen = []
        unique_count = 0

        event_stream = ['a', 'b', 'c', 'a', 'd', 'b', 'e', 'f', 'a', 'g']

        for event in event_stream:
            if not bf.contains(event):
                # New event (probably)
                bf.add(event)
                events_seen.append(event)
                unique_count += 1

        # Should catch most duplicates
        # True unique: 7 (a, b, c, d, e, f, g)
        assert 6 <= unique_count <= 7  # Might have false positives

    def test_distributed_membership_check(self):
        """Test combining Bloom Filters from multiple sources."""
        # Cache 1: Items in server 1 cache
        cache1 = BloomFilter(capacity=1000, error_rate=0.01)
        for i in range(500):
            cache1.add(f"item_{i}")

        # Cache 2: Items in server 2 cache
        cache2 = BloomFilter(capacity=1000, error_rate=0.01)
        for i in range(300, 800):
            cache2.add(f"item_{i}")

        # Merge caches (monoid combine)
        combined_cache = cache1 + cache2

        # Check membership
        assert combined_cache.contains("item_100")  # In cache1
        assert combined_cache.contains("item_700")  # In cache2
        assert combined_cache.contains("item_400")  # In both
        assert not combined_cache.contains("item_900")  # Not in either (probably)


class TestTopKAndCountMinSketch:
    """Test TopK and CountMinSketch working together."""

    def test_heavy_hitters_detection(self):
        """Use TopK and CountMinSketch together to find heavy hitters."""
        topk = TopK(k=5)
        cms = CountMinSketch(width=1000, depth=5)

        # Simulate event stream with zipf distribution
        events = (
            ['user_1'] * 100 +  # Heavy hitter
            ['user_2'] * 80 +   # Heavy hitter
            ['user_3'] * 60 +   # Heavy hitter
            ['user_4'] * 40 +   # Medium
            ['user_5'] * 30 +   # Medium
            ['user_6'] * 20 +
            ['user_7'] * 15 +
            ['user_8'] * 10 +
            ['user_9'] * 5 +
            ['user_10'] * 2
        )

        # Process stream
        for event in events:
            topk.add(event)
            cms.add(event)

        # Get top 5 items
        top_items = topk.top()

        # Verify top items
        top_keys = [item for item, _ in top_items]
        assert 'user_1' in top_keys
        assert 'user_2' in top_keys
        assert 'user_3' in top_keys

        # Verify counts with CountMinSketch
        for item, count in top_items:
            estimated = cms.estimate(item)
            # CountMinSketch never underestimates
            assert estimated >= count

    def test_distributed_topk_merge(self):
        """Test merging TopK structures from multiple partitions."""
        # Partition 1
        topk1 = TopK(k=3)
        for _ in range(50):
            topk1.add('a')
        for _ in range(30):
            topk1.add('b')
        for _ in range(20):
            topk1.add('c')

        # Partition 2
        topk2 = TopK(k=3)
        for _ in range(40):
            topk2.add('b')
        for _ in range(25):
            topk2.add('d')
        for _ in range(15):
            topk2.add('a')

        # Merge partitions
        merged = topk1 + topk2

        # Top items should be: a(65), b(70), d(25), c(20)
        top_items = merged.top()
        top_keys = [item for item, _ in top_items]

        assert 'a' in top_keys  # 65 total
        assert 'b' in top_keys  # 70 total


class TestTDigestIntegration:
    """Test TDigest for percentile estimation."""

    def test_latency_monitoring_across_services(self):
        """Monitor latency percentiles across multiple services."""
        # Service 1 latencies (ms)
        service1 = TDigest(compression=100)
        for latency in [10, 12, 15, 11, 14, 13, 16, 12, 11, 15]:
            service1.add(latency)

        # Service 2 latencies (ms)
        service2 = TDigest(compression=100)
        for latency in [20, 22, 25, 21, 24, 23, 26, 22, 21, 25]:
            service2.add(latency)

        # Service 3 latencies (ms)
        service3 = TDigest(compression=100)
        for latency in [30, 32, 35, 31, 34, 33, 36, 32, 31, 35]:
            service3.add(latency)

        # Merge all services (monoid combine)
        global_latency = service1 + service2 + service3

        # Get global percentiles
        p50 = global_latency.quantile(0.5)
        p95 = global_latency.quantile(0.95)
        p99 = global_latency.quantile(0.99)

        # Verify approximate ranges
        assert 15 < p50 < 25  # Median around 20-ish
        assert p95 > 30  # 95th percentile in upper range
        assert p99 > p95  # 99th higher than 95th

    def test_streaming_percentile_estimation(self):
        """Test percentile estimation on streaming data."""
        td = TDigest(compression=100)

        # Stream data in batches
        batch1 = list(range(0, 1000, 10))  # 0, 10, 20, ..., 990
        for value in batch1:
            td.add(value)

        # Check percentiles after first batch
        p50_batch1 = td.quantile(0.5)
        assert 400 < p50_batch1 < 600  # Around 500

        # Stream more data
        batch2 = list(range(1000, 2000, 10))
        for value in batch2:
            td.add(value)

        # Percentiles should shift
        p50_batch2 = td.quantile(0.5)
        assert 900 < p50_batch2 < 1100  # Around 1000


class TestCombinedWorkflow:
    """Test combining multiple approximate structures in a workflow."""

    def test_realtime_analytics_dashboard(self):
        """Simulate real-time analytics with multiple structures."""
        # Initialize structures
        unique_visitors = HyperLogLog(precision=12)
        seen_items = BloomFilter(capacity=10000, error_rate=0.01)
        popular_items = TopK(k=10)
        latencies = TDigest(compression=100)

        # Simulate event stream
        events = [
            {'user': 'alice', 'item': 'product_1', 'latency': 15},
            {'user': 'bob', 'item': 'product_2', 'latency': 20},
            {'user': 'alice', 'item': 'product_1', 'latency': 12},  # Duplicate user
            {'user': 'charlie', 'item': 'product_3', 'latency': 18},
            {'user': 'bob', 'item': 'product_1', 'latency': 25},
            {'user': 'david', 'item': 'product_2', 'latency': 22},
            {'user': 'eve', 'item': 'product_1', 'latency': 16},
        ]

        for event in events:
            # Track unique visitors
            unique_visitors.add(event['user'])

            # Track seen items
            seen_items.add(event['item'])

            # Track popular items
            popular_items.add(event['item'])

            # Track latencies
            latencies.add(event['latency'])

        # Analytics results
        unique_count = unique_visitors.cardinality()
        assert 4 < unique_count < 6  # 5 unique users

        assert seen_items.contains('product_1')
        assert seen_items.contains('product_2')
        assert seen_items.contains('product_3')

        top_products = popular_items.top()
        top_product = top_products[0][0]
        assert top_product == 'product_1'  # Appears 3 times

        p50_latency = latencies.quantile(0.5)
        assert 15 < p50_latency < 20  # Median latency

    def test_distributed_merge_all_structures(self):
        """Test merging all structures from multiple nodes."""
        # Node 1
        hll1 = HyperLogLog(precision=10)
        bf1 = BloomFilter(capacity=100, error_rate=0.01)
        topk1 = TopK(k=3)
        td1 = TDigest(compression=50)

        for i in range(50):
            hll1.add(f"user_{i}")
            bf1.add(f"item_{i}")
            topk1.add(f"product_{i % 5}")
            td1.add(float(i))

        # Node 2
        hll2 = HyperLogLog(precision=10)
        bf2 = BloomFilter(capacity=100, error_rate=0.01)
        topk2 = TopK(k=3)
        td2 = TDigest(compression=50)

        for i in range(30, 80):
            hll2.add(f"user_{i}")
            bf2.add(f"item_{i}")
            topk2.add(f"product_{i % 5}")
            td2.add(float(i))

        # Merge all structures (monoid combines)
        hll_merged = hll1 + hll2
        bf_merged = bf1 + bf2
        topk_merged = topk1 + topk2
        td_merged = td1 + td2

        # Verify merges worked
        assert hll_merged.cardinality() > 50  # At least 50 unique
        assert bf_merged.contains("item_10")
        assert bf_merged.contains("item_70")
        assert len(topk_merged.top()) > 0
        assert td_merged.quantile(0.5) > 0


class TestMonoidProperties:
    """Test that all approximate structures satisfy monoid properties."""

    def test_hll_monoid_properties(self):
        """Test HyperLogLog satisfies monoid laws."""
        hll1 = HyperLogLog(precision=10)
        hll2 = HyperLogLog(precision=10)
        hll3 = HyperLogLog(precision=10)

        hll1.add("a")
        hll2.add("b")
        hll3.add("c")

        # Associativity: (a + b) + c = a + (b + c)
        left = (hll1 + hll2) + hll3
        right = hll1 + (hll2 + hll3)
        assert abs(left.cardinality() - right.cardinality()) < 1

        # Identity: zero + a = a
        zero = HyperLogLog(precision=10)
        identity_check = zero + hll1
        assert abs(identity_check.cardinality() - hll1.cardinality()) < 1

    def test_bloom_filter_monoid_properties(self):
        """Test BloomFilter satisfies monoid laws."""
        bf1 = BloomFilter(capacity=100, error_rate=0.01)
        bf2 = BloomFilter(capacity=100, error_rate=0.01)
        bf3 = BloomFilter(capacity=100, error_rate=0.01)

        bf1.add("a")
        bf2.add("b")
        bf3.add("c")

        # Associativity
        left = (bf1 + bf2) + bf3
        right = bf1 + (bf2 + bf3)

        assert left.contains("a")
        assert left.contains("b")
        assert left.contains("c")
        assert right.contains("a")
        assert right.contains("b")
        assert right.contains("c")

    def test_tdigest_monoid_properties(self):
        """Test TDigest satisfies monoid laws."""
        td1 = TDigest(compression=50)
        td2 = TDigest(compression=50)
        td3 = TDigest(compression=50)

        td1.add(10.0)
        td2.add(20.0)
        td3.add(30.0)

        # Associativity: (a + b) + c ≈ a + (b + c)
        left = (td1 + td2) + td3
        right = td1 + (td2 + td3)

        # Medians should be close
        assert abs(left.quantile(0.5) - right.quantile(0.5)) < 5

        # Identity: zero + a ≈ a
        zero = TDigest(compression=50)
        identity_check = zero + td1
        assert abs(identity_check.quantile(0.5) - td1.quantile(0.5)) < 1
