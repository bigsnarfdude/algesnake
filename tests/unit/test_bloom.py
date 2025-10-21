"""Tests for Bloom Filter membership testing."""

import pytest
from algesnake.approximate import BloomFilter
from algesnake.approximate.bloom import create_bloom_filter, merge_bloom_filters


class TestBloomFilterBasics:
    """Tests for basic Bloom Filter functionality."""
    
    def test_initialization(self):
        """Test Bloom Filter initialization."""
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        
        assert bf.capacity == 1000
        assert bf.error_rate == 0.01
        assert bf.size > 0
        assert bf.num_hashes > 0
        assert bf.is_zero()
    
    def test_capacity_validation(self):
        """Test capacity parameter validation."""
        # Valid capacity
        bf = BloomFilter(capacity=100, error_rate=0.01)
        assert bf.capacity == 100
        
        # Invalid capacity
        with pytest.raises(ValueError, match="Capacity must be positive"):
            BloomFilter(capacity=0, error_rate=0.01)
        
        with pytest.raises(ValueError, match="Capacity must be positive"):
            BloomFilter(capacity=-10, error_rate=0.01)
    
    def test_error_rate_validation(self):
        """Test error rate parameter validation."""
        # Valid error rates
        for rate in [0.001, 0.01, 0.1, 0.5]:
            bf = BloomFilter(capacity=1000, error_rate=rate)
            assert bf.error_rate == rate
        
        # Invalid error rates
        with pytest.raises(ValueError, match="between 0 and 1"):
            BloomFilter(capacity=1000, error_rate=0.0)
        
        with pytest.raises(ValueError, match="between 0 and 1"):
            BloomFilter(capacity=1000, error_rate=1.0)
        
        with pytest.raises(ValueError, match="between 0 and 1"):
            BloomFilter(capacity=1000, error_rate=1.5)
    
    def test_add_and_contains(self):
        """Test adding items and checking membership."""
        bf = BloomFilter(capacity=100, error_rate=0.01)
        
        # Add items
        bf.add("apple")
        bf.add("banana")
        bf.add("cherry")
        
        # Check membership (should all be True)
        assert "apple" in bf
        assert "banana" in bf
        assert "cherry" in bf
        assert bf.contains("apple")
        
        # Items not added (should be False)
        assert "grape" not in bf
        assert "orange" not in bf
    
    def test_no_false_negatives(self):
        """Test that Bloom Filters never have false negatives."""
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        
        items = [f"item{i}" for i in range(500)]
        
        # Add all items
        for item in items:
            bf.add(item)
        
        # All added items must be found
        for item in items:
            assert item in bf, f"False negative for {item}"
    
    def test_zero_element(self):
        """Test monoid zero property."""
        bf = BloomFilter(capacity=100, error_rate=0.01)
        zero = bf.zero
        
        assert isinstance(zero, BloomFilter)
        assert zero.capacity == bf.capacity
        assert zero.error_rate == bf.error_rate
        assert zero.is_zero()
    
    def test_is_zero(self):
        """Test is_zero method."""
        bf = BloomFilter(capacity=100, error_rate=0.01)
        assert bf.is_zero()
        
        bf.add("item")
        assert not bf.is_zero()


class TestBloomFilterAccuracy:
    """Tests for Bloom Filter accuracy and false positive rate."""
    
    def test_false_positive_rate_small(self):
        """Test FPR with small dataset."""
        bf = BloomFilter(capacity=100, error_rate=0.01)
        
        # Add 50 items (well within capacity)
        added = set()
        for i in range(50):
            item = f"added_{i}"
            bf.add(item)
            added.add(item)
        
        # Test 1000 items not in the set
        false_positives = 0
        for i in range(1000):
            item = f"not_added_{i}"
            if item in bf:
                false_positives += 1
        
        fpr = false_positives / 1000
        
        # Should be close to target error rate (0.01)
        # Allow up to 3x the target rate for small sample
        assert fpr < 0.03, f"FPR {fpr:.4f} exceeds threshold"
    
    def test_false_positive_rate_at_capacity(self):
        """Test FPR when at capacity."""
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        
        # Add exactly capacity items
        for i in range(1000):
            bf.add(f"item{i}")
        
        # Test false positive rate
        false_positives = 0
        test_count = 10000
        
        for i in range(test_count):
            if f"test{i}" in bf:
                false_positives += 1
        
        fpr = false_positives / test_count
        
        # Should be close to target error rate
        assert fpr < 0.02, f"FPR {fpr:.4f} exceeds threshold"
    
    def test_saturation(self):
        """Test saturation calculation."""
        bf = BloomFilter(capacity=100, error_rate=0.01)
        
        # Empty filter should have 0 saturation
        assert bf.saturation() == 0.0
        
        # Add items
        for i in range(50):
            bf.add(f"item{i}")
        
        saturation = bf.saturation()
        
        # Saturation should be between 0 and 1
        assert 0.0 < saturation < 1.0
    
    def test_expected_fpr(self):
        """Test expected FPR calculation."""
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        
        # Add items
        for i in range(500):
            bf.add(i)
        
        expected_fpr = bf.expected_fpr()
        
        # Expected FPR should be reasonable
        assert 0.0 <= expected_fpr <= 1.0


class TestBloomFilterMonoid:
    """Tests for Bloom Filter monoid properties."""
    
    def test_combine_basic(self):
        """Test basic combine operation."""
        bf1 = BloomFilter(capacity=100, error_rate=0.01)
        bf2 = BloomFilter(capacity=100, error_rate=0.01)
        
        bf1.add("apple")
        bf1.add("banana")
        
        bf2.add("cherry")
        bf2.add("date")
        
        combined = bf1.combine(bf2)
        
        # All items should be in combined filter
        assert "apple" in combined
        assert "banana" in combined
        assert "cherry" in combined
        assert "date" in combined
    
    def test_combine_overlapping(self):
        """Test combining filters with overlapping elements."""
        bf1 = BloomFilter(capacity=100, error_rate=0.01)
        bf2 = BloomFilter(capacity=100, error_rate=0.01)
        
        # Both add "shared"
        bf1.add("shared")
        bf1.add("unique1")
        
        bf2.add("shared")
        bf2.add("unique2")
        
        combined = bf1.combine(bf2)
        
        # All items should be present
        assert "shared" in combined
        assert "unique1" in combined
        assert "unique2" in combined
    
    def test_operator_overload(self):
        """Test + operator for combine."""
        bf1 = BloomFilter(capacity=100, error_rate=0.01)
        bf2 = BloomFilter(capacity=100, error_rate=0.01)
        
        bf1.add("a")
        bf2.add("b")
        
        combined = bf1 + bf2
        
        assert isinstance(combined, BloomFilter)
        assert "a" in combined
        assert "b" in combined
    
    def test_sum_builtin(self):
        """Test sum() builtin support."""
        filters = [BloomFilter(capacity=100, error_rate=0.01) for _ in range(3)]
        
        filters[0].add("a")
        filters[1].add("b")
        filters[2].add("c")
        
        combined = sum(filters)
        
        assert "a" in combined
        assert "b" in combined
        assert "c" in combined
    
    def test_associativity(self):
        """Test (a + b) + c = a + (b + c)."""
        bf_a = BloomFilter(capacity=100, error_rate=0.01)
        bf_b = BloomFilter(capacity=100, error_rate=0.01)
        bf_c = BloomFilter(capacity=100, error_rate=0.01)
        
        bf_a.add("a")
        bf_b.add("b")
        bf_c.add("c")
        
        left = (bf_a + bf_b) + bf_c
        right = bf_a + (bf_b + bf_c)
        
        # Both should contain all items
        for item in ["a", "b", "c"]:
            assert item in left
            assert item in right
        
        # Bit arrays should be identical
        assert left == right
    
    def test_identity(self):
        """Test zero + a = a + zero = a."""
        bf = BloomFilter(capacity=100, error_rate=0.01)
        bf.add("item1")
        bf.add("item2")
        
        zero = bf.zero
        
        left = zero + bf
        right = bf + zero
        
        # All should contain the same items
        for item in ["item1", "item2"]:
            assert item in left
            assert item in right
    
    def test_combine_different_sizes_fails(self):
        """Test that combining different sizes raises error."""
        bf1 = BloomFilter(capacity=100, error_rate=0.01)
        bf2 = BloomFilter(capacity=200, error_rate=0.01)
        
        with pytest.raises(ValueError, match="different sizes"):
            bf1.combine(bf2)
    
    def test_combine_different_hash_counts_fails(self):
        """Test that combining different hash counts raises error."""
        bf1 = BloomFilter(capacity=100, error_rate=0.01)
        bf2 = BloomFilter(capacity=100, error_rate=0.001)  # Different error rate

        # Different error rates lead to different sizes (and hash counts)
        # The error will be caught by size check first
        with pytest.raises(ValueError, match="different sizes|different hash counts"):
            bf1.combine(bf2)


class TestBloomFilterHelpers:
    """Tests for helper functions."""
    
    def test_create_bloom_filter(self):
        """Test create_bloom_filter convenience function."""
        items = ["apple", "banana", "cherry"]
        
        bf = create_bloom_filter(items, capacity=10, error_rate=0.01)
        
        for item in items:
            assert item in bf
    
    def test_create_bloom_filter_default_capacity(self):
        """Test create_bloom_filter with default capacity."""
        items = ["a", "b", "c", "d", "e"]
        
        bf = create_bloom_filter(items, error_rate=0.01)
        
        # Should use len(items) as capacity
        assert bf.capacity >= len(items)
        
        for item in items:
            assert item in bf
    
    def test_merge_bloom_filters(self):
        """Test merge_bloom_filters convenience function."""
        filters = [BloomFilter(capacity=100, error_rate=0.01) for _ in range(3)]
        
        filters[0].add("a")
        filters[1].add("b")
        filters[2].add("c")
        
        merged = merge_bloom_filters(filters)
        
        assert "a" in merged
        assert "b" in merged
        assert "c" in merged
    
    def test_merge_bloom_filters_empty_list_fails(self):
        """Test that merging empty list raises error."""
        with pytest.raises(ValueError, match="empty list"):
            merge_bloom_filters([])


class TestBloomFilterEdgeCases:
    """Tests for edge cases."""
    
    def test_single_item(self):
        """Test Bloom Filter with single item."""
        bf = BloomFilter(capacity=10, error_rate=0.01)
        bf.add("single")
        
        assert "single" in bf
    
    def test_integer_items(self):
        """Test Bloom Filter with integer items."""
        bf = BloomFilter(capacity=100, error_rate=0.01)
        
        for i in range(50):
            bf.add(i)
        
        for i in range(50):
            assert i in bf
    
    def test_string_items(self):
        """Test Bloom Filter with string items."""
        bf = BloomFilter(capacity=100, error_rate=0.01)
        
        items = [f"user_{i}" for i in range(50)]
        
        for item in items:
            bf.add(item)
        
        for item in items:
            assert item in bf
    
    def test_tuple_items(self):
        """Test Bloom Filter with tuple items."""
        bf = BloomFilter(capacity=100, error_rate=0.01)
        
        items = [(i, i*2) for i in range(50)]
        
        for item in items:
            bf.add(item)
        
        for item in items:
            assert item in bf
    
    def test_equality(self):
        """Test equality operator."""
        bf1 = BloomFilter(capacity=100, error_rate=0.01)
        bf2 = BloomFilter(capacity=100, error_rate=0.01)
        
        # Empty filters should be equal
        assert bf1 == bf2
        
        # Add same items
        for i in range(10):
            bf1.add(i)
            bf2.add(i)
        
        assert bf1 == bf2
        
        # Different items should not be equal
        bf3 = BloomFilter(capacity=100, error_rate=0.01)
        bf3.add(999)
        
        assert bf1 != bf3
    
    def test_repr(self):
        """Test string representation."""
        bf = BloomFilter(capacity=1000, error_rate=0.01)
        repr_str = repr(bf)
        
        assert "BloomFilter" in repr_str
        assert "capacity=1000" in repr_str
        assert "error_rate=0.01" in repr_str


class TestBloomFilterRealWorld:
    """Real-world use case tests."""
    
    def test_spam_filter_scenario(self):
        """Test spam email filtering scenario."""
        # Create blocklist
        spam_filter = BloomFilter(capacity=10000, error_rate=0.001)
        
        # Add known spam emails
        spam_emails = [f"spam{i}@bad.com" for i in range(5000)]
        for email in spam_emails:
            spam_filter.add(email)
        
        # Check known spam (should all be caught)
        for email in spam_emails[:100]:  # Test subset
            assert email in spam_filter
        
        # Check legitimate emails (should mostly pass)
        legit_emails = [f"user{i}@good.com" for i in range(1000)]
        false_positives = sum(1 for email in legit_emails if email in spam_filter)
        
        fpr = false_positives / len(legit_emails)
        
        # Should be close to target error rate (0.001)
        assert fpr < 0.005, f"Too many false positives: {fpr:.4f}"
    
    def test_distributed_blocklist_merge(self):
        """Test merging blocklists from different servers."""
        # Three servers maintain separate blocklists
        server1 = BloomFilter(capacity=1000, error_rate=0.01)
        server2 = BloomFilter(capacity=1000, error_rate=0.01)
        server3 = BloomFilter(capacity=1000, error_rate=0.01)
        
        # Each server adds different spam IPs
        for i in range(100):
            server1.add(f"192.168.1.{i}")
        
        for i in range(100, 200):
            server2.add(f"192.168.1.{i}")
        
        for i in range(200, 300):
            server3.add(f"192.168.1.{i}")
        
        # Merge all blocklists
        global_blocklist = server1 + server2 + server3
        
        # All IPs should be in global blocklist
        for i in range(300):
            assert f"192.168.1.{i}" in global_blocklist
    
    def test_web_crawler_duplicate_detection(self):
        """Test duplicate URL detection in web crawler."""
        visited = BloomFilter(capacity=100000, error_rate=0.01)
        
        # Simulate crawling URLs
        urls = [f"https://example.com/page{i}" for i in range(10000)]
        
        # Add all URLs
        for url in urls:
            visited.add(url)
        
        # Check that all URLs are marked as visited
        for url in urls[:1000]:  # Test subset
            assert url in visited
        
        # New URL should not be in filter
        new_url = "https://example.com/new_page"
        assert new_url not in visited
