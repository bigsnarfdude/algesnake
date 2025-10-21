"""Tests for Count-Min Sketch frequency estimator."""

import pytest
from algesnake.approximate import CountMinSketch
from algesnake.approximate.countminsketch import (
    count_frequencies, merge_cms, heavy_hitters
)


class TestCountMinSketchBasics:
    """Tests for basic CMS functionality."""
    
    def test_initialization(self):
        """Test CMS initialization."""
        cms = CountMinSketch(width=1000, depth=5)
        
        assert cms.width == 1000
        assert cms.depth == 5
        assert len(cms.counters) == 5
        assert all(len(row) == 1000 for row in cms.counters)
        assert cms.is_zero()
    
    def test_initialization_validation(self):
        """Test parameter validation."""
        # Valid parameters
        cms = CountMinSketch(width=100, depth=4)
        assert cms.width == 100
        
        # Invalid width
        with pytest.raises(ValueError, match="Width must be positive"):
            CountMinSketch(width=0, depth=5)
        
        # Invalid depth
        with pytest.raises(ValueError, match="Depth must be positive"):
            CountMinSketch(width=100, depth=0)
    
    def test_from_error_rate(self):
        """Test CMS creation from error parameters."""
        cms = CountMinSketch.from_error_rate(epsilon=0.01, delta=0.01)
        
        # Should have reasonable dimensions
        assert cms.width > 0
        assert cms.depth > 0
        # width ≈ e/ε ≈ 2.71/0.01 ≈ 271
        assert 200 <= cms.width <= 400
        # depth ≈ ln(1/δ) ≈ ln(100) ≈ 4.6
        assert 4 <= cms.depth <= 6
    
    def test_add_single_item(self):
        """Test adding single item."""
        cms = CountMinSketch(width=100, depth=5)
        
        cms.add("apple")
        
        # Should estimate at least 1
        assert cms.estimate("apple") >= 1
        assert not cms.is_zero()
    
    def test_add_multiple_counts(self):
        """Test adding item with count."""
        cms = CountMinSketch(width=100, depth=5)
        
        cms.add("apple", count=10)
        
        # Should estimate at least 10
        assert cms.estimate("apple") >= 10
    
    def test_estimate_not_added(self):
        """Test estimating item not added."""
        cms = CountMinSketch(width=100, depth=5)
        
        cms.add("apple")
        
        # Item not added might have non-zero estimate (collision)
        # but should be much less than added item
        apple_est = cms.estimate("apple")
        banana_est = cms.estimate("banana")
        
        assert apple_est >= 1
        assert banana_est < apple_est
    
    def test_zero_element(self):
        """Test monoid zero property."""
        cms = CountMinSketch(width=100, depth=5)
        zero = cms.zero
        
        assert isinstance(zero, CountMinSketch)
        assert zero.width == cms.width
        assert zero.depth == cms.depth
        assert zero.is_zero()
    
    def test_is_zero(self):
        """Test is_zero method."""
        cms = CountMinSketch(width=100, depth=5)
        assert cms.is_zero()
        
        cms.add("item")
        assert not cms.is_zero()


class TestCountMinSketchAccuracy:
    """Tests for CMS accuracy and error bounds."""
    
    def test_exact_counts_small(self):
        """Test accuracy with small distinct counts."""
        cms = CountMinSketch(width=1000, depth=7)
        
        # Add items with known frequencies
        items = {"apple": 10, "banana": 5, "cherry": 3}
        
        for item, count in items.items():
            cms.add(item, count=count)
        
        # Estimates should be exact or slightly over (never under)
        for item, actual_count in items.items():
            estimate = cms.estimate(item)
            assert estimate >= actual_count
            assert estimate <= actual_count * 1.2  # Within 20% for small data
    
    def test_frequency_ordering(self):
        """Test that relative frequencies are preserved."""
        cms = CountMinSketch(width=1000, depth=5)
        
        # Add items with different frequencies
        cms.add("very_common", count=100)
        cms.add("common", count=50)
        cms.add("uncommon", count=10)
        cms.add("rare", count=1)
        
        # Estimates should preserve ordering
        est_very_common = cms.estimate("very_common")
        est_common = cms.estimate("common")
        est_uncommon = cms.estimate("uncommon")
        est_rare = cms.estimate("rare")
        
        assert est_very_common > est_common
        assert est_common > est_uncommon
        assert est_uncommon > est_rare
    
    def test_never_underestimates(self):
        """Test that CMS never underestimates."""
        cms = CountMinSketch(width=500, depth=5)
        
        # Add items with known counts
        items = [f"item{i}" for i in range(100)]
        actual_counts = {}
        
        for i, item in enumerate(items):
            count = i + 1  # 1, 2, 3, ..., 100
            cms.add(item, count=count)
            actual_counts[item] = count
        
        # All estimates should be >= actual
        for item, actual in actual_counts.items():
            estimate = cms.estimate(item)
            assert estimate >= actual, f"{item}: estimate {estimate} < actual {actual}"


class TestCountMinSketchMonoid:
    """Tests for CMS monoid properties."""
    
    def test_combine_basic(self):
        """Test basic combine operation."""
        cms1 = CountMinSketch(width=1000, depth=5)
        cms2 = CountMinSketch(width=1000, depth=5)
        
        cms1.add("apple", count=10)
        cms2.add("apple", count=5)
        
        combined = cms1.combine(cms2)
        
        # Combined should estimate at least 15
        assert combined.estimate("apple") >= 15
    
    def test_combine_disjoint(self):
        """Test combining CMS with disjoint items."""
        cms1 = CountMinSketch(width=1000, depth=5)
        cms2 = CountMinSketch(width=1000, depth=5)
        
        cms1.add("apple", count=10)
        cms2.add("banana", count=20)
        
        combined = cms1.combine(cms2)
        
        assert combined.estimate("apple") >= 10
        assert combined.estimate("banana") >= 20
    
    def test_operator_overload(self):
        """Test + operator for combine."""
        cms1 = CountMinSketch(width=100, depth=5)
        cms2 = CountMinSketch(width=100, depth=5)
        
        cms1.add("item", count=5)
        cms2.add("item", count=3)
        
        combined = cms1 + cms2
        
        assert isinstance(combined, CountMinSketch)
        assert combined.estimate("item") >= 8
    
    def test_sum_builtin(self):
        """Test sum() builtin support."""
        sketches = [CountMinSketch(width=100, depth=5) for _ in range(3)]
        
        sketches[0].add("item", count=10)
        sketches[1].add("item", count=20)
        sketches[2].add("item", count=30)
        
        combined = sum(sketches)
        
        assert combined.estimate("item") >= 60
    
    def test_associativity(self):
        """Test (a + b) + c = a + (b + c)."""
        cms_a = CountMinSketch(width=100, depth=5)
        cms_b = CountMinSketch(width=100, depth=5)
        cms_c = CountMinSketch(width=100, depth=5)
        
        cms_a.add("item", count=10)
        cms_b.add("item", count=20)
        cms_c.add("item", count=30)
        
        left = (cms_a + cms_b) + cms_c
        right = cms_a + (cms_b + cms_c)
        
        # Should produce same estimates
        assert left.estimate("item") == right.estimate("item")
    
    def test_identity(self):
        """Test zero + a = a + zero = a."""
        cms = CountMinSketch(width=100, depth=5)
        cms.add("item", count=42)
        
        zero = cms.zero
        
        left = zero + cms
        right = cms + zero
        
        # All should have same estimate
        original = cms.estimate("item")
        assert left.estimate("item") == original
        assert right.estimate("item") == original
    
    def test_combine_different_widths_fails(self):
        """Test that combining different widths raises error."""
        cms1 = CountMinSketch(width=100, depth=5)
        cms2 = CountMinSketch(width=200, depth=5)
        
        with pytest.raises(ValueError, match="different widths"):
            cms1.combine(cms2)
    
    def test_combine_different_depths_fails(self):
        """Test that combining different depths raises error."""
        cms1 = CountMinSketch(width=100, depth=4)
        cms2 = CountMinSketch(width=100, depth=5)
        
        with pytest.raises(ValueError, match="different depths"):
            cms1.combine(cms2)


class TestCountMinSketchHelpers:
    """Tests for helper functions."""
    
    def test_count_frequencies(self):
        """Test count_frequencies convenience function."""
        words = ["the", "quick", "brown", "the", "fox", "the"]
        
        cms = count_frequencies(words, width=100, depth=5)
        
        assert cms.estimate("the") >= 3
        assert cms.estimate("quick") >= 1
        assert cms.estimate("brown") >= 1
    
    def test_merge_cms(self):
        """Test merge_cms convenience function."""
        sketches = [CountMinSketch(width=100, depth=5) for _ in range(3)]
        
        for i, cms in enumerate(sketches):
            cms.add("item", count=(i + 1) * 10)
        
        merged = merge_cms(sketches)
        
        # Should estimate at least 10+20+30=60
        assert merged.estimate("item") >= 60
    
    def test_merge_cms_empty_list_fails(self):
        """Test that merging empty list raises error."""
        with pytest.raises(ValueError, match="empty list"):
            merge_cms([])
    
    def test_heavy_hitters(self):
        """Test heavy_hitters function."""
        cms = CountMinSketch(width=1000, depth=5)
        
        cms.add("frequent1", count=100)
        cms.add("frequent2", count=80)
        cms.add("rare1", count=5)
        cms.add("rare2", count=3)
        
        hitters = heavy_hitters(
            cms,
            ["frequent1", "frequent2", "rare1", "rare2"],
            threshold=50
        )
        
        # Should find items with freq >= 50
        assert len(hitters) == 2
        assert hitters[0][0] in ["frequent1", "frequent2"]
        assert hitters[1][0] in ["frequent1", "frequent2"]
    
    def test_top_k_estimates(self):
        """Test top_k_estimates method."""
        cms = CountMinSketch(width=1000, depth=5)
        
        cms.add("a", count=100)
        cms.add("b", count=50)
        cms.add("c", count=10)
        
        top_items = cms.top_k_estimates(["a", "b", "c"])
        
        # Should be sorted by frequency
        assert top_items[0][0] == "a"
        assert top_items[1][0] == "b"
        assert top_items[2][0] == "c"


class TestCountMinSketchEdgeCases:
    """Tests for edge cases."""
    
    def test_single_item(self):
        """Test CMS with single item."""
        cms = CountMinSketch(width=100, depth=5)
        cms.add("single")
        
        assert cms.estimate("single") >= 1
    
    def test_large_counts(self):
        """Test CMS with large counts."""
        cms = CountMinSketch(width=1000, depth=5)
        
        cms.add("item", count=1000000)
        
        assert cms.estimate("item") >= 1000000
    
    def test_many_distinct_items(self):
        """Test CMS with many distinct items."""
        cms = CountMinSketch(width=1000, depth=7)
        
        # Add 500 distinct items
        for i in range(500):
            cms.add(f"item{i}")
        
        # All should have estimate >= 1
        for i in range(500):
            assert cms.estimate(f"item{i}") >= 1
    
    def test_equality(self):
        """Test equality operator."""
        cms1 = CountMinSketch(width=100, depth=5)
        cms2 = CountMinSketch(width=100, depth=5)
        
        # Empty CMS should be equal
        assert cms1 == cms2
        
        # Add same items
        cms1.add("item", count=10)
        cms2.add("item", count=10)
        
        assert cms1 == cms2
        
        # Different items should not be equal
        cms3 = CountMinSketch(width=100, depth=5)
        cms3.add("other", count=10)
        
        assert cms1 != cms3
    
    def test_repr(self):
        """Test string representation."""
        cms = CountMinSketch(width=1000, depth=5)
        cms.add("item", count=42)
        
        repr_str = repr(cms)
        
        assert "CountMinSketch" in repr_str
        assert "width=1000" in repr_str
        assert "depth=5" in repr_str
        assert "total" in repr_str


class TestCountMinSketchRealWorld:
    """Real-world use case tests."""
    
    def test_word_frequency(self):
        """Test word frequency counting."""
        text = """
        the quick brown fox jumps over the lazy dog
        the fox was quick and the dog was lazy
        """.split()
        
        cms = CountMinSketch(width=1000, depth=5)
        
        for word in text:
            cms.add(word)
        
        # "the" appears 4 times
        assert cms.estimate("the") >= 4
        
        # "quick" appears 2 times
        assert cms.estimate("quick") >= 2
        
        # "fox" appears 2 times
        assert cms.estimate("fox") >= 2
    
    def test_distributed_log_analysis(self):
        """Test distributed log analysis scenario."""
        # Simulate 3 server logs
        server1_cms = CountMinSketch(width=1000, depth=5)
        server2_cms = CountMinSketch(width=1000, depth=5)
        server3_cms = CountMinSketch(width=1000, depth=5)
        
        # Server 1 logs
        for _ in range(100):
            server1_cms.add("ERROR: Connection timeout")
        
        # Server 2 logs
        for _ in range(50):
            server2_cms.add("ERROR: Connection timeout")
        
        # Server 3 logs
        for _ in range(75):
            server3_cms.add("ERROR: Connection timeout")
        
        # Merge all logs
        global_cms = server1_cms + server2_cms + server3_cms
        
        # Should estimate at least 225 total
        assert global_cms.estimate("ERROR: Connection timeout") >= 225
    
    def test_ip_address_tracking(self):
        """Test IP address request tracking."""
        cms = CountMinSketch(width=1000, depth=7)
        
        # Simulate requests from different IPs
        ips = {
            "192.168.1.1": 100,
            "192.168.1.2": 50,
            "192.168.1.3": 25,
            "192.168.1.4": 10,
        }
        
        for ip, count in ips.items():
            cms.add(ip, count=count)
        
        # Find heavy hitters (> 40 requests)
        hitters = heavy_hitters(cms, list(ips.keys()), threshold=40)
        
        assert len(hitters) == 2  # Should find 100 and 50
        assert hitters[0][1] >= 100
        assert hitters[1][1] >= 50
