"""Tests for HyperLogLog cardinality estimator."""

import pytest
from algesnake.approximate import HyperLogLog
from algesnake.approximate.hyperloglog import estimate_cardinality, merge_hlls


class TestHyperLogLogBasics:
    """Tests for basic HyperLogLog functionality."""
    
    def test_initialization(self):
        """Test HLL initialization with different precisions."""
        hll = HyperLogLog(precision=10)
        assert hll.precision == 10
        assert hll.m == 1024  # 2^10
        assert len(hll.registers) == 1024
        assert all(reg == 0 for reg in hll.registers)
    
    def test_precision_validation(self):
        """Test precision parameter validation."""
        # Valid precisions
        for p in [4, 8, 12, 16]:
            hll = HyperLogLog(precision=p)
            assert hll.precision == p
        
        # Invalid precisions
        with pytest.raises(ValueError, match="between 4 and 16"):
            HyperLogLog(precision=3)
        
        with pytest.raises(ValueError, match="between 4 and 16"):
            HyperLogLog(precision=17)
    
    def test_add_items(self):
        """Test adding items to HLL."""
        hll = HyperLogLog(precision=10)
        
        hll.add("user1")
        hll.add("user2")
        hll.add("user1")  # Duplicate
        
        # Should estimate ~2 unique items
        cardinality = hll.cardinality()
        assert 1 <= cardinality <= 4  # Allow some error
    
    def test_empty_cardinality(self):
        """Test cardinality of empty HLL."""
        hll = HyperLogLog(precision=10)
        assert hll.cardinality() == 0
    
    def test_zero_element(self):
        """Test monoid zero property."""
        hll = HyperLogLog(precision=10)
        zero = hll.zero
        
        assert isinstance(zero, HyperLogLog)
        assert zero.precision == hll.precision
        assert zero.is_zero()
    
    def test_is_zero(self):
        """Test is_zero method."""
        hll = HyperLogLog(precision=10)
        assert hll.is_zero()
        
        hll.add("item")
        assert not hll.is_zero()


class TestHyperLogLogAccuracy:
    """Tests for HLL accuracy and error bounds."""
    
    def test_small_cardinality(self):
        """Test accuracy for small cardinalities (< 100)."""
        hll = HyperLogLog(precision=12)
        
        # Add 50 unique items
        for i in range(50):
            hll.add(f"item{i}")
        
        estimate = hll.cardinality()
        error = abs(estimate - 50) / 50
        
        # Should be within 10% for small sets
        assert error < 0.10, f"Error {error:.2%} too high for small set"
    
    def test_medium_cardinality(self):
        """Test accuracy for medium cardinalities (100-10000)."""
        hll = HyperLogLog(precision=14)
        
        # Add 1000 unique items
        for i in range(1000):
            hll.add(f"user{i}")
        
        estimate = hll.cardinality()
        error = abs(estimate - 1000) / 1000
        
        # Should be within 5% for medium sets
        assert error < 0.05, f"Error {error:.2%} too high for medium set"
    
    def test_large_cardinality(self):
        """Test accuracy for large cardinalities (> 10000)."""
        hll = HyperLogLog(precision=14)
        
        # Add 100,000 unique items
        for i in range(100000):
            hll.add(i)
        
        estimate = hll.cardinality()
        error = abs(estimate - 100000) / 100000
        
        # Should be within 3% for large sets (matches theoretical bound)
        assert error < 0.03, f"Error {error:.2%} exceeds theoretical bound"
    
    def test_duplicates_dont_affect_cardinality(self):
        """Test that duplicates don't increase cardinality."""
        hll = HyperLogLog(precision=12)
        
        # Add same items multiple times
        for _ in range(10):
            for i in range(100):
                hll.add(f"item{i}")
        
        estimate = hll.cardinality()
        error = abs(estimate - 100) / 100
        
        assert error < 0.10, "Duplicates incorrectly increased cardinality"


class TestHyperLogLogMonoid:
    """Tests for HLL monoid properties."""
    
    def test_combine_basic(self):
        """Test basic combine operation."""
        hll1 = HyperLogLog(precision=10)
        hll2 = HyperLogLog(precision=10)
        
        for i in range(50):
            hll1.add(f"item{i}")
        
        for i in range(50, 100):
            hll2.add(f"item{i}")
        
        combined = hll1.combine(hll2)
        
        # Should estimate ~100 unique items
        estimate = combined.cardinality()
        assert 80 <= estimate <= 120  # Within reasonable error
    
    def test_combine_overlapping(self):
        """Test combining HLLs with overlapping elements."""
        hll1 = HyperLogLog(precision=12)
        hll2 = HyperLogLog(precision=12)
        
        # hll1: items 0-99
        for i in range(100):
            hll1.add(i)
        
        # hll2: items 50-149 (50% overlap)
        for i in range(50, 150):
            hll2.add(i)
        
        combined = hll1.combine(hll2)
        
        # Should estimate ~150 unique items (0-149)
        estimate = combined.cardinality()
        error = abs(estimate - 150) / 150
        assert error < 0.10
    
    def test_operator_overload(self):
        """Test + operator for combine."""
        hll1 = HyperLogLog(precision=10)
        hll2 = HyperLogLog(precision=10)
        
        for i in range(50):
            hll1.add(i)
            hll2.add(i + 50)
        
        combined = hll1 + hll2
        
        assert isinstance(combined, HyperLogLog)
        assert combined.precision == hll1.precision
    
    def test_sum_builtin(self):
        """Test sum() builtin support."""
        hlls = [HyperLogLog(precision=10) for _ in range(5)]
        
        for i, hll in enumerate(hlls):
            for j in range(20):
                hll.add(i * 20 + j)
        
        combined = sum(hlls)
        
        # Should estimate ~100 unique items
        estimate = combined.cardinality()
        assert 80 <= estimate <= 120
    
    def test_associativity(self):
        """Test (a + b) + c = a + (b + c)."""
        hll_a = HyperLogLog(precision=10)
        hll_b = HyperLogLog(precision=10)
        hll_c = HyperLogLog(precision=10)
        
        for i in range(30):
            hll_a.add(i)
            hll_b.add(i + 30)
            hll_c.add(i + 60)
        
        left = (hll_a + hll_b) + hll_c
        right = hll_a + (hll_b + hll_c)
        
        # Cardinality should be the same
        assert abs(left.cardinality() - right.cardinality()) <= 5
    
    def test_identity(self):
        """Test zero + a = a + zero = a."""
        hll = HyperLogLog(precision=10)
        for i in range(50):
            hll.add(i)
        
        zero = hll.zero
        
        left = zero + hll
        right = hll + zero
        
        # All should have same cardinality
        original = hll.cardinality()
        assert abs(left.cardinality() - original) <= 2
        assert abs(right.cardinality() - original) <= 2
    
    def test_combine_different_precisions_fails(self):
        """Test that combining different precisions raises error."""
        hll1 = HyperLogLog(precision=10)
        hll2 = HyperLogLog(precision=12)
        
        with pytest.raises(ValueError, match="different precisions"):
            hll1.combine(hll2)


class TestHyperLogLogHelpers:
    """Tests for helper functions."""
    
    def test_estimate_cardinality_function(self):
        """Test estimate_cardinality convenience function."""
        items = [f"item{i}" for i in range(100)]
        
        estimate = estimate_cardinality(items, precision=12)
        
        error = abs(estimate - 100) / 100
        assert error < 0.10
    
    def test_estimate_cardinality_with_duplicates(self):
        """Test estimate_cardinality with duplicate items."""
        items = ["a", "b", "c", "a", "b", "d"]
        
        estimate = estimate_cardinality(items, precision=10)
        
        # Should estimate 4 unique items
        assert 3 <= estimate <= 6
    
    def test_merge_hlls_function(self):
        """Test merge_hlls convenience function."""
        hlls = [HyperLogLog(precision=10) for _ in range(3)]
        
        for i, hll in enumerate(hlls):
            for j in range(30):
                hll.add(i * 30 + j)
        
        merged = merge_hlls(hlls)
        
        # Should estimate ~90 unique items
        estimate = merged.cardinality()
        assert 70 <= estimate <= 110
    
    def test_merge_hlls_empty_list_fails(self):
        """Test that merging empty list raises error."""
        with pytest.raises(ValueError, match="empty list"):
            merge_hlls([])


class TestHyperLogLogEdgeCases:
    """Tests for edge cases."""
    
    def test_single_item(self):
        """Test HLL with single item."""
        hll = HyperLogLog(precision=10)
        hll.add("single")
        
        estimate = hll.cardinality()
        assert estimate >= 1
    
    def test_integer_items(self):
        """Test HLL with integer items."""
        hll = HyperLogLog(precision=12)
        
        for i in range(1000):
            hll.add(i)
        
        estimate = hll.cardinality()
        error = abs(estimate - 1000) / 1000
        assert error < 0.05
    
    def test_string_items(self):
        """Test HLL with string items."""
        hll = HyperLogLog(precision=12)
        
        for i in range(1000):
            hll.add(f"user_{i}")
        
        estimate = hll.cardinality()
        error = abs(estimate - 1000) / 1000
        assert error < 0.05
    
    def test_mixed_types(self):
        """Test HLL with mixed hashable types."""
        hll = HyperLogLog(precision=10)
        
        hll.add(1)
        hll.add("string")
        hll.add((1, 2))
        hll.add(3.14)
        
        estimate = hll.cardinality()
        assert 3 <= estimate <= 6
    
    def test_equality(self):
        """Test equality operator."""
        hll1 = HyperLogLog(precision=10)
        hll2 = HyperLogLog(precision=10)
        
        # Empty HLLs should be equal
        assert hll1 == hll2
        
        # Add same items
        for i in range(10):
            hll1.add(i)
            hll2.add(i)
        
        assert hll1 == hll2
        
        # Different items should not be equal
        hll3 = HyperLogLog(precision=10)
        hll3.add(999)
        
        assert hll1 != hll3
    
    def test_repr(self):
        """Test string representation."""
        hll = HyperLogLog(precision=12)
        repr_str = repr(hll)
        
        assert "HyperLogLog" in repr_str
        assert "precision=12" in repr_str
        assert "cardinality" in repr_str


class TestHyperLogLogRealWorld:
    """Real-world use case tests."""
    
    def test_unique_users_scenario(self):
        """Test counting unique users across partitions."""
        # Simulate 3 server partitions
        partition1 = HyperLogLog(precision=14)
        partition2 = HyperLogLog(precision=14)
        partition3 = HyperLogLog(precision=14)
        
        # Partition 1: users 0-999
        for i in range(1000):
            partition1.add(f"user{i}")
        
        # Partition 2: users 500-1499 (50% overlap with partition1)
        for i in range(500, 1500):
            partition2.add(f"user{i}")
        
        # Partition 3: users 1000-1999 (50% overlap with partition2)
        for i in range(1000, 2000):
            partition3.add(f"user{i}")
        
        # Merge all partitions
        total = partition1 + partition2 + partition3
        
        # Should estimate ~2000 unique users (0-1999)
        estimate = total.cardinality()
        error = abs(estimate - 2000) / 2000
        
        assert error < 0.05, f"Unique user count error {error:.2%} too high"
    
    def test_streaming_aggregation(self):
        """Test incremental aggregation in streaming scenario."""
        hll = HyperLogLog(precision=14)
        
        # Simulate streaming data in batches
        for batch in range(10):
            for i in range(1000):
                user_id = f"batch{batch}_user{i}"
                hll.add(user_id)
        
        # Should estimate ~10,000 unique users
        estimate = hll.cardinality()
        error = abs(estimate - 10000) / 10000
        
        assert error < 0.03
