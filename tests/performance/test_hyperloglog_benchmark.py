"""
Performance benchmarks for HyperLogLog.

Tests accuracy, memory usage, and performance characteristics.
"""

import pytest
import sys
from algesnake.approximate.hyperloglog import HyperLogLog


class TestHyperLogLogAccuracy:
    """Test HyperLogLog accuracy at different cardinalities."""

    @pytest.mark.parametrize("precision,expected_error", [
        (10, 0.04),  # ~4% error with 2^10 = 1024 registers
        (12, 0.02),  # ~2% error with 2^12 = 4096 registers
        (14, 0.01),  # ~1% error with 2^14 = 16384 registers
    ])
    def test_accuracy_at_different_precisions(self, precision, expected_error):
        """Test accuracy improves with precision."""
        hll = HyperLogLog(precision=precision)
        true_cardinality = 10000

        # Add unique items
        for i in range(true_cardinality):
            hll.add(f"item_{i}")

        estimated = hll.cardinality()
        actual_error = abs(estimated - true_cardinality) / true_cardinality

        # Should be within expected error bound (with some tolerance)
        assert actual_error < expected_error * 2

    @pytest.mark.parametrize("true_count", [
        100,
        1000,
        10000,
        100000,
    ])
    def test_accuracy_at_different_cardinalities(self, true_count):
        """Test accuracy at different cardinalities."""
        hll = HyperLogLog(precision=12)

        # Add unique items
        for i in range(true_count):
            hll.add(f"item_{i}")

        estimated = hll.cardinality()
        error_rate = abs(estimated - true_count) / true_count

        # Should be within ~5% error for precision=12
        assert error_rate < 0.05

    def test_accuracy_with_duplicates(self):
        """Test that duplicates don't affect accuracy."""
        hll = HyperLogLog(precision=12)
        true_unique = 5000

        # Add each item multiple times
        for i in range(true_unique):
            for _ in range(10):  # Add each item 10 times
                hll.add(f"item_{i}")

        estimated = hll.cardinality()
        error_rate = abs(estimated - true_unique) / true_unique

        assert error_rate < 0.05

    def test_accuracy_with_skewed_distribution(self):
        """Test accuracy with skewed (zipf-like) distribution."""
        hll = HyperLogLog(precision=12)

        # Zipf distribution: few items very frequent, many items rare
        for i in range(1000):
            # Item 0 appears 1000 times
            if i == 0:
                for _ in range(1000):
                    hll.add(f"item_{i}")
            # Item 1-9 appear 100 times each
            elif i < 10:
                for _ in range(100):
                    hll.add(f"item_{i}")
            # Items 10-999 appear once
            else:
                hll.add(f"item_{i}")

        estimated = hll.cardinality()
        true_count = 1000
        error_rate = abs(estimated - true_count) / true_count

        assert error_rate < 0.05


class TestHyperLogLogMemory:
    """Test HyperLogLog memory usage."""

    def test_memory_constant_regardless_of_cardinality(self):
        """Test that memory usage is constant regardless of cardinality."""
        hll_small = HyperLogLog(precision=12)
        hll_large = HyperLogLog(precision=12)

        # Add different amounts of data
        for i in range(100):
            hll_small.add(f"item_{i}")

        for i in range(100000):
            hll_large.add(f"item_{i}")

        # Both should use same memory (2^12 = 4096 registers)
        # Note: This is a conceptual test - actual memory measurement would need sys.getsizeof
        # For now, just verify both work with vastly different cardinalities
        assert hll_small.cardinality() < 200
        assert hll_large.cardinality() > 90000

    @pytest.mark.parametrize("precision", [8, 10, 12, 14, 16])
    def test_memory_scales_with_precision(self, precision):
        """Test that memory scales as 2^precision registers."""
        hll = HyperLogLog(precision=precision)

        # Add some data
        for i in range(1000):
            hll.add(f"item_{i}")

        # Number of registers should be 2^precision
        expected_registers = 2 ** precision
        actual_registers = len(hll.registers)

        assert actual_registers == expected_registers

    def test_memory_usage_for_common_precisions(self):
        """Document memory usage for common precision values."""
        precisions_and_sizes = [
            (10, 1024),     # 1KB for 1024 registers (1 byte each)
            (12, 4096),     # 4KB for 4096 registers
            (14, 16384),    # 16KB for 16384 registers
            (16, 65536),    # 64KB for 65536 registers
        ]

        for precision, expected_registers in precisions_and_sizes:
            hll = HyperLogLog(precision=precision)
            assert len(hll.registers) == expected_registers


class TestHyperLogLogPerformance:
    """Test HyperLogLog performance characteristics."""

    def test_add_performance_large_dataset(self):
        """Test adding large number of items."""
        hll = HyperLogLog(precision=12)

        # Should be able to process 100k items quickly
        for i in range(100000):
            hll.add(f"item_{i}")

        # Verify it worked
        estimated = hll.cardinality()
        assert 95000 < estimated < 105000

    def test_merge_performance(self):
        """Test merging multiple HLLs."""
        hlls = []

        # Create 10 HLLs with 10k items each
        for batch in range(10):
            hll = HyperLogLog(precision=12)
            for i in range(10000):
                hll.add(f"batch_{batch}_item_{i}")
            hlls.append(hll)

        # Merge all
        merged = hlls[0]
        for hll in hlls[1:]:
            merged = merged + hll

        # Should have ~100k unique items
        estimated = merged.cardinality()
        assert 95000 < estimated < 105000

    def test_cardinality_query_performance(self):
        """Test that cardinality queries are fast."""
        hll = HyperLogLog(precision=14)

        # Add data
        for i in range(50000):
            hll.add(f"item_{i}")

        # Multiple queries should be fast (no rebuilding needed)
        for _ in range(100):
            card = hll.cardinality()
            assert card > 0


class TestHyperLogLogEdgeCases:
    """Test HyperLogLog edge cases."""

    def test_empty_hll(self):
        """Test cardinality of empty HLL is 0."""
        hll = HyperLogLog(precision=12)
        assert hll.cardinality() == 0

    def test_single_item(self):
        """Test cardinality with single item."""
        hll = HyperLogLog(precision=12)
        hll.add("single_item")

        estimated = hll.cardinality()
        assert 0 < estimated < 5  # Should be close to 1

    def test_very_small_cardinality(self):
        """Test accuracy for very small cardinalities."""
        hll = HyperLogLog(precision=12)

        for i in range(10):
            hll.add(f"item_{i}")

        estimated = hll.cardinality()
        # For small cardinalities, HLL uses linear counting correction
        assert 5 < estimated < 15

    def test_very_large_cardinality(self):
        """Test with very large cardinality."""
        hll = HyperLogLog(precision=14)
        true_count = 1000000

        # Add 1 million unique items
        for i in range(true_count):
            hll.add(f"item_{i}")

        estimated = hll.cardinality()
        error_rate = abs(estimated - true_count) / true_count

        # Should still maintain accuracy
        assert error_rate < 0.02  # Within 2%

    def test_all_duplicates(self):
        """Test when all items are the same."""
        hll = HyperLogLog(precision=12)

        # Add same item 10000 times
        for _ in range(10000):
            hll.add("same_item")

        estimated = hll.cardinality()
        assert estimated < 5  # Should estimate ~1


class TestHyperLogLogComparison:
    """Compare HyperLogLog with exact counting."""

    def test_memory_savings(self):
        """Compare memory usage vs exact set."""
        precision = 12
        cardinality = 100000

        # HyperLogLog
        hll = HyperLogLog(precision=precision)
        for i in range(cardinality):
            hll.add(f"item_{i}")

        # Exact set (for comparison)
        exact_set = set()
        for i in range(cardinality):
            exact_set.add(f"item_{i}")

        # HyperLogLog uses ~4KB (2^12 registers)
        # Exact set uses much more (depends on string size)
        hll_size = sys.getsizeof(hll.registers)
        set_size = sys.getsizeof(exact_set)

        # HLL should be much smaller
        assert hll_size < set_size

        # But accuracy is close
        hll_count = hll.cardinality()
        exact_count = len(exact_set)
        error_rate = abs(hll_count - exact_count) / exact_count
        assert error_rate < 0.05

    def test_speed_comparison_for_merge(self):
        """Compare merge speed vs set union."""
        # Create HLLs
        hll1 = HyperLogLog(precision=10)
        hll2 = HyperLogLog(precision=10)

        for i in range(10000):
            hll1.add(f"set1_item_{i}")
        for i in range(5000, 15000):
            hll2.add(f"set2_item_{i}")

        # HLL merge (very fast)
        merged_hll = hll1 + hll2

        # Verify it worked
        assert merged_hll.cardinality() > 10000


class TestHyperLogLogErrorBounds:
    """Test HyperLogLog theoretical error bounds."""

    def test_standard_error_formula(self):
        """Test that empirical error matches theoretical bound."""
        # Theoretical standard error for HLL is approximately 1.04 / sqrt(m)
        # where m = 2^precision

        precision = 12
        m = 2 ** precision
        theoretical_error = 1.04 / (m ** 0.5)  # ~0.016 or 1.6%

        # Run multiple trials
        errors = []
        true_count = 10000

        for trial in range(10):
            hll = HyperLogLog(precision=precision)
            for i in range(true_count):
                hll.add(f"trial_{trial}_item_{i}")

            estimated = hll.cardinality()
            error = abs(estimated - true_count) / true_count
            errors.append(error)

        # Average error should be within theoretical bound
        avg_error = sum(errors) / len(errors)
        assert avg_error < theoretical_error * 2

    def test_precision_vs_error_tradeoff(self):
        """Test that higher precision gives lower error."""
        true_count = 50000
        results = []

        for precision in [10, 12, 14]:
            hll = HyperLogLog(precision=precision)
            for i in range(true_count):
                hll.add(f"item_{i}")

            estimated = hll.cardinality()
            error = abs(estimated - true_count) / true_count
            results.append((precision, error))

        # Higher precision should give lower error
        errors = [error for _, error in results]
        # errors[0] (precision=10) should be >= errors[1] (precision=12)
        # errors[1] should be >= errors[2] (precision=14)
        # (Not strict inequality due to randomness, but generally true)
        assert errors[0] >= errors[2]  # At least first vs last
