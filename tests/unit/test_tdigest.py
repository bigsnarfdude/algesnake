"""Tests for T-Digest quantile estimator."""

import pytest
import random
from algesnake.approximate import TDigest
from algesnake.approximate.tdigest import estimate_quantiles, merge_tdigests


class TestTDigestBasics:
    """Tests for basic T-Digest functionality."""
    
    def test_initialization(self):
        """Test T-Digest initialization."""
        td = TDigest(compression=100)
        
        assert td.compression == 100
        assert len(td.centroids) == 0
        assert td.is_zero()
        assert td.count == 0
    
    def test_initialization_validation(self):
        """Test parameter validation."""
        # Valid compression
        td = TDigest(compression=50)
        assert td.compression == 50
        
        # Invalid compression
        with pytest.raises(ValueError, match="Compression must be positive"):
            TDigest(compression=0)
        
        with pytest.raises(ValueError, match="Compression must be positive"):
            TDigest(compression=-10)
    
    def test_add_single_value(self):
        """Test adding single value."""
        td = TDigest(compression=100)
        
        td.add(42.0)
        
        assert not td.is_zero()
        assert td.count == 1
    
    def test_add_multiple_values(self):
        """Test adding multiple values."""
        td = TDigest(compression=100)
        
        for i in range(100):
            td.add(float(i))
        
        assert td.count == 100
    
    def test_add_with_weight(self):
        """Test adding value with weight."""
        td = TDigest(compression=100)
        
        td.add(10.0, weight=5.0)
        
        assert td.count == 5.0
    
    def test_zero_element(self):
        """Test monoid zero property."""
        td = TDigest(compression=100)
        zero = td.zero
        
        assert isinstance(zero, TDigest)
        assert zero.compression == td.compression
        assert zero.is_zero()
    
    def test_is_zero(self):
        """Test is_zero method."""
        td = TDigest(compression=100)
        assert td.is_zero()
        
        td.add(1.0)
        assert not td.is_zero()


class TestTDigestQuantiles:
    """Tests for quantile estimation."""
    
    def test_quantile_validation(self):
        """Test quantile parameter validation."""
        td = TDigest(compression=100)
        for i in range(10):
            td.add(float(i))
        
        # Valid quantiles
        assert td.quantile(0.0) >= 0
        assert td.quantile(0.5) >= 0
        assert td.quantile(1.0) >= 0
        
        # Invalid quantiles
        with pytest.raises(ValueError, match="between 0 and 1"):
            td.quantile(-0.1)
        
        with pytest.raises(ValueError, match="between 0 and 1"):
            td.quantile(1.1)
    
    def test_quantile_empty_fails(self):
        """Test that quantile on empty digest fails."""
        td = TDigest(compression=100)
        
        with pytest.raises(ValueError, match="empty digest"):
            td.quantile(0.5)
    
    def test_quantile_single_value(self):
        """Test quantile with single value."""
        td = TDigest(compression=100)
        td.add(42.0)
        
        # All quantiles should return the single value
        assert td.quantile(0.0) == 42.0
        assert td.quantile(0.5) == 42.0
        assert td.quantile(1.0) == 42.0
    
    def test_quantile_uniform_distribution(self):
        """Test quantile accuracy on uniform distribution."""
        td = TDigest(compression=100)
        
        # Add values 0-999
        for i in range(1000):
            td.add(float(i))
        
        # Check quantiles
        q50 = td.quantile(0.50)
        q95 = td.quantile(0.95)
        q99 = td.quantile(0.99)
        
        # Should be reasonably close
        assert 450 <= q50 <= 550  # Median around 500
        assert 900 <= q95 <= 970  # p95 around 950
        assert 960 <= q99 <= 1000  # p99 around 990
    
    def test_percentile_convenience(self):
        """Test percentile convenience method."""
        td = TDigest(compression=100)
        
        for i in range(100):
            td.add(float(i))
        
        # percentile(p) should equal quantile(p/100)
        assert td.percentile(50) == td.quantile(0.50)
        assert td.percentile(95) == td.quantile(0.95)
        assert td.percentile(99) == td.quantile(0.99)
    
    def test_median(self):
        """Test median (50th percentile)."""
        td = TDigest(compression=100)
        
        # Add 1-100
        for i in range(1, 101):
            td.add(float(i))
        
        median = td.quantile(0.50)
        
        # Median should be around 50-51
        assert 45 <= median <= 55
    
    def test_quantiles_ordered(self):
        """Test that quantiles maintain ordering."""
        td = TDigest(compression=100)
        
        for i in range(100):
            td.add(float(i))
        
        q25 = td.quantile(0.25)
        q50 = td.quantile(0.50)
        q75 = td.quantile(0.75)
        
        # Should be ordered
        assert q25 <= q50 <= q75


class TestTDigestCDF:
    """Tests for CDF (cumulative distribution function)."""
    
    def test_cdf_empty(self):
        """Test CDF on empty digest."""
        td = TDigest(compression=100)
        
        assert td.cdf(50.0) == 0.0
    
    def test_cdf_uniform(self):
        """Test CDF on uniform distribution."""
        td = TDigest(compression=100)
        
        for i in range(100):
            td.add(float(i))
        
        # CDF should be approximately linear
        assert 0.0 <= td.cdf(0.0) <= 0.1
        assert 0.4 <= td.cdf(50.0) <= 0.6
        assert 0.9 <= td.cdf(99.0) <= 1.0
    
    def test_cdf_bounds(self):
        """Test CDF returns values in [0, 1]."""
        td = TDigest(compression=100)
        
        for i in range(100):
            td.add(float(i))
        
        # Below min should be 0
        assert td.cdf(-100.0) == 0.0
        
        # Above max should be 1
        assert td.cdf(200.0) == 1.0


class TestTDigestMinMax:
    """Tests for min/max values."""
    
    def test_min_max_empty_fails(self):
        """Test min/max on empty digest fails."""
        td = TDigest(compression=100)
        
        with pytest.raises(ValueError, match="empty digest"):
            td.min()
        
        with pytest.raises(ValueError, match="empty digest"):
            td.max()
    
    def test_min_max_basic(self):
        """Test min and max."""
        td = TDigest(compression=100)
        
        values = [10, 5, 20, 15, 3, 25]
        for v in values:
            td.add(float(v))
        
        assert td.min() == 3.0
        assert td.max() == 25.0


class TestTDigestMonoid:
    """Tests for T-Digest monoid properties."""
    
    def test_combine_basic(self):
        """Test basic combine operation."""
        td1 = TDigest(compression=100)
        td2 = TDigest(compression=100)
        
        for i in range(50):
            td1.add(float(i))
        
        for i in range(50, 100):
            td2.add(float(i))
        
        combined = td1.combine(td2)
        
        # Combined should have all 100 values
        assert combined.count == 100
        
        # Median should be around 50
        median = combined.quantile(0.50)
        assert 40 <= median <= 60
    
    def test_combine_overlapping(self):
        """Test combining digests with overlapping ranges."""
        td1 = TDigest(compression=100)
        td2 = TDigest(compression=100)
        
        # Both have values 0-99
        for i in range(100):
            td1.add(float(i))
            td2.add(float(i))
        
        combined = td1.combine(td2)
        
        # Should have 200 total values
        assert combined.count == 200
    
    def test_operator_overload(self):
        """Test + operator for combine."""
        td1 = TDigest(compression=100)
        td2 = TDigest(compression=100)
        
        for i in range(10):
            td1.add(float(i))
            td2.add(float(i + 10))
        
        combined = td1 + td2
        
        assert isinstance(combined, TDigest)
        assert combined.count == 20
    
    def test_sum_builtin(self):
        """Test sum() builtin support."""
        digests = [TDigest(compression=100) for _ in range(3)]
        
        for i, td in enumerate(digests):
            for j in range(10):
                td.add(float(i * 10 + j))
        
        combined = sum(digests)
        
        assert combined.count == 30
    
    def test_associativity(self):
        """Test (a + b) + c = a + (b + c)."""
        td_a = TDigest(compression=100)
        td_b = TDigest(compression=100)
        td_c = TDigest(compression=100)
        
        for i in range(10):
            td_a.add(float(i))
            td_b.add(float(i + 10))
            td_c.add(float(i + 20))
        
        left = (td_a + td_b) + td_c
        right = td_a + (td_b + td_c)
        
        # Should have same count
        assert left.count == right.count == 30
        
        # Quantiles should be similar
        assert abs(left.quantile(0.5) - right.quantile(0.5)) < 2.0
    
    def test_identity(self):
        """Test zero + a = a + zero = a."""
        td = TDigest(compression=100)
        for i in range(50):
            td.add(float(i))
        
        zero = td.zero
        
        left = zero + td
        right = td + zero
        
        # All should have same count
        assert left.count == td.count == right.count
        
        # Quantiles should be similar
        assert abs(left.quantile(0.5) - td.quantile(0.5)) < 1.0
        assert abs(right.quantile(0.5) - td.quantile(0.5)) < 1.0


class TestTDigestHelpers:
    """Tests for helper functions."""
    
    def test_estimate_quantiles(self):
        """Test estimate_quantiles convenience function."""
        values = list(range(1000))
        
        q50, q95, q99 = estimate_quantiles(values, [0.50, 0.95, 0.99])
        
        # Should be reasonably close
        assert 450 <= q50 <= 550
        assert 900 <= q95 <= 970
        assert 960 <= q99 <= 1000
    
    def test_merge_tdigests(self):
        """Test merge_tdigests convenience function."""
        digests = [TDigest(compression=100) for _ in range(3)]
        
        for i, td in enumerate(digests):
            for j in range(100):
                td.add(float(i * 100 + j))
        
        merged = merge_tdigests(digests)
        
        assert merged.count == 300
    
    def test_merge_tdigests_empty_fails(self):
        """Test that merging empty list raises error."""
        with pytest.raises(ValueError, match="empty list"):
            merge_tdigests([])


class TestTDigestEdgeCases:
    """Tests for edge cases."""
    
    def test_all_same_value(self):
        """Test digest with all same values."""
        td = TDigest(compression=100)
        
        for _ in range(100):
            td.add(42.0)
        
        # All quantiles should return 42
        assert td.quantile(0.0) == 42.0
        assert td.quantile(0.5) == 42.0
        assert td.quantile(1.0) == 42.0
    
    def test_two_distinct_values(self):
        """Test digest with two distinct values."""
        td = TDigest(compression=100)
        
        for _ in range(50):
            td.add(10.0)
        for _ in range(50):
            td.add(20.0)
        
        # Median should be between 10 and 20
        median = td.quantile(0.50)
        assert 10.0 <= median <= 20.0
    
    def test_large_values(self):
        """Test digest with large values."""
        td = TDigest(compression=100)
        
        for i in range(100):
            td.add(float(i * 1000000))
        
        # Should handle large values
        q50 = td.quantile(0.50)
        assert 40_000_000 <= q50 <= 60_000_000
    
    def test_negative_values(self):
        """Test digest with negative values."""
        td = TDigest(compression=100)
        
        for i in range(-50, 50):
            td.add(float(i))
        
        median = td.quantile(0.50)
        assert -5 <= median <= 5
    
    def test_float_values(self):
        """Test digest with float values."""
        td = TDigest(compression=100)
        
        for i in range(100):
            td.add(i + 0.5)
        
        q50 = td.quantile(0.50)
        assert 45 <= q50 <= 55
    
    def test_repr(self):
        """Test string representation."""
        td = TDigest(compression=100)
        for i in range(10):
            td.add(float(i))
        
        repr_str = repr(td)
        
        assert "TDigest" in repr_str
        assert "compression=100" in repr_str
        assert "count=10" in repr_str


class TestTDigestRealWorld:
    """Real-world use case tests."""
    
    def test_latency_monitoring(self):
        """Test latency percentile monitoring."""
        td = TDigest(compression=100)
        
        # Simulate latencies (ms)
        latencies = [random.gauss(100, 20) for _ in range(1000)]
        
        for latency in latencies:
            td.add(max(0, latency))  # No negative latencies
        
        # Get SLA percentiles
        p50 = td.percentile(50)
        p95 = td.percentile(95)
        p99 = td.percentile(99)
        
        # Should be ordered and reasonable
        assert p50 < p95 < p99
        assert 50 <= p50 <= 150  # Around 100ms
        assert p95 > 100  # Tail should be higher
        assert p99 > p95
    
    def test_distributed_latency_aggregation(self):
        """Test aggregating latency data from multiple servers."""
        # Three servers with latency data
        server1 = TDigest(compression=100)
        server2 = TDigest(compression=100)
        server3 = TDigest(compression=100)
        
        # Server 1: Fast responses
        for _ in range(100):
            server1.add(random.gauss(50, 10))
        
        # Server 2: Medium responses
        for _ in range(100):
            server2.add(random.gauss(100, 15))
        
        # Server 3: Slow responses
        for _ in range(100):
            server3.add(random.gauss(200, 30))
        
        # Merge using monoid operation
        global_td = server1 + server2 + server3
        
        # Global p99 should be higher than individual servers
        p99_global = global_td.percentile(99)
        
        assert p99_global > 100  # Should capture slow tail
    
    def test_request_size_distribution(self):
        """Test analyzing request size distribution."""
        td = TDigest(compression=100)
        
        # Simulate request sizes (bytes)
        # Most small, some large
        for _ in range(900):
            td.add(random.uniform(100, 1000))
        
        for _ in range(100):
            td.add(random.uniform(10000, 100000))
        
        # Check distribution
        p50 = td.quantile(0.50)
        p95 = td.quantile(0.95)
        
        # Median should be small
        assert p50 < 1000
        
        # p95 should capture large requests
        assert p95 > 1000
    
    def test_error_rate_analysis(self):
        """Test analyzing response time distribution for errors."""
        td_success = TDigest(compression=100)
        td_error = TDigest(compression=100)
        
        # Successful requests: fast
        for _ in range(950):
            td_success.add(random.gauss(50, 10))
        
        # Error requests: slow (timeouts)
        for _ in range(50):
            td_error.add(random.gauss(5000, 500))
        
        # Compare distributions
        p99_success = td_success.percentile(99)
        p50_error = td_error.percentile(50)
        
        # Errors should be much slower
        assert p50_error > p99_success


class TestTDigestAccuracy:
    """Tests for accuracy benchmarks."""
    
    def test_accuracy_uniform_distribution(self):
        """Test accuracy on uniform distribution."""
        td = TDigest(compression=200)  # Higher compression for accuracy
        
        # Add 10,000 values uniformly distributed
        for i in range(10000):
            td.add(float(i))
        
        # Check accuracy at various quantiles
        quantiles_to_test = [0.01, 0.10, 0.50, 0.90, 0.99]
        
        for q in quantiles_to_test:
            actual = q * 10000
            estimated = td.quantile(q)
            error = abs(estimated - actual) / actual
            
            # Should be within 5% for uniform distribution
            assert error < 0.05, f"Error {error:.2%} too high for q={q}"
    
    def test_tail_accuracy(self):
        """Test that tail quantiles (p99, p999) are accurate."""
        td = TDigest(compression=200)
        
        for i in range(100000):
            td.add(float(i))
        
        # p99 should be around 99,000
        p99 = td.quantile(0.99)
        error_p99 = abs(p99 - 99000) / 99000
        
        # p999 should be around 99,900
        p999 = td.quantile(0.999)
        error_p999 = abs(p999 - 99900) / 99900
        
        # Tail accuracy should be good
        assert error_p99 < 0.02  # Within 2%
        assert error_p999 < 0.05  # Within 5%
