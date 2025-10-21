"""T-Digest: Probabilistic quantile estimation.

T-Digest estimates quantiles (percentiles, median) of streaming data
with high accuracy, especially at the tails (p99, p999). It uses
clustered centroids to approximate the distribution.

Perfect for: latency monitoring, SLA tracking, percentile calculations.

References:
- Dunning and Ertl "Computing Extremely Accurate Quantiles Using t-Digests" (2019)
- https://github.com/tdunning/t-digest
"""

import bisect
import math
from typing import List, Tuple, Optional


class Centroid:
    """A centroid represents a cluster of values.
    
    Attributes:
        mean: Average value of the cluster
        weight: Number of values in the cluster
    """
    
    def __init__(self, mean: float, weight: float):
        self.mean = mean
        self.weight = weight
    
    def __repr__(self):
        return f"Centroid(mean={self.mean:.2f}, weight={self.weight})"
    
    def __lt__(self, other):
        """Compare by mean for sorting."""
        return self.mean < other.mean


class TDigest:
    """T-Digest for quantile estimation.
    
    Maintains a compressed representation of a distribution using centroids.
    Provides high accuracy for quantile queries, especially at tails.
    
    Typical accuracy: 0.1-1% error for extreme percentiles (p99, p999)
    
    This implementation forms a monoid:
    - Identity: Empty T-Digest (no centroids)
    - Combine: Merge centroids and recompress
    
    Examples:
        >>> td = TDigest(compression=100)
        >>> for latency in latencies:
        ...     td.add(latency)
        >>> print(f"p50: {td.quantile(0.50)}ms")
        >>> print(f"p99: {td.quantile(0.99)}ms")
        
        >>> # Distributed aggregation
        >>> td1 = TDigest(compression=100)
        >>> td2 = TDigest(compression=100)
        >>> combined = td1 + td2  # Monoid merge
    """
    
    def __init__(self, compression: int = 100, centroids: Optional[List[Centroid]] = None):
        """Initialize T-Digest.
        
        Args:
            compression: Controls number of centroids (higher = more accurate)
                        Typical values: 50-200
            centroids: Initial centroids (for deserialization)
        
        Raises:
            ValueError: If compression is not positive
        """
        if compression <= 0:
            raise ValueError("Compression must be positive")
        
        self.compression = compression
        
        if centroids is not None:
            self.centroids = sorted(centroids, key=lambda c: c.mean)
        else:
            self.centroids = []
        
        self.count = sum(c.weight for c in self.centroids)
        self._unmerged = []  # Buffer for new values
        self._max_unmerged = compression  # Compress when buffer full
    
    def add(self, value: float, weight: float = 1.0) -> None:
        """Add a value to the digest.
        
        Args:
            value: Value to add
            weight: Weight of the value (default 1.0)
        """
        self._unmerged.append(Centroid(value, weight))
        self.count += weight
        
        # Compress if buffer is full
        if len(self._unmerged) >= self._max_unmerged:
            self._compress()
    
    def quantile(self, q: float) -> float:
        """Estimate quantile.
        
        Args:
            q: Quantile to estimate (0.0 to 1.0)
              0.5 = median, 0.95 = 95th percentile, 0.99 = 99th percentile
        
        Returns:
            Estimated value at quantile q
        
        Raises:
            ValueError: If q not in [0, 1] or digest is empty
        """
        if not 0 <= q <= 1:
            raise ValueError("Quantile must be between 0 and 1")
        
        if self.count == 0:
            raise ValueError("Cannot compute quantile of empty digest")
        
        # Ensure all values are merged
        self._compress()
        
        if len(self.centroids) == 0:
            raise ValueError("No centroids available")
        
        if len(self.centroids) == 1:
            return self.centroids[0].mean
        
        # Find quantile by interpolating through centroids
        index = q * self.count
        
        weight_so_far = 0
        for i, centroid in enumerate(self.centroids):
            weight_so_far += centroid.weight
            
            if weight_so_far >= index:
                # Found the centroid containing the quantile
                if i == 0:
                    return centroid.mean
                
                # Interpolate between this and previous centroid
                prev_centroid = self.centroids[i - 1]
                prev_weight = weight_so_far - centroid.weight
                
                # Linear interpolation
                delta = index - prev_weight
                frac = delta / centroid.weight if centroid.weight > 0 else 0
                
                return prev_centroid.mean + frac * (centroid.mean - prev_centroid.mean)
        
        # Edge case: return last centroid
        return self.centroids[-1].mean
    
    def percentile(self, p: float) -> float:
        """Estimate percentile (convenience wrapper for quantile).
        
        Args:
            p: Percentile (0-100), e.g., 50 for median, 99 for p99
        
        Returns:
            Estimated value at percentile p
        """
        return self.quantile(p / 100.0)
    
    def cdf(self, value: float) -> float:
        """Estimate cumulative distribution function at value.
        
        Returns the fraction of values <= value.
        
        Args:
            value: Value to query
        
        Returns:
            Estimated fraction (0.0 to 1.0)
        """
        if self.count == 0:
            return 0.0
        
        self._compress()
        
        if len(self.centroids) == 0:
            return 0.0
        
        if value < self.centroids[0].mean:
            return 0.0
        
        if value > self.centroids[-1].mean:
            return 1.0
        
        # Find centroids around value
        weight_so_far = 0
        for i, centroid in enumerate(self.centroids):
            if centroid.mean > value:
                if i == 0:
                    return 0.0
                
                # Interpolate
                prev_centroid = self.centroids[i - 1]
                frac = (value - prev_centroid.mean) / (centroid.mean - prev_centroid.mean) if centroid.mean != prev_centroid.mean else 0.5
                
                return (weight_so_far - centroid.weight + frac * centroid.weight) / self.count
            
            weight_so_far += centroid.weight
        
        return weight_so_far / self.count
    
    def combine(self, other: 'TDigest') -> 'TDigest':
        """Combine two T-Digests (monoid operation).
        
        Args:
            other: Another TDigest
        
        Returns:
            New TDigest with merged data
        """
        # Use average compression
        compression = (self.compression + other.compression) // 2
        
        # Merge centroids
        merged_centroids = self.centroids + other.centroids + self._unmerged + other._unmerged
        
        result = TDigest(compression=compression, centroids=merged_centroids)
        result._compress()
        
        return result
    
    def __add__(self, other: 'TDigest') -> 'TDigest':
        """Combine using + operator (monoid operation)."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support sum() builtin."""
        if other == 0:
            return self
        return self.__add__(other)
    
    def __eq__(self, other) -> bool:
        """Check equality based on centroids."""
        if not isinstance(other, TDigest):
            return False
        
        # Compress both first
        self._compress()
        other._compress()
        
        if len(self.centroids) != len(other.centroids):
            return False
        
        for c1, c2 in zip(self.centroids, other.centroids):
            if abs(c1.mean - c2.mean) > 1e-9 or abs(c1.weight - c2.weight) > 1e-9:
                return False
        
        return True
    
    def __repr__(self) -> str:
        """String representation."""
        return f"TDigest(compression={self.compression}, centroids={len(self.centroids)}, count={self.count:.0f})"
    
    @property
    def zero(self) -> 'TDigest':
        """Monoid identity: empty TDigest."""
        return TDigest(compression=self.compression)
    
    def is_zero(self) -> bool:
        """Check if this is the zero element."""
        return self.count == 0
    
    def min(self) -> float:
        """Get minimum value."""
        if len(self.centroids) == 0 and len(self._unmerged) == 0:
            raise ValueError("Cannot get min of empty digest")
        
        all_centroids = self.centroids + self._unmerged
        return min(c.mean for c in all_centroids)
    
    def max(self) -> float:
        """Get maximum value."""
        if len(self.centroids) == 0 and len(self._unmerged) == 0:
            raise ValueError("Cannot get max of empty digest")
        
        all_centroids = self.centroids + self._unmerged
        return max(c.mean for c in all_centroids)
    
    def _compress(self) -> None:
        """Compress centroids to maintain size limit.
        
        Uses scale function to determine centroid sizes.
        Smaller centroids at tails for better accuracy.
        """
        if len(self._unmerged) == 0:
            return
        
        # Merge unmerged with existing centroids
        all_centroids = sorted(self.centroids + self._unmerged, key=lambda c: c.mean)
        self._unmerged = []
        
        if len(all_centroids) <= self.compression:
            self.centroids = all_centroids
            return
        
        # Compress using scale function
        compressed = []
        current = None
        weight_so_far = 0
        
        for centroid in all_centroids:
            if current is None:
                current = centroid
                weight_so_far = centroid.weight
            else:
                # Check if we should merge with current
                q = (weight_so_far + current.weight / 2) / self.count
                k = self._scale_function(q)
                max_weight = self.count * k / self.compression
                
                if current.weight + centroid.weight <= max_weight:
                    # Merge
                    total_weight = current.weight + centroid.weight
                    current = Centroid(
                        mean=(current.mean * current.weight + centroid.mean * centroid.weight) / total_weight,
                        weight=total_weight
                    )
                else:
                    # Start new centroid
                    compressed.append(current)
                    weight_so_far += current.weight
                    current = centroid
        
        if current is not None:
            compressed.append(current)
        
        self.centroids = compressed
    
    def _scale_function(self, q: float) -> float:
        """Scale function for centroid sizing.
        
        Returns smaller values at tails (q near 0 or 1) for better accuracy.
        
        Args:
            q: Quantile (0 to 1)
        
        Returns:
            Scale factor
        """
        # k(q) = 2*arcsin(2q-1) / π  (gives smaller centroids at tails)
        # Simplified version: k(q) = q(1-q)
        return 4 * q * (1 - q) + 0.01  # +0.01 to avoid division by zero


# Convenience functions
def estimate_quantiles(
    values: List[float],
    quantiles: List[float],
    compression: int = 100
) -> List[float]:
    """Estimate multiple quantiles from a list of values.
    
    Args:
        values: List of values
        quantiles: List of quantiles to estimate (0.0 to 1.0)
        compression: T-Digest compression parameter
    
    Returns:
        List of estimated quantile values
    
    Examples:
        >>> latencies = [10, 20, 30, ..., 1000]
        >>> p50, p95, p99 = estimate_quantiles(latencies, [0.50, 0.95, 0.99])
    """
    td = TDigest(compression=compression)
    for value in values:
        td.add(value)
    
    return [td.quantile(q) for q in quantiles]


def merge_tdigests(digests: List[TDigest]) -> TDigest:
    """Merge multiple T-Digests using sum().
    
    Args:
        digests: List of TDigests
    
    Returns:
        Merged TDigest
    
    Examples:
        >>> td1, td2, td3 = [TDigest(100) for _ in range(3)]
        >>> merged = merge_tdigests([td1, td2, td3])
    """
    if not digests:
        raise ValueError("Cannot merge empty list of TDigests")
    
    return sum(digests)
