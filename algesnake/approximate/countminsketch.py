"""Count-Min Sketch: Probabilistic frequency estimation.

Count-Min Sketch (CMS) estimates the frequency of items in a data stream
using sublinear space. It provides conservative estimates (never underestimates)
with configurable accuracy and error bounds.

Perfect for: word counting, heavy hitters, frequency queries at scale.

References:
- Cormode and Muthukrishnan "An Improved Data Stream Summary: The Count-Min 
  Sketch and its Applications" (2005)
- https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch
"""

import hashlib
import math
from typing import Any, Hashable, Dict, List, Optional, Tuple


class CountMinSketch:
    """Count-Min Sketch for frequency estimation.
    
    Uses a 2D array of counters (depth × width) with multiple hash functions
    to estimate item frequencies with probabilistic guarantees.
    
    Guarantees:
    - Never underestimates (actual_freq <= estimate)
    - Error bound: estimate <= actual_freq + ε × N with probability 1-δ
      where N is total items added
    
    Parameters:
    - width (w): Number of counters per row
    - depth (d): Number of hash functions / rows
    
    Sizing formulas:
    - width = ceil(e / ε) where ε is relative error
    - depth = ceil(ln(1/δ)) where δ is failure probability
    
    This implementation forms a monoid:
    - Identity: Empty CMS (all counters = 0)
    - Combine: Element-wise sum of counter arrays
    
    Examples:
        >>> cms = CountMinSketch(width=1000, depth=5)
        >>> cms.add("apple", count=5)
        >>> cms.add("banana", count=3)
        >>> print(cms.estimate("apple"))  # >= 5
        
        >>> # Distributed aggregation
        >>> cms1 = CountMinSketch(1000, 5)
        >>> cms2 = CountMinSketch(1000, 5)
        >>> combined = cms1 + cms2  # Monoid merge
    """
    
    def __init__(
        self,
        width: int,
        depth: int,
        counters: Optional[List[List[int]]] = None
    ):
        """Initialize Count-Min Sketch.
        
        Args:
            width: Number of counters per row (typically 1000-10000)
            depth: Number of hash functions (typically 4-7)
            counters: Internal counter array (for deserialization)
        
        Raises:
            ValueError: If width or depth are not positive
        """
        if width <= 0:
            raise ValueError("Width must be positive")
        if depth <= 0:
            raise ValueError("Depth must be positive")
        
        self.width = width
        self.depth = depth
        
        # Counter array: depth rows × width columns
        if counters is not None:
            if len(counters) != depth or any(len(row) != width for row in counters):
                raise ValueError("Counter array dimensions mismatch")
            self.counters = counters
        else:
            self.counters = [[0] * width for _ in range(depth)]
        
        self.total_count = sum(sum(row) for row in self.counters) // depth
    
    @classmethod
    def from_error_rate(cls, epsilon: float, delta: float) -> 'CountMinSketch':
        """Create CMS with desired error bounds.
        
        Args:
            epsilon: Relative error (e.g., 0.01 for 1% error)
            delta: Failure probability (e.g., 0.01 for 99% confidence)
        
        Returns:
            CountMinSketch configured for error bounds
        
        Examples:
            >>> cms = CountMinSketch.from_error_rate(epsilon=0.01, delta=0.01)
            >>> # Guarantees: estimate <= actual + 0.01*total with 99% confidence
        """
        width = int(math.ceil(math.e / epsilon))
        depth = int(math.ceil(math.log(1 / delta)))
        return cls(width, depth)
    
    def add(self, item: Hashable, count: int = 1) -> None:
        """Add item to the sketch with optional count.
        
        Args:
            item: Hashable value to add
            count: Number of occurrences to add (default 1)
        """
        for row in range(self.depth):
            col = self._hash(item, row) % self.width
            self.counters[row][col] += count
        
        self.total_count += count
    
    def estimate(self, item: Hashable) -> int:
        """Estimate frequency of an item.
        
        Returns the minimum count across all hash positions.
        Guaranteed to never underestimate (actual <= estimate).
        
        Args:
            item: Item to query
        
        Returns:
            Estimated frequency (>= actual frequency)
        """
        estimates = []
        for row in range(self.depth):
            col = self._hash(item, row) % self.width
            estimates.append(self.counters[row][col])
        
        return min(estimates)
    
    def combine(self, other: 'CountMinSketch') -> 'CountMinSketch':
        """Combine two Count-Min Sketches (monoid operation).
        
        Merges by element-wise addition of counter arrays.
        
        Args:
            other: Another CMS with same dimensions
        
        Returns:
            New CMS with merged counters
        
        Raises:
            ValueError: If dimensions don't match
        """
        if self.width != other.width:
            raise ValueError(
                f"Cannot combine CMS with different widths: "
                f"{self.width} vs {other.width}"
            )
        if self.depth != other.depth:
            raise ValueError(
                f"Cannot combine CMS with different depths: "
                f"{self.depth} vs {other.depth}"
            )
        
        # Element-wise sum of counter arrays
        merged_counters = [
            [a + b for a, b in zip(row_a, row_b)]
            for row_a, row_b in zip(self.counters, other.counters)
        ]
        
        result = CountMinSketch(self.width, self.depth, merged_counters)
        result.total_count = self.total_count + other.total_count
        return result
    
    def __add__(self, other: 'CountMinSketch') -> 'CountMinSketch':
        """Combine using + operator (monoid operation)."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support sum() builtin."""
        if other == 0:
            return self
        return self.__add__(other)
    
    def __eq__(self, other: Any) -> bool:
        """Check equality based on counters."""
        if not isinstance(other, CountMinSketch):
            return False
        return (
            self.width == other.width and
            self.depth == other.depth and
            self.counters == other.counters
        )
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"CountMinSketch(width={self.width}, depth={self.depth}, "
            f"total={self.total_count})"
        )
    
    @property
    def zero(self) -> 'CountMinSketch':
        """Monoid identity: empty CMS."""
        return CountMinSketch(self.width, self.depth)
    
    def is_zero(self) -> bool:
        """Check if this is the zero element."""
        return all(count == 0 for row in self.counters for count in row)
    
    def top_k_estimates(self, items: List[Hashable]) -> List[Tuple[Hashable, int]]:
        """Get frequency estimates for a list of items.
        
        Args:
            items: List of items to query
        
        Returns:
            List of (item, estimated_frequency) tuples, sorted by frequency
        """
        estimates = [(item, self.estimate(item)) for item in items]
        return sorted(estimates, key=lambda x: x[1], reverse=True)
    
    def _hash(self, item: Hashable, seed: int) -> int:
        """Hash an item with a seed.
        
        Args:
            item: Item to hash
            seed: Seed value for hash function (row index)
        
        Returns:
            Hash value
        """
        data = f"{seed}:{item}".encode('utf-8')
        h = hashlib.sha256(data).digest()
        return int.from_bytes(h[:8], byteorder='big')


# Convenience functions
def count_frequencies(
    items: List[Hashable],
    width: int = 1000,
    depth: int = 5
) -> CountMinSketch:
    """Create and populate a CMS from items.
    
    Args:
        items: List of items to count
        width: CMS width parameter
        depth: CMS depth parameter
    
    Returns:
        Populated CountMinSketch
    
    Examples:
        >>> words = ["the", "quick", "brown", "the", "fox"]
        >>> cms = count_frequencies(words)
        >>> print(cms.estimate("the"))  # 2
    """
    cms = CountMinSketch(width, depth)
    for item in items:
        cms.add(item)
    return cms


def merge_cms(sketches: List[CountMinSketch]) -> CountMinSketch:
    """Merge multiple Count-Min Sketches using sum().
    
    Args:
        sketches: List of CMS with same dimensions
    
    Returns:
        Merged CountMinSketch
    
    Examples:
        >>> cms1, cms2 = CountMinSketch(1000, 5), CountMinSketch(1000, 5)
        >>> merged = merge_cms([cms1, cms2])
    """
    if not sketches:
        raise ValueError("Cannot merge empty list of CMS")
    
    return sum(sketches)


def heavy_hitters(
    cms: CountMinSketch,
    items: List[Hashable],
    threshold: int
) -> List[Tuple[Hashable, int]]:
    """Find items with frequency above threshold.
    
    Args:
        cms: Populated CountMinSketch
        items: Candidate items to check
        threshold: Minimum frequency threshold
    
    Returns:
        List of (item, estimated_frequency) tuples above threshold
    
    Examples:
        >>> cms = count_frequencies(["a"]*100 + ["b"]*50 + ["c"]*10)
        >>> hitters = heavy_hitters(cms, ["a", "b", "c"], threshold=40)
        >>> # Returns [("a", 100), ("b", 50)]
    """
    results = []
    for item in items:
        freq = cms.estimate(item)
        if freq >= threshold:
            results.append((item, freq))
    
    return sorted(results, key=lambda x: x[1], reverse=True)
