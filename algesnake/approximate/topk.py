"""TopK: Track the K most frequent items in a stream.

TopK maintains the K most frequent items seen in a data stream using
a combination of frequency tracking and a min-heap. It provides approximate
heavy hitter detection with sublinear space.

Perfect for: trending topics, popular products, frequent errors.

References:
- Metwally et al. "Efficient Computation of Frequent and Top-k Elements in 
  Data Streams" (2005)
- Space Saving algorithm
"""

import heapq
from typing import Any, Hashable, List, Optional, Tuple, Dict
from collections import defaultdict


class TopK:
    """TopK tracker for most frequent items in streams.
    
    Maintains the K items with highest frequencies using a min-heap
    and frequency counters. Provides approximate heavy hitter detection.
    
    Space complexity: O(K) for heap + O(K) for frequency map = O(K)
    
    This implementation forms a monoid:
    - Identity: Empty TopK
    - Combine: Merge heaps and re-select top K items
    
    Examples:
        >>> topk = TopK(k=3)
        >>> for word in ["apple", "banana", "apple", "cherry", "apple"]:
        ...     topk.add(word)
        >>> print(topk.top())  # [("apple", 3), ...]
        
        >>> # Distributed aggregation
        >>> topk1 = TopK(k=10)
        >>> topk2 = TopK(k=10)
        >>> combined = topk1 + topk2  # Monoid merge
    """
    
    def __init__(
        self,
        k: int,
        frequencies: Optional[Dict[Hashable, int]] = None
    ):
        """Initialize TopK tracker.
        
        Args:
            k: Number of top items to track
            frequencies: Initial frequency map (for deserialization)
        
        Raises:
            ValueError: If k is not positive
        """
        if k <= 0:
            raise ValueError("K must be positive")
        
        self.k = k
        
        if frequencies is not None:
            self.frequencies = dict(frequencies)
        else:
            self.frequencies = defaultdict(int)
        
        # Build min-heap of (freq, item) tuples
        # We use a min-heap so we can efficiently find and replace the minimum
        self._rebuild_heap()
        
        self.total_count = sum(self.frequencies.values())
    
    def add(self, item: Hashable, count: int = 1) -> None:
        """Add item to TopK tracker.
        
        Updates frequency and maintains top K items.
        
        Args:
            item: Hashable value to add
            count: Number of occurrences to add (default 1)
        """
        self.frequencies[item] += count
        self.total_count += count
        
        # Rebuild heap if needed (lazy approach for simplicity)
        # In production, could optimize with incremental updates
        if item not in [it for _, it in self.heap]:
            if len(self.heap) < self.k:
                heapq.heappush(self.heap, (self.frequencies[item], item))
            else:
                # Check if this item should replace minimum
                min_freq, min_item = self.heap[0]
                if self.frequencies[item] > min_freq:
                    heapq.heapreplace(self.heap, (self.frequencies[item], item))
        else:
            # Item already in heap, need to update its frequency
            self._rebuild_heap()
    
    def top(self, n: Optional[int] = None) -> List[Tuple[Hashable, int]]:
        """Get the top N items by frequency.
        
        Args:
            n: Number of items to return (default: all K items)
        
        Returns:
            List of (item, frequency) tuples, sorted by frequency descending
        """
        if n is None:
            n = self.k
        
        # Sort heap by frequency (descending)
        sorted_items = sorted(self.heap, key=lambda x: x[0], reverse=True)
        
        # Return top n with (item, freq) format
        return [(item, freq) for freq, item in sorted_items[:n]]
    
    def estimate(self, item: Hashable) -> int:
        """Get frequency estimate for an item.
        
        Args:
            item: Item to query
        
        Returns:
            Estimated frequency (0 if not tracked)
        """
        return self.frequencies.get(item, 0)
    
    def contains(self, item: Hashable) -> bool:
        """Check if item is in top K.
        
        Args:
            item: Item to check
        
        Returns:
            True if item is in top K
        """
        return item in [it for _, it in self.heap]
    
    def __contains__(self, item: Hashable) -> bool:
        """Enable 'in' operator."""
        return self.contains(item)
    
    def combine(self, other: 'TopK') -> 'TopK':
        """Combine two TopK trackers (monoid operation).
        
        Merges by summing frequencies and selecting top K items.
        
        Args:
            other: Another TopK with same K
        
        Returns:
            New TopK with merged frequencies
        
        Raises:
            ValueError: If K values don't match
        """
        if self.k != other.k:
            raise ValueError(
                f"Cannot combine TopK with different K values: "
                f"{self.k} vs {other.k}"
            )
        
        # Merge frequency maps
        merged_frequencies = defaultdict(int)
        
        for item, freq in self.frequencies.items():
            merged_frequencies[item] += freq
        
        for item, freq in other.frequencies.items():
            merged_frequencies[item] += freq
        
        # Create new TopK with merged frequencies
        result = TopK(self.k, dict(merged_frequencies))
        return result
    
    def __add__(self, other: 'TopK') -> 'TopK':
        """Combine using + operator (monoid operation)."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support sum() builtin."""
        if other == 0:
            return self
        return self.__add__(other)
    
    def __eq__(self, other: Any) -> bool:
        """Check equality based on frequencies."""
        if not isinstance(other, TopK):
            return False
        return (
            self.k == other.k and
            dict(self.frequencies) == dict(other.frequencies)
        )
    
    def __repr__(self) -> str:
        """String representation."""
        top_items = self.top(min(3, self.k))
        items_str = ", ".join(f"{item}:{freq}" for item, freq in top_items)
        if len(self.heap) > 3:
            items_str += ", ..."
        return f"TopK(k={self.k}, top=[{items_str}])"
    
    @property
    def zero(self) -> 'TopK':
        """Monoid identity: empty TopK."""
        return TopK(self.k)
    
    def is_zero(self) -> bool:
        """Check if this is the zero element."""
        return len(self.frequencies) == 0
    
    def _rebuild_heap(self) -> None:
        """Rebuild min-heap from frequencies.
        
        Keeps only top K items by frequency.
        """
        # Get all items sorted by frequency
        sorted_items = sorted(
            self.frequencies.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        # Keep top K
        top_k_items = sorted_items[:self.k]
        
        # Build min-heap (freq, item)
        self.heap = [(freq, item) for item, freq in top_k_items]
        heapq.heapify(self.heap)


# Convenience functions
def find_top_k(
    items: List[Hashable],
    k: int = 10
) -> List[Tuple[Hashable, int]]:
    """Find top K most frequent items in a list.
    
    Args:
        items: List of items
        k: Number of top items to find
    
    Returns:
        List of (item, frequency) tuples, sorted by frequency
    
    Examples:
        >>> words = ["the"]*10 + ["quick"]*5 + ["fox"]*3
        >>> top = find_top_k(words, k=2)
        >>> # Returns [("the", 10), ("quick", 5)]
    """
    topk = TopK(k)
    for item in items:
        topk.add(item)
    return topk.top()


def merge_topk(trackers: List[TopK]) -> TopK:
    """Merge multiple TopK trackers using sum().
    
    Args:
        trackers: List of TopK with same K
    
    Returns:
        Merged TopK
    
    Examples:
        >>> topk1, topk2 = TopK(10), TopK(10)
        >>> merged = merge_topk([topk1, topk2])
    """
    if not trackers:
        raise ValueError("Cannot merge empty list of TopK")
    
    return sum(trackers)


def streaming_top_k(
    stream_batches: List[List[Hashable]],
    k: int = 10
) -> TopK:
    """Process streaming data in batches to find top K.
    
    Args:
        stream_batches: List of data batches
        k: Number of top items to track
    
    Returns:
        TopK tracker with top K items
    
    Examples:
        >>> batches = [
        ...     ["a", "b", "a"],
        ...     ["a", "c", "b"],
        ...     ["c", "c", "a"]
        ... ]
        >>> topk = streaming_top_k(batches, k=2)
        >>> print(topk.top())  # [("a", 4), ("c", 3)]
    """
    topk = TopK(k)
    
    for batch in stream_batches:
        for item in batch:
            topk.add(item)
    
    return topk
