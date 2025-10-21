"""Bloom Filter: Probabilistic membership testing.

A Bloom Filter tests whether an element is in a set with possible false
positives but no false negatives. It uses minimal memory (~10 bits per element)
compared to storing the actual set.

Perfect for: spam filtering, database query optimization, web crawlers.

References:
- Burton H. Bloom "Space/time trade-offs in hash coding with allowable errors" (1970)
- https://en.wikipedia.org/wiki/Bloom_filter
"""

import hashlib
import math
from typing import Any, Hashable, List, Optional


class BloomFilter:
    """Bloom Filter for probabilistic membership testing.
    
    A Bloom Filter uses a bit array and multiple hash functions to
    efficiently test set membership.
    
    - Space: ~10 bits per element
    - False positives: Possible (configurable rate)
    - False negatives: Never occur
    - Monoid: Bitwise OR of bit arrays
    
    Memory calculation:
        m = -(n * ln(p)) / (ln(2)^2)  (number of bits)
        k = (m/n) * ln(2)              (number of hash functions)
    
    where n = expected elements, p = false positive rate
    
    Examples:
        >>> bf = BloomFilter(capacity=10000, error_rate=0.01)
        >>> bf.add("spam@example.com")
        >>> "spam@example.com" in bf  # True
        >>> "legit@example.com" in bf  # False (or 1% chance True)
        
        >>> # Distributed merge
        >>> bf1 = BloomFilter(1000, 0.01)
        >>> bf2 = BloomFilter(1000, 0.01)
        >>> merged = bf1 + bf2  # Monoid operation
    """
    
    def __init__(
        self,
        capacity: int,
        error_rate: float = 0.01,
        bit_array: Optional[List[int]] = None
    ):
        """Initialize Bloom Filter.
        
        Args:
            capacity: Expected number of elements
            error_rate: Desired false positive rate (0.0-1.0)
            bit_array: Internal bit array (for deserialization)
        
        Raises:
            ValueError: If capacity <= 0 or error_rate not in (0, 1)
        """
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        if not 0 < error_rate < 1:
            raise ValueError("Error rate must be between 0 and 1")
        
        self.capacity = capacity
        self.error_rate = error_rate
        
        # Calculate optimal bit array size
        self.size = self._optimal_size(capacity, error_rate)
        
        # Calculate optimal number of hash functions
        self.num_hashes = self._optimal_hash_count(self.size, capacity)
        
        # Bit array (using integers for efficiency)
        # Each int stores 64 bits
        self.num_words = (self.size + 63) // 64
        if bit_array is not None:
            if len(bit_array) != self.num_words:
                raise ValueError("Bit array size mismatch")
            self.bits = bit_array
        else:
            self.bits = [0] * self.num_words
        
        self.count = 0  # Approximate number of items added
    
    def add(self, item: Hashable) -> None:
        """Add an item to the Bloom Filter.
        
        Args:
            item: Hashable value to add
        """
        for bit_index in self._hash_indices(item):
            word_index = bit_index // 64
            bit_offset = bit_index % 64
            self.bits[word_index] |= (1 << bit_offset)
        
        self.count += 1
    
    def contains(self, item: Hashable) -> bool:
        """Check if item might be in the set.
        
        Args:
            item: Hashable value to check
        
        Returns:
            True if item might be in set (or false positive)
            False if item is definitely not in set
        """
        for bit_index in self._hash_indices(item):
            word_index = bit_index // 64
            bit_offset = bit_index % 64
            if not (self.bits[word_index] & (1 << bit_offset)):
                return False
        return True
    
    def __contains__(self, item: Hashable) -> bool:
        """Enable 'in' operator."""
        return self.contains(item)
    
    def combine(self, other: 'BloomFilter') -> 'BloomFilter':
        """Combine two Bloom Filters (monoid operation).
        
        Merges by taking bitwise OR of bit arrays.
        
        Args:
            other: Another BloomFilter with same size/hash count
        
        Returns:
            New BloomFilter with merged bit arrays
        
        Raises:
            ValueError: If filters have different parameters
        """
        if self.size != other.size:
            raise ValueError(
                f"Cannot combine Bloom Filters with different sizes: "
                f"{self.size} vs {other.size}"
            )
        if self.num_hashes != other.num_hashes:
            raise ValueError(
                f"Cannot combine Bloom Filters with different hash counts: "
                f"{self.num_hashes} vs {other.num_hashes}"
            )
        
        # Bitwise OR of all words
        merged_bits = [
            a | b for a, b in zip(self.bits, other.bits)
        ]
        
        result = BloomFilter(
            capacity=self.capacity,
            error_rate=self.error_rate,
            bit_array=merged_bits
        )
        result.count = self.count + other.count
        return result
    
    def __add__(self, other: 'BloomFilter') -> 'BloomFilter':
        """Combine using + operator (monoid operation)."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support sum() builtin."""
        if other == 0:
            return self
        return self.__add__(other)
    
    def __eq__(self, other: Any) -> bool:
        """Check equality based on bit array."""
        if not isinstance(other, BloomFilter):
            return False
        return (
            self.size == other.size and
            self.num_hashes == other.num_hashes and
            self.bits == other.bits
        )
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"BloomFilter(capacity={self.capacity}, "
            f"error_rate={self.error_rate:.4f}, "
            f"items≈{self.count})"
        )
    
    @property
    def zero(self) -> 'BloomFilter':
        """Monoid identity: empty Bloom Filter."""
        return BloomFilter(self.capacity, self.error_rate)
    
    def is_zero(self) -> bool:
        """Check if this is the zero element."""
        return all(word == 0 for word in self.bits)
    
    def saturation(self) -> float:
        """Calculate saturation (fraction of bits set).
        
        Returns:
            Fraction of bits set (0.0-1.0)
        """
        bits_set = sum(bin(word).count('1') for word in self.bits)
        return bits_set / self.size
    
    def expected_fpr(self) -> float:
        """Calculate expected false positive rate given current saturation.
        
        Returns:
            Expected false positive probability
        """
        saturation = self.saturation()
        return (1 - math.exp(-self.num_hashes * saturation)) ** self.num_hashes
    
    def _hash_indices(self, item: Hashable) -> List[int]:
        """Generate k hash values for an item.
        
        Uses double hashing technique with SHA-256.
        
        Args:
            item: Item to hash
        
        Returns:
            List of k bit indices
        """
        # Generate two independent hashes
        h1 = self._hash(item, 0)
        h2 = self._hash(item, 1)
        
        # Use double hashing: h(i) = h1 + i*h2
        indices = []
        for i in range(self.num_hashes):
            index = (h1 + i * h2) % self.size
            indices.append(index)
        
        return indices
    
    def _hash(self, item: Hashable, seed: int) -> int:
        """Hash an item with a seed.
        
        Args:
            item: Item to hash
            seed: Seed value for hash function
        
        Returns:
            Hash value
        """
        data = f"{seed}:{item}".encode('utf-8')
        h = hashlib.sha256(data).digest()
        return int.from_bytes(h[:8], byteorder='big')
    
    @staticmethod
    def _optimal_size(n: int, p: float) -> int:
        """Calculate optimal bit array size.
        
        Formula: m = -(n * ln(p)) / (ln(2)^2)
        
        Args:
            n: Expected number of elements
            p: Desired false positive rate
        
        Returns:
            Optimal bit array size
        """
        m = -(n * math.log(p)) / (math.log(2) ** 2)
        return int(math.ceil(m))
    
    @staticmethod
    def _optimal_hash_count(m: int, n: int) -> int:
        """Calculate optimal number of hash functions.
        
        Formula: k = (m/n) * ln(2)
        
        Args:
            m: Bit array size
            n: Expected number of elements
        
        Returns:
            Optimal number of hash functions
        """
        k = (m / n) * math.log(2)
        return int(math.ceil(k))


# Convenience functions
def create_bloom_filter(
    items: List[Hashable],
    capacity: Optional[int] = None,
    error_rate: float = 0.01
) -> BloomFilter:
    """Create and populate a Bloom Filter from items.
    
    Args:
        items: List of items to add
        capacity: Expected capacity (defaults to len(items))
        error_rate: Desired false positive rate
    
    Returns:
        Populated BloomFilter
    
    Examples:
        >>> emails = ['spam1@bad.com', 'spam2@bad.com']
        >>> bf = create_bloom_filter(emails, error_rate=0.01)
        >>> 'spam1@bad.com' in bf  # True
    """
    if capacity is None:
        capacity = max(len(items), 1)
    
    bf = BloomFilter(capacity, error_rate)
    for item in items:
        bf.add(item)
    return bf


def merge_bloom_filters(filters: List[BloomFilter]) -> BloomFilter:
    """Merge multiple Bloom Filters using sum().
    
    Args:
        filters: List of BloomFilters with same parameters
    
    Returns:
        Merged BloomFilter
    
    Examples:
        >>> bf1, bf2 = BloomFilter(1000, 0.01), BloomFilter(1000, 0.01)
        >>> merged = merge_bloom_filters([bf1, bf2])
    """
    if not filters:
        raise ValueError("Cannot merge empty list of Bloom Filters")
    
    return sum(filters)
