"""MinHash: Probabilistic Jaccard similarity estimation.

MinHash estimates the Jaccard similarity between sets using a compact signature.
It's perfect for finding similar documents, images, or any set-based data.

Jaccard similarity: J(A, B) = |A ∩ B| / |A ∪ B|
MinHash property: P(min_hash(A) = min_hash(B)) = J(A, B)

Typical accuracy: ±0.01 error with 128-256 hash functions.

References:
- Broder, Andrei Z. "On the resemblance and containment of documents" (1997)
- https://en.wikipedia.org/wiki/MinHash
"""

import hashlib
import random
from typing import Any, Hashable, List, Set, Iterable, Optional


class MinHash:
    """MinHash signature for Jaccard similarity estimation.

    A MinHash computes k minimum hash values across a set to create
    a compact signature. The fraction of matching values approximates
    Jaccard similarity.

    - Space: O(k) integers (e.g., 128 hashes = 1KB)
    - Accuracy: ~1/sqrt(k) standard error
    - Monoid: Element-wise minimum of signatures

    Standard error: 1 / sqrt(k)
    For k=128: error ≈ 8.8%
    For k=256: error ≈ 6.3%

    Examples:
        >>> # Create signatures for two documents
        >>> mh1 = MinHash(num_perm=128)
        >>> mh2 = MinHash(num_perm=128)
        >>>
        >>> for word in "the quick brown fox".split():
        ...     mh1.update(word)
        >>>
        >>> for word in "the quick brown dog".split():
        ...     mh2.update(word)
        >>>
        >>> # Estimate Jaccard similarity
        >>> similarity = mh1.jaccard(mh2)
        >>> print(f"Similarity: {similarity:.2f}")

        >>> # Distributed aggregation
        >>> mh = mh1 + mh2  # Equivalent to union of sets
    """

    # Maximum hash value (32-bit)
    _MAX_HASH = (1 << 32) - 1

    def __init__(
        self,
        num_perm: int = 128,
        seed: int = 1,
        hashvalues: Optional[List[int]] = None
    ):
        """Initialize MinHash.

        Args:
            num_perm: Number of permutation hash functions (k).
                     Higher = more accurate but more memory.
                     Common values: 64, 128, 256
            seed: Random seed for hash function generation
            hashvalues: Pre-existing hash values (for deserialization)

        Raises:
            ValueError: If num_perm < 1
        """
        if num_perm < 1:
            raise ValueError("num_perm must be at least 1")

        self.num_perm = num_perm
        self.seed = seed

        # Initialize hash values to maximum (identity for min)
        if hashvalues is not None:
            if len(hashvalues) != num_perm:
                raise ValueError(f"Expected {num_perm} hash values, got {len(hashvalues)}")
            self.hashvalues = list(hashvalues)
        else:
            self.hashvalues = [self._MAX_HASH] * num_perm

        # Generate hash functions (a and b parameters for universal hashing)
        random.seed(seed)
        self._hash_params = [
            (random.randint(0, self._MAX_HASH), random.randint(0, self._MAX_HASH))
            for _ in range(num_perm)
        ]

    def update(self, item: Hashable) -> None:
        """Add an item to the MinHash signature.

        Args:
            item: Hashable value (word, shingle, feature, etc.)
        """
        # Hash the item once
        h = self._hash(item)

        # Apply k permutations and update minimums
        for i, (a, b) in enumerate(self._hash_params):
            # Universal hashing: h'(x) = (a*h(x) + b) mod prime
            permuted = (a * h + b) & self._MAX_HASH
            self.hashvalues[i] = min(self.hashvalues[i], permuted)

    def update_batch(self, items: Iterable[Hashable]) -> None:
        """Add multiple items to the MinHash signature.

        Args:
            items: Iterable of hashable values
        """
        for item in items:
            self.update(item)

    def jaccard(self, other: 'MinHash') -> float:
        """Estimate Jaccard similarity with another MinHash.

        Args:
            other: Another MinHash with same num_perm and seed

        Returns:
            Estimated Jaccard similarity (0.0-1.0)

        Raises:
            ValueError: If MinHash parameters don't match
        """
        if self.num_perm != other.num_perm:
            raise ValueError(
                f"Cannot compare MinHashes with different num_perm: "
                f"{self.num_perm} vs {other.num_perm}"
            )
        if self.seed != other.seed:
            raise ValueError(
                f"Cannot compare MinHashes with different seeds: "
                f"{self.seed} vs {other.seed}"
            )

        # Count matching hash values
        matches = sum(
            1 for a, b in zip(self.hashvalues, other.hashvalues) if a == b
        )

        return matches / self.num_perm

    def combine(self, other: 'MinHash') -> 'MinHash':
        """Combine two MinHashes (monoid operation = set union).

        Takes element-wise minimum of hash values, equivalent to
        computing MinHash of the union of both sets.

        Args:
            other: Another MinHash with same parameters

        Returns:
            New MinHash representing union of both sets

        Raises:
            ValueError: If MinHash parameters don't match
        """
        if self.num_perm != other.num_perm:
            raise ValueError(
                f"Cannot combine MinHashes with different num_perm: "
                f"{self.num_perm} vs {other.num_perm}"
            )
        if self.seed != other.seed:
            raise ValueError(
                f"Cannot combine MinHashes with different seeds: "
                f"{self.seed} vs {other.seed}"
            )

        # Element-wise minimum
        merged_values = [
            min(a, b) for a, b in zip(self.hashvalues, other.hashvalues)
        ]

        return MinHash(
            num_perm=self.num_perm,
            seed=self.seed,
            hashvalues=merged_values
        )

    def __add__(self, other: 'MinHash') -> 'MinHash':
        """Combine using + operator (monoid operation)."""
        return self.combine(other)

    def __radd__(self, other):
        """Support sum() builtin."""
        if other == 0:
            return self
        return self.__add__(other)

    def __eq__(self, other: Any) -> bool:
        """Check equality based on hash values."""
        if not isinstance(other, MinHash):
            return False
        return (
            self.num_perm == other.num_perm and
            self.seed == other.seed and
            self.hashvalues == other.hashvalues
        )

    def __repr__(self) -> str:
        """String representation."""
        return f"MinHash(num_perm={self.num_perm}, seed={self.seed})"

    @property
    def zero(self) -> 'MinHash':
        """Monoid identity: empty MinHash (all max values)."""
        return MinHash(num_perm=self.num_perm, seed=self.seed)

    def is_zero(self) -> bool:
        """Check if this is the zero element (no items added)."""
        return all(h == self._MAX_HASH for h in self.hashvalues)

    def is_empty(self) -> bool:
        """Alias for is_zero()."""
        return self.is_zero()

    def copy(self) -> 'MinHash':
        """Create a deep copy of this MinHash."""
        return MinHash(
            num_perm=self.num_perm,
            seed=self.seed,
            hashvalues=self.hashvalues.copy()
        )

    def _hash(self, item: Hashable) -> int:
        """Hash an item to 32-bit integer.

        Uses SHA-256 for good distribution across all types.

        Args:
            item: Item to hash

        Returns:
            32-bit hash value
        """
        h = hashlib.sha256(str(item).encode('utf-8')).digest()
        return int.from_bytes(h[:4], byteorder='big') & self._MAX_HASH


# Convenience functions

def jaccard_similarity(mh1: MinHash, mh2: MinHash) -> float:
    """Compute Jaccard similarity between two MinHashes.

    Args:
        mh1: First MinHash
        mh2: Second MinHash

    Returns:
        Estimated Jaccard similarity (0.0-1.0)

    Examples:
        >>> mh1 = MinHash(num_perm=128)
        >>> mh2 = MinHash(num_perm=128)
        >>> mh1.update_batch(["a", "b", "c"])
        >>> mh2.update_batch(["b", "c", "d"])
        >>> jaccard_similarity(mh1, mh2)  # ~0.5 (2 common out of 4 total)
    """
    return mh1.jaccard(mh2)


def create_minhash(
    items: Iterable[Hashable],
    num_perm: int = 128,
    seed: int = 1
) -> MinHash:
    """Create MinHash from a collection of items.

    Args:
        items: Iterable of hashable values
        num_perm: Number of permutation hash functions
        seed: Random seed

    Returns:
        MinHash signature

    Examples:
        >>> words = ["the", "quick", "brown", "fox"]
        >>> mh = create_minhash(words, num_perm=128)
        >>> print(mh)
    """
    mh = MinHash(num_perm=num_perm, seed=seed)
    mh.update_batch(items)
    return mh


def merge_minhashes(minhashes: List[MinHash]) -> MinHash:
    """Merge multiple MinHashes using sum() (set union).

    Args:
        minhashes: List of MinHashes with same parameters

    Returns:
        Merged MinHash representing union

    Examples:
        >>> mh1, mh2, mh3 = [MinHash(128) for _ in range(3)]
        >>> merged = merge_minhashes([mh1, mh2, mh3])
    """
    if not minhashes:
        raise ValueError("Cannot merge empty list of MinHashes")

    return sum(minhashes)


def estimate_jaccard(set1: Set[Hashable], set2: Set[Hashable], num_perm: int = 128) -> float:
    """Estimate Jaccard similarity between two sets using MinHash.

    Args:
        set1: First set
        set2: Second set
        num_perm: Number of hash functions (accuracy)

    Returns:
        Estimated Jaccard similarity

    Examples:
        >>> set1 = {"a", "b", "c"}
        >>> set2 = {"b", "c", "d"}
        >>> estimate_jaccard(set1, set2)  # ≈ 0.5
    """
    mh1 = create_minhash(set1, num_perm=num_perm)
    mh2 = create_minhash(set2, num_perm=num_perm)
    return mh1.jaccard(mh2)
