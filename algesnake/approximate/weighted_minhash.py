"""Weighted MinHash: Probabilistic weighted Jaccard similarity estimation.

Weighted MinHash extends MinHash to handle sets where elements have weights.
Perfect for document similarity with term frequencies, user behavior analysis,
or any weighted set comparison.

Weighted Jaccard: J_w(A, B) = sum(min(w_A(x), w_B(x))) / sum(max(w_A(x), w_B(x)))

References:
- Ioffe, Sergey. "Improved consistent sampling, weighted minhash and L1 sketching" (2010)
- https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36928.pdf
"""

import hashlib
import math
import random
from typing import Any, Hashable, Dict, List, Optional, Tuple


class WeightedMinHash:
    """Weighted MinHash signature for weighted Jaccard similarity.

    Extends MinHash to handle weighted sets, where each element has
    an associated weight (frequency, importance, etc.).

    - Space: O(k) floats (e.g., 128 hashes = 1KB)
    - Accuracy: ~1/sqrt(k) standard error
    - Supports: TF-IDF, frequency counts, importance weights

    Examples:
        >>> # Document similarity with term frequencies
        >>> wmh1 = WeightedMinHash(num_perm=128)
        >>> wmh2 = WeightedMinHash(num_perm=128)
        >>>
        >>> # Document 1: term frequencies
        >>> wmh1.update("cat", 3)
        >>> wmh1.update("dog", 2)
        >>> wmh1.update("bird", 1)
        >>>
        >>> # Document 2: term frequencies
        >>> wmh2.update("cat", 2)
        >>> wmh2.update("dog", 3)
        >>> wmh2.update("fish", 2)
        >>>
        >>> # Estimate weighted Jaccard similarity
        >>> similarity = wmh1.jaccard(wmh2)
        >>> print(f"Weighted similarity: {similarity:.2f}")
    """

    _MAX_HASH = (1 << 32) - 1
    _PRIME = 4294967311  # Large prime for universal hashing

    def __init__(
        self,
        num_perm: int = 128,
        seed: int = 1,
        hashvalues: Optional[List[Tuple[int, float]]] = None
    ):
        """Initialize Weighted MinHash.

        Args:
            num_perm: Number of permutation hash functions
            seed: Random seed for hash function generation
            hashvalues: Pre-existing hash values (for deserialization)

        Raises:
            ValueError: If num_perm < 1
        """
        if num_perm < 1:
            raise ValueError("num_perm must be at least 1")

        self.num_perm = num_perm
        self.seed = seed

        # Store (hash, weight) pairs for each permutation
        if hashvalues is not None:
            if len(hashvalues) != num_perm:
                raise ValueError(f"Expected {num_perm} hash values")
            self.hashvalues = list(hashvalues)
        else:
            # Initialize with (max_hash, 0.0) - identity for weighted min
            self.hashvalues = [(self._MAX_HASH, 0.0)] * num_perm

        # Generate hash functions
        random.seed(seed)
        self._hash_params = [
            (
                random.randint(1, self._PRIME - 1),  # a (non-zero)
                random.randint(0, self._PRIME - 1),  # b
                random.random() + 0.5                 # gamma for exponential
            )
            for _ in range(num_perm)
        ]

    def update(self, item: Hashable, weight: float = 1.0) -> None:
        """Add a weighted item to the signature.

        Args:
            item: Hashable value
            weight: Non-negative weight (frequency, importance, etc.)

        Raises:
            ValueError: If weight is negative
        """
        if weight < 0:
            raise ValueError("Weight must be non-negative")

        if weight == 0:
            return

        # Hash the item
        h = self._hash(item)

        # Update each permutation
        for i, (a, b, gamma) in enumerate(self._hash_params):
            # Consistent weighted sampling:
            # t = floor(-ln(r) / w), k = h'(x)
            # where r ~ Uniform(0,1), w = weight

            # Use hash to generate deterministic "random" value
            r = self._hash_to_uniform(h, i)

            # Compute t-value (rank based on weight)
            t_val = math.floor(-math.log(r) / weight)

            # Permuted hash
            k_val = ((a * h + b) % self._PRIME) & self._MAX_HASH

            # Combined hash for comparison
            combined = (t_val, k_val)

            # Keep minimum based on t-value, then k-value
            current_h, current_w = self.hashvalues[i]
            current_t = math.floor(-math.log(max(current_w, 1e-10)) / max(current_w, 1e-10)) if current_w > 0 else float('inf')

            if combined[0] < current_t or (combined[0] == current_t and combined[1] < current_h):
                self.hashvalues[i] = (k_val, weight)

    def update_batch(self, items: Dict[Hashable, float]) -> None:
        """Add multiple weighted items.

        Args:
            items: Dictionary mapping items to weights
        """
        for item, weight in items.items():
            self.update(item, weight)

    def jaccard(self, other: 'WeightedMinHash') -> float:
        """Estimate weighted Jaccard similarity.

        Args:
            other: Another WeightedMinHash with same parameters

        Returns:
            Estimated weighted Jaccard similarity (0.0-1.0)

        Raises:
            ValueError: If parameters don't match
        """
        if self.num_perm != other.num_perm:
            raise ValueError(
                f"Cannot compare WeightedMinHashes with different num_perm: "
                f"{self.num_perm} vs {other.num_perm}"
            )
        if self.seed != other.seed:
            raise ValueError(
                f"Cannot compare WeightedMinHashes with different seeds: "
                f"{self.seed} vs {other.seed}"
            )

        # Count matching hash values
        matches = sum(
            1 for (h1, w1), (h2, w2) in zip(self.hashvalues, other.hashvalues)
            if h1 == h2
        )

        return matches / self.num_perm

    def combine(self, other: 'WeightedMinHash') -> 'WeightedMinHash':
        """Combine two Weighted MinHashes (union operation).

        Args:
            other: Another WeightedMinHash with same parameters

        Returns:
            New WeightedMinHash representing union

        Raises:
            ValueError: If parameters don't match
        """
        if self.num_perm != other.num_perm:
            raise ValueError(
                f"Cannot combine WeightedMinHashes with different num_perm"
            )
        if self.seed != other.seed:
            raise ValueError(
                f"Cannot combine WeightedMinHashes with different seeds"
            )

        # Take element-wise minimum based on (t-value, hash)
        merged = []
        for (h1, w1), (h2, w2) in zip(self.hashvalues, other.hashvalues):
            # Choose the one with smaller t-value (or hash if tied)
            if w1 == 0:
                merged.append((h2, w2))
            elif w2 == 0:
                merged.append((h1, w1))
            else:
                t1 = -math.log(max(w1, 1e-10)) / max(w1, 1e-10)
                t2 = -math.log(max(w2, 1e-10)) / max(w2, 1e-10)
                if t1 < t2 or (t1 == t2 and h1 < h2):
                    merged.append((h1, w1))
                else:
                    merged.append((h2, w2))

        return WeightedMinHash(
            num_perm=self.num_perm,
            seed=self.seed,
            hashvalues=merged
        )

    def __add__(self, other: 'WeightedMinHash') -> 'WeightedMinHash':
        """Combine using + operator (monoid operation)."""
        return self.combine(other)

    def __radd__(self, other):
        """Support sum() builtin."""
        if other == 0:
            return self
        return self.__add__(other)

    def __eq__(self, other: Any) -> bool:
        """Check equality."""
        if not isinstance(other, WeightedMinHash):
            return False
        return (
            self.num_perm == other.num_perm and
            self.seed == other.seed and
            self.hashvalues == other.hashvalues
        )

    def __repr__(self) -> str:
        """String representation."""
        return f"WeightedMinHash(num_perm={self.num_perm}, seed={self.seed})"

    @property
    def zero(self) -> 'WeightedMinHash':
        """Monoid identity: empty WeightedMinHash."""
        return WeightedMinHash(num_perm=self.num_perm, seed=self.seed)

    def is_zero(self) -> bool:
        """Check if this is the zero element."""
        return all(w == 0.0 for _, w in self.hashvalues)

    def copy(self) -> 'WeightedMinHash':
        """Create a deep copy."""
        return WeightedMinHash(
            num_perm=self.num_perm,
            seed=self.seed,
            hashvalues=self.hashvalues.copy()
        )

    def _hash(self, item: Hashable) -> int:
        """Hash an item to 32-bit integer."""
        h = hashlib.sha256(str(item).encode('utf-8')).digest()
        return int.from_bytes(h[:4], byteorder='big') & self._MAX_HASH

    def _hash_to_uniform(self, h: int, i: int) -> float:
        """Convert hash to uniform random value in (0, 1).

        Args:
            h: Hash value
            i: Permutation index

        Returns:
            Float in (0, 1)
        """
        # Use hash and index to generate deterministic value
        combined = f"{h}:{i}".encode('utf-8')
        hash_bytes = hashlib.sha256(combined).digest()
        hash_int = int.from_bytes(hash_bytes[:8], byteorder='big')
        # Map to (0, 1) excluding exact 0 and 1
        return (hash_int & ((1 << 53) - 1)) / (1 << 53) + 1e-10


# Convenience functions

def create_weighted_minhash(
    items: Dict[Hashable, float],
    num_perm: int = 128,
    seed: int = 1
) -> WeightedMinHash:
    """Create WeightedMinHash from weighted items.

    Args:
        items: Dictionary mapping items to weights
        num_perm: Number of permutation hash functions
        seed: Random seed

    Returns:
        WeightedMinHash signature

    Examples:
        >>> term_freq = {"cat": 3, "dog": 2, "bird": 1}
        >>> wmh = create_weighted_minhash(term_freq, num_perm=128)
    """
    wmh = WeightedMinHash(num_perm=num_perm, seed=seed)
    wmh.update_batch(items)
    return wmh


def estimate_weighted_jaccard(
    items1: Dict[Hashable, float],
    items2: Dict[Hashable, float],
    num_perm: int = 128
) -> float:
    """Estimate weighted Jaccard similarity between two weighted sets.

    Args:
        items1: First weighted set (item -> weight)
        items2: Second weighted set (item -> weight)
        num_perm: Number of hash functions

    Returns:
        Estimated weighted Jaccard similarity

    Examples:
        >>> doc1 = {"cat": 3, "dog": 2}
        >>> doc2 = {"cat": 2, "dog": 3, "fish": 1}
        >>> estimate_weighted_jaccard(doc1, doc2)  # ≈ weighted Jaccard
    """
    wmh1 = create_weighted_minhash(items1, num_perm=num_perm)
    wmh2 = create_weighted_minhash(items2, num_perm=num_perm)
    return wmh1.jaccard(wmh2)
