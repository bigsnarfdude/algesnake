"""MinHash LSH: Fast similarity search with Jaccard threshold.

MinHash LSH (Locality-Sensitive Hashing) provides sub-linear time similarity
search by indexing MinHash signatures into buckets. Perfect for finding all
similar documents, duplicate detection, or clustering.

Query complexity: O(n^ρ) where ρ < 1, much faster than O(n) brute force.

References:
- Leskovec, Rajaraman, Ullman "Mining of Massive Datasets" Chapter 3
- http://infolab.stanford.edu/~ullman/mmds/ch3.pdf
"""

import hashlib
from collections import defaultdict
from typing import Any, Hashable, List, Dict, Set, Tuple, Optional
from algesnake.approximate.minhash import MinHash


class MinHashLSH:
    """MinHash LSH index for fast Jaccard similarity search.

    Uses locality-sensitive hashing to index MinHash signatures, enabling
    fast queries for items above a Jaccard similarity threshold.

    Theory:
    - Divides signature into b bands of r rows (b*r = num_perm)
    - Probability of match in ≥1 band: P ≈ 1 - (1 - J^r)^b
    - S-curve: steep transition at threshold ≈ (1/b)^(1/r)

    Query time: O(n^ρ) where ρ = log(1/P_1) / log(1/P_2)
    For P_1=0.9, P_2=0.1: ρ ≈ 0.6 (much faster than O(n))

    Examples:
        >>> # Build index
        >>> lsh = MinHashLSH(threshold=0.5, num_perm=128)
        >>>
        >>> # Insert documents
        >>> for doc_id, doc in documents.items():
        ...     mh = MinHash(128)
        ...     mh.update_batch(doc.split())
        ...     lsh.insert(doc_id, mh)
        >>>
        >>> # Query for similar documents
        >>> query_mh = MinHash(128)
        >>> query_mh.update_batch(query_doc.split())
        >>> similar = lsh.query(query_mh)  # Returns all doc_ids with J > 0.5
    """

    def __init__(
        self,
        threshold: float = 0.5,
        num_perm: int = 128,
        params: Optional[Tuple[int, int]] = None
    ):
        """Initialize MinHash LSH index.

        Args:
            threshold: Jaccard similarity threshold (0.0-1.0)
            num_perm: Number of permutations (must match MinHash)
            params: Optional (b, r) band parameters. If None, auto-computed.
                   b = number of bands, r = rows per band
                   Constraint: b * r = num_perm

        Raises:
            ValueError: If threshold not in (0, 1) or params invalid
        """
        if not 0 < threshold < 1:
            raise ValueError("Threshold must be between 0 and 1")

        self.threshold = threshold
        self.num_perm = num_perm

        # Compute or validate band parameters
        if params is not None:
            b, r = params
            if b * r != num_perm:
                raise ValueError(f"b * r must equal num_perm: {b}*{r} != {num_perm}")
            self.b = b
            self.r = r
        else:
            # Auto-compute optimal b and r
            self.b, self.r = self._optimal_params(threshold, num_perm)

        # LSH buckets: {band_idx: {hash_val: [keys]}}
        self.hashtables: List[Dict[int, List[Hashable]]] = [
            defaultdict(list) for _ in range(self.b)
        ]

        # Store MinHashes for verification
        self.minhashes: Dict[Hashable, MinHash] = {}

        # Track insertions
        self.keys: Set[Hashable] = set()

    def insert(self, key: Hashable, minhash: MinHash) -> None:
        """Insert a MinHash into the index.

        Args:
            key: Unique identifier (document ID, user ID, etc.)
            minhash: MinHash signature to index

        Raises:
            ValueError: If MinHash num_perm doesn't match
        """
        if minhash.num_perm != self.num_perm:
            raise ValueError(
                f"MinHash num_perm mismatch: expected {self.num_perm}, "
                f"got {minhash.num_perm}"
            )

        # Store MinHash for later verification
        self.minhashes[key] = minhash.copy()
        self.keys.add(key)

        # Hash each band and insert into buckets
        for i in range(self.b):
            # Extract band: rows [i*r, (i+1)*r)
            band = tuple(minhash.hashvalues[i * self.r:(i + 1) * self.r])

            # Hash the band
            band_hash = self._hash_band(band)

            # Insert key into bucket
            self.hashtables[i][band_hash].append(key)

    def query(self, minhash: MinHash) -> List[Hashable]:
        """Query for similar items above threshold.

        Args:
            minhash: Query MinHash signature

        Returns:
            List of keys with estimated Jaccard > threshold

        Raises:
            ValueError: If MinHash num_perm doesn't match
        """
        if minhash.num_perm != self.num_perm:
            raise ValueError(
                f"MinHash num_perm mismatch: expected {self.num_perm}, "
                f"got {minhash.num_perm}"
            )

        # Collect candidates from all bands
        candidates = set()

        for i in range(self.b):
            # Extract band
            band = tuple(minhash.hashvalues[i * self.r:(i + 1) * self.r])
            band_hash = self._hash_band(band)

            # Get all keys in this bucket
            if band_hash in self.hashtables[i]:
                candidates.update(self.hashtables[i][band_hash])

        # Verify candidates against actual threshold
        results = []
        for key in candidates:
            stored_mh = self.minhashes[key]
            similarity = minhash.jaccard(stored_mh)
            if similarity >= self.threshold:
                results.append(key)

        return results

    def query_with_similarity(self, minhash: MinHash) -> List[Tuple[Hashable, float]]:
        """Query for similar items with their similarity scores.

        Args:
            minhash: Query MinHash signature

        Returns:
            List of (key, similarity) pairs sorted by similarity (descending)

        Raises:
            ValueError: If MinHash num_perm doesn't match
        """
        if minhash.num_perm != self.num_perm:
            raise ValueError(
                f"MinHash num_perm mismatch: expected {self.num_perm}"
            )

        # Collect candidates
        candidates = set()
        for i in range(self.b):
            band = tuple(minhash.hashvalues[i * self.r:(i + 1) * self.r])
            band_hash = self._hash_band(band)
            if band_hash in self.hashtables[i]:
                candidates.update(self.hashtables[i][band_hash])

        # Compute similarities
        results = []
        for key in candidates:
            stored_mh = self.minhashes[key]
            similarity = minhash.jaccard(stored_mh)
            if similarity >= self.threshold:
                results.append((key, similarity))

        # Sort by similarity (descending)
        results.sort(key=lambda x: x[1], reverse=True)
        return results

    def remove(self, key: Hashable) -> bool:
        """Remove an item from the index.

        Args:
            key: Key to remove

        Returns:
            True if key was found and removed, False otherwise
        """
        if key not in self.keys:
            return False

        # Get MinHash
        minhash = self.minhashes[key]

        # Remove from all buckets
        for i in range(self.b):
            band = tuple(minhash.hashvalues[i * self.r:(i + 1) * self.r])
            band_hash = self._hash_band(band)

            if band_hash in self.hashtables[i]:
                bucket = self.hashtables[i][band_hash]
                if key in bucket:
                    bucket.remove(key)
                # Clean up empty buckets
                if not bucket:
                    del self.hashtables[i][band_hash]

        # Remove from storage
        del self.minhashes[key]
        self.keys.remove(key)

        return True

    def __contains__(self, key: Hashable) -> bool:
        """Check if key is in index."""
        return key in self.keys

    def __len__(self) -> int:
        """Return number of items in index."""
        return len(self.keys)

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"MinHashLSH(threshold={self.threshold:.2f}, "
            f"num_perm={self.num_perm}, "
            f"bands={self.b}, rows={self.r}, "
            f"items={len(self)})"
        )

    def get_counts(self) -> Dict[str, Any]:
        """Get statistics about the index.

        Returns:
            Dictionary with index statistics
        """
        total_buckets = sum(len(ht) for ht in self.hashtables)
        bucket_sizes = [
            len(bucket)
            for ht in self.hashtables
            for bucket in ht.values()
        ]
        avg_bucket_size = sum(bucket_sizes) / max(len(bucket_sizes), 1)

        return {
            'num_items': len(self),
            'num_bands': self.b,
            'rows_per_band': self.r,
            'total_buckets': total_buckets,
            'avg_bucket_size': avg_bucket_size,
            'threshold': self.threshold,
        }

    def _hash_band(self, band: Tuple[int, ...]) -> int:
        """Hash a band to an integer.

        Args:
            band: Tuple of hash values

        Returns:
            Hash of the band
        """
        # Convert band to bytes and hash
        band_bytes = str(band).encode('utf-8')
        h = hashlib.sha256(band_bytes).digest()
        return int.from_bytes(h[:8], byteorder='big')

    @staticmethod
    def _optimal_params(threshold: float, num_perm: int) -> Tuple[int, int]:
        """Compute optimal (b, r) parameters for threshold.

        Optimal threshold ≈ (1/b)^(1/r)
        We want to maximize b*r = num_perm while keeping threshold close.

        Args:
            threshold: Desired Jaccard threshold
            num_perm: Number of permutations

        Returns:
            (b, r) tuple where b = bands, r = rows per band
        """
        # Try to find b, r such that (1/b)^(1/r) ≈ threshold
        # This creates an S-curve with steep transition at threshold

        best_b, best_r = 1, num_perm
        best_diff = float('inf')

        # Try all valid factorizations of num_perm
        for r in range(1, num_perm + 1):
            if num_perm % r == 0:
                b = num_perm // r
                # Optimal threshold for this (b, r)
                opt_threshold = (1.0 / b) ** (1.0 / r)
                diff = abs(opt_threshold - threshold)

                if diff < best_diff:
                    best_diff = diff
                    best_b = b
                    best_r = r

        return best_b, best_r


# Convenience functions

def build_lsh_index(
    items: Dict[Hashable, MinHash],
    threshold: float = 0.5
) -> MinHashLSH:
    """Build LSH index from MinHash signatures.

    Args:
        items: Dictionary mapping keys to MinHash signatures
        threshold: Jaccard similarity threshold

    Returns:
        Populated MinHashLSH index

    Examples:
        >>> minhashes = {
        ...     'doc1': mh1,
        ...     'doc2': mh2,
        ...     'doc3': mh3
        ... }
        >>> lsh = build_lsh_index(minhashes, threshold=0.6)
        >>> results = lsh.query(query_mh)
    """
    if not items:
        raise ValueError("Cannot build index from empty items")

    # Get num_perm from first item
    first_mh = next(iter(items.values()))
    num_perm = first_mh.num_perm

    # Create index
    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)

    # Insert all items
    for key, minhash in items.items():
        lsh.insert(key, minhash)

    return lsh
