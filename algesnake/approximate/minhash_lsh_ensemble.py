"""MinHash LSH Ensemble: Fast containment search.

LSH Ensemble is optimized for containment queries: finding sets that contain
or are contained in the query. Unlike Jaccard similarity, containment is
asymmetric and better suited for subset/superset searches.

Containment: C(A, B) = |A ∩ B| / |A|
(fraction of A contained in B)

References:
- Zhu, Erkang, et al. "LSH Ensemble: Internet-scale domain search" (2016)
- http://www.vldb.org/pvldb/vol9/p1185-zhu.pdf
"""

import hashlib
from collections import defaultdict
from typing import Hashable, List, Dict, Tuple, Optional, Set
from algesnake.approximate.minhash import MinHash


class MinHashLSHEnsemble:
    """MinHash LSH Ensemble for containment queries.

    Partitions the index by set size to optimize containment search.
    Each partition uses LSH parameters tuned for its size range.

    Containment C(A, B) = |A ∩ B| / |A| answers:
    - "Which documents contain most of this query?"
    - "Which supersets contain this set?"
    - "Find similar but smaller/larger items"

    Examples:
        >>> # Build ensemble
        >>> ensemble = MinHashLSHEnsemble(
        ...     threshold=0.7,
        ...     num_perm=128,
        ...     num_part=8
        ... )
        >>>
        >>> # Insert documents with their sizes
        >>> for doc_id, doc in documents.items():
        ...     mh = MinHash(128)
        ...     mh.update_batch(doc.split())
        ...     doc_size = len(doc.split())
        ...     ensemble.insert(doc_id, mh, doc_size)
        >>>
        >>> # Index must be built before querying
        >>> ensemble.index()
        >>>
        >>> # Find documents containing >70% of query
        >>> query_mh = MinHash(128)
        >>> query_mh.update_batch(query.split())
        >>> query_size = len(query.split())
        >>> results = ensemble.query(query_mh, query_size)
    """

    def __init__(
        self,
        threshold: float = 0.5,
        num_perm: int = 128,
        num_part: int = 16
    ):
        """Initialize LSH Ensemble.

        Args:
            threshold: Containment threshold (0.0-1.0)
            num_perm: Number of permutations (must match MinHash)
            num_part: Number of partitions (more = better accuracy)

        Raises:
            ValueError: If threshold not in (0, 1) or parameters invalid
        """
        if not 0 < threshold < 1:
            raise ValueError("Threshold must be between 0 and 1")

        if num_perm < 1:
            raise ValueError("num_perm must be at least 1")

        if num_part < 1:
            raise ValueError("num_part must be at least 1")

        self.threshold = threshold
        self.num_perm = num_perm
        self.num_part = num_part

        # Data storage before indexing
        self.buffer: List[Tuple[Hashable, MinHash, int]] = []

        # Partitions: each partition is an LSH index for a size range
        self.partitions: List[Dict[str, any]] = []

        # Size boundaries for partitions
        self.size_boundaries: List[float] = []

        # Tracking
        self.keys: Set[Hashable] = set()
        self.is_indexed = False

    def insert(self, key: Hashable, minhash: MinHash, size: int) -> None:
        """Insert a MinHash with its set size.

        Args:
            key: Unique identifier
            minhash: MinHash signature
            size: Set size (number of elements)

        Raises:
            ValueError: If MinHash num_perm doesn't match or size < 1
            RuntimeError: If ensemble already indexed
        """
        if minhash.num_perm != self.num_perm:
            raise ValueError(
                f"MinHash num_perm mismatch: expected {self.num_perm}, "
                f"got {minhash.num_perm}"
            )

        if size < 1:
            raise ValueError("Size must be at least 1")

        if self.is_indexed:
            raise RuntimeError(
                "Cannot insert into indexed ensemble. Call clear() first."
            )

        # Add to buffer
        self.buffer.append((key, minhash.copy(), size))
        self.keys.add(key)

    def index(self) -> None:
        """Build the ensemble index.

        Partitions data by size and creates optimized LSH for each partition.
        Must be called after all insertions and before queries.
        """
        if self.is_indexed:
            return

        if not self.buffer:
            self.is_indexed = True
            return

        # Sort by size
        self.buffer.sort(key=lambda x: x[2])

        # Determine size boundaries (equal-count partitions)
        total = len(self.buffer)
        partition_size = max(1, total // self.num_part)

        self.size_boundaries = []
        for i in range(self.num_part):
            start_idx = i * partition_size
            if start_idx >= total:
                break

            # Size at start of partition
            size = self.buffer[start_idx][2]
            self.size_boundaries.append(size)

        # Add infinity as final boundary
        self.size_boundaries.append(float('inf'))

        # Create partitions
        self.partitions = []
        for i in range(len(self.size_boundaries) - 1):
            lower = self.size_boundaries[i]
            upper = self.size_boundaries[i + 1]

            # Collect items in this size range
            partition_data = [
                (key, mh, size)
                for key, mh, size in self.buffer
                if lower <= size < upper
            ]

            if not partition_data:
                continue

            # Compute optimal LSH parameters for this partition
            avg_size = sum(s for _, _, s in partition_data) / len(partition_data)
            b, r = self._optimal_params(self.threshold, self.num_perm, avg_size)

            # Create LSH index for partition
            partition = {
                'lower': lower,
                'upper': upper,
                'b': b,
                'r': r,
                'hashtables': [defaultdict(list) for _ in range(b)],
                'minhashes': {},
            }

            # Insert into partition LSH
            for key, mh, size in partition_data:
                partition['minhashes'][key] = mh

                # Hash each band
                for band_idx in range(b):
                    band = tuple(mh.hashvalues[band_idx * r:(band_idx + 1) * r])
                    band_hash = self._hash_band(band)
                    partition['hashtables'][band_idx][band_hash].append(key)

            self.partitions.append(partition)

        self.is_indexed = True

    def query(self, minhash: MinHash, size: int) -> List[Hashable]:
        """Query for items with containment above threshold.

        Args:
            minhash: Query MinHash
            size: Query set size

        Returns:
            List of keys with containment >= threshold

        Raises:
            ValueError: If parameters invalid
            RuntimeError: If not indexed
        """
        if not self.is_indexed:
            raise RuntimeError("Ensemble not indexed. Call index() first.")

        if minhash.num_perm != self.num_perm:
            raise ValueError(
                f"MinHash num_perm mismatch: expected {self.num_perm}"
            )

        if size < 1:
            raise ValueError("Size must be at least 1")

        # Find relevant partitions
        candidates = set()

        for partition in self.partitions:
            # Check if this partition overlaps with query range
            # For containment C(Q, X) >= t, we need |X| >= t * |Q|
            min_target_size = self.threshold * size

            if partition['upper'] < min_target_size:
                continue  # Partition too small

            # Query LSH for this partition
            b = partition['b']
            r = partition['r']

            for band_idx in range(b):
                band = tuple(minhash.hashvalues[band_idx * r:(band_idx + 1) * r])
                band_hash = self._hash_band(band)

                if band_hash in partition['hashtables'][band_idx]:
                    candidates.update(partition['hashtables'][band_idx][band_hash])

        # Verify candidates
        results = []
        for key in candidates:
            # Find partition containing this key
            stored_mh = None
            for partition in self.partitions:
                if key in partition['minhashes']:
                    stored_mh = partition['minhashes'][key]
                    break

            if stored_mh is None:
                continue

            # Compute containment
            containment = self._estimate_containment(minhash, stored_mh)

            if containment >= self.threshold:
                results.append(key)

        return results

    def query_with_containment(
        self,
        minhash: MinHash,
        size: int
    ) -> List[Tuple[Hashable, float]]:
        """Query with containment scores.

        Args:
            minhash: Query MinHash
            size: Query set size

        Returns:
            List of (key, containment) tuples sorted by containment

        Raises:
            ValueError: If parameters invalid
            RuntimeError: If not indexed
        """
        if not self.is_indexed:
            raise RuntimeError("Ensemble not indexed. Call index() first.")

        # Get candidates
        candidates = set()
        for partition in self.partitions:
            min_target_size = self.threshold * size
            if partition['upper'] < min_target_size:
                continue

            b = partition['b']
            r = partition['r']

            for band_idx in range(b):
                band = tuple(minhash.hashvalues[band_idx * r:(band_idx + 1) * r])
                band_hash = self._hash_band(band)
                if band_hash in partition['hashtables'][band_idx]:
                    candidates.update(partition['hashtables'][band_idx][band_hash])

        # Compute containment scores
        results = []
        for key in candidates:
            stored_mh = None
            for partition in self.partitions:
                if key in partition['minhashes']:
                    stored_mh = partition['minhashes'][key]
                    break

            if stored_mh:
                containment = self._estimate_containment(minhash, stored_mh)
                if containment >= self.threshold:
                    results.append((key, containment))

        # Sort by containment
        results.sort(key=lambda x: x[1], reverse=True)
        return results

    def clear(self) -> None:
        """Clear the ensemble."""
        self.buffer.clear()
        self.partitions.clear()
        self.size_boundaries.clear()
        self.keys.clear()
        self.is_indexed = False

    def __len__(self) -> int:
        """Return number of items."""
        return len(self.keys)

    def __repr__(self) -> str:
        """String representation."""
        status = "indexed" if self.is_indexed else "not indexed"
        return (
            f"MinHashLSHEnsemble(threshold={self.threshold:.2f}, "
            f"num_perm={self.num_perm}, partitions={len(self.partitions)}, "
            f"items={len(self)}, {status})"
        )

    def _estimate_containment(self, mh1: MinHash, mh2: MinHash) -> float:
        """Estimate containment C(mh1, mh2).

        Args:
            mh1: Query MinHash
            mh2: Stored MinHash

        Returns:
            Estimated containment
        """
        # Containment ≈ fraction of mh1's hashes present in mh2
        matches = sum(
            1 for h1, h2 in zip(mh1.hashvalues, mh2.hashvalues)
            if h1 == h2
        )
        return matches / self.num_perm

    def _hash_band(self, band: Tuple[int, ...]) -> int:
        """Hash a band to an integer."""
        band_bytes = str(band).encode('utf-8')
        h = hashlib.sha256(band_bytes).digest()
        return int.from_bytes(h[:8], byteorder='big')

    def _optimal_params(
        self,
        threshold: float,
        num_perm: int,
        avg_size: float
    ) -> Tuple[int, int]:
        """Compute optimal (b, r) for containment queries.

        Args:
            threshold: Containment threshold
            num_perm: Number of permutations
            avg_size: Average set size in partition

        Returns:
            (b, r) tuple
        """
        # For containment, optimal params differ from Jaccard
        # Use heuristic similar to standard LSH
        best_b, best_r = 1, num_perm
        best_diff = float('inf')

        for r in range(1, num_perm + 1):
            if num_perm % r == 0:
                b = num_perm // r
                # Approximate optimal threshold
                opt_threshold = (1.0 / b) ** (1.0 / r)
                diff = abs(opt_threshold - threshold)

                if diff < best_diff:
                    best_diff = diff
                    best_b = b
                    best_r = r

        return best_b, best_r
