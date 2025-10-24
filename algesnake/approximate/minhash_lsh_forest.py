"""MinHash LSH Forest: Fast Top-K similarity search.

LSH Forest extends LSH to support Top-K queries, finding the K most similar
items without requiring a fixed threshold. Uses prefix trees (tries) for
efficient nearest neighbor search.

References:
- Bawa, Mayank, et al. "LSH forest: self-tuning indexes for similarity search" (2005)
- https://dl.acm.org/doi/10.1145/1060745.1060840
"""

import hashlib
from collections import defaultdict
from typing import Hashable, List, Dict, Tuple, Optional, Set
from algesnake.approximate.minhash import MinHash


class LSHForestNode:
    """Node in the LSH Forest prefix tree."""

    def __init__(self):
        """Initialize tree node."""
        self.children: Dict[int, 'LSHForestNode'] = {}
        self.data: Set[Hashable] = set()  # Leaf nodes store keys


class MinHashLSHForest:
    """MinHash LSH Forest for Top-K similarity search.

    Uses multiple prefix trees (one per permutation) to enable efficient
    Top-K nearest neighbor queries. Automatically adapts query depth to
    find exactly K results.

    Advantages over standard LSH:
    - No fixed threshold needed
    - Returns exactly K results (or fewer if < K items in index)
    - Better for recommendation systems, clustering

    Examples:
        >>> # Build forest
        >>> forest = MinHashLSHForest(num_perm=128)
        >>>
        >>> # Insert documents
        >>> for doc_id, doc in documents.items():
        ...     mh = MinHash(128)
        ...     mh.update_batch(doc.split())
        ...     forest.insert(doc_id, mh)
        >>>
        >>> # Index must be built before querying
        >>> forest.index()
        >>>
        >>> # Find top-10 most similar documents
        >>> query_mh = MinHash(128)
        >>> query_mh.update_batch(query_doc.split())
        >>> top_k = forest.query(query_mh, k=10)
    """

    def __init__(self, num_perm: int = 128):
        """Initialize LSH Forest.

        Args:
            num_perm: Number of permutations (must match MinHash)

        Raises:
            ValueError: If num_perm < 1
        """
        if num_perm < 1:
            raise ValueError("num_perm must be at least 1")

        self.num_perm = num_perm

        # One prefix tree per permutation
        self.trees: List[LSHForestNode] = [LSHForestNode() for _ in range(num_perm)]

        # Store MinHashes for verification
        self.minhashes: Dict[Hashable, MinHash] = {}

        # Track state
        self.keys: Set[Hashable] = set()
        self.is_indexed = False

    def insert(self, key: Hashable, minhash: MinHash) -> None:
        """Insert a MinHash into the forest.

        Args:
            key: Unique identifier
            minhash: MinHash signature to index

        Raises:
            ValueError: If MinHash num_perm doesn't match
            RuntimeError: If forest is already indexed (call clear() first)
        """
        if minhash.num_perm != self.num_perm:
            raise ValueError(
                f"MinHash num_perm mismatch: expected {self.num_perm}, "
                f"got {minhash.num_perm}"
            )

        if self.is_indexed:
            raise RuntimeError(
                "Cannot insert into indexed forest. Call clear() first."
            )

        # Store MinHash
        self.minhashes[key] = minhash.copy()
        self.keys.add(key)

    def index(self) -> None:
        """Build the forest index from inserted items.

        Must be called after all insertions and before queries.
        """
        if self.is_indexed:
            return

        # Build prefix tree for each permutation
        for i in range(self.num_perm):
            # Sort keys by their i-th hash value
            sorted_keys = sorted(
                self.keys,
                key=lambda k: self.minhashes[k].hashvalues[i]
            )

            # Build prefix tree for this permutation
            for key in sorted_keys:
                hash_val = self.minhashes[key].hashvalues[i]
                self._insert_tree(self.trees[i], hash_val, key)

        self.is_indexed = True

    def query(self, minhash: MinHash, k: int = 10) -> List[Hashable]:
        """Query for top-K most similar items.

        Args:
            minhash: Query MinHash signature
            k: Number of results to return

        Returns:
            List of up to K keys, sorted by similarity (descending)

        Raises:
            ValueError: If MinHash num_perm doesn't match or k < 1
            RuntimeError: If forest not indexed
        """
        if not self.is_indexed:
            raise RuntimeError("Forest not indexed. Call index() first.")

        if minhash.num_perm != self.num_perm:
            raise ValueError(
                f"MinHash num_perm mismatch: expected {self.num_perm}"
            )

        if k < 1:
            raise ValueError("k must be at least 1")

        # Query with similarity scores
        results = self.query_with_similarity(minhash, k)
        return [key for key, _ in results]

    def query_with_similarity(
        self,
        minhash: MinHash,
        k: int = 10
    ) -> List[Tuple[Hashable, float]]:
        """Query for top-K most similar items with scores.

        Args:
            minhash: Query MinHash signature
            k: Number of results to return

        Returns:
            List of (key, similarity) tuples sorted by similarity (descending)

        Raises:
            ValueError: If parameters invalid
            RuntimeError: If forest not indexed
        """
        if not self.is_indexed:
            raise RuntimeError("Forest not indexed. Call index() first.")

        if minhash.num_perm != self.num_perm:
            raise ValueError(
                f"MinHash num_perm mismatch: expected {self.num_perm}"
            )

        if k < 1:
            raise ValueError("k must be at least 1")

        # Adaptive prefix length search
        candidates = set()
        depth = 0
        max_depth = 32  # Maximum hash bits to consider

        # Gradually increase search depth until we have enough candidates
        while len(candidates) < k * 2 and depth <= max_depth:
            # Query each tree with current depth
            for i in range(self.num_perm):
                hash_val = minhash.hashvalues[i]
                tree_candidates = self._query_tree(
                    self.trees[i],
                    hash_val,
                    depth
                )
                candidates.update(tree_candidates)

            depth += 4  # Increase depth by 4 bits

            # Stop if we've found all items
            if len(candidates) >= len(self.keys):
                break

        # Compute actual similarities
        results = []
        for key in candidates:
            stored_mh = self.minhashes[key]
            similarity = minhash.jaccard(stored_mh)
            results.append((key, similarity))

        # Sort by similarity and return top K
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:k]

    def clear(self) -> None:
        """Clear the forest, removing all items and index."""
        self.trees = [LSHForestNode() for _ in range(self.num_perm)]
        self.minhashes.clear()
        self.keys.clear()
        self.is_indexed = False

    def __len__(self) -> int:
        """Return number of items in forest."""
        return len(self.keys)

    def __repr__(self) -> str:
        """String representation."""
        status = "indexed" if self.is_indexed else "not indexed"
        return (
            f"MinHashLSHForest(num_perm={self.num_perm}, "
            f"items={len(self)}, {status})"
        )

    def _insert_tree(self, node: LSHForestNode, hash_val: int, key: Hashable) -> None:
        """Insert key into prefix tree based on hash value.

        Args:
            node: Current tree node
            hash_val: Hash value to insert
            key: Key to store
        """
        # Convert hash to binary string for prefix matching
        # Use 32-bit representation
        bits = bin(hash_val)[2:].zfill(32)

        # Traverse tree, creating nodes as needed
        current = node
        for bit_char in bits[:8]:  # Use first 8 bits for tree depth
            bit = int(bit_char)
            if bit not in current.children:
                current.children[bit] = LSHForestNode()
            current = current.children[bit]

        # Store key at leaf
        current.data.add(key)

    def _query_tree(
        self,
        node: LSHForestNode,
        hash_val: int,
        depth: int
    ) -> Set[Hashable]:
        """Query prefix tree for candidates.

        Args:
            node: Root of tree
            hash_val: Query hash value
            depth: Number of prefix bits to match

        Returns:
            Set of candidate keys
        """
        if depth <= 0:
            # Return all keys in subtree
            return self._collect_all(node)

        # Convert hash to bits
        bits = bin(hash_val)[2:].zfill(32)

        # Traverse tree following prefix
        current = node
        for i in range(min(depth, len(bits), 8)):
            bit = int(bits[i])
            if bit not in current.children:
                return set()  # No matches
            current = current.children[bit]

        # Return all keys in matching subtree
        return self._collect_all(current)

    def _collect_all(self, node: LSHForestNode) -> Set[Hashable]:
        """Collect all keys from a subtree.

        Args:
            node: Root of subtree

        Returns:
            Set of all keys in subtree
        """
        result = set(node.data)

        # Recursively collect from children
        for child in node.children.values():
            result.update(self._collect_all(child))

        return result


# Convenience functions

def build_lsh_forest(
    items: Dict[Hashable, MinHash]
) -> MinHashLSHForest:
    """Build LSH Forest from MinHash signatures.

    Args:
        items: Dictionary mapping keys to MinHash signatures

    Returns:
        Indexed MinHashLSHForest ready for queries

    Examples:
        >>> minhashes = {
        ...     'doc1': mh1,
        ...     'doc2': mh2,
        ...     'doc3': mh3
        ... }
        >>> forest = build_lsh_forest(minhashes)
        >>> top_10 = forest.query(query_mh, k=10)
    """
    if not items:
        raise ValueError("Cannot build forest from empty items")

    # Get num_perm from first item
    first_mh = next(iter(items.values()))
    num_perm = first_mh.num_perm

    # Create forest
    forest = MinHashLSHForest(num_perm=num_perm)

    # Insert all items
    for key, minhash in items.items():
        forest.insert(key, minhash)

    # Build index
    forest.index()

    return forest
