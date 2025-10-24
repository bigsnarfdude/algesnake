"""HNSW: Hierarchical Navigable Small World graph for ANN search.

HNSW is a graph-based approximate nearest neighbor search algorithm that
provides excellent performance for high-dimensional vector search with
logarithmic complexity.

Query complexity: O(log N) with high probability
Build complexity: O(N log N)

References:
- Malkov, Yu A., and Yashunin, D. A. "Efficient and robust approximate nearest neighbor
  search using hierarchical navigable small world graphs" (2018)
- https://arxiv.org/abs/1603.09320
"""

import heapq
import random
from typing import Any, Callable, List, Tuple, Set, Dict, Optional, Hashable
import math


class HNSW:
    """Hierarchical Navigable Small World graph for ANN search.

    Uses a multi-layer graph structure where higher layers contain
    long-range connections and lower layers contain fine-grained local
    connections. Provides sub-linear search time.

    Works with any distance function and vector type.

    Examples:
        >>> # Define distance function (e.g., Euclidean)
        >>> def euclidean(v1, v2):
        ...     return sum((a - b) ** 2 for a, b in zip(v1, v2)) ** 0.5
        >>>
        >>> # Create HNSW index
        >>> hnsw = HNSW(distance_func=euclidean, m=16, ef_construction=200)
        >>>
        >>> # Insert vectors
        >>> for vec_id, vector in vectors.items():
        ...     hnsw.insert(vec_id, vector)
        >>>
        >>> # Find K nearest neighbors
        >>> query_vector = [0.1, 0.2, 0.3, ...]
        >>> neighbors = hnsw.query(query_vector, k=10)
    """

    def __init__(
        self,
        distance_func: Callable[[Any, Any], float],
        m: int = 16,
        ef_construction: int = 200,
        ef: int = 50,
        ml: float = 1.0 / math.log(2.0),
        seed: int = 0
    ):
        """Initialize HNSW index.

        Args:
            distance_func: Distance function (smaller = more similar)
            m: Number of bi-directional links per node (higher = better recall)
            ef_construction: Size of dynamic candidate list during construction
                           (higher = better index quality but slower build)
            ef: Size of dynamic candidate list during search
               (higher = better recall but slower search)
            ml: Normalization factor for level assignment
            seed: Random seed for reproducibility

        Raises:
            ValueError: If parameters are invalid
        """
        if m < 1:
            raise ValueError("m must be at least 1")
        if ef_construction < m:
            raise ValueError("ef_construction must be >= m")
        if ef < 1:
            raise ValueError("ef must be at least 1")

        self.distance_func = distance_func
        self.m = m  # Max connections per layer
        self.m_max = m  # Max for layer 0
        self.m_max0 = m * 2  # Max for layer > 0
        self.ef_construction = ef_construction
        self.ef = ef
        self.ml = ml

        # Graph structure: {level: {node: [neighbors]}}
        self.graph: Dict[int, Dict[Hashable, List[Hashable]]] = {}

        # Data storage: {node: vector}
        self.data: Dict[Hashable, Any] = {}

        # Node levels: {node: level}
        self.node_level: Dict[Hashable, int] = {}

        # Entry point (top-level node)
        self.entry_point: Optional[Hashable] = None
        self.max_level: int = -1

        # Random state
        random.seed(seed)

    def insert(self, key: Hashable, vector: Any) -> None:
        """Insert a vector into the index.

        Args:
            key: Unique identifier for the vector
            vector: Vector data (list, tuple, numpy array, etc.)

        Raises:
            ValueError: If key already exists
        """
        if key in self.data:
            raise ValueError(f"Key {key} already exists")

        # Store vector
        self.data[key] = vector

        # Assign random level
        level = self._random_level()
        self.node_level[key] = level

        # Initialize graph connections for this node
        for lc in range(level + 1):
            if lc not in self.graph:
                self.graph[lc] = {}
            self.graph[lc][key] = []

        # If this is the first element, make it the entry point
        if self.entry_point is None:
            self.entry_point = key
            self.max_level = level
            return

        # Search for nearest neighbors at each layer
        nearest = [self.entry_point]
        for lc in range(self.max_level, level, -1):
            nearest = self._search_layer(vector, nearest, 1, lc)

        # Insert into layers from level down to 0
        for lc in range(level, -1, -1):
            # Find ef_construction nearest neighbors
            candidates = self._search_layer(
                vector, nearest, self.ef_construction, lc
            )

            # Select m neighbors
            m = self.m_max0 if lc == 0 else self.m_max

            # Get M nearest neighbors
            neighbors = self._get_neighbors(candidates, m)

            # Add bidirectional links
            self.graph[lc][key] = [n for n, _ in neighbors]

            for neighbor, _ in neighbors:
                self.graph[lc][neighbor].append(key)

                # Prune neighbor's connections if needed
                max_conn = self.m_max0 if lc == 0 else self.m_max
                if len(self.graph[lc][neighbor]) > max_conn:
                    self._prune_connections(neighbor, max_conn, lc)

            nearest = [n for n, _ in candidates]

        # Update entry point if new node is at higher level
        if level > self.max_level:
            self.max_level = level
            self.entry_point = key

    def query(self, vector: Any, k: int = 10, ef: Optional[int] = None) -> List[Hashable]:
        """Find K nearest neighbors.

        Args:
            vector: Query vector
            k: Number of neighbors to return
            ef: Search parameter (if None, uses self.ef)

        Returns:
            List of up to K nearest neighbor keys

        Raises:
            RuntimeError: If index is empty
            ValueError: If k < 1
        """
        if self.entry_point is None:
            raise RuntimeError("Index is empty")

        if k < 1:
            raise ValueError("k must be at least 1")

        results = self.query_with_distances(vector, k, ef)
        return [key for key, _ in results]

    def query_with_distances(
        self,
        vector: Any,
        k: int = 10,
        ef: Optional[int] = None
    ) -> List[Tuple[Hashable, float]]:
        """Find K nearest neighbors with distances.

        Args:
            vector: Query vector
            k: Number of neighbors to return
            ef: Search parameter (if None, uses self.ef)

        Returns:
            List of (key, distance) tuples sorted by distance

        Raises:
            RuntimeError: If index is empty
            ValueError: If k < 1
        """
        if self.entry_point is None:
            raise RuntimeError("Index is empty")

        if k < 1:
            raise ValueError("k must be at least 1")

        if ef is None:
            ef = max(self.ef, k)

        # Search from entry point down to layer 0
        nearest = [self.entry_point]

        for lc in range(self.max_level, 0, -1):
            nearest = self._search_layer(vector, nearest, 1, lc)

        # Search layer 0 with ef
        candidates = self._search_layer(vector, nearest, ef, 0)

        # Return top K
        return candidates[:k]

    def __len__(self) -> int:
        """Return number of vectors in index."""
        return len(self.data)

    def __contains__(self, key: Hashable) -> bool:
        """Check if key is in index."""
        return key in self.data

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"HNSW(m={self.m}, ef={self.ef}, "
            f"levels={self.max_level + 1}, items={len(self)})"
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics.

        Returns:
            Dictionary with statistics
        """
        if not self.data:
            return {
                'num_items': 0,
                'num_levels': 0,
                'avg_degree': 0,
                'max_degree': 0,
            }

        # Compute statistics
        degrees = []
        for level in self.graph.values():
            for neighbors in level.values():
                degrees.append(len(neighbors))

        return {
            'num_items': len(self.data),
            'num_levels': self.max_level + 1,
            'avg_degree': sum(degrees) / max(len(degrees), 1),
            'max_degree': max(degrees) if degrees else 0,
            'ef': self.ef,
            'm': self.m,
        }

    def _random_level(self) -> int:
        """Assign random level using exponential decay.

        Returns:
            Random level (0, 1, 2, ...)
        """
        # Exponential decay: P(level) = exp(-level / ml)
        return int(-math.log(random.uniform(0, 1)) * self.ml)

    def _search_layer(
        self,
        query: Any,
        entry_points: List[Hashable],
        num_closest: int,
        layer: int
    ) -> List[Tuple[Hashable, float]]:
        """Search for nearest neighbors at a specific layer.

        Args:
            query: Query vector
            entry_points: Starting nodes
            num_closest: Number of closest neighbors to return
            layer: Layer to search

        Returns:
            List of (key, distance) tuples
        """
        visited = set()
        candidates = []
        w = []

        # Initialize with entry points
        for point in entry_points:
            dist = self.distance_func(query, self.data[point])
            heapq.heappush(candidates, (-dist, point))
            heapq.heappush(w, (dist, point))
            visited.add(point)

        while candidates:
            # Get closest candidate
            _, current = heapq.heappop(candidates)

            # Get furthest result
            if w:
                f_dist, _ = w[0]  # Min heap, so first is smallest
                # Get largest in w using max
                furthest_dist = max(d for d, _ in w)

                c_dist = self.distance_func(query, self.data[current])
                if c_dist > furthest_dist:
                    break

            # Check neighbors
            for neighbor in self.graph[layer].get(current, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    dist = self.distance_func(query, self.data[neighbor])

                    # Get furthest in w
                    if len(w) < num_closest:
                        heapq.heappush(candidates, (-dist, neighbor))
                        heapq.heappush(w, (dist, neighbor))
                    else:
                        furthest_dist = max(d for d, _ in w)
                        if dist < furthest_dist:
                            heapq.heappush(candidates, (-dist, neighbor))
                            heapq.heappush(w, (dist, neighbor))

                            # Remove furthest
                            if len(w) > num_closest:
                                w = sorted(w, key=lambda x: x[0])[:num_closest]

        return sorted(w, key=lambda x: x[0])

    def _get_neighbors(
        self,
        candidates: List[Tuple[Hashable, float]],
        m: int
    ) -> List[Tuple[Hashable, float]]:
        """Select M neighbors from candidates.

        Args:
            candidates: List of (key, distance) candidates
            m: Number to select

        Returns:
            List of M closest neighbors
        """
        return candidates[:m]

    def _prune_connections(self, node: Hashable, max_conn: int, layer: int) -> None:
        """Prune connections of a node to maintain max_conn.

        Args:
            node: Node to prune
            max_conn: Maximum number of connections
            layer: Graph layer
        """
        neighbors = self.graph[layer][node]

        if len(neighbors) <= max_conn:
            return

        # Compute distances to all neighbors
        node_vec = self.data[node]
        distances = [
            (neighbor, self.distance_func(node_vec, self.data[neighbor]))
            for neighbor in neighbors
        ]

        # Keep M closest
        distances.sort(key=lambda x: x[1])
        self.graph[layer][node] = [n for n, _ in distances[:max_conn]]


# Convenience functions

def create_hnsw_index(
    items: Dict[Hashable, Any],
    distance_func: Callable[[Any, Any], float],
    m: int = 16,
    ef_construction: int = 200
) -> HNSW:
    """Create HNSW index from items.

    Args:
        items: Dictionary mapping keys to vectors
        distance_func: Distance function
        m: HNSW parameter
        ef_construction: Construction parameter

    Returns:
        Populated HNSW index

    Examples:
        >>> vectors = {'v1': [1, 2, 3], 'v2': [4, 5, 6]}
        >>> def euclidean(a, b):
        ...     return sum((x - y) ** 2 for x, y in zip(a, b)) ** 0.5
        >>> hnsw = create_hnsw_index(vectors, euclidean)
    """
    hnsw = HNSW(distance_func=distance_func, m=m, ef_construction=ef_construction)

    for key, vector in items.items():
        hnsw.insert(key, vector)

    return hnsw
