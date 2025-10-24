"""Tests for HNSW (Hierarchical Navigable Small World) graph."""

import pytest
import math
from algesnake.approximate import HNSW
from algesnake.approximate.hnsw import create_hnsw_index


def euclidean_distance(v1, v2):
    """Euclidean distance function for testing."""
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(v1, v2)))


def manhattan_distance(v1, v2):
    """Manhattan distance function for testing."""
    return sum(abs(a - b) for a, b in zip(v1, v2))


class TestHNSWBasics:
    """Tests for HNSW basic functionality."""

    def test_initialization(self):
        """Test HNSW initialization."""
        hnsw = HNSW(distance_func=euclidean_distance, m=16, ef=50)
        assert hnsw.m == 16
        assert hnsw.ef == 50
        assert len(hnsw) == 0
        assert hnsw.entry_point is None

    def test_invalid_parameters(self):
        """Test parameter validation."""
        with pytest.raises(ValueError, match="m must be at least 1"):
            HNSW(distance_func=euclidean_distance, m=0)

        with pytest.raises(ValueError, match="ef must be at least 1"):
            HNSW(distance_func=euclidean_distance, ef=0)

        with pytest.raises(ValueError, match="ef_construction must be >= m"):
            HNSW(distance_func=euclidean_distance, m=20, ef_construction=10)

    def test_insert_first_element(self):
        """Test inserting first element."""
        hnsw = HNSW(distance_func=euclidean_distance, m=16)

        vector = [1.0, 2.0, 3.0]
        hnsw.insert("v1", vector)

        assert len(hnsw) == 1
        assert "v1" in hnsw
        assert hnsw.entry_point == "v1"

    def test_insert_duplicate_key(self):
        """Test error on duplicate key."""
        hnsw = HNSW(distance_func=euclidean_distance)

        hnsw.insert("v1", [1.0, 2.0])

        with pytest.raises(ValueError, match="already exists"):
            hnsw.insert("v1", [3.0, 4.0])

    def test_insert_multiple_elements(self):
        """Test inserting multiple elements."""
        hnsw = HNSW(distance_func=euclidean_distance, m=8, seed=42)

        vectors = {
            "v1": [1.0, 2.0],
            "v2": [2.0, 3.0],
            "v3": [3.0, 4.0],
            "v4": [10.0, 11.0],
        }

        for key, vector in vectors.items():
            hnsw.insert(key, vector)

        assert len(hnsw) == 4


class TestHNSWQuery:
    """Tests for HNSW query operations."""

    def test_query_empty_index(self):
        """Test error on querying empty index."""
        hnsw = HNSW(distance_func=euclidean_distance)

        with pytest.raises(RuntimeError, match="empty"):
            hnsw.query([1.0, 2.0], k=5)

    def test_query_invalid_k(self):
        """Test error on invalid k."""
        hnsw = HNSW(distance_func=euclidean_distance)
        hnsw.insert("v1", [1.0, 2.0])

        with pytest.raises(ValueError, match="k must be at least 1"):
            hnsw.query([1.0, 2.0], k=0)

        with pytest.raises(ValueError, match="k must be at least 1"):
            hnsw.query([1.0, 2.0], k=-1)

    def test_query_single_neighbor(self):
        """Test querying for single nearest neighbor."""
        hnsw = HNSW(distance_func=euclidean_distance, m=16, seed=42)

        vectors = {
            "v1": [1.0, 1.0],
            "v2": [2.0, 2.0],
            "v3": [10.0, 10.0],
        }

        for key, vector in vectors.items():
            hnsw.insert(key, vector)

        # Query near v1
        query = [1.1, 1.1]
        results = hnsw.query(query, k=1)

        assert len(results) == 1
        assert results[0] == "v1"

    def test_query_k_neighbors(self):
        """Test querying for K nearest neighbors."""
        hnsw = HNSW(distance_func=euclidean_distance, m=8, ef=50, seed=42)

        # Create a grid of vectors
        vectors = {}
        for i in range(10):
            for j in range(10):
                key = f"v_{i}_{j}"
                vectors[key] = [float(i), float(j)]

        for key, vector in vectors.items():
            hnsw.insert(key, vector)

        # Query near (5, 5)
        query = [5.0, 5.0]
        results = hnsw.query(query, k=5)

        assert len(results) == 5
        # v_5_5 should be nearest
        assert "v_5_5" in results[:3]

    def test_query_with_distances(self):
        """Test query with distance scores."""
        hnsw = HNSW(distance_func=euclidean_distance, m=16, seed=42)

        vectors = {
            "v1": [1.0, 1.0],
            "v2": [2.0, 2.0],
            "v3": [3.0, 3.0],
        }

        for key, vector in vectors.items():
            hnsw.insert(key, vector)

        query = [1.0, 1.0]
        results = hnsw.query_with_distances(query, k=3)

        assert len(results) == 3
        assert all(isinstance(r, tuple) and len(r) == 2 for r in results)

        # Check distances are sorted (ascending)
        for i in range(len(results) - 1):
            assert results[i][1] <= results[i + 1][1]

        # First result should be v1 with distance ~0
        assert results[0][0] == "v1"
        assert results[0][1] < 0.1

    def test_query_k_larger_than_index(self):
        """Test querying for more neighbors than exist."""
        hnsw = HNSW(distance_func=euclidean_distance, m=16)

        # Only 3 vectors
        for i in range(3):
            hnsw.insert(f"v{i}", [float(i), float(i)])

        query = [1.0, 1.0]
        results = hnsw.query(query, k=10)

        # Should return at most 3
        assert len(results) <= 3


class TestHNSWAccuracy:
    """Tests for HNSW accuracy."""

    def test_nearest_neighbor_accuracy(self):
        """Test that HNSW finds correct nearest neighbor."""
        hnsw = HNSW(
            distance_func=euclidean_distance,
            m=32,
            ef_construction=200,
            ef=100,
            seed=42
        )

        # Insert 100 random-ish vectors
        vectors = {}
        for i in range(100):
            key = f"v{i}"
            # Create somewhat structured data
            x = (i % 10) * 10.0
            y = (i // 10) * 10.0
            vectors[key] = [x, y]
            hnsw.insert(key, [x, y])

        # Query each vector - should find itself
        for key, vector in vectors.items():
            results = hnsw.query(vector, k=1)
            assert results[0] == key, f"Failed to find exact match for {key}"

    def test_recall_with_multiple_neighbors(self):
        """Test recall for multiple neighbors."""
        hnsw = HNSW(
            distance_func=euclidean_distance,
            m=16,
            ef_construction=100,
            ef=50,
            seed=42
        )

        # Create clustered data
        vectors = {}
        for i in range(50):
            key = f"v{i}"
            vectors[key] = [float(i), 0.0]
            hnsw.insert(key, [float(i), 0.0])

        # Query near v25
        query = [25.0, 0.0]
        results = hnsw.query(query, k=5)

        # Should find v25 and its neighbors
        assert "v25" in results
        # At least a few close neighbors
        close_neighbors = [k for k in results if abs(int(k[1:]) - 25) <= 2]
        assert len(close_neighbors) >= 3


class TestHNSWCustomDistance:
    """Tests with custom distance functions."""

    def test_manhattan_distance(self):
        """Test HNSW with Manhattan distance."""
        hnsw = HNSW(distance_func=manhattan_distance, m=16, seed=42)

        vectors = {
            "v1": [1.0, 1.0],
            "v2": [2.0, 2.0],
            "v3": [10.0, 10.0],
        }

        for key, vector in vectors.items():
            hnsw.insert(key, vector)

        query = [1.5, 1.5]
        results = hnsw.query(query, k=1)

        # v1 or v2 should be nearest
        assert results[0] in ["v1", "v2"]

    def test_cosine_similarity(self):
        """Test HNSW with cosine distance."""
        def cosine_distance(v1, v2):
            dot = sum(a * b for a, b in zip(v1, v2))
            norm1 = math.sqrt(sum(a * a for a in v1))
            norm2 = math.sqrt(sum(b * b for b in v2))
            return 1.0 - (dot / (norm1 * norm2 + 1e-10))

        hnsw = HNSW(distance_func=cosine_distance, m=16, seed=42)

        vectors = {
            "v1": [1.0, 0.0],
            "v2": [0.0, 1.0],
            "v3": [1.0, 1.0],
        }

        for key, vector in vectors.items():
            hnsw.insert(key, vector)

        # Query similar to v3 [1, 1]
        query = [2.0, 2.0]
        results = hnsw.query(query, k=1)

        assert results[0] == "v3"


class TestHNSWStatistics:
    """Tests for HNSW statistics."""

    def test_get_stats_empty(self):
        """Test statistics on empty index."""
        hnsw = HNSW(distance_func=euclidean_distance)
        stats = hnsw.get_stats()

        assert stats['num_items'] == 0
        assert stats['num_levels'] == 0

    def test_get_stats(self):
        """Test statistics on populated index."""
        hnsw = HNSW(distance_func=euclidean_distance, m=16, seed=42)

        for i in range(20):
            hnsw.insert(f"v{i}", [float(i), float(i)])

        stats = hnsw.get_stats()

        assert stats['num_items'] == 20
        assert stats['num_levels'] >= 1
        assert stats['avg_degree'] > 0
        assert stats['max_degree'] > 0
        assert stats['m'] == 16

    def test_repr(self):
        """Test string representation."""
        hnsw = HNSW(distance_func=euclidean_distance, m=16, ef=50)
        hnsw.insert("v1", [1.0, 2.0])

        repr_str = repr(hnsw)
        assert "HNSW" in repr_str
        assert "m=16" in repr_str
        assert "ef=50" in repr_str
        assert "items=1" in repr_str


class TestHNSWEdgeCases:
    """Tests for edge cases."""

    def test_single_vector(self):
        """Test with only one vector."""
        hnsw = HNSW(distance_func=euclidean_distance)
        hnsw.insert("v1", [1.0, 2.0, 3.0])

        results = hnsw.query([1.0, 2.0, 3.0], k=1)
        assert results == ["v1"]

    def test_identical_vectors(self):
        """Test with identical vectors."""
        hnsw = HNSW(distance_func=euclidean_distance, m=16, seed=42)

        # Insert identical vectors
        for i in range(5):
            hnsw.insert(f"v{i}", [1.0, 1.0])

        query = [1.0, 1.0]
        results = hnsw.query(query, k=3)

        # Should find 3 of them
        assert len(results) == 3

    def test_high_dimensional_vectors(self):
        """Test with high-dimensional vectors."""
        hnsw = HNSW(distance_func=euclidean_distance, m=16, seed=42)

        # 100-dimensional vectors
        dim = 100
        for i in range(10):
            vector = [float(i)] * dim
            hnsw.insert(f"v{i}", vector)

        query = [5.0] * dim
        results = hnsw.query(query, k=1)

        assert results[0] == "v5"

    def test_varying_ef(self):
        """Test query with different ef values."""
        hnsw = HNSW(distance_func=euclidean_distance, m=16, ef=10, seed=42)

        for i in range(20):
            hnsw.insert(f"v{i}", [float(i), 0.0])

        query = [10.0, 0.0]

        # Lower ef - might be less accurate
        results_low = hnsw.query(query, k=5, ef=5)
        assert len(results_low) == 5

        # Higher ef - should be more accurate
        results_high = hnsw.query(query, k=5, ef=100)
        assert len(results_high) == 5

        # Both should find v10
        assert "v10" in results_low
        assert "v10" in results_high


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_create_hnsw_index(self):
        """Test create_hnsw_index function."""
        vectors = {
            "v1": [1.0, 2.0],
            "v2": [3.0, 4.0],
            "v3": [5.0, 6.0],
        }

        hnsw = create_hnsw_index(
            vectors,
            distance_func=euclidean_distance,
            m=16
        )

        assert isinstance(hnsw, HNSW)
        assert len(hnsw) == 3

        # Test querying
        results = hnsw.query([1.0, 2.0], k=1)
        assert results[0] == "v1"

    def test_create_empty_index(self):
        """Test creating from empty dict."""
        hnsw = create_hnsw_index({}, distance_func=euclidean_distance)
        assert len(hnsw) == 0
