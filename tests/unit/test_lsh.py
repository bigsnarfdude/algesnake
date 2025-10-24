"""Tests for LSH structures: MinHashLSH, LSH Forest, and LSH Ensemble."""

import pytest
from algesnake.approximate import (
    MinHash, MinHashLSH, MinHashLSHForest, MinHashLSHEnsemble
)
from algesnake.approximate.minhash import create_minhash
from algesnake.approximate.minhash_lsh import build_lsh_index
from algesnake.approximate.minhash_lsh_forest import build_lsh_forest


class TestMinHashLSHBasics:
    """Tests for MinHashLSH basic functionality."""

    def test_initialization(self):
        """Test LSH initialization."""
        lsh = MinHashLSH(threshold=0.5, num_perm=128)
        assert lsh.threshold == 0.5
        assert lsh.num_perm == 128
        assert len(lsh) == 0

    def test_invalid_threshold(self):
        """Test threshold validation."""
        with pytest.raises(ValueError, match="between 0 and 1"):
            MinHashLSH(threshold=0.0)

        with pytest.raises(ValueError, match="between 0 and 1"):
            MinHashLSH(threshold=1.0)

        with pytest.raises(ValueError, match="between 0 and 1"):
            MinHashLSH(threshold=1.5)

    def test_optimal_params(self):
        """Test automatic b, r parameter computation."""
        lsh = MinHashLSH(threshold=0.5, num_perm=128)

        # b * r should equal num_perm
        assert lsh.b * lsh.r == 128

    def test_manual_params(self):
        """Test manual b, r parameters."""
        lsh = MinHashLSH(threshold=0.5, num_perm=128, params=(16, 8))
        assert lsh.b == 16
        assert lsh.r == 8

    def test_invalid_params(self):
        """Test invalid manual parameters."""
        with pytest.raises(ValueError, match="b \\* r must equal num_perm"):
            MinHashLSH(threshold=0.5, num_perm=128, params=(10, 10))


class TestMinHashLSHInsertQuery:
    """Tests for LSH insert and query operations."""

    def test_insert(self):
        """Test inserting MinHashes."""
        lsh = MinHashLSH(threshold=0.5, num_perm=128)

        mh = create_minhash(["a", "b", "c"], num_perm=128)
        lsh.insert("doc1", mh)

        assert len(lsh) == 1
        assert "doc1" in lsh

    def test_insert_wrong_num_perm(self):
        """Test error on wrong num_perm."""
        lsh = MinHashLSH(threshold=0.5, num_perm=128)
        mh = MinHash(num_perm=64)

        with pytest.raises(ValueError, match="num_perm mismatch"):
            lsh.insert("doc1", mh)

    def test_query_similar_items(self):
        """Test querying for similar items."""
        lsh = MinHashLSH(threshold=0.6, num_perm=128)

        # Insert documents
        docs = {
            "doc1": ["a", "b", "c", "d"],
            "doc2": ["a", "b", "c", "e"],  # Similar to doc1
            "doc3": ["x", "y", "z"],       # Different
        }

        for doc_id, words in docs.items():
            mh = create_minhash(words, num_perm=128)
            lsh.insert(doc_id, mh)

        # Query with doc1
        query = create_minhash(["a", "b", "c", "d"], num_perm=128)
        results = lsh.query(query)

        # Should find doc1 and doc2 (similar), not doc3
        assert "doc1" in results
        assert "doc2" in results
        # doc3 might be in candidates but shouldn't pass threshold

    def test_query_with_similarity(self):
        """Test query with similarity scores."""
        lsh = MinHashLSH(threshold=0.4, num_perm=128)

        # Insert documents
        docs = {
            "doc1": ["a", "b", "c"],
            "doc2": ["a", "b", "d"],
            "doc3": ["a", "e", "f"],
        }

        for doc_id, words in docs.items():
            mh = create_minhash(words, num_perm=128)
            lsh.insert(doc_id, mh)

        # Query
        query = create_minhash(["a", "b", "c"], num_perm=128)
        results = lsh.query_with_similarity(query)

        # Results should be sorted by similarity
        assert len(results) > 0
        assert all(isinstance(r, tuple) and len(r) == 2 for r in results)

        # Check sorting
        for i in range(len(results) - 1):
            assert results[i][1] >= results[i + 1][1]

    def test_query_empty_index(self):
        """Test querying empty index."""
        lsh = MinHashLSH(threshold=0.5, num_perm=128)
        query = create_minhash(["a", "b"], num_perm=128)

        results = lsh.query(query)
        assert results == []

    def test_remove(self):
        """Test removing items."""
        lsh = MinHashLSH(threshold=0.5, num_perm=128)

        mh = create_minhash(["a", "b"], num_perm=128)
        lsh.insert("doc1", mh)

        assert len(lsh) == 1
        assert "doc1" in lsh

        # Remove
        removed = lsh.remove("doc1")
        assert removed is True
        assert len(lsh) == 0
        assert "doc1" not in lsh

        # Remove non-existent
        removed = lsh.remove("doc2")
        assert removed is False

    def test_get_counts(self):
        """Test statistics retrieval."""
        lsh = MinHashLSH(threshold=0.5, num_perm=128)

        for i in range(10):
            mh = create_minhash([f"item{i}"], num_perm=128)
            lsh.insert(f"doc{i}", mh)

        stats = lsh.get_counts()
        assert stats['num_items'] == 10
        assert stats['num_bands'] == lsh.b
        assert stats['rows_per_band'] == lsh.r


class TestMinHashLSHForestBasics:
    """Tests for MinHashLSHForest basic functionality."""

    def test_initialization(self):
        """Test LSH Forest initialization."""
        forest = MinHashLSHForest(num_perm=128)
        assert forest.num_perm == 128
        assert len(forest) == 0
        assert not forest.is_indexed

    def test_insert_before_index(self):
        """Test inserting before indexing."""
        forest = MinHashLSHForest(num_perm=128)

        mh = create_minhash(["a", "b", "c"], num_perm=128)
        forest.insert("doc1", mh)

        assert len(forest) == 1
        assert not forest.is_indexed

    def test_insert_after_index(self):
        """Test error on insert after indexing."""
        forest = MinHashLSHForest(num_perm=128)

        mh = create_minhash(["a", "b"], num_perm=128)
        forest.insert("doc1", mh)
        forest.index()

        mh2 = create_minhash(["c", "d"], num_perm=128)
        with pytest.raises(RuntimeError, match="already indexed"):
            forest.insert("doc2", mh2)

    def test_index(self):
        """Test building index."""
        forest = MinHashLSHForest(num_perm=128)

        for i in range(10):
            mh = create_minhash([f"item{i}"], num_perm=128)
            forest.insert(f"doc{i}", mh)

        forest.index()
        assert forest.is_indexed

        # Indexing again should be idempotent
        forest.index()
        assert forest.is_indexed


class TestMinHashLSHForestQuery:
    """Tests for LSH Forest query operations."""

    def test_query_before_index(self):
        """Test error on query before indexing."""
        forest = MinHashLSHForest(num_perm=128)

        mh = create_minhash(["a", "b"], num_perm=128)
        forest.insert("doc1", mh)

        query = create_minhash(["a", "b"], num_perm=128)
        with pytest.raises(RuntimeError, match="not indexed"):
            forest.query(query, k=5)

    def test_query_top_k(self):
        """Test Top-K query."""
        forest = MinHashLSHForest(num_perm=128)

        # Insert documents with varying similarity
        docs = {
            "doc1": ["a", "b", "c", "d"],
            "doc2": ["a", "b", "c", "e"],
            "doc3": ["a", "b", "f", "g"],
            "doc4": ["a", "h", "i", "j"],
            "doc5": ["x", "y", "z", "w"],
        }

        for doc_id, words in docs.items():
            mh = create_minhash(words, num_perm=128)
            forest.insert(doc_id, mh)

        forest.index()

        # Query for top 3
        query = create_minhash(["a", "b", "c", "d"], num_perm=128)
        results = forest.query(query, k=3)

        assert len(results) <= 3
        assert "doc1" in results  # Should find itself or very similar

    def test_query_with_similarity(self):
        """Test Top-K query with scores."""
        forest = MinHashLSHForest(num_perm=128)

        docs = {
            "doc1": ["a", "b", "c"],
            "doc2": ["a", "b", "d"],
            "doc3": ["a", "e", "f"],
        }

        for doc_id, words in docs.items():
            mh = create_minhash(words, num_perm=128)
            forest.insert(doc_id, mh)

        forest.index()

        query = create_minhash(["a", "b", "c"], num_perm=128)
        results = forest.query_with_similarity(query, k=2)

        assert len(results) <= 2
        # Should be sorted by similarity
        for i in range(len(results) - 1):
            assert results[i][1] >= results[i + 1][1]

    def test_query_k_larger_than_index(self):
        """Test querying for more items than in index."""
        forest = MinHashLSHForest(num_perm=128)

        # Only 3 items
        for i in range(3):
            mh = create_minhash([f"item{i}"], num_perm=128)
            forest.insert(f"doc{i}", mh)

        forest.index()

        query = create_minhash(["item0"], num_perm=128)
        results = forest.query(query, k=10)

        # Should return at most 3
        assert len(results) <= 3

    def test_clear(self):
        """Test clearing forest."""
        forest = MinHashLSHForest(num_perm=128)

        mh = create_minhash(["a", "b"], num_perm=128)
        forest.insert("doc1", mh)
        forest.index()

        assert len(forest) == 1
        assert forest.is_indexed

        forest.clear()

        assert len(forest) == 0
        assert not forest.is_indexed


class TestMinHashLSHEnsembleBasics:
    """Tests for MinHashLSHEnsemble basic functionality."""

    def test_initialization(self):
        """Test LSH Ensemble initialization."""
        ensemble = MinHashLSHEnsemble(threshold=0.5, num_perm=128, num_part=8)
        assert ensemble.threshold == 0.5
        assert ensemble.num_perm == 128
        assert ensemble.num_part == 8
        assert len(ensemble) == 0
        assert not ensemble.is_indexed

    def test_invalid_parameters(self):
        """Test parameter validation."""
        with pytest.raises(ValueError, match="between 0 and 1"):
            MinHashLSHEnsemble(threshold=0.0)

        with pytest.raises(ValueError, match="num_perm"):
            MinHashLSHEnsemble(num_perm=0)

        with pytest.raises(ValueError, match="num_part"):
            MinHashLSHEnsemble(num_part=0)

    def test_insert_with_size(self):
        """Test inserting with set size."""
        ensemble = MinHashLSHEnsemble(threshold=0.5, num_perm=128)

        mh = create_minhash(["a", "b", "c"], num_perm=128)
        ensemble.insert("doc1", mh, size=3)

        assert len(ensemble) == 1

    def test_insert_invalid_size(self):
        """Test error on invalid size."""
        ensemble = MinHashLSHEnsemble(threshold=0.5, num_perm=128)
        mh = create_minhash(["a", "b"], num_perm=128)

        with pytest.raises(ValueError, match="at least 1"):
            ensemble.insert("doc1", mh, size=0)

        with pytest.raises(ValueError, match="at least 1"):
            ensemble.insert("doc1", mh, size=-1)

    def test_index(self):
        """Test building index."""
        ensemble = MinHashLSHEnsemble(threshold=0.5, num_perm=128)

        # Insert documents with varying sizes
        for i in range(20):
            mh = create_minhash([f"item{j}" for j in range(i + 1)], num_perm=128)
            ensemble.insert(f"doc{i}", mh, size=i + 1)

        ensemble.index()
        assert ensemble.is_indexed


class TestMinHashLSHEnsembleQuery:
    """Tests for LSH Ensemble containment queries."""

    def test_query_containment(self):
        """Test containment query."""
        ensemble = MinHashLSHEnsemble(threshold=0.7, num_perm=128, num_part=4)

        # Create documents: some contain others
        docs = {
            "doc1": (["a", "b"], 2),
            "doc2": (["a", "b", "c"], 3),
            "doc3": (["a", "b", "c", "d"], 4),
            "doc4": (["x", "y", "z"], 3),
        }

        for doc_id, (words, size) in docs.items():
            mh = create_minhash(words, num_perm=128)
            ensemble.insert(doc_id, mh, size)

        ensemble.index()

        # Query with ["a", "b"] - should find supersets
        query = create_minhash(["a", "b"], num_perm=128)
        results = ensemble.query(query, size=2)

        # doc2 and doc3 contain most of query
        # (actual results depend on threshold and estimation)
        assert isinstance(results, list)

    def test_query_with_containment_scores(self):
        """Test query with containment scores."""
        ensemble = MinHashLSHEnsemble(threshold=0.5, num_perm=128, num_part=4)

        docs = {
            "doc1": (["a", "b", "c"], 3),
            "doc2": (["a", "b", "d"], 3),
            "doc3": (["a", "e", "f"], 3),
        }

        for doc_id, (words, size) in docs.items():
            mh = create_minhash(words, num_perm=128)
            ensemble.insert(doc_id, mh, size)

        ensemble.index()

        query = create_minhash(["a", "b"], num_perm=128)
        results = ensemble.query_with_containment(query, size=2)

        # Should return (key, score) tuples
        assert all(isinstance(r, tuple) and len(r) == 2 for r in results)

        # Should be sorted by containment
        for i in range(len(results) - 1):
            assert results[i][1] >= results[i + 1][1]

    def test_query_empty_index(self):
        """Test querying empty ensemble."""
        ensemble = MinHashLSHEnsemble(threshold=0.5, num_perm=128)
        ensemble.index()  # Index empty ensemble

        query = create_minhash(["a", "b"], num_perm=128)
        results = ensemble.query(query, size=2)

        assert results == []

    def test_clear(self):
        """Test clearing ensemble."""
        ensemble = MinHashLSHEnsemble(threshold=0.5, num_perm=128)

        mh = create_minhash(["a", "b"], num_perm=128)
        ensemble.insert("doc1", mh, size=2)
        ensemble.index()

        assert len(ensemble) == 1
        assert ensemble.is_indexed

        ensemble.clear()

        assert len(ensemble) == 0
        assert not ensemble.is_indexed


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_build_lsh_index(self):
        """Test build_lsh_index function."""
        minhashes = {
            "doc1": create_minhash(["a", "b"], num_perm=128),
            "doc2": create_minhash(["c", "d"], num_perm=128),
        }

        lsh = build_lsh_index(minhashes, threshold=0.5)

        assert isinstance(lsh, MinHashLSH)
        assert len(lsh) == 2

    def test_build_lsh_forest(self):
        """Test build_lsh_forest function."""
        minhashes = {
            "doc1": create_minhash(["a", "b"], num_perm=128),
            "doc2": create_minhash(["c", "d"], num_perm=128),
        }

        forest = build_lsh_forest(minhashes)

        assert isinstance(forest, MinHashLSHForest)
        assert len(forest) == 2
        assert forest.is_indexed

    def test_build_empty_index(self):
        """Test error on building from empty dict."""
        with pytest.raises(ValueError, match="empty"):
            build_lsh_index({}, threshold=0.5)

        with pytest.raises(ValueError, match="empty"):
            build_lsh_forest({})


class TestMinHashLSHMonoid:
    """Tests for MinHashLSH monoid properties."""

    def test_combine(self):
        """Test combining two LSH indexes."""
        lsh1 = MinHashLSH(threshold=0.5, num_perm=128)
        lsh2 = MinHashLSH(threshold=0.5, num_perm=128)

        # Insert different items into each
        mh1 = create_minhash(["a", "b"], num_perm=128)
        mh2 = create_minhash(["c", "d"], num_perm=128)

        lsh1.insert("doc1", mh1)
        lsh2.insert("doc2", mh2)

        # Combine
        merged = lsh1.combine(lsh2)

        assert len(merged) == 2
        assert "doc1" in merged
        assert "doc2" in merged

    def test_combine_with_overlapping_keys(self):
        """Test combining indexes with same keys (later overwrites)."""
        lsh1 = MinHashLSH(threshold=0.5, num_perm=128)
        lsh2 = MinHashLSH(threshold=0.5, num_perm=128)

        # Insert same key with different MinHash
        mh1 = create_minhash(["a", "b"], num_perm=128)
        mh2 = create_minhash(["c", "d"], num_perm=128)

        lsh1.insert("doc1", mh1)
        lsh2.insert("doc1", mh2)

        # Combine - lsh2's version should win
        merged = lsh1.combine(lsh2)

        assert len(merged) == 1
        assert "doc1" in merged
        # Check that it's the second MinHash
        assert merged.minhashes["doc1"] == mh2

    def test_combine_invalid_threshold(self):
        """Test error on combining indexes with different thresholds."""
        lsh1 = MinHashLSH(threshold=0.5, num_perm=128)
        lsh2 = MinHashLSH(threshold=0.7, num_perm=128)

        with pytest.raises(ValueError, match="different thresholds"):
            lsh1.combine(lsh2)

    def test_combine_invalid_num_perm(self):
        """Test error on combining indexes with different num_perm."""
        lsh1 = MinHashLSH(threshold=0.5, num_perm=128)
        lsh2 = MinHashLSH(threshold=0.5, num_perm=64)

        with pytest.raises(ValueError, match="different num_perm"):
            lsh1.combine(lsh2)

    def test_combine_invalid_params(self):
        """Test error on combining indexes with different (b, r)."""
        lsh1 = MinHashLSH(threshold=0.5, num_perm=128, params=(16, 8))
        lsh2 = MinHashLSH(threshold=0.5, num_perm=128, params=(8, 16))

        with pytest.raises(ValueError, match="different \\(b, r\\) parameters"):
            lsh1.combine(lsh2)

    def test_add_operator(self):
        """Test + operator for combining."""
        lsh1 = MinHashLSH(threshold=0.5, num_perm=128)
        lsh2 = MinHashLSH(threshold=0.5, num_perm=128)

        mh1 = create_minhash(["a", "b"], num_perm=128)
        mh2 = create_minhash(["c", "d"], num_perm=128)

        lsh1.insert("doc1", mh1)
        lsh2.insert("doc2", mh2)

        # Use + operator
        merged = lsh1 + lsh2

        assert isinstance(merged, MinHashLSH)
        assert len(merged) == 2
        assert "doc1" in merged
        assert "doc2" in merged

    def test_sum_builtin(self):
        """Test sum() builtin support."""
        indexes = []
        for i in range(3):
            lsh = MinHashLSH(threshold=0.5, num_perm=128)
            mh = create_minhash([f"word{i}"], num_perm=128)
            lsh.insert(f"doc{i}", mh)
            indexes.append(lsh)

        # Use sum()
        merged = sum(indexes)

        assert isinstance(merged, MinHashLSH)
        assert len(merged) == 3
        assert "doc0" in merged
        assert "doc1" in merged
        assert "doc2" in merged

    def test_zero_property(self):
        """Test monoid zero property."""
        lsh = MinHashLSH(threshold=0.7, num_perm=128)
        mh = create_minhash(["a", "b"], num_perm=128)
        lsh.insert("doc1", mh)

        zero = lsh.zero

        assert isinstance(zero, MinHashLSH)
        assert zero.threshold == lsh.threshold
        assert zero.num_perm == lsh.num_perm
        assert zero.b == lsh.b
        assert zero.r == lsh.r
        assert zero.is_zero()

    def test_identity_property(self):
        """Test monoid identity: lsh + zero = lsh."""
        lsh = MinHashLSH(threshold=0.5, num_perm=128)
        mh = create_minhash(["a", "b", "c"], num_perm=128)
        lsh.insert("doc1", mh)

        zero = lsh.zero
        result = lsh + zero

        # Should be equal (same items)
        assert len(result) == len(lsh)
        assert result.keys == lsh.keys

    def test_associativity(self):
        """Test monoid associativity: (a + b) + c = a + (b + c)."""
        lsh1 = MinHashLSH(threshold=0.5, num_perm=128)
        lsh2 = MinHashLSH(threshold=0.5, num_perm=128)
        lsh3 = MinHashLSH(threshold=0.5, num_perm=128)

        mh1 = create_minhash(["a", "b"], num_perm=128)
        mh2 = create_minhash(["c", "d"], num_perm=128)
        mh3 = create_minhash(["e", "f"], num_perm=128)

        lsh1.insert("doc1", mh1)
        lsh2.insert("doc2", mh2)
        lsh3.insert("doc3", mh3)

        # (lsh1 + lsh2) + lsh3
        result1 = (lsh1 + lsh2) + lsh3

        # lsh1 + (lsh2 + lsh3)
        result2 = lsh1 + (lsh2 + lsh3)

        # Should have same keys
        assert result1.keys == result2.keys
        assert len(result1) == 3
        assert len(result2) == 3

    def test_is_zero(self):
        """Test is_zero method."""
        lsh = MinHashLSH(threshold=0.5, num_perm=128)
        assert lsh.is_zero()

        mh = create_minhash(["a"], num_perm=128)
        lsh.insert("doc1", mh)

        assert not lsh.is_zero()

    def test_equality(self):
        """Test LSH equality."""
        lsh1 = MinHashLSH(threshold=0.5, num_perm=128)
        lsh2 = MinHashLSH(threshold=0.5, num_perm=128)

        mh = create_minhash(["a", "b", "c"], num_perm=128)

        lsh1.insert("doc1", mh)
        lsh2.insert("doc1", mh)

        assert lsh1 == lsh2

    def test_inequality_different_items(self):
        """Test inequality with different items."""
        lsh1 = MinHashLSH(threshold=0.5, num_perm=128)
        lsh2 = MinHashLSH(threshold=0.5, num_perm=128)

        mh1 = create_minhash(["a", "b"], num_perm=128)
        mh2 = create_minhash(["c", "d"], num_perm=128)

        lsh1.insert("doc1", mh1)
        lsh2.insert("doc2", mh2)

        assert lsh1 != lsh2

    def test_inequality_different_params(self):
        """Test inequality with different parameters."""
        lsh1 = MinHashLSH(threshold=0.5, num_perm=128)
        lsh2 = MinHashLSH(threshold=0.7, num_perm=128)

        assert lsh1 != lsh2

    def test_distributed_construction(self):
        """Test distributed LSH construction pattern."""
        # Simulate 3 servers building LSH indexes
        server_indexes = []

        for server_id in range(3):
            lsh = MinHashLSH(threshold=0.6, num_perm=128)

            # Each server processes different documents
            for doc_id in range(10):
                words = [f"server{server_id}_word{doc_id}"]
                mh = create_minhash(words, num_perm=128)
                lsh.insert(f"server{server_id}_doc{doc_id}", mh)

            server_indexes.append(lsh)

        # Merge all server indexes
        global_lsh = sum(server_indexes)

        # Should contain all documents
        assert len(global_lsh) == 30

        # Can query the merged index
        query = create_minhash(["server1_word5"], num_perm=128)
        results = global_lsh.query(query)
        assert isinstance(results, list)
