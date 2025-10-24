"""Tests for MinHash and Weighted MinHash."""

import pytest
from algesnake.approximate import MinHash, WeightedMinHash
from algesnake.approximate.minhash import (
    create_minhash, estimate_jaccard, jaccard_similarity
)
from algesnake.approximate.weighted_minhash import (
    create_weighted_minhash, estimate_weighted_jaccard
)


class TestMinHashBasics:
    """Tests for basic MinHash functionality."""

    def test_initialization(self):
        """Test MinHash initialization."""
        mh = MinHash(num_perm=128)
        assert mh.num_perm == 128
        assert mh.seed == 1
        assert len(mh.hashvalues) == 128
        assert mh.is_zero()

    def test_invalid_num_perm(self):
        """Test validation of num_perm."""
        with pytest.raises(ValueError, match="num_perm must be at least 1"):
            MinHash(num_perm=0)

        with pytest.raises(ValueError, match="num_perm must be at least 1"):
            MinHash(num_perm=-1)

    def test_update(self):
        """Test adding items to MinHash."""
        mh = MinHash(num_perm=128)
        assert mh.is_empty()

        mh.update("item1")
        assert not mh.is_empty()

        # Adding same item again shouldn't change signature
        before = mh.hashvalues.copy()
        mh.update("item1")
        assert mh.hashvalues == before

    def test_update_batch(self):
        """Test batch update."""
        mh = MinHash(num_perm=128)
        items = ["a", "b", "c", "d"]
        mh.update_batch(items)

        # Compare with individual updates
        mh2 = MinHash(num_perm=128, seed=1)
        for item in items:
            mh2.update(item)

        assert mh == mh2


class TestMinHashJaccardSimilarity:
    """Tests for Jaccard similarity estimation."""

    def test_identical_sets(self):
        """Test Jaccard similarity of identical sets."""
        items = ["a", "b", "c", "d", "e"]

        mh1 = MinHash(num_perm=128)
        mh1.update_batch(items)

        mh2 = MinHash(num_perm=128)
        mh2.update_batch(items)

        similarity = mh1.jaccard(mh2)
        assert similarity == 1.0

    def test_disjoint_sets(self):
        """Test Jaccard similarity of disjoint sets."""
        mh1 = MinHash(num_perm=128)
        mh1.update_batch(["a", "b", "c"])

        mh2 = MinHash(num_perm=128)
        mh2.update_batch(["d", "e", "f"])

        similarity = mh1.jaccard(mh2)
        # Disjoint sets should have ~0 similarity
        assert similarity < 0.1

    def test_partial_overlap(self):
        """Test Jaccard similarity with partial overlap."""
        mh1 = MinHash(num_perm=256)
        mh1.update_batch(["a", "b", "c"])

        mh2 = MinHash(num_perm=256)
        mh2.update_batch(["b", "c", "d"])

        # Jaccard = |{b, c}| / |{a, b, c, d}| = 2/4 = 0.5
        similarity = mh1.jaccard(mh2)
        assert 0.3 < similarity < 0.7  # Allow some error

    def test_accuracy_with_large_sets(self):
        """Test accuracy with larger sets."""
        set1 = set(f"item{i}" for i in range(1000))
        set2 = set(f"item{i}" for i in range(500, 1500))

        # True Jaccard = 500 / 1500 = 0.333...
        true_jaccard = len(set1 & set2) / len(set1 | set2)

        mh1 = create_minhash(set1, num_perm=256)
        mh2 = create_minhash(set2, num_perm=256)

        estimated = mh1.jaccard(mh2)
        error = abs(estimated - true_jaccard)

        # Should be within 5% with 256 permutations
        assert error < 0.05

    def test_jaccard_different_num_perm(self):
        """Test error when comparing MinHashes with different num_perm."""
        mh1 = MinHash(num_perm=128)
        mh2 = MinHash(num_perm=64)

        with pytest.raises(ValueError, match="different num_perm"):
            mh1.jaccard(mh2)

    def test_jaccard_different_seeds(self):
        """Test error when comparing MinHashes with different seeds."""
        mh1 = MinHash(num_perm=128, seed=1)
        mh2 = MinHash(num_perm=128, seed=2)

        with pytest.raises(ValueError, match="different seeds"):
            mh1.jaccard(mh2)


class TestMinHashMonoid:
    """Tests for MinHash monoid properties."""

    def test_combine(self):
        """Test combining MinHashes (union operation)."""
        mh1 = MinHash(num_perm=128)
        mh1.update_batch(["a", "b", "c"])

        mh2 = MinHash(num_perm=128)
        mh2.update_batch(["c", "d", "e"])

        # Combine should be equivalent to union
        combined = mh1.combine(mh2)

        # Compare with direct union
        mh_union = MinHash(num_perm=128)
        mh_union.update_batch(["a", "b", "c", "d", "e"])

        assert combined == mh_union

    def test_add_operator(self):
        """Test + operator for combining."""
        mh1 = MinHash(num_perm=128)
        mh1.update_batch(["a", "b"])

        mh2 = MinHash(num_perm=128)
        mh2.update_batch(["c", "d"])

        combined = mh1 + mh2
        assert isinstance(combined, MinHash)

    def test_sum_builtin(self):
        """Test sum() builtin support."""
        minhashes = [
            create_minhash(["a", "b"], num_perm=128),
            create_minhash(["c", "d"], num_perm=128),
            create_minhash(["e", "f"], num_perm=128),
        ]

        result = sum(minhashes)
        assert isinstance(result, MinHash)

        # Should equal union of all
        expected = create_minhash(["a", "b", "c", "d", "e", "f"], num_perm=128)
        assert result == expected

    def test_zero_property(self):
        """Test monoid zero property."""
        mh = MinHash(num_perm=128)
        mh.update_batch(["a", "b", "c"])

        zero = mh.zero
        assert zero.is_zero()

        # Adding zero shouldn't change result
        result = mh + zero
        assert result == mh

    def test_associativity(self):
        """Test associative property of combine."""
        mh1 = create_minhash(["a", "b"], num_perm=128)
        mh2 = create_minhash(["c", "d"], num_perm=128)
        mh3 = create_minhash(["e", "f"], num_perm=128)

        # (a + b) + c
        result1 = (mh1 + mh2) + mh3

        # a + (b + c)
        result2 = mh1 + (mh2 + mh3)

        assert result1 == result2


class TestWeightedMinHashBasics:
    """Tests for Weighted MinHash."""

    def test_initialization(self):
        """Test Weighted MinHash initialization."""
        wmh = WeightedMinHash(num_perm=128)
        assert wmh.num_perm == 128
        assert wmh.seed == 1
        assert len(wmh.hashvalues) == 128
        assert wmh.is_zero()

    def test_update_with_weight(self):
        """Test adding weighted items."""
        wmh = WeightedMinHash(num_perm=128)
        wmh.update("cat", 3.0)
        wmh.update("dog", 2.0)

        assert not wmh.is_zero()

    def test_negative_weight(self):
        """Test error on negative weight."""
        wmh = WeightedMinHash(num_perm=128)

        with pytest.raises(ValueError, match="non-negative"):
            wmh.update("item", -1.0)

    def test_zero_weight(self):
        """Test zero weight (should be ignored)."""
        wmh = WeightedMinHash(num_perm=128)
        wmh.update("item", 0.0)

        assert wmh.is_zero()

    def test_update_batch(self):
        """Test batch weighted update."""
        wmh = WeightedMinHash(num_perm=128)
        items = {"cat": 3.0, "dog": 2.0, "bird": 1.0}
        wmh.update_batch(items)

        assert not wmh.is_zero()


class TestWeightedJaccardSimilarity:
    """Tests for weighted Jaccard similarity."""

    def test_identical_weighted_sets(self):
        """Test similarity of identical weighted sets."""
        items = {"a": 1.0, "b": 2.0, "c": 3.0}

        wmh1 = create_weighted_minhash(items, num_perm=128)
        wmh2 = create_weighted_minhash(items, num_perm=128)

        similarity = wmh1.jaccard(wmh2)
        assert similarity == 1.0

    def test_disjoint_weighted_sets(self):
        """Test similarity of disjoint weighted sets."""
        wmh1 = create_weighted_minhash({"a": 1.0, "b": 2.0}, num_perm=128)
        wmh2 = create_weighted_minhash({"c": 1.0, "d": 2.0}, num_perm=128)

        similarity = wmh1.jaccard(wmh2)
        assert similarity < 0.1

    def test_weighted_vs_unweighted(self):
        """Test that weights matter."""
        # Heavy weight on common item
        wmh1 = create_weighted_minhash({"a": 10.0, "b": 1.0}, num_perm=256)
        wmh2 = create_weighted_minhash({"a": 10.0, "c": 1.0}, num_perm=256)

        # Light weight on common item
        wmh3 = create_weighted_minhash({"a": 1.0, "b": 10.0}, num_perm=256)
        wmh4 = create_weighted_minhash({"a": 1.0, "c": 10.0}, num_perm=256)

        sim_heavy = wmh1.jaccard(wmh2)
        sim_light = wmh3.jaccard(wmh4)

        # Heavy overlap should have higher similarity
        assert sim_heavy > sim_light


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_estimate_jaccard(self):
        """Test estimate_jaccard function."""
        set1 = {"a", "b", "c"}
        set2 = {"b", "c", "d"}

        similarity = estimate_jaccard(set1, set2, num_perm=128)
        assert 0 <= similarity <= 1.0

        # True Jaccard = 2/4 = 0.5
        assert 0.3 < similarity < 0.7

    def test_estimate_weighted_jaccard(self):
        """Test estimate_weighted_jaccard function."""
        items1 = {"a": 1.0, "b": 2.0, "c": 3.0}
        items2 = {"b": 2.0, "c": 3.0, "d": 4.0}

        similarity = estimate_weighted_jaccard(items1, items2, num_perm=128)
        assert 0 <= similarity <= 1.0

    def test_create_minhash(self):
        """Test create_minhash function."""
        items = ["a", "b", "c"]
        mh = create_minhash(items, num_perm=64)

        assert isinstance(mh, MinHash)
        assert mh.num_perm == 64
        assert not mh.is_empty()


class TestMinHashEquality:
    """Tests for MinHash equality."""

    def test_equality(self):
        """Test MinHash equality."""
        mh1 = create_minhash(["a", "b", "c"], num_perm=128)
        mh2 = create_minhash(["a", "b", "c"], num_perm=128)

        assert mh1 == mh2

    def test_inequality_different_items(self):
        """Test inequality with different items."""
        mh1 = create_minhash(["a", "b"], num_perm=128)
        mh2 = create_minhash(["c", "d"], num_perm=128)

        assert mh1 != mh2

    def test_inequality_different_type(self):
        """Test inequality with different type."""
        mh = MinHash(num_perm=128)
        assert mh != "not a minhash"
        assert mh != 42


class TestMinHashCopy:
    """Tests for MinHash copy."""

    def test_copy(self):
        """Test MinHash copy."""
        mh1 = create_minhash(["a", "b", "c"], num_perm=128)
        mh2 = mh1.copy()

        assert mh1 == mh2
        assert mh1 is not mh2
        assert mh1.hashvalues is not mh2.hashvalues

        # Modifying copy shouldn't affect original
        mh2.update("d")
        assert mh1 != mh2
