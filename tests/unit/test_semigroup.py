"""
Unit tests for Semigroup abstract base class.
"""

import pytest
from algesnake.abstract.semigroup import Semigroup, SemigroupWrapper


class MaxSemigroup(Semigroup[int]):
    """Example semigroup for testing: max operation."""

    def combine(self, a: int, b: int) -> int:
        return max(a, b)


class MinSemigroup(Semigroup[int]):
    """Example semigroup for testing: min operation."""

    def combine(self, a: int, b: int) -> int:
        return min(a, b)


class AddSemigroup(Semigroup[int]):
    """Example semigroup for testing: addition."""

    def combine(self, a: int, b: int) -> int:
        return a + b


class TestSemigroup:
    """Test Semigroup abstract base class."""

    def test_max_combine(self):
        """Test max semigroup combines correctly."""
        sg = MaxSemigroup()
        assert sg.combine(5, 3) == 5
        assert sg.combine(2, 7) == 7
        assert sg.combine(4, 4) == 4

    def test_min_combine(self):
        """Test min semigroup combines correctly."""
        sg = MinSemigroup()
        assert sg.combine(5, 3) == 3
        assert sg.combine(2, 7) == 2
        assert sg.combine(4, 4) == 4

    def test_add_combine(self):
        """Test addition semigroup combines correctly."""
        sg = AddSemigroup()
        assert sg.combine(5, 3) == 8
        assert sg.combine(2, 7) == 9
        assert sg.combine(-4, 4) == 0

    def test_combine_all_non_empty(self):
        """Test combine_all with non-empty list."""
        sg = MaxSemigroup()
        result = sg.combine_all([1, 5, 3, 2, 4])
        assert result == 5

    def test_combine_all_single_element(self):
        """Test combine_all with single element."""
        sg = MaxSemigroup()
        result = sg.combine_all([42])
        assert result == 42

    def test_combine_all_empty(self):
        """Test combine_all with empty list returns None."""
        sg = MaxSemigroup()
        result = sg.combine_all([])
        assert result is None

    def test_verify_associativity_true(self):
        """Test associativity verification succeeds for valid semigroups."""
        sg = MaxSemigroup()
        assert sg.verify_associativity(1, 2, 3) is True
        assert sg.verify_associativity(5, 5, 5) is True
        assert sg.verify_associativity(10, 1, 7) is True

        sg2 = AddSemigroup()
        assert sg2.verify_associativity(1, 2, 3) is True
        assert sg2.verify_associativity(-5, 10, 3) is True

    def test_verify_associativity_with_floats(self):
        """Test associativity verification works with floating point."""

        class FloatAddSemigroup(Semigroup[float]):
            def combine(self, a: float, b: float) -> float:
                return a + b

        sg = FloatAddSemigroup()
        assert sg.verify_associativity(1.1, 2.2, 3.3) is True
        assert sg.verify_associativity(0.1, 0.2, 0.3, epsilon=1e-10) is True

    def test_semigroup_wrapper(self):
        """Test SemigroupWrapper creates semigroup from function."""
        max_sg = SemigroupWrapper(lambda a, b: max(a, b))
        assert max_sg.combine(5, 3) == 5
        assert max_sg.combine(2, 7) == 7

        add_sg = SemigroupWrapper(lambda a, b: a + b)
        assert add_sg.combine(5, 3) == 8

    def test_semigroup_wrapper_combine_all(self):
        """Test SemigroupWrapper with combine_all."""
        max_sg = SemigroupWrapper(max)
        result = max_sg.combine_all([1, 5, 3, 2])
        assert result == 5

    def test_repr(self):
        """Test string representation."""
        sg = MaxSemigroup()
        assert repr(sg) == "MaxSemigroup()"

        wrapped = SemigroupWrapper(max)
        assert "SemigroupWrapper" in repr(wrapped)
        assert "max" in repr(wrapped)


class TestSemigroupLaws:
    """Test that example semigroups satisfy algebraic laws using property-based testing."""

    @pytest.mark.parametrize("a,b,c", [
        (1, 2, 3),
        (5, 5, 5),
        (10, -5, 7),
        (0, 0, 0),
        (-100, 50, -25),
    ])
    def test_max_associativity(self, a, b, c):
        """Test max semigroup is associative."""
        sg = MaxSemigroup()
        left = sg.combine(sg.combine(a, b), c)
        right = sg.combine(a, sg.combine(b, c))
        assert left == right

    @pytest.mark.parametrize("a,b,c", [
        (1, 2, 3),
        (5, 5, 5),
        (10, -5, 7),
        (0, 0, 0),
        (-100, 50, -25),
    ])
    def test_add_associativity(self, a, b, c):
        """Test addition semigroup is associative."""
        sg = AddSemigroup()
        left = sg.combine(sg.combine(a, b), c)
        right = sg.combine(a, sg.combine(b, c))
        assert left == right
