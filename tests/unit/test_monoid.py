"""
Unit tests for Monoid abstract base class.
"""

import pytest
from algesnake.abstract.monoid import Monoid, MonoidWrapper


class AddMonoid(Monoid[int]):
    """Addition monoid for testing."""

    @property
    def zero(self) -> int:
        return 0

    def combine(self, a: int, b: int) -> int:
        return a + b


class MaxMonoid(Monoid[int]):
    """Max monoid for testing (with -inf as zero)."""

    @property
    def zero(self) -> int:
        return -2**31  # Approximate -infinity for int

    def combine(self, a: int, b: int) -> int:
        return max(a, b)


class MinMonoid(Monoid[int]):
    """Min monoid for testing (with +inf as zero)."""

    @property
    def zero(self) -> int:
        return 2**31 - 1  # Approximate +infinity for int

    def combine(self, a: int, b: int) -> int:
        return min(a, b)


class MultiplyMonoid(Monoid[int]):
    """Multiplication monoid for testing."""

    @property
    def zero(self) -> int:
        return 1

    def combine(self, a: int, b: int) -> int:
        return a * b


class TestMonoid:
    """Test Monoid abstract base class."""

    def test_add_monoid_zero(self):
        """Test add monoid has correct zero."""
        m = AddMonoid()
        assert m.zero == 0

    def test_add_monoid_combine(self):
        """Test add monoid combines correctly."""
        m = AddMonoid()
        assert m.combine(5, 3) == 8
        assert m.combine(0, 5) == 5
        assert m.combine(5, 0) == 5

    def test_max_monoid(self):
        """Test max monoid."""
        m = MaxMonoid()
        assert m.combine(5, 3) == 5
        assert m.combine(m.zero, 5) == 5
        assert m.combine(5, m.zero) == 5

    def test_multiply_monoid(self):
        """Test multiplication monoid."""
        m = MultiplyMonoid()
        assert m.zero == 1
        assert m.combine(5, 3) == 15
        assert m.combine(m.zero, 5) == 5
        assert m.combine(5, m.zero) == 5

    def test_combine_all_empty(self):
        """Test combine_all with empty list returns zero."""
        m = AddMonoid()
        assert m.combine_all([]) == 0

        m2 = MultiplyMonoid()
        assert m2.combine_all([]) == 1

    def test_combine_all_non_empty(self):
        """Test combine_all with non-empty list."""
        m = AddMonoid()
        assert m.combine_all([1, 2, 3, 4]) == 10

        m2 = MultiplyMonoid()
        assert m2.combine_all([2, 3, 4]) == 24

    def test_combine_all_single(self):
        """Test combine_all with single element."""
        m = AddMonoid()
        assert m.combine_all([42]) == 42

    def test_combine_option_both_values(self):
        """Test combine_option with two values."""
        m = AddMonoid()
        assert m.combine_option(5, 3) == 8

    def test_combine_option_first_none(self):
        """Test combine_option with first value None."""
        m = AddMonoid()
        assert m.combine_option(None, 3) == 3

    def test_combine_option_second_none(self):
        """Test combine_option with second value None."""
        m = AddMonoid()
        assert m.combine_option(5, None) == 5

    def test_combine_option_both_none(self):
        """Test combine_option with both values None."""
        m = AddMonoid()
        assert m.combine_option(None, None) == 0

    def test_verify_left_identity(self):
        """Test left identity verification."""
        m = AddMonoid()
        assert m.verify_left_identity(42) is True
        assert m.verify_left_identity(0) is True
        assert m.verify_left_identity(-100) is True

        m2 = MultiplyMonoid()
        assert m2.verify_left_identity(42) is True
        assert m2.verify_left_identity(1) is True

    def test_verify_right_identity(self):
        """Test right identity verification."""
        m = AddMonoid()
        assert m.verify_right_identity(42) is True
        assert m.verify_right_identity(0) is True
        assert m.verify_right_identity(-100) is True

        m2 = MultiplyMonoid()
        assert m2.verify_right_identity(42) is True
        assert m2.verify_right_identity(1) is True

    def test_verify_identity(self):
        """Test identity verification (both left and right)."""
        m = AddMonoid()
        assert m.verify_identity(42) is True
        assert m.verify_identity(0) is True

        m2 = MultiplyMonoid()
        assert m2.verify_identity(42) is True

    def test_verify_monoid_laws(self):
        """Test full monoid law verification."""
        m = AddMonoid()
        assert m.verify_monoid_laws(1, 2, 3) is True
        assert m.verify_monoid_laws(0, 0, 0) is True
        assert m.verify_monoid_laws(-5, 10, 3) is True

        m2 = MultiplyMonoid()
        assert m2.verify_monoid_laws(2, 3, 4) is True

    def test_monoid_wrapper(self):
        """Test MonoidWrapper creates monoid from functions."""
        add_m = MonoidWrapper(lambda a, b: a + b, 0)
        assert add_m.zero == 0
        assert add_m.combine(5, 3) == 8
        assert add_m.combine_all([1, 2, 3]) == 6
        assert add_m.combine_all([]) == 0

    def test_monoid_wrapper_multiply(self):
        """Test MonoidWrapper with multiplication."""
        mul_m = MonoidWrapper(lambda a, b: a * b, 1)
        assert mul_m.zero == 1
        assert mul_m.combine(5, 3) == 15
        assert mul_m.combine_all([2, 3, 4]) == 24

    def test_repr(self):
        """Test string representation."""
        m = AddMonoid()
        assert repr(m) == "AddMonoid()"

        wrapped = MonoidWrapper(lambda a, b: a + b, 0)
        assert "MonoidWrapper" in repr(wrapped)


class TestMonoidLaws:
    """Property-based tests for monoid laws."""

    @pytest.mark.parametrize("a,b,c", [
        (1, 2, 3),
        (5, 5, 5),
        (0, 10, -5),
        (-100, 50, -25),
    ])
    def test_add_monoid_laws(self, a, b, c):
        """Test addition monoid satisfies all laws."""
        m = AddMonoid()

        # Associativity
        left = m.combine(m.combine(a, b), c)
        right = m.combine(a, m.combine(b, c))
        assert left == right

        # Identity
        assert m.combine(m.zero, a) == a
        assert m.combine(a, m.zero) == a

    @pytest.mark.parametrize("a,b,c", [
        (2, 3, 4),
        (5, 5, 5),
        (1, 10, 2),
    ])
    def test_multiply_monoid_laws(self, a, b, c):
        """Test multiplication monoid satisfies all laws."""
        m = MultiplyMonoid()

        # Associativity
        left = m.combine(m.combine(a, b), c)
        right = m.combine(a, m.combine(b, c))
        assert left == right

        # Identity
        assert m.combine(m.zero, a) == a
        assert m.combine(a, m.zero) == a

    @pytest.mark.parametrize("value", [0, 1, 5, -10, 42, 100])
    def test_identity_law_comprehensive(self, value):
        """Test identity law holds for various values."""
        m = AddMonoid()
        assert m.combine(m.zero, value) == value
        assert m.combine(value, m.zero) == value

        m2 = MultiplyMonoid()
        assert m2.combine(m2.zero, value) == value
        assert m2.combine(value, m2.zero) == value
