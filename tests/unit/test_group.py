"""
Unit tests for Group abstract base class.
"""

import pytest
from algesnake.abstract.group import Group, GroupWrapper


class IntAddGroup(Group[int]):
    """Integer addition group for testing."""

    @property
    def zero(self) -> int:
        return 0

    def combine(self, a: int, b: int) -> int:
        return a + b

    def inverse(self, a: int) -> int:
        return -a


class FloatAddGroup(Group[float]):
    """Float addition group for testing."""

    @property
    def zero(self) -> float:
        return 0.0

    def combine(self, a: float, b: float) -> float:
        return a + b

    def inverse(self, a: float) -> float:
        return -a


class TestGroup:
    """Test Group abstract base class."""

    def test_group_zero(self):
        """Test group has correct zero element."""
        g = IntAddGroup()
        assert g.zero == 0

    def test_group_combine(self):
        """Test group combines correctly."""
        g = IntAddGroup()
        assert g.combine(5, 3) == 8
        assert g.combine(-5, 3) == -2
        assert g.combine(0, 5) == 5

    def test_inverse(self):
        """Test inverse operation."""
        g = IntAddGroup()
        assert g.inverse(5) == -5
        assert g.inverse(-5) == 5
        assert g.inverse(0) == 0

    def test_subtract(self):
        """Test subtract operation."""
        g = IntAddGroup()
        assert g.subtract(5, 3) == 2
        assert g.subtract(3, 5) == -2
        assert g.subtract(10, 10) == 0
        assert g.subtract(0, 5) == -5

    def test_subtract_is_combine_with_inverse(self):
        """Test that subtract is implemented as combine(a, inverse(b))."""
        g = IntAddGroup()
        a, b = 10, 3
        assert g.subtract(a, b) == g.combine(a, g.inverse(b))

    def test_verify_left_inverse(self):
        """Test left inverse verification."""
        g = IntAddGroup()
        assert g.verify_left_inverse(5) is True
        assert g.verify_left_inverse(-5) is True
        assert g.verify_left_inverse(0) is True
        assert g.verify_left_inverse(100) is True

    def test_verify_right_inverse(self):
        """Test right inverse verification."""
        g = IntAddGroup()
        assert g.verify_right_inverse(5) is True
        assert g.verify_right_inverse(-5) is True
        assert g.verify_right_inverse(0) is True
        assert g.verify_right_inverse(100) is True

    def test_verify_inverse(self):
        """Test both left and right inverse verification."""
        g = IntAddGroup()
        assert g.verify_inverse(5) is True
        assert g.verify_inverse(-5) is True
        assert g.verify_inverse(0) is True
        assert g.verify_inverse(42) is True

    def test_verify_inverse_float(self):
        """Test inverse verification with floats."""
        g = FloatAddGroup()
        assert g.verify_inverse(5.5) is True
        assert g.verify_inverse(-3.14) is True
        assert g.verify_inverse(0.0) is True

    def test_verify_group_laws(self):
        """Test full group law verification."""
        g = IntAddGroup()
        assert g.verify_group_laws(1, 2, 3) is True
        assert g.verify_group_laws(0, 0, 0) is True
        assert g.verify_group_laws(-5, 10, 3) is True
        assert g.verify_group_laws(100, -50, 25) is True

    def test_verify_group_laws_float(self):
        """Test group laws with floating point."""
        g = FloatAddGroup()
        assert g.verify_group_laws(1.5, 2.5, 3.5) is True
        assert g.verify_group_laws(-5.1, 10.2, 3.3) is True

    def test_group_wrapper(self):
        """Test GroupWrapper creates group from functions."""
        int_add = GroupWrapper(
            combine_fn=lambda a, b: a + b,
            zero_value=0,
            inverse_fn=lambda a: -a
        )
        assert int_add.zero == 0
        assert int_add.combine(5, 3) == 8
        assert int_add.inverse(5) == -5
        assert int_add.subtract(10, 3) == 7

    def test_group_wrapper_verify_laws(self):
        """Test GroupWrapper satisfies group laws."""
        int_add = GroupWrapper(
            combine_fn=lambda a, b: a + b,
            zero_value=0,
            inverse_fn=lambda a: -a
        )
        assert int_add.verify_group_laws(1, 2, 3) is True
        assert int_add.verify_inverse(5) is True

    def test_group_inherits_monoid_methods(self):
        """Test that Group inherits methods from Monoid."""
        g = IntAddGroup()

        # Test combine_all (from Monoid)
        assert g.combine_all([]) == 0
        assert g.combine_all([1, 2, 3, 4]) == 10

        # Test combine_option (from Monoid)
        assert g.combine_option(5, 3) == 8
        assert g.combine_option(None, 3) == 3
        assert g.combine_option(5, None) == 5
        assert g.combine_option(None, None) == 0

    def test_group_inherits_semigroup_methods(self):
        """Test that Group inherits methods from Semigroup."""
        g = IntAddGroup()

        # Test verify_associativity (from Semigroup)
        assert g.verify_associativity(1, 2, 3) is True
        assert g.verify_associativity(-5, 10, 3) is True

    def test_repr(self):
        """Test string representation."""
        g = IntAddGroup()
        assert repr(g) == "IntAddGroup()"

        wrapped = GroupWrapper(
            combine_fn=lambda a, b: a + b,
            zero_value=0,
            inverse_fn=lambda a: -a
        )
        assert "GroupWrapper" in repr(wrapped)


class TestGroupLaws:
    """Property-based tests for group laws."""

    @pytest.mark.parametrize("a,b,c", [
        (1, 2, 3),
        (5, 5, 5),
        (0, 10, -5),
        (-100, 50, -25),
        (42, -42, 0),
    ])
    def test_int_add_group_laws(self, a, b, c):
        """Test integer addition group satisfies all laws."""
        g = IntAddGroup()

        # Associativity (inherited from Monoid/Semigroup)
        left = g.combine(g.combine(a, b), c)
        right = g.combine(a, g.combine(b, c))
        assert left == right

        # Identity (inherited from Monoid)
        assert g.combine(g.zero, a) == a
        assert g.combine(a, g.zero) == a

        # Inverse (Group-specific)
        assert g.combine(a, g.inverse(a)) == g.zero
        assert g.combine(g.inverse(a), a) == g.zero

    @pytest.mark.parametrize("value", [0, 1, 5, -10, 42, 100, -100])
    def test_inverse_property(self, value):
        """Test inverse property holds for various values."""
        g = IntAddGroup()

        # a + (-a) = 0
        assert g.combine(value, g.inverse(value)) == g.zero

        # (-a) + a = 0
        assert g.combine(g.inverse(value), value) == g.zero

        # -(-a) = a
        assert g.inverse(g.inverse(value)) == value

    @pytest.mark.parametrize("a,b", [
        (5, 3),
        (10, 7),
        (0, 5),
        (-5, 3),
        (100, -50),
    ])
    def test_subtraction_property(self, a, b):
        """Test subtraction property."""
        g = IntAddGroup()

        # a - b = a + (-b)
        assert g.subtract(a, b) == g.combine(a, g.inverse(b))

        # (a - b) + b = a
        assert g.combine(g.subtract(a, b), b) == a

    @pytest.mark.parametrize("a,b,c", [
        (10, 3, 2),
        (5, 5, 5),
        (100, 25, 50),
    ])
    def test_subtraction_not_associative(self, a, b, c):
        """Test that subtraction is NOT associative (sanity check)."""
        g = IntAddGroup()

        # (a - b) - c != a - (b - c) in general
        left = g.subtract(g.subtract(a, b), c)
        right = g.subtract(a, g.subtract(b, c))

        # They're equal when a=b=c, but not in general
        if not (a == b == c):
            # For most cases they differ
            # e.g., (10 - 3) - 2 = 5, but 10 - (3 - 2) = 9
            assert left != right or (a == b == c)

    def test_float_group_with_epsilon(self):
        """Test float group handles epsilon correctly."""
        g = FloatAddGroup()

        # Test with values that might have floating point errors
        a = 0.1 + 0.2  # Famous floating point issue
        b = 0.3

        # Should still verify correctly with epsilon
        assert g.verify_inverse(a, epsilon=1e-10) is True
        assert g.verify_group_laws(a, b, 0.5, epsilon=1e-10) is True
