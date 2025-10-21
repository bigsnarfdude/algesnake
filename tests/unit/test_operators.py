"""
Unit tests for operator overloading system.
"""

import pytest
from algesnake.operators import (
    provides_monoid,
    provides_group,
    provides_ring,
    provides_semiring,
    MonoidMixin,
    GroupMixin,
    RingMixin,
    SemiringMixin,
)


@provides_monoid
class Max:
    """Max monoid using decorator."""

    def __init__(self, value):
        self.value = value

    def combine(self, other):
        return Max(max(self.value, other.value))

    @property
    def zero(self):
        return Max(float('-inf'))

    def __eq__(self, other):
        return self.value == other.value

    def __repr__(self):
        return f"Max({self.value})"


@provides_group
class IntAdd:
    """Integer addition group using decorator."""

    def __init__(self, value):
        self.value = value

    def combine(self, other):
        return IntAdd(self.value + other.value)

    @property
    def zero(self):
        return IntAdd(0)

    def inverse(self):
        return IntAdd(-self.value)

    def __eq__(self, other):
        return self.value == other.value

    def __repr__(self):
        return f"IntAdd({self.value})"


@provides_ring
class IntRing:
    """Integer ring using decorator."""

    def __init__(self, value):
        self.value = value

    def plus(self, other):
        return IntRing(self.value + other.value)

    def times(self, other):
        return IntRing(self.value * other.value)

    @property
    def zero(self):
        return IntRing(0)

    @property
    def one(self):
        return IntRing(1)

    def negate(self):
        return IntRing(-self.value)

    def __eq__(self, other):
        return self.value == other.value

    def __repr__(self):
        return f"IntRing({self.value})"


@provides_semiring
class NatSemiring:
    """Natural number semiring using decorator."""

    def __init__(self, value):
        self.value = value

    def plus(self, other):
        return NatSemiring(self.value + other.value)

    def times(self, other):
        return NatSemiring(self.value * other.value)

    @property
    def zero(self):
        return NatSemiring(0)

    @property
    def one(self):
        return NatSemiring(1)

    def __eq__(self, other):
        return self.value == other.value

    def __repr__(self):
        return f"NatSemiring({self.value})"


class TestMonoidDecorator:
    """Test @provides_monoid decorator."""

    def test_add_operator(self):
        """Test + operator calls combine."""
        result = Max(5) + Max(3)
        assert result == Max(5)

        result = Max(2) + Max(7)
        assert result == Max(7)

    def test_add_chain(self):
        """Test chaining + operations."""
        result = Max(5) + Max(3) + Max(1)
        assert result == Max(5)

        result = Max(1) + Max(5) + Max(3) + Max(2)
        assert result == Max(5)

    def test_sum_builtin(self):
        """Test sum() builtin works with monoid."""
        values = [Max(1), Max(5), Max(3), Max(2)]
        result = sum(values)
        assert result == Max(5)

    def test_sum_empty(self):
        """Test sum() with empty list uses __radd__."""
        result = sum([])
        assert result == 0

    def test_radd_with_zero(self):
        """Test __radd__ with 0 (for sum() support)."""
        result = 0 + Max(5)
        assert result == Max(5)


class TestGroupDecorator:
    """Test @provides_group decorator."""

    def test_add_operator(self):
        """Test + operator calls combine."""
        result = IntAdd(5) + IntAdd(3)
        assert result == IntAdd(8)

    def test_subtract_operator(self):
        """Test - operator calls subtract."""
        result = IntAdd(5) - IntAdd(3)
        assert result == IntAdd(2)

    def test_negate_operator(self):
        """Test unary - operator calls inverse."""
        result = -IntAdd(5)
        assert result == IntAdd(-5)

    def test_chained_operations(self):
        """Test chaining operations."""
        result = IntAdd(10) + IntAdd(5) - IntAdd(3)
        assert result == IntAdd(12)

    def test_double_negation(self):
        """Test double negation."""
        result = -(-IntAdd(5))
        assert result == IntAdd(5)


class TestRingDecorator:
    """Test @provides_ring decorator."""

    def test_add_operator(self):
        """Test + operator calls plus."""
        result = IntRing(5) + IntRing(3)
        assert result == IntRing(8)

    def test_multiply_operator(self):
        """Test * operator calls times."""
        result = IntRing(5) * IntRing(3)
        assert result == IntRing(15)

    def test_subtract_operator(self):
        """Test - operator works."""
        result = IntRing(5) - IntRing(3)
        assert result == IntRing(2)

    def test_negate_operator(self):
        """Test unary - operator calls negate."""
        result = -IntRing(5)
        assert result == IntRing(-5)

    def test_distributivity(self):
        """Test that operations follow ring distributivity."""
        a = IntRing(2)
        b = IntRing(3)
        c = IntRing(4)

        # a * (b + c) = (a * b) + (a * c)
        left = a * (b + c)
        right = (a * b) + (a * c)
        assert left == right

    def test_complex_expression(self):
        """Test complex ring expression."""
        result = (IntRing(2) + IntRing(3)) * IntRing(4)
        assert result == IntRing(20)

        result2 = IntRing(2) * IntRing(3) + IntRing(4) * IntRing(5)
        assert result2 == IntRing(26)


class TestSemiringDecorator:
    """Test @provides_semiring decorator."""

    def test_add_operator(self):
        """Test + operator calls plus."""
        result = NatSemiring(5) + NatSemiring(3)
        assert result == NatSemiring(8)

    def test_multiply_operator(self):
        """Test * operator calls times."""
        result = NatSemiring(5) * NatSemiring(3)
        assert result == NatSemiring(15)

    def test_distributivity(self):
        """Test that operations follow semiring distributivity."""
        a = NatSemiring(2)
        b = NatSemiring(3)
        c = NatSemiring(4)

        left = a * (b + c)
        right = (a * b) + (a * c)
        assert left == right

    def test_complex_expression(self):
        """Test complex semiring expression."""
        result = (NatSemiring(2) + NatSemiring(3)) * NatSemiring(4)
        assert result == NatSemiring(20)


class TestMonoidMixin:
    """Test MonoidMixin class."""

    def test_mixin_add_operator(self):
        """Test MonoidMixin provides + operator."""

        class TestMonoid(MonoidMixin):
            def __init__(self, value):
                self.value = value

            def combine(self, other):
                return TestMonoid(self.value + other.value)

            @property
            def zero(self):
                return TestMonoid(0)

        result = TestMonoid(5) + TestMonoid(3)
        assert result.value == 8

    def test_mixin_sum(self):
        """Test MonoidMixin works with sum()."""

        class TestMonoid(MonoidMixin):
            def __init__(self, value):
                self.value = value

            def combine(self, other):
                return TestMonoid(self.value + other.value)

            @property
            def zero(self):
                return TestMonoid(0)

        values = [TestMonoid(1), TestMonoid(2), TestMonoid(3)]
        result = sum(values)
        assert result.value == 6


class TestGroupMixin:
    """Test GroupMixin class."""

    def test_mixin_operators(self):
        """Test GroupMixin provides +, -, and unary - operators."""

        class TestGroup(GroupMixin):
            def __init__(self, value):
                self.value = value

            def combine(self, other):
                return TestGroup(self.value + other.value)

            @property
            def zero(self):
                return TestGroup(0)

            def inverse(self):
                return TestGroup(-self.value)

        a = TestGroup(5)
        b = TestGroup(3)

        assert (a + b).value == 8
        assert (a - b).value == 2
        assert (-a).value == -5


class TestRingMixin:
    """Test RingMixin class."""

    def test_mixin_operators(self):
        """Test RingMixin provides +, -, *, and unary - operators."""

        class TestRing(RingMixin):
            def __init__(self, value):
                self.value = value

            def plus(self, other):
                return TestRing(self.value + other.value)

            def times(self, other):
                return TestRing(self.value * other.value)

            @property
            def zero(self):
                return TestRing(0)

            @property
            def one(self):
                return TestRing(1)

            def negate(self):
                return TestRing(-self.value)

        a = TestRing(5)
        b = TestRing(3)

        assert (a + b).value == 8
        assert (a - b).value == 2
        assert (a * b).value == 15
        assert (-a).value == -5


class TestSemiringMixin:
    """Test SemiringMixin class."""

    def test_mixin_operators(self):
        """Test SemiringMixin provides + and * operators."""

        class TestSemiring(SemiringMixin):
            def __init__(self, value):
                self.value = value

            def plus(self, other):
                return TestSemiring(self.value + other.value)

            def times(self, other):
                return TestSemiring(self.value * other.value)

            @property
            def zero(self):
                return TestSemiring(0)

            @property
            def one(self):
                return TestSemiring(1)

        a = TestSemiring(5)
        b = TestSemiring(3)

        assert (a + b).value == 8
        assert (a * b).value == 15
