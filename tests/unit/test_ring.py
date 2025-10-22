"""
Unit tests for Ring abstract base class.
"""

import pytest
from algesnake.abstract.ring import Ring, RingWrapper


class IntRing(Ring[int]):
    """Integer ring for testing."""

    @property
    def zero(self) -> int:
        return 0

    @property
    def one(self) -> int:
        return 1

    def plus(self, a: int, b: int) -> int:
        return a + b

    def times(self, a: int, b: int) -> int:
        return a * b

    def negate(self, a: int) -> int:
        return -a


class FloatRing(Ring[float]):
    """Float ring for testing."""

    @property
    def zero(self) -> float:
        return 0.0

    @property
    def one(self) -> float:
        return 1.0

    def plus(self, a: float, b: float) -> float:
        return a + b

    def times(self, a: float, b: float) -> float:
        return a * b

    def negate(self, a: float) -> float:
        return -a


class TestRing:
    """Test Ring abstract base class."""

    def test_ring_zero(self):
        """Test ring has correct zero element."""
        r = IntRing()
        assert r.zero == 0

    def test_ring_one(self):
        """Test ring has correct one element."""
        r = IntRing()
        assert r.one == 1

    def test_plus(self):
        """Test addition operation."""
        r = IntRing()
        assert r.plus(5, 3) == 8
        assert r.plus(-5, 3) == -2
        assert r.plus(0, 5) == 5
        assert r.plus(5, 0) == 5

    def test_times(self):
        """Test multiplication operation."""
        r = IntRing()
        assert r.times(5, 3) == 15
        assert r.times(-5, 3) == -15
        assert r.times(0, 5) == 0
        assert r.times(5, 0) == 0
        assert r.times(1, 5) == 5
        assert r.times(5, 1) == 5

    def test_negate(self):
        """Test negation operation."""
        r = IntRing()
        assert r.negate(5) == -5
        assert r.negate(-5) == 5
        assert r.negate(0) == 0

    def test_minus(self):
        """Test subtraction operation."""
        r = IntRing()
        assert r.minus(5, 3) == 2
        assert r.minus(3, 5) == -2
        assert r.minus(10, 10) == 0
        assert r.minus(0, 5) == -5

    def test_minus_is_plus_with_negate(self):
        """Test that minus is implemented as plus(a, negate(b))."""
        r = IntRing()
        a, b = 10, 3
        assert r.minus(a, b) == r.plus(a, r.negate(b))

    def test_verify_left_distributivity(self):
        """Test left distributivity: a × (b + c) = (a × b) + (a × c)."""
        r = IntRing()
        assert r.verify_left_distributivity(2, 3, 4) is True
        assert r.verify_left_distributivity(5, 5, 5) is True
        assert r.verify_left_distributivity(0, 10, 5) is True
        assert r.verify_left_distributivity(-2, 3, 4) is True

    def test_verify_right_distributivity(self):
        """Test right distributivity: (a + b) × c = (a × c) + (b × c)."""
        r = IntRing()
        assert r.verify_right_distributivity(2, 3, 4) is True
        assert r.verify_right_distributivity(5, 5, 5) is True
        assert r.verify_right_distributivity(0, 10, 5) is True
        assert r.verify_right_distributivity(-2, 3, 4) is True

    def test_verify_distributivity(self):
        """Test both left and right distributivity."""
        r = IntRing()
        assert r.verify_distributivity(2, 3, 4) is True
        assert r.verify_distributivity(5, 5, 5) is True
        assert r.verify_distributivity(-2, 3, 4) is True

    def test_verify_distributivity_float(self):
        """Test distributivity with floating point."""
        r = FloatRing()
        assert r.verify_distributivity(2.5, 3.5, 4.5) is True
        assert r.verify_distributivity(-2.1, 3.2, 4.3) is True

    def test_verify_additive_commutativity(self):
        """Test that addition is commutative."""
        r = IntRing()
        assert r.verify_additive_commutativity(2, 3) is True
        assert r.verify_additive_commutativity(5, 5) is True
        assert r.verify_additive_commutativity(-5, 10) is True

    def test_verify_ring_laws(self):
        """Test full ring law verification."""
        r = IntRing()
        assert r.verify_ring_laws(1, 2, 3) is True
        assert r.verify_ring_laws(0, 0, 0) is True
        assert r.verify_ring_laws(-5, 10, 3) is True
        assert r.verify_ring_laws(100, -50, 25) is True

    def test_verify_ring_laws_float(self):
        """Test ring laws with floating point."""
        r = FloatRing()
        assert r.verify_ring_laws(1.5, 2.5, 3.5) is True
        assert r.verify_ring_laws(-5.1, 10.2, 3.3) is True

    def test_ring_wrapper(self):
        """Test RingWrapper creates ring from functions."""
        int_ring = RingWrapper(
            plus_fn=lambda a, b: a + b,
            times_fn=lambda a, b: a * b,
            zero_value=0,
            one_value=1,
            negate_fn=lambda a: -a
        )
        assert int_ring.zero == 0
        assert int_ring.one == 1
        assert int_ring.plus(5, 3) == 8
        assert int_ring.times(5, 3) == 15
        assert int_ring.negate(5) == -5
        assert int_ring.minus(10, 3) == 7

    def test_ring_wrapper_verify_laws(self):
        """Test RingWrapper satisfies ring laws."""
        int_ring = RingWrapper(
            plus_fn=lambda a, b: a + b,
            times_fn=lambda a, b: a * b,
            zero_value=0,
            one_value=1,
            negate_fn=lambda a: -a
        )
        assert int_ring.verify_ring_laws(1, 2, 3) is True
        assert int_ring.verify_distributivity(2, 3, 4) is True

    def test_repr(self):
        """Test string representation."""
        r = IntRing()
        assert repr(r) == "IntRing()"

        wrapped = RingWrapper(
            plus_fn=lambda a, b: a + b,
            times_fn=lambda a, b: a * b,
            zero_value=0,
            one_value=1,
            negate_fn=lambda a: -a
        )
        assert "RingWrapper" in repr(wrapped)
        assert "zero=0" in repr(wrapped)
        assert "one=1" in repr(wrapped)


class TestRingLaws:
    """Property-based tests for ring laws."""

    @pytest.mark.parametrize("a,b,c", [
        (1, 2, 3),
        (2, 3, 4),
        (5, 5, 5),
        (0, 10, -5),
        (-100, 50, -25),
        (7, -3, 11),
    ])
    def test_distributivity_comprehensive(self, a, b, c):
        """Test distributivity law comprehensively."""
        r = IntRing()

        # Left distributivity: a × (b + c) = (a × b) + (a × c)
        left_lhs = r.times(a, r.plus(b, c))
        left_rhs = r.plus(r.times(a, b), r.times(a, c))
        assert left_lhs == left_rhs

        # Right distributivity: (a + b) × c = (a × c) + (b × c)
        right_lhs = r.times(r.plus(a, b), c)
        right_rhs = r.plus(r.times(a, c), r.times(b, c))
        assert right_lhs == right_rhs

    @pytest.mark.parametrize("a,b", [
        (5, 3),
        (10, 7),
        (0, 5),
        (-5, 3),
        (100, -50),
    ])
    def test_additive_commutativity(self, a, b):
        """Test addition is commutative."""
        r = IntRing()
        assert r.plus(a, b) == r.plus(b, a)

    @pytest.mark.parametrize("a,b,c", [
        (2, 3, 4),
        (5, 5, 5),
        (10, 1, 2),
    ])
    def test_additive_associativity(self, a, b, c):
        """Test addition is associative."""
        r = IntRing()
        left = r.plus(r.plus(a, b), c)
        right = r.plus(a, r.plus(b, c))
        assert left == right

    @pytest.mark.parametrize("a,b,c", [
        (2, 3, 4),
        (5, 5, 5),
        (10, 1, 2),
    ])
    def test_multiplicative_associativity(self, a, b, c):
        """Test multiplication is associative."""
        r = IntRing()
        left = r.times(r.times(a, b), c)
        right = r.times(a, r.times(b, c))
        assert left == right

    @pytest.mark.parametrize("value", [0, 1, 5, -10, 42, 100])
    def test_additive_identity(self, value):
        """Test zero is additive identity."""
        r = IntRing()
        assert r.plus(r.zero, value) == value
        assert r.plus(value, r.zero) == value

    @pytest.mark.parametrize("value", [0, 1, 5, -10, 42, 100])
    def test_multiplicative_identity(self, value):
        """Test one is multiplicative identity."""
        r = IntRing()
        assert r.times(r.one, value) == value
        assert r.times(value, r.one) == value

    @pytest.mark.parametrize("value", [1, 5, -10, 42, 100])
    def test_zero_annihilator(self, value):
        """Test zero annihilates in multiplication."""
        r = IntRing()
        assert r.times(r.zero, value) == r.zero
        assert r.times(value, r.zero) == r.zero

    @pytest.mark.parametrize("value", [0, 1, 5, -10, 42, 100, -100])
    def test_additive_inverse(self, value):
        """Test negation is additive inverse."""
        r = IntRing()

        # a + (-a) = 0
        assert r.plus(value, r.negate(value)) == r.zero

        # (-a) + a = 0
        assert r.plus(r.negate(value), value) == r.zero

        # -(-a) = a
        assert r.negate(r.negate(value)) == value

    @pytest.mark.parametrize("a,b", [
        (5, 3),
        (10, 7),
        (0, 5),
        (-5, 3),
    ])
    def test_subtraction_property(self, a, b):
        """Test subtraction property."""
        r = IntRing()

        # a - b = a + (-b)
        assert r.minus(a, b) == r.plus(a, r.negate(b))

        # (a - b) + b = a
        assert r.plus(r.minus(a, b), b) == a

    def test_float_ring_with_epsilon(self):
        """Test float ring handles epsilon correctly."""
        r = FloatRing()

        # Test with values that might have floating point errors
        a = 0.1 + 0.2
        b = 0.3
        c = 0.5

        # Should still verify correctly with epsilon
        assert r.verify_ring_laws(a, b, c, epsilon=1e-10) is True
        assert r.verify_distributivity(a, b, c, epsilon=1e-10) is True

    def test_complex_ring_expression(self):
        """Test complex ring expressions."""
        r = IntRing()

        # (a + b) × (c + d) = ac + ad + bc + bd
        a, b, c, d = 2, 3, 5, 7

        left = r.times(r.plus(a, b), r.plus(c, d))
        right = r.plus(
            r.plus(r.times(a, c), r.times(a, d)),
            r.plus(r.times(b, c), r.times(b, d))
        )
        assert left == right
