"""
Unit tests for Semiring abstract base class.
"""

import pytest
from algesnake.abstract.semiring import Semiring, SemiringWrapper


class NaturalSemiring(Semiring[int]):
    """Natural number semiring for testing (non-negative integers)."""

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


class BooleanSemiring(Semiring[bool]):
    """Boolean semiring for testing (OR, AND)."""

    @property
    def zero(self) -> bool:
        return False

    @property
    def one(self) -> bool:
        return True

    def plus(self, a: bool, b: bool) -> bool:
        """OR operation."""
        return a or b

    def times(self, a: bool, b: bool) -> bool:
        """AND operation."""
        return a and b


class TropicalSemiring(Semiring[float]):
    """Tropical semiring (min, +) for testing."""

    @property
    def zero(self) -> float:
        """Infinity is the additive identity."""
        return float('inf')

    @property
    def one(self) -> float:
        """0 is the multiplicative identity."""
        return 0.0

    def plus(self, a: float, b: float) -> float:
        """Min operation."""
        return min(a, b)

    def times(self, a: float, b: float) -> float:
        """Addition."""
        return a + b


class TestSemiring:
    """Test Semiring abstract base class."""

    def test_semiring_zero(self):
        """Test semiring has correct zero element."""
        sr = NaturalSemiring()
        assert sr.zero == 0

    def test_semiring_one(self):
        """Test semiring has correct one element."""
        sr = NaturalSemiring()
        assert sr.one == 1

    def test_plus(self):
        """Test addition operation."""
        sr = NaturalSemiring()
        assert sr.plus(5, 3) == 8
        assert sr.plus(0, 5) == 5
        assert sr.plus(5, 0) == 5

    def test_times(self):
        """Test multiplication operation."""
        sr = NaturalSemiring()
        assert sr.times(5, 3) == 15
        assert sr.times(0, 5) == 0
        assert sr.times(5, 0) == 0
        assert sr.times(1, 5) == 5
        assert sr.times(5, 1) == 5

    def test_verify_left_distributivity(self):
        """Test left distributivity: a × (b + c) = (a × b) + (a × c)."""
        sr = NaturalSemiring()
        assert sr.verify_left_distributivity(2, 3, 4) is True
        assert sr.verify_left_distributivity(5, 5, 5) is True
        assert sr.verify_left_distributivity(0, 10, 5) is True

    def test_verify_right_distributivity(self):
        """Test right distributivity: (a + b) × c = (a × c) + (b × c)."""
        sr = NaturalSemiring()
        assert sr.verify_right_distributivity(2, 3, 4) is True
        assert sr.verify_right_distributivity(5, 5, 5) is True
        assert sr.verify_right_distributivity(0, 10, 5) is True

    def test_verify_distributivity(self):
        """Test both left and right distributivity."""
        sr = NaturalSemiring()
        assert sr.verify_distributivity(2, 3, 4) is True
        assert sr.verify_distributivity(5, 5, 5) is True

    def test_verify_additive_commutativity(self):
        """Test that addition is commutative."""
        sr = NaturalSemiring()
        assert sr.verify_additive_commutativity(2, 3) is True
        assert sr.verify_additive_commutativity(5, 5) is True
        assert sr.verify_additive_commutativity(10, 7) is True

    def test_verify_zero_annihilator(self):
        """Test zero annihilates in multiplication."""
        sr = NaturalSemiring()
        assert sr.verify_zero_annihilator(5) is True
        assert sr.verify_zero_annihilator(0) is True
        assert sr.verify_zero_annihilator(100) is True

    def test_verify_semiring_laws(self):
        """Test full semiring law verification."""
        sr = NaturalSemiring()
        assert sr.verify_semiring_laws(1, 2, 3) is True
        assert sr.verify_semiring_laws(0, 0, 0) is True
        assert sr.verify_semiring_laws(5, 10, 3) is True
        assert sr.verify_semiring_laws(100, 50, 25) is True

    def test_boolean_semiring(self):
        """Test boolean semiring (OR, AND)."""
        sr = BooleanSemiring()

        # Test OR (plus)
        assert sr.plus(True, False) is True
        assert sr.plus(False, True) is True
        assert sr.plus(False, False) is False
        assert sr.plus(True, True) is True

        # Test AND (times)
        assert sr.times(True, False) is False
        assert sr.times(False, True) is False
        assert sr.times(False, False) is False
        assert sr.times(True, True) is True

        # Test identities
        assert sr.zero is False
        assert sr.one is True

    def test_boolean_semiring_laws(self):
        """Test boolean semiring satisfies laws."""
        sr = BooleanSemiring()

        # Verify all combinations
        for a in [True, False]:
            for b in [True, False]:
                for c in [True, False]:
                    assert sr.verify_semiring_laws(a, b, c) is True

    def test_tropical_semiring(self):
        """Test tropical semiring (min, +)."""
        sr = TropicalSemiring()

        # Test min (plus)
        assert sr.plus(5.0, 3.0) == 3.0
        assert sr.plus(3.0, 5.0) == 3.0
        assert sr.plus(float('inf'), 5.0) == 5.0

        # Test addition (times)
        assert sr.times(5.0, 3.0) == 8.0
        assert sr.times(0.0, 5.0) == 5.0
        assert sr.times(5.0, 0.0) == 5.0

        # Test identities
        assert sr.zero == float('inf')
        assert sr.one == 0.0

    def test_tropical_semiring_laws(self):
        """Test tropical semiring satisfies laws."""
        sr = TropicalSemiring()
        # Test distributivity only (zero annihilator doesn't work well with float('inf'))
        assert sr.verify_distributivity(2.0, 3.0, 5.0) is True
        assert sr.verify_distributivity(0.0, 1.0, 2.0) is True
        assert sr.verify_additive_commutativity(2.0, 3.0) is True

    def test_semiring_wrapper(self):
        """Test SemiringWrapper creates semiring from functions."""
        nat_sr = SemiringWrapper(
            plus_fn=lambda a, b: a + b,
            times_fn=lambda a, b: a * b,
            zero_value=0,
            one_value=1
        )
        assert nat_sr.zero == 0
        assert nat_sr.one == 1
        assert nat_sr.plus(5, 3) == 8
        assert nat_sr.times(5, 3) == 15

    def test_semiring_wrapper_verify_laws(self):
        """Test SemiringWrapper satisfies semiring laws."""
        nat_sr = SemiringWrapper(
            plus_fn=lambda a, b: a + b,
            times_fn=lambda a, b: a * b,
            zero_value=0,
            one_value=1
        )
        assert nat_sr.verify_semiring_laws(1, 2, 3) is True
        assert nat_sr.verify_distributivity(2, 3, 4) is True
        assert nat_sr.verify_zero_annihilator(5) is True

    def test_repr(self):
        """Test string representation."""
        sr = NaturalSemiring()
        assert repr(sr) == "NaturalSemiring()"

        wrapped = SemiringWrapper(
            plus_fn=lambda a, b: a + b,
            times_fn=lambda a, b: a * b,
            zero_value=0,
            one_value=1
        )
        assert "SemiringWrapper" in repr(wrapped)
        assert "zero=0" in repr(wrapped)
        assert "one=1" in repr(wrapped)


class TestSemiringLaws:
    """Property-based tests for semiring laws."""

    @pytest.mark.parametrize("a,b,c", [
        (1, 2, 3),
        (2, 3, 4),
        (5, 5, 5),
        (0, 10, 5),
        (7, 3, 11),
    ])
    def test_distributivity_comprehensive(self, a, b, c):
        """Test distributivity law comprehensively."""
        sr = NaturalSemiring()

        # Left distributivity: a × (b + c) = (a × b) + (a × c)
        left_lhs = sr.times(a, sr.plus(b, c))
        left_rhs = sr.plus(sr.times(a, b), sr.times(a, c))
        assert left_lhs == left_rhs

        # Right distributivity: (a + b) × c = (a × c) + (b × c)
        right_lhs = sr.times(sr.plus(a, b), c)
        right_rhs = sr.plus(sr.times(a, c), sr.times(b, c))
        assert right_lhs == right_rhs

    @pytest.mark.parametrize("a,b", [
        (5, 3),
        (10, 7),
        (0, 5),
        (100, 50),
    ])
    def test_additive_commutativity(self, a, b):
        """Test addition is commutative."""
        sr = NaturalSemiring()
        assert sr.plus(a, b) == sr.plus(b, a)

    @pytest.mark.parametrize("a,b,c", [
        (2, 3, 4),
        (5, 5, 5),
        (10, 1, 2),
    ])
    def test_additive_associativity(self, a, b, c):
        """Test addition is associative."""
        sr = NaturalSemiring()
        left = sr.plus(sr.plus(a, b), c)
        right = sr.plus(a, sr.plus(b, c))
        assert left == right

    @pytest.mark.parametrize("a,b,c", [
        (2, 3, 4),
        (5, 5, 5),
        (10, 1, 2),
    ])
    def test_multiplicative_associativity(self, a, b, c):
        """Test multiplication is associative."""
        sr = NaturalSemiring()
        left = sr.times(sr.times(a, b), c)
        right = sr.times(a, sr.times(b, c))
        assert left == right

    @pytest.mark.parametrize("value", [0, 1, 5, 10, 42, 100])
    def test_additive_identity(self, value):
        """Test zero is additive identity."""
        sr = NaturalSemiring()
        assert sr.plus(sr.zero, value) == value
        assert sr.plus(value, sr.zero) == value

    @pytest.mark.parametrize("value", [0, 1, 5, 10, 42, 100])
    def test_multiplicative_identity(self, value):
        """Test one is multiplicative identity."""
        sr = NaturalSemiring()
        assert sr.times(sr.one, value) == value
        assert sr.times(value, sr.one) == value

    @pytest.mark.parametrize("value", [1, 5, 10, 42, 100])
    def test_zero_annihilator(self, value):
        """Test zero annihilates in multiplication."""
        sr = NaturalSemiring()
        assert sr.times(sr.zero, value) == sr.zero
        assert sr.times(value, sr.zero) == sr.zero

    def test_complex_semiring_expression(self):
        """Test complex semiring expressions."""
        sr = NaturalSemiring()

        # (a + b) × (c + d) = ac + ad + bc + bd
        a, b, c, d = 2, 3, 5, 7

        left = sr.times(sr.plus(a, b), sr.plus(c, d))
        right = sr.plus(
            sr.plus(sr.times(a, c), sr.times(a, d)),
            sr.plus(sr.times(b, c), sr.times(b, d))
        )
        assert left == right

    @pytest.mark.parametrize("a,b", [
        (True, True),
        (True, False),
        (False, True),
        (False, False),
    ])
    def test_boolean_distributivity(self, a, b):
        """Test boolean semiring distributivity."""
        sr = BooleanSemiring()

        # AND distributes over OR
        # a ∧ (b ∨ c) = (a ∧ b) ∨ (a ∧ c)
        for c in [True, False]:
            assert sr.verify_distributivity(a, b, c) is True

    def test_tropical_distributivity(self):
        """Test tropical semiring distributivity."""
        sr = TropicalSemiring()

        # (min, +) semiring: + distributes over min
        # a + min(b, c) = min(a + b, a + c)
        assert sr.verify_distributivity(2.0, 3.0, 5.0) is True
        assert sr.verify_distributivity(1.0, 4.0, 2.0) is True

    def test_semiring_no_additive_inverse(self):
        """Test that semirings don't require additive inverses (unlike rings)."""
        sr = NaturalSemiring()

        # Natural numbers have no additive inverse for positive numbers
        # This is expected behavior - semirings don't have subtraction
        # This test just documents that difference from rings
        value = 5

        # We can add
        assert sr.plus(value, 3) == 8

        # But there's no built-in subtraction (unlike Ring)
        assert not hasattr(sr, 'minus')
        assert not hasattr(sr, 'negate')
