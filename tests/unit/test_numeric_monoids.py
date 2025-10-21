"""
Tests for numeric monoids.
"""

import pytest
from algesnake.monoid.numeric import Add, Multiply, Max, Min, add, multiply, max_of, min_of


class TestAdd:
    """Tests for Add monoid."""
    
    def test_combine(self):
        assert Add(5).combine(Add(3)) == Add(8)
        assert Add(0).combine(Add(5)) == Add(5)
        assert Add(5).combine(Add(0)) == Add(5)
    
    def test_operator_overload(self):
        assert Add(5) + Add(3) == Add(8)
        assert Add(10) + Add(-5) == Add(5)
    
    def test_sum_builtin(self):
        values = [Add(1), Add(2), Add(3), Add(4), Add(5)]
        assert sum(values) == Add(15)
    
    def test_zero(self):
        assert Add.zero() == Add(0)
        assert Add(5) + Add.zero() == Add(5)
        assert Add.zero() + Add(5) == Add(5)
    
    def test_is_zero(self):
        assert Add(0).is_zero
        assert not Add(5).is_zero
    
    def test_associativity(self):
        a, b, c = Add(2), Add(3), Add(5)
        assert (a + b) + c == a + (b + c)
    
    def test_identity(self):
        a = Add(7)
        zero = Add.zero()
        assert a + zero == a
        assert zero + a == a
    
    def test_convenience_function(self):
        assert add(1, 2, 3, 4, 5) == Add(15)
        assert add() == Add.zero()
    
    def test_float_values(self):
        assert Add(5.5) + Add(3.2) == Add(8.7)
        assert sum([Add(1.1), Add(2.2), Add(3.3)]) == Add(6.6)


class TestMultiply:
    """Tests for Multiply monoid."""
    
    def test_combine(self):
        assert Multiply(5).combine(Multiply(3)) == Multiply(15)
        assert Multiply(1).combine(Multiply(5)) == Multiply(5)
        assert Multiply(5).combine(Multiply(1)) == Multiply(5)
    
    def test_operator_overload(self):
        assert Multiply(5) * Multiply(3) == Multiply(15)
        assert Multiply(10) * Multiply(2) == Multiply(20)
    
    def test_zero(self):
        assert Multiply.zero() == Multiply(1)
        assert Multiply(5) * Multiply.zero() == Multiply(5)
        assert Multiply.zero() * Multiply(5) == Multiply(5)
    
    def test_is_zero(self):
        assert Multiply(1).is_zero
        assert not Multiply(5).is_zero
    
    def test_associativity(self):
        a, b, c = Multiply(2), Multiply(3), Multiply(5)
        assert (a * b) * c == a * (b * c)
    
    def test_identity(self):
        a = Multiply(7)
        zero = Multiply.zero()
        assert a * zero == a
        assert zero * a == a
    
    def test_convenience_function(self):
        assert multiply(2, 3, 4) == Multiply(24)
        assert multiply() == Multiply.zero()
    
    def test_float_values(self):
        assert Multiply(2.5) * Multiply(4.0) == Multiply(10.0)


class TestMax:
    """Tests for Max monoid."""
    
    def test_combine(self):
        assert Max(5).combine(Max(3)) == Max(5)
        assert Max(3).combine(Max(5)) == Max(5)
        assert Max(-5).combine(Max(-3)) == Max(-3)
    
    def test_operator_overload(self):
        assert Max(5) + Max(3) == Max(5)
        assert Max(3) + Max(5) == Max(5)
    
    def test_sum_builtin(self):
        values = [Max(1), Max(5), Max(3), Max(2)]
        assert sum(values) == Max(5)
    
    def test_zero(self):
        assert Max.zero() == Max(float('-inf'))
        assert Max(5) + Max.zero() == Max(5)
        assert Max.zero() + Max(5) == Max(5)
    
    def test_is_zero(self):
        assert Max(float('-inf')).is_zero
        assert not Max(5).is_zero
        assert not Max(0).is_zero
    
    def test_associativity(self):
        a, b, c = Max(2), Max(3), Max(5)
        assert (a + b) + c == a + (b + c)
    
    def test_identity(self):
        a = Max(7)
        zero = Max.zero()
        assert a + zero == a
        assert zero + a == a
    
    def test_commutativity(self):
        a, b = Max(5), Max(3)
        assert a + b == b + a
    
    def test_convenience_function(self):
        assert max_of(1, 5, 3, 2, 4) == Max(5)
        assert max_of() == Max.zero()
    
    def test_negative_infinity(self):
        assert Max(float('-inf')) + Max(0) == Max(0)
        assert Max(float('-inf')) + Max(-100) == Max(-100)


class TestMin:
    """Tests for Min monoid."""
    
    def test_combine(self):
        assert Min(5).combine(Min(3)) == Min(3)
        assert Min(3).combine(Min(5)) == Min(3)
        assert Min(-5).combine(Min(-3)) == Min(-5)
    
    def test_operator_overload(self):
        assert Min(5) + Min(3) == Min(3)
        assert Min(3) + Min(5) == Min(3)
    
    def test_sum_builtin(self):
        values = [Min(5), Min(1), Min(3), Min(2)]
        assert sum(values) == Min(1)
    
    def test_zero(self):
        assert Min.zero() == Min(float('inf'))
        assert Min(5) + Min.zero() == Min(5)
        assert Min.zero() + Min(5) == Min(5)
    
    def test_is_zero(self):
        assert Min(float('inf')).is_zero
        assert not Min(5).is_zero
        assert not Min(0).is_zero
    
    def test_associativity(self):
        a, b, c = Min(2), Min(3), Min(5)
        assert (a + b) + c == a + (b + c)
    
    def test_identity(self):
        a = Min(7)
        zero = Min.zero()
        assert a + zero == a
        assert zero + a == a
    
    def test_commutativity(self):
        a, b = Min(5), Min(3)
        assert a + b == b + a
    
    def test_convenience_function(self):
        assert min_of(5, 1, 3, 2, 4) == Min(1)
        assert min_of() == Min.zero()
    
    def test_positive_infinity(self):
        assert Min(float('inf')) + Min(0) == Min(0)
        assert Min(float('inf')) + Min(100) == Min(100)


class TestMonoidLaws:
    """Comprehensive tests for monoid laws."""
    
    @pytest.mark.parametrize("monoid_class,values", [
        (Add, [5, 3, 7]),
        (Multiply, [2, 3, 5]),
        (Max, [1, 5, 3]),
        (Min, [5, 1, 3]),
    ])
    def test_associativity(self, monoid_class, values):
        """Test (a • b) • c = a • (b • c)"""
        a, b, c = [monoid_class(v) for v in values]
        if monoid_class in (Add, Max, Min):
            assert (a + b) + c == a + (b + c)
        else:  # Multiply
            assert (a * b) * c == a * (b * c)
    
    @pytest.mark.parametrize("monoid_class,value", [
        (Add, 7),
        (Multiply, 7),
        (Max, 7),
        (Min, 7),
    ])
    def test_identity(self, monoid_class, value):
        """Test zero • a = a • zero = a"""
        a = monoid_class(value)
        zero = monoid_class.zero()
        if monoid_class in (Add, Max, Min):
            assert zero + a == a
            assert a + zero == a
        else:  # Multiply
            assert zero * a == a
            assert a * zero == a


class TestEdgeCases:
    """Tests for edge cases and special values."""
    
    def test_empty_sum(self):
        assert sum([]) == 0  # Built-in behavior
    
    def test_single_value(self):
        assert sum([Add(5)]) == Add(5)
        assert sum([Max(5)]) == Max(5)
        assert sum([Min(5)]) == Min(5)
    
    def test_large_values(self):
        assert Add(10**100) + Add(10**100) == Add(2 * 10**100)
        assert Multiply(10**50) * Multiply(10**50) == Multiply(10**100)
    
    def test_zero_values(self):
        assert Add(0) + Add(0) == Add(0)
        assert Max(0) + Max(0) == Max(0)
        assert Min(0) + Min(0) == Min(0)
    
    def test_negative_values(self):
        assert Add(-5) + Add(-3) == Add(-8)
        assert Max(-5) + Max(-3) == Max(-3)
        assert Min(-5) + Min(-3) == Min(-5)
