"""
Numeric monoids for common mathematical operations.

This module provides concrete implementations of monoids for numeric types,
including addition, multiplication, maximum, and minimum operations.
"""

from typing import TypeVar, Generic, Union
from abc import ABC

T = TypeVar('T', int, float)


class NumericMonoid(ABC, Generic[T]):
    """Base class for numeric monoids with operator overloading support."""
    
    def __init__(self, value: T):
        self.value = value
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value})"
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.value == other.value
    
    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.value))


class Add(NumericMonoid[T]):
    """
    Addition monoid for numeric types.
    
    Identity element: 0
    Operation: a + b
    
    Examples:
        >>> Add(5) + Add(3)
        Add(8)
        >>> sum([Add(1), Add(2), Add(3)])
        Add(6)
        >>> Add.zero()
        Add(0)
    """
    
    def combine(self, other: 'Add[T]') -> 'Add[T]':
        """Combine two Add monoids by adding their values."""
        return Add(self.value + other.value)
    
    def __add__(self, other: 'Add[T]') -> 'Add[T]':
        """Enable + operator for Add monoids."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support for sum() builtin."""
        if other == 0:
            return self
        return self.combine(other)
    
    @classmethod
    def zero(cls) -> 'Add[T]':
        """Return the identity element (0)."""
        return cls(0)
    
    @property
    def is_zero(self) -> bool:
        """Check if this is the identity element."""
        return self.value == 0


class Multiply(NumericMonoid[T]):
    """
    Multiplication monoid for numeric types.
    
    Identity element: 1
    Operation: a * b
    
    Examples:
        >>> Multiply(5) * Multiply(3)
        Multiply(15)
        >>> Multiply(2) * Multiply(3) * Multiply(4)
        Multiply(24)
        >>> Multiply.zero()
        Multiply(1)
    """
    
    def combine(self, other: 'Multiply[T]') -> 'Multiply[T]':
        """Combine two Multiply monoids by multiplying their values."""
        return Multiply(self.value * other.value)
    
    def __mul__(self, other: 'Multiply[T]') -> 'Multiply[T]':
        """Enable * operator for Multiply monoids."""
        return self.combine(other)
    
    @classmethod
    def zero(cls) -> 'Multiply[T]':
        """Return the identity element (1)."""
        return cls(1)
    
    @property
    def is_zero(self) -> bool:
        """Check if this is the identity element."""
        return self.value == 1


class Max(NumericMonoid[T]):
    """
    Maximum value monoid for numeric types.
    
    Identity element: -infinity
    Operation: max(a, b)
    
    Examples:
        >>> Max(5) + Max(3)
        Max(5)
        >>> sum([Max(1), Max(5), Max(3)])
        Max(5)
        >>> Max.zero()
        Max(-inf)
    """
    
    def combine(self, other: 'Max[T]') -> 'Max[T]':
        """Combine two Max monoids by taking the maximum value."""
        return Max(max(self.value, other.value))
    
    def __add__(self, other: 'Max[T]') -> 'Max[T]':
        """Enable + operator for Max monoids (representing combination)."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support for sum() builtin."""
        if other == 0:
            return self
        return self.combine(other)
    
    @classmethod
    def zero(cls) -> 'Max[T]':
        """Return the identity element (-infinity)."""
        return cls(float('-inf'))
    
    @property
    def is_zero(self) -> bool:
        """Check if this is the identity element."""
        return self.value == float('-inf')


class Min(NumericMonoid[T]):
    """
    Minimum value monoid for numeric types.
    
    Identity element: +infinity
    Operation: min(a, b)
    
    Examples:
        >>> Min(5) + Min(3)
        Min(3)
        >>> sum([Min(1), Min(5), Min(3)])
        Min(1)
        >>> Min.zero()
        Min(inf)
    """
    
    def combine(self, other: 'Min[T]') -> 'Min[T]':
        """Combine two Min monoids by taking the minimum value."""
        return Min(min(self.value, other.value))
    
    def __add__(self, other: 'Min[T]') -> 'Min[T]':
        """Enable + operator for Min monoids (representing combination)."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support for sum() builtin."""
        if other == 0:
            return self
        return self.combine(other)
    
    @classmethod
    def zero(cls) -> 'Min[T]':
        """Return the identity element (+infinity)."""
        return cls(float('inf'))
    
    @property
    def is_zero(self) -> bool:
        """Check if this is the identity element."""
        return self.value == float('inf')


# Convenience functions for creating numeric monoids
def add(*values: T) -> Add[T]:
    """Create an Add monoid from values."""
    if not values:
        return Add.zero()
    return sum(Add(v) for v in values)


def multiply(*values: T) -> Multiply[T]:
    """Create a Multiply monoid from values."""
    if not values:
        return Multiply.zero()
    result = Multiply.zero()
    for v in values:
        result = result * Multiply(v)
    return result


def max_of(*values: T) -> Max[T]:
    """Create a Max monoid from values."""
    if not values:
        return Max.zero()
    return sum(Max(v) for v in values)


def min_of(*values: T) -> Min[T]:
    """Create a Min monoid from values."""
    if not values:
        return Min.zero()
    return sum(Min(v) for v in values)
