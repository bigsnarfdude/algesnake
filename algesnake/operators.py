"""
Operator overloading support for algebraic structures.

This module provides decorators and mixins to enable Pythonic syntax for
algebraic operations like + (combine), - (subtract), * (times), etc.

Example:
    >>> @provides_monoid
    ... class Max:
    ...     def __init__(self, value):
    ...         self.value = value
    ...
    ...     def combine(self, other):
    ...         return Max(max(self.value, other.value))
    ...
    ...     @property
    ...     def zero(self):
    ...         return Max(float('-inf'))
    >>>
    >>> result = Max(5) + Max(3) + Max(1)
    >>> print(result.value)  # 5
"""

from typing import TypeVar, Generic, Protocol, runtime_checkable
from functools import wraps
import sys

if sys.version_info >= (3, 10):
    from typing import Self
else:
    from typing_extensions import Self


T = TypeVar("T")


@runtime_checkable
class HasCombine(Protocol):
    """Protocol for types that support the combine operation."""

    def combine(self, other: Self) -> Self:
        """Combine two values."""
        ...


@runtime_checkable
class HasZero(Protocol):
    """Protocol for types that have a zero element."""

    @property
    def zero(self) -> Self:
        """Return the identity element."""
        ...


@runtime_checkable
class HasInverse(Protocol):
    """Protocol for types that support inversion."""

    def inverse(self) -> Self:
        """Return the inverse of this element."""
        ...


@runtime_checkable
class HasPlus(Protocol):
    """Protocol for types that support addition."""

    def plus(self, other: Self) -> Self:
        """Add two values."""
        ...


@runtime_checkable
class HasTimes(Protocol):
    """Protocol for types that support multiplication."""

    def times(self, other: Self) -> Self:
        """Multiply two values."""
        ...


@runtime_checkable
class HasNegate(Protocol):
    """Protocol for types that support negation."""

    def negate(self) -> Self:
        """Negate this value."""
        ...


def provides_monoid(cls):
    """
    Class decorator that adds operator overloading for Monoid operations.

    The decorated class must have:
        - combine(self, other) -> Self
        - zero property

    Adds support for:
        - a + b: calls a.combine(b)
        - sum([a, b, c]): uses zero as initial value

    Example:
        >>> @provides_monoid
        ... class Max:
        ...     def __init__(self, value):
        ...         self.value = value
        ...
        ...     def combine(self, other):
        ...         return Max(max(self.value, other.value))
        ...
        ...     @property
        ...     def zero(self):
        ...         return Max(float('-inf'))
        >>>
        >>> Max(5) + Max(3)
        Max(5)
    """
    original_add = getattr(cls, '__add__', None)

    def __add__(self, other):
        if isinstance(other, cls):
            return self.combine(other)
        elif original_add is not None:
            return original_add(self, other)
        else:
            return NotImplemented

    def __radd__(self, other):
        # Support sum() which starts with 0
        if other == 0:
            return self
        elif isinstance(other, cls):
            return other.combine(self)
        else:
            return NotImplemented

    cls.__add__ = __add__
    cls.__radd__ = __radd__

    return cls


def provides_group(cls):
    """
    Class decorator that adds operator overloading for Group operations.

    The decorated class must have:
        - combine(self, other) -> Self
        - zero property
        - inverse(self) -> Self

    Adds support for:
        - a + b: calls a.combine(b)
        - a - b: calls a.combine(b.inverse())
        - -a: calls a.inverse()

    Example:
        >>> @provides_group
        ... class IntGroup:
        ...     def __init__(self, value):
        ...         self.value = value
        ...
        ...     def combine(self, other):
        ...         return IntGroup(self.value + other.value)
        ...
        ...     @property
        ...     def zero(self):
        ...         return IntGroup(0)
        ...
        ...     def inverse(self):
        ...         return IntGroup(-self.value)
        >>>
        >>> IntGroup(5) - IntGroup(3)
        IntGroup(2)
    """
    # First apply monoid operators
    cls = provides_monoid(cls)

    original_sub = getattr(cls, '__sub__', None)
    original_neg = getattr(cls, '__neg__', None)

    def __sub__(self, other):
        if isinstance(other, cls):
            return self.combine(other.inverse())
        elif original_sub is not None:
            return original_sub(self, other)
        else:
            return NotImplemented

    def __neg__(self):
        if hasattr(self, 'inverse'):
            return self.inverse()
        elif original_neg is not None:
            return original_neg(self)
        else:
            return NotImplemented

    cls.__sub__ = __sub__
    cls.__neg__ = __neg__

    return cls


def provides_ring(cls):
    """
    Class decorator that adds operator overloading for Ring operations.

    The decorated class must have:
        - plus(self, other) -> Self
        - times(self, other) -> Self
        - zero property
        - one property
        - negate(self) -> Self

    Adds support for:
        - a + b: calls a.plus(b)
        - a - b: calls a.plus(b.negate())
        - a * b: calls a.times(b)
        - -a: calls a.negate()

    Example:
        >>> @provides_ring
        ... class IntRing:
        ...     def __init__(self, value):
        ...         self.value = value
        ...
        ...     def plus(self, other):
        ...         return IntRing(self.value + other.value)
        ...
        ...     def times(self, other):
        ...         return IntRing(self.value * other.value)
        ...
        ...     @property
        ...     def zero(self):
        ...         return IntRing(0)
        ...
        ...     @property
        ...     def one(self):
        ...         return IntRing(1)
        ...
        ...     def negate(self):
        ...         return IntRing(-self.value)
        >>>
        >>> (IntRing(2) + IntRing(3)) * IntRing(4)
        IntRing(20)
    """
    original_add = getattr(cls, '__add__', None)
    original_sub = getattr(cls, '__sub__', None)
    original_mul = getattr(cls, '__mul__', None)
    original_neg = getattr(cls, '__neg__', None)

    def __add__(self, other):
        if isinstance(other, cls):
            return self.plus(other)
        elif other == 0:
            return self
        elif original_add is not None:
            return original_add(self, other)
        else:
            return NotImplemented

    def __radd__(self, other):
        if other == 0:
            return self
        elif isinstance(other, cls):
            return other.plus(self)
        else:
            return NotImplemented

    def __sub__(self, other):
        if isinstance(other, cls):
            return self.plus(other.negate())
        elif original_sub is not None:
            return original_sub(self, other)
        else:
            return NotImplemented

    def __mul__(self, other):
        if isinstance(other, cls):
            return self.times(other)
        elif original_mul is not None:
            return original_mul(self, other)
        else:
            return NotImplemented

    def __rmul__(self, other):
        if isinstance(other, cls):
            return other.times(self)
        else:
            return NotImplemented

    def __neg__(self):
        if hasattr(self, 'negate'):
            return self.negate()
        elif original_neg is not None:
            return original_neg(self)
        else:
            return NotImplemented

    cls.__add__ = __add__
    cls.__radd__ = __radd__
    cls.__sub__ = __sub__
    cls.__mul__ = __mul__
    cls.__rmul__ = __rmul__
    cls.__neg__ = __neg__

    return cls


def provides_semiring(cls):
    """
    Class decorator that adds operator overloading for Semiring operations.

    The decorated class must have:
        - plus(self, other) -> Self
        - times(self, other) -> Self
        - zero property
        - one property

    Adds support for:
        - a + b: calls a.plus(b)
        - a * b: calls a.times(b)

    Example:
        >>> @provides_semiring
        ... class NaturalSemiring:
        ...     def __init__(self, value):
        ...         self.value = value
        ...
        ...     def plus(self, other):
        ...         return NaturalSemiring(self.value + other.value)
        ...
        ...     def times(self, other):
        ...         return NaturalSemiring(self.value * other.value)
        ...
        ...     @property
        ...     def zero(self):
        ...         return NaturalSemiring(0)
        ...
        ...     @property
        ...     def one(self):
        ...         return NaturalSemiring(1)
        >>>
        >>> (NaturalSemiring(2) + NaturalSemiring(3)) * NaturalSemiring(4)
        NaturalSemiring(20)
    """
    original_add = getattr(cls, '__add__', None)
    original_mul = getattr(cls, '__mul__', None)

    def __add__(self, other):
        if isinstance(other, cls):
            return self.plus(other)
        elif other == 0:
            return self
        elif original_add is not None:
            return original_add(self, other)
        else:
            return NotImplemented

    def __radd__(self, other):
        if other == 0:
            return self
        elif isinstance(other, cls):
            return other.plus(self)
        else:
            return NotImplemented

    def __mul__(self, other):
        if isinstance(other, cls):
            return self.times(other)
        elif original_mul is not None:
            return original_mul(self, other)
        else:
            return NotImplemented

    def __rmul__(self, other):
        if isinstance(other, cls):
            return other.times(self)
        else:
            return NotImplemented

    cls.__add__ = __add__
    cls.__radd__ = __radd__
    cls.__mul__ = __mul__
    cls.__rmul__ = __rmul__

    return cls


class MonoidMixin:
    """
    Mixin class that adds operator overloading for Monoid operations.

    Use this when you can't use the decorator approach (e.g., when decorating
    existing classes is not possible).

    Example:
        >>> class Max(MonoidMixin):
        ...     def __init__(self, value):
        ...         self.value = value
        ...
        ...     def combine(self, other):
        ...         return Max(max(self.value, other.value))
        ...
        ...     @property
        ...     def zero(self):
        ...         return Max(float('-inf'))
        >>>
        >>> Max(5) + Max(3)
        Max(5)
    """

    def __add__(self, other):
        if isinstance(other, self.__class__):
            return self.combine(other)
        else:
            return NotImplemented

    def __radd__(self, other):
        if other == 0:
            return self
        elif isinstance(other, self.__class__):
            return other.combine(self)
        else:
            return NotImplemented


class GroupMixin(MonoidMixin):
    """
    Mixin class that adds operator overloading for Group operations.

    Example:
        >>> class IntGroup(GroupMixin):
        ...     def __init__(self, value):
        ...         self.value = value
        ...
        ...     def combine(self, other):
        ...         return IntGroup(self.value + other.value)
        ...
        ...     @property
        ...     def zero(self):
        ...         return IntGroup(0)
        ...
        ...     def inverse(self):
        ...         return IntGroup(-self.value)
        >>>
        >>> IntGroup(5) - IntGroup(3)
        IntGroup(2)
    """

    def __sub__(self, other):
        if isinstance(other, self.__class__):
            return self.combine(other.inverse())
        else:
            return NotImplemented

    def __neg__(self):
        return self.inverse()


class RingMixin:
    """
    Mixin class that adds operator overloading for Ring operations.

    Example:
        >>> class IntRing(RingMixin):
        ...     def __init__(self, value):
        ...         self.value = value
        ...
        ...     def plus(self, other):
        ...         return IntRing(self.value + other.value)
        ...
        ...     def times(self, other):
        ...         return IntRing(self.value * other.value)
        ...
        ...     @property
        ...     def zero(self):
        ...         return IntRing(0)
        ...
        ...     @property
        ...     def one(self):
        ...         return IntRing(1)
        ...
        ...     def negate(self):
        ...         return IntRing(-self.value)
        >>>
        >>> (IntRing(2) + IntRing(3)) * IntRing(4)
        IntRing(20)
    """

    def __add__(self, other):
        if isinstance(other, self.__class__):
            return self.plus(other)
        elif other == 0:
            return self
        else:
            return NotImplemented

    def __radd__(self, other):
        if other == 0:
            return self
        elif isinstance(other, self.__class__):
            return other.plus(self)
        else:
            return NotImplemented

    def __sub__(self, other):
        if isinstance(other, self.__class__):
            return self.plus(other.negate())
        else:
            return NotImplemented

    def __mul__(self, other):
        if isinstance(other, self.__class__):
            return self.times(other)
        else:
            return NotImplemented

    def __rmul__(self, other):
        if isinstance(other, self.__class__):
            return other.times(self)
        else:
            return NotImplemented

    def __neg__(self):
        return self.negate()


class SemiringMixin:
    """
    Mixin class that adds operator overloading for Semiring operations.

    Example:
        >>> class NaturalSemiring(SemiringMixin):
        ...     def __init__(self, value):
        ...         self.value = value
        ...
        ...     def plus(self, other):
        ...         return NaturalSemiring(self.value + other.value)
        ...
        ...     def times(self, other):
        ...         return NaturalSemiring(self.value * other.value)
        ...
        ...     @property
        ...     def zero(self):
        ...         return NaturalSemiring(0)
        ...
        ...     @property
        ...     def one(self):
        ...         return NaturalSemiring(1)
        >>>
        >>> (NaturalSemiring(2) + NaturalSemiring(3)) * NaturalSemiring(4)
        NaturalSemiring(20)
    """

    def __add__(self, other):
        if isinstance(other, self.__class__):
            return self.plus(other)
        elif other == 0:
            return self
        else:
            return NotImplemented

    def __radd__(self, other):
        if other == 0:
            return self
        elif isinstance(other, self.__class__):
            return other.plus(self)
        else:
            return NotImplemented

    def __mul__(self, other):
        if isinstance(other, self.__class__):
            return self.times(other)
        else:
            return NotImplemented

    def __rmul__(self, other):
        if isinstance(other, self.__class__):
            return other.times(self)
        else:
            return NotImplemented
