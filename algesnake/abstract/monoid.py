"""
Monoid abstract base class.

A Monoid is a Semigroup with an identity element (zero).

Mathematical definition:
    A monoid (M, •, e) satisfies:
    - Closure: For all a, b in M, a • b is in M
    - Associativity: For all a, b, c in M, (a • b) • c = a • (b • c)
    - Identity: There exists e in M such that for all a in M, e • a = a • e = a
"""

from abc import abstractmethod
from typing import TypeVar, Generic, List
import sys

from algesnake.abstract.semigroup import Semigroup

if sys.version_info >= (3, 10):
    from typing import Self
else:
    from typing_extensions import Self


T = TypeVar("T")


class Monoid(Semigroup[T], Generic[T]):
    """
    Abstract base class for Monoid.

    A Monoid is a Semigroup with an identity element (zero).

    Subclasses must implement:
        - combine(a: T, b: T) -> T: The associative binary operation (from Semigroup)
        - zero: Property that returns the identity element

    Example:
        >>> class AddMonoid(Monoid[int]):
        ...     @property
        ...     def zero(self) -> int:
        ...         return 0
        ...
        ...     def combine(self, a: int, b: int) -> int:
        ...         return a + b
        >>>
        >>> m = AddMonoid()
        >>> m.combine(5, m.zero)
        5
    """

    @property
    @abstractmethod
    def zero(self) -> T:
        """
        The identity element of the monoid.

        Must satisfy:
            combine(zero, a) == a
            combine(a, zero) == a

        Returns:
            The identity element
        """
        pass

    def combine_all(self, elements: List[T]) -> T:
        """
        Combine all elements in a list using the monoid operation.

        Unlike Semigroup.combine_all, this always returns a value (zero for empty lists).

        Args:
            elements: List of elements to combine

        Returns:
            Combined result, or zero if the list is empty

        Example:
            >>> m = AddMonoid()
            >>> m.combine_all([1, 2, 3, 4])
            10
            >>> m.combine_all([])
            0
        """
        if not elements:
            return self.zero

        result = self.zero
        for elem in elements:
            result = self.combine(result, elem)
        return result

    def combine_option(self, a: T, b: T) -> T:
        """
        Combine two elements, treating None as zero.

        This is a convenience method for working with optional values.

        Args:
            a: First element (or None)
            b: Second element (or None)

        Returns:
            Combined result

        Example:
            >>> m = AddMonoid()
            >>> m.combine_option(5, None)
            5
            >>> m.combine_option(None, 3)
            3
        """
        if a is None:
            return b if b is not None else self.zero
        if b is None:
            return a
        return self.combine(a, b)

    def verify_left_identity(self, a: T, epsilon: float = 1e-10) -> bool:
        """
        Verify that zero is a left identity for the given element.

        Checks: combine(zero, a) == a

        Args:
            a: Test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if left identity holds, False otherwise

        Example:
            >>> m = AddMonoid()
            >>> m.verify_left_identity(42)
            True
        """
        result = self.combine(self.zero, a)

        if isinstance(result, (int, float)) and isinstance(a, (int, float)):
            return abs(result - a) < epsilon
        else:
            return result == a

    def verify_right_identity(self, a: T, epsilon: float = 1e-10) -> bool:
        """
        Verify that zero is a right identity for the given element.

        Checks: combine(a, zero) == a

        Args:
            a: Test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if right identity holds, False otherwise

        Example:
            >>> m = AddMonoid()
            >>> m.verify_right_identity(42)
            True
        """
        result = self.combine(a, self.zero)

        if isinstance(result, (int, float)) and isinstance(a, (int, float)):
            return abs(result - a) < epsilon
        else:
            return result == a

    def verify_identity(self, a: T, epsilon: float = 1e-10) -> bool:
        """
        Verify that zero is both a left and right identity for the given element.

        Checks: combine(zero, a) == a and combine(a, zero) == a

        Args:
            a: Test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if identity holds, False otherwise

        Example:
            >>> m = AddMonoid()
            >>> m.verify_identity(42)
            True
        """
        return (
            self.verify_left_identity(a, epsilon) and
            self.verify_right_identity(a, epsilon)
        )

    def verify_monoid_laws(self, a: T, b: T, c: T, epsilon: float = 1e-10) -> bool:
        """
        Verify all monoid laws for the given elements.

        Checks:
            1. Associativity: (a • b) • c == a • (b • c)
            2. Left identity: zero • a == a
            3. Right identity: a • zero == a

        Args:
            a: First test element
            b: Second test element
            c: Third test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if all monoid laws hold, False otherwise

        Example:
            >>> m = AddMonoid()
            >>> m.verify_monoid_laws(1, 2, 3)
            True
        """
        return (
            self.verify_associativity(a, b, c, epsilon) and
            self.verify_identity(a, epsilon) and
            self.verify_identity(b, epsilon) and
            self.verify_identity(c, epsilon)
        )


class MonoidWrapper(Monoid[T]):
    """
    Wrapper class that allows creating monoids from functions and a zero value.

    This is useful for quick prototyping and testing.

    Example:
        >>> add_m = MonoidWrapper(lambda a, b: a + b, 0)
        >>> add_m.combine(2, 3)
        5
        >>> add_m.zero
        0
    """

    def __init__(self, combine_fn, zero_value: T):
        """
        Initialize a monoid from a combining function and identity element.

        Args:
            combine_fn: A function (a, b) -> result that implements the monoid operation
            zero_value: The identity element
        """
        self._combine_fn = combine_fn
        self._zero = zero_value

    def combine(self, a: T, b: T) -> T:
        """Apply the wrapped combining function."""
        return self._combine_fn(a, b)

    @property
    def zero(self) -> T:
        """Return the identity element."""
        return self._zero

    def __repr__(self) -> str:
        """String representation."""
        return f"MonoidWrapper({self._combine_fn.__name__}, zero={self._zero})"
