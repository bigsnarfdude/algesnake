"""
Group abstract base class.

A Group is a Monoid with an inverse operation for every element.

Mathematical definition:
    A group (G, •, e, ⁻¹) satisfies:
    - Closure: For all a, b in G, a • b is in G
    - Associativity: For all a, b, c in G, (a • b) • c = a • (b • c)
    - Identity: There exists e in G such that for all a in G, e • a = a • e = a
    - Inverse: For all a in G, there exists a⁻¹ in G such that a • a⁻¹ = a⁻¹ • a = e
"""

from abc import abstractmethod
from typing import TypeVar, Generic
import sys

from algesnake.abstract.monoid import Monoid

if sys.version_info >= (3, 10):
    from typing import Self
else:
    from typing_extensions import Self


T = TypeVar("T")


class Group(Monoid[T], Generic[T]):
    """
    Abstract base class for Group.

    A Group is a Monoid with an inverse operation for every element.

    Subclasses must implement:
        - combine(a: T, b: T) -> T: The associative binary operation (from Semigroup)
        - zero: Property that returns the identity element (from Monoid)
        - inverse(a: T) -> T: The inverse operation

    Example:
        >>> class IntAddGroup(Group[int]):
        ...     @property
        ...     def zero(self) -> int:
        ...         return 0
        ...
        ...     def combine(self, a: int, b: int) -> int:
        ...         return a + b
        ...
        ...     def inverse(self, a: int) -> int:
        ...         return -a
        >>>
        >>> g = IntAddGroup()
        >>> g.inverse(5)
        -5
        >>> g.combine(5, g.inverse(5))
        0
    """

    @abstractmethod
    def inverse(self, a: T) -> T:
        """
        Return the inverse of an element.

        Must satisfy:
            combine(a, inverse(a)) == zero
            combine(inverse(a), a) == zero

        Args:
            a: Element to invert

        Returns:
            The inverse element

        Example:
            >>> g = IntAddGroup()
            >>> g.inverse(42)
            -42
        """
        pass

    def subtract(self, a: T, b: T) -> T:
        """
        Subtract b from a using the group operation.

        This is defined as: a - b = combine(a, inverse(b))

        Args:
            a: First element
            b: Second element (to be subtracted)

        Returns:
            Result of a - b

        Example:
            >>> g = IntAddGroup()
            >>> g.subtract(10, 3)
            7
        """
        return self.combine(a, self.inverse(b))

    def verify_left_inverse(self, a: T, epsilon: float = 1e-10) -> bool:
        """
        Verify that inverse(a) is a left inverse for a.

        Checks: combine(inverse(a), a) == zero

        Args:
            a: Test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if left inverse holds, False otherwise

        Example:
            >>> g = IntAddGroup()
            >>> g.verify_left_inverse(42)
            True
        """
        result = self.combine(self.inverse(a), a)
        zero = self.zero

        if isinstance(result, (int, float)) and isinstance(zero, (int, float)):
            return abs(result - zero) < epsilon
        else:
            return result == zero

    def verify_right_inverse(self, a: T, epsilon: float = 1e-10) -> bool:
        """
        Verify that inverse(a) is a right inverse for a.

        Checks: combine(a, inverse(a)) == zero

        Args:
            a: Test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if right inverse holds, False otherwise

        Example:
            >>> g = IntAddGroup()
            >>> g.verify_right_inverse(42)
            True
        """
        result = self.combine(a, self.inverse(a))
        zero = self.zero

        if isinstance(result, (int, float)) and isinstance(zero, (int, float)):
            return abs(result - zero) < epsilon
        else:
            return result == zero

    def verify_inverse(self, a: T, epsilon: float = 1e-10) -> bool:
        """
        Verify that inverse(a) is both a left and right inverse for a.

        Checks:
            combine(inverse(a), a) == zero
            combine(a, inverse(a)) == zero

        Args:
            a: Test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if inverse holds, False otherwise

        Example:
            >>> g = IntAddGroup()
            >>> g.verify_inverse(42)
            True
        """
        return (
            self.verify_left_inverse(a, epsilon) and
            self.verify_right_inverse(a, epsilon)
        )

    def verify_group_laws(self, a: T, b: T, c: T, epsilon: float = 1e-10) -> bool:
        """
        Verify all group laws for the given elements.

        Checks:
            1. Associativity: (a • b) • c == a • (b • c)
            2. Identity: zero • a == a • zero == a
            3. Inverse: a • inverse(a) == inverse(a) • a == zero

        Args:
            a: First test element
            b: Second test element
            c: Third test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if all group laws hold, False otherwise

        Example:
            >>> g = IntAddGroup()
            >>> g.verify_group_laws(1, 2, 3)
            True
        """
        return (
            self.verify_monoid_laws(a, b, c, epsilon) and
            self.verify_inverse(a, epsilon) and
            self.verify_inverse(b, epsilon) and
            self.verify_inverse(c, epsilon)
        )


class GroupWrapper(Group[T]):
    """
    Wrapper class that allows creating groups from functions.

    This is useful for quick prototyping and testing.

    Example:
        >>> int_add = GroupWrapper(
        ...     combine_fn=lambda a, b: a + b,
        ...     zero_value=0,
        ...     inverse_fn=lambda a: -a
        ... )
        >>> int_add.combine(5, 3)
        8
        >>> int_add.inverse(5)
        -5
    """

    def __init__(self, combine_fn, zero_value: T, inverse_fn):
        """
        Initialize a group from functions.

        Args:
            combine_fn: A function (a, b) -> result that implements the group operation
            zero_value: The identity element
            inverse_fn: A function a -> inverse(a) that implements the inverse operation
        """
        self._combine_fn = combine_fn
        self._zero = zero_value
        self._inverse_fn = inverse_fn

    def combine(self, a: T, b: T) -> T:
        """Apply the wrapped combining function."""
        return self._combine_fn(a, b)

    @property
    def zero(self) -> T:
        """Return the identity element."""
        return self._zero

    def inverse(self, a: T) -> T:
        """Apply the wrapped inverse function."""
        return self._inverse_fn(a)

    def __repr__(self) -> str:
        """String representation."""
        return f"GroupWrapper({self._combine_fn.__name__}, zero={self._zero})"
