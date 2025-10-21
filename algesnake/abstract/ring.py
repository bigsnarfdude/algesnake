"""
Ring abstract base class.

A Ring is an algebraic structure with two binary operations: addition and multiplication.

Mathematical definition:
    A ring (R, +, ×, 0, 1) satisfies:
    - (R, +, 0) is an abelian group (commutative group)
    - (R, ×, 1) is a monoid
    - Multiplication distributes over addition:
        a × (b + c) = (a × b) + (a × c)  (left distributivity)
        (a + b) × c = (a × c) + (b × c)  (right distributivity)
"""

from abc import ABC, abstractmethod
from typing import TypeVar, Generic
import sys

if sys.version_info >= (3, 10):
    from typing import Self
else:
    from typing_extensions import Self


T = TypeVar("T")


class Ring(ABC, Generic[T]):
    """
    Abstract base class for Ring.

    A Ring defines two binary operations: addition (plus) and multiplication (times).
    Addition forms an abelian group, multiplication forms a monoid, and
    multiplication distributes over addition.

    Subclasses must implement:
        - plus(a: T, b: T) -> T: The additive operation
        - times(a: T, b: T) -> T: The multiplicative operation
        - zero: Property that returns the additive identity
        - one: Property that returns the multiplicative identity
        - negate(a: T) -> T: The additive inverse

    Example:
        >>> class IntRing(Ring[int]):
        ...     @property
        ...     def zero(self) -> int:
        ...         return 0
        ...
        ...     @property
        ...     def one(self) -> int:
        ...         return 1
        ...
        ...     def plus(self, a: int, b: int) -> int:
        ...         return a + b
        ...
        ...     def times(self, a: int, b: int) -> int:
        ...         return a * b
        ...
        ...     def negate(self, a: int) -> int:
        ...         return -a
        >>>
        >>> r = IntRing()
        >>> r.plus(2, 3)
        5
        >>> r.times(2, 3)
        6
    """

    @property
    @abstractmethod
    def zero(self) -> T:
        """
        The additive identity element.

        Must satisfy:
            plus(zero, a) == a
            plus(a, zero) == a

        Returns:
            The additive identity (0)
        """
        pass

    @property
    @abstractmethod
    def one(self) -> T:
        """
        The multiplicative identity element.

        Must satisfy:
            times(one, a) == a
            times(a, one) == a

        Returns:
            The multiplicative identity (1)
        """
        pass

    @abstractmethod
    def plus(self, a: T, b: T) -> T:
        """
        The additive operation.

        Must be associative and commutative, with zero as the identity.

        Args:
            a: First element
            b: Second element

        Returns:
            Sum of a and b
        """
        pass

    @abstractmethod
    def times(self, a: T, b: T) -> T:
        """
        The multiplicative operation.

        Must be associative with one as the identity.
        Must distribute over plus.

        Args:
            a: First element
            b: Second element

        Returns:
            Product of a and b
        """
        pass

    @abstractmethod
    def negate(self, a: T) -> T:
        """
        The additive inverse.

        Must satisfy:
            plus(a, negate(a)) == zero
            plus(negate(a), a) == zero

        Args:
            a: Element to negate

        Returns:
            The additive inverse of a
        """
        pass

    def minus(self, a: T, b: T) -> T:
        """
        Subtract b from a.

        Defined as: a - b = plus(a, negate(b))

        Args:
            a: First element
            b: Second element (to be subtracted)

        Returns:
            Difference of a and b
        """
        return self.plus(a, self.negate(b))

    def verify_left_distributivity(self, a: T, b: T, c: T, epsilon: float = 1e-10) -> bool:
        """
        Verify left distributivity: a × (b + c) = (a × b) + (a × c)

        Args:
            a: First test element
            b: Second test element
            c: Third test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if left distributivity holds, False otherwise

        Example:
            >>> r = IntRing()
            >>> r.verify_left_distributivity(2, 3, 4)
            True
        """
        left = self.times(a, self.plus(b, c))
        right = self.plus(self.times(a, b), self.times(a, c))

        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return abs(left - right) < epsilon
        else:
            return left == right

    def verify_right_distributivity(self, a: T, b: T, c: T, epsilon: float = 1e-10) -> bool:
        """
        Verify right distributivity: (a + b) × c = (a × c) + (b × c)

        Args:
            a: First test element
            b: Second test element
            c: Third test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if right distributivity holds, False otherwise

        Example:
            >>> r = IntRing()
            >>> r.verify_right_distributivity(2, 3, 4)
            True
        """
        left = self.times(self.plus(a, b), c)
        right = self.plus(self.times(a, c), self.times(b, c))

        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return abs(left - right) < epsilon
        else:
            return left == right

    def verify_distributivity(self, a: T, b: T, c: T, epsilon: float = 1e-10) -> bool:
        """
        Verify both left and right distributivity.

        Args:
            a: First test element
            b: Second test element
            c: Third test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if distributivity holds, False otherwise

        Example:
            >>> r = IntRing()
            >>> r.verify_distributivity(2, 3, 4)
            True
        """
        return (
            self.verify_left_distributivity(a, b, c, epsilon) and
            self.verify_right_distributivity(a, b, c, epsilon)
        )

    def verify_additive_commutativity(self, a: T, b: T, epsilon: float = 1e-10) -> bool:
        """
        Verify that addition is commutative: a + b = b + a

        Args:
            a: First test element
            b: Second test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if commutativity holds, False otherwise

        Example:
            >>> r = IntRing()
            >>> r.verify_additive_commutativity(2, 3)
            True
        """
        left = self.plus(a, b)
        right = self.plus(b, a)

        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return abs(left - right) < epsilon
        else:
            return left == right

    def verify_ring_laws(self, a: T, b: T, c: T, epsilon: float = 1e-10) -> bool:
        """
        Verify core ring laws for the given elements.

        Checks:
            1. Distributivity (left and right)
            2. Additive commutativity

        Note: This does not verify all group/monoid properties - use specialized
        verifiers for complete validation.

        Args:
            a: First test element
            b: Second test element
            c: Third test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if ring laws hold, False otherwise

        Example:
            >>> r = IntRing()
            >>> r.verify_ring_laws(2, 3, 4)
            True
        """
        return (
            self.verify_distributivity(a, b, c, epsilon) and
            self.verify_additive_commutativity(a, b, epsilon) and
            self.verify_additive_commutativity(b, c, epsilon) and
            self.verify_additive_commutativity(a, c, epsilon)
        )

    def __repr__(self) -> str:
        """String representation of the ring."""
        return f"{self.__class__.__name__}()"


class RingWrapper(Ring[T]):
    """
    Wrapper class that allows creating rings from functions.

    This is useful for quick prototyping and testing.

    Example:
        >>> int_ring = RingWrapper(
        ...     plus_fn=lambda a, b: a + b,
        ...     times_fn=lambda a, b: a * b,
        ...     zero_value=0,
        ...     one_value=1,
        ...     negate_fn=lambda a: -a
        ... )
        >>> int_ring.plus(2, 3)
        5
        >>> int_ring.times(2, 3)
        6
    """

    def __init__(self, plus_fn, times_fn, zero_value: T, one_value: T, negate_fn):
        """
        Initialize a ring from functions.

        Args:
            plus_fn: Function (a, b) -> a + b
            times_fn: Function (a, b) -> a * b
            zero_value: Additive identity
            one_value: Multiplicative identity
            negate_fn: Function a -> -a
        """
        self._plus_fn = plus_fn
        self._times_fn = times_fn
        self._zero = zero_value
        self._one = one_value
        self._negate_fn = negate_fn

    def plus(self, a: T, b: T) -> T:
        """Apply the wrapped addition function."""
        return self._plus_fn(a, b)

    def times(self, a: T, b: T) -> T:
        """Apply the wrapped multiplication function."""
        return self._times_fn(a, b)

    @property
    def zero(self) -> T:
        """Return the additive identity."""
        return self._zero

    @property
    def one(self) -> T:
        """Return the multiplicative identity."""
        return self._one

    def negate(self, a: T) -> T:
        """Apply the wrapped negation function."""
        return self._negate_fn(a)

    def __repr__(self) -> str:
        """String representation."""
        return f"RingWrapper(zero={self._zero}, one={self._one})"
