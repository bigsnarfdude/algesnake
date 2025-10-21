"""
Semiring abstract base class.

A Semiring is like a Ring but without requiring additive inverses.

Mathematical definition:
    A semiring (S, +, ×, 0, 1) satisfies:
    - (S, +, 0) is a commutative monoid
    - (S, ×, 1) is a monoid
    - Multiplication distributes over addition:
        a × (b + c) = (a × b) + (a × c)  (left distributivity)
        (a + b) × c = (a × c) + (b × c)  (right distributivity)
    - 0 is an annihilator for ×:
        0 × a = a × 0 = 0

Common examples:
    - Natural numbers (ℕ, +, ×, 0, 1)
    - Boolean algebra ({0, 1}, ∨, ∧, 0, 1)
    - Tropical semiring (ℝ ∪ {∞}, min, +, ∞, 0)
"""

from abc import ABC, abstractmethod
from typing import TypeVar, Generic
import sys

if sys.version_info >= (3, 10):
    from typing import Self
else:
    from typing_extensions import Self


T = TypeVar("T")


class Semiring(ABC, Generic[T]):
    """
    Abstract base class for Semiring.

    A Semiring defines two binary operations: addition (plus) and multiplication (times).
    Unlike a Ring, addition does not require inverses (no subtraction).

    Subclasses must implement:
        - plus(a: T, b: T) -> T: The additive operation
        - times(a: T, b: T) -> T: The multiplicative operation
        - zero: Property that returns the additive identity
        - one: Property that returns the multiplicative identity

    Example:
        >>> class NaturalSemiring(Semiring[int]):
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
        >>>
        >>> sr = NaturalSemiring()
        >>> sr.plus(2, 3)
        5
        >>> sr.times(2, 3)
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
            times(zero, a) == zero
            times(a, zero) == zero

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
            >>> sr = NaturalSemiring()
            >>> sr.verify_left_distributivity(2, 3, 4)
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
            >>> sr = NaturalSemiring()
            >>> sr.verify_right_distributivity(2, 3, 4)
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
            >>> sr = NaturalSemiring()
            >>> sr.verify_distributivity(2, 3, 4)
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
            >>> sr = NaturalSemiring()
            >>> sr.verify_additive_commutativity(2, 3)
            True
        """
        left = self.plus(a, b)
        right = self.plus(b, a)

        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return abs(left - right) < epsilon
        else:
            return left == right

    def verify_zero_annihilator(self, a: T, epsilon: float = 1e-10) -> bool:
        """
        Verify that zero is an annihilator for multiplication.

        Checks:
            times(zero, a) == zero
            times(a, zero) == zero

        Args:
            a: Test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if zero annihilator property holds, False otherwise

        Example:
            >>> sr = NaturalSemiring()
            >>> sr.verify_zero_annihilator(42)
            True
        """
        left_result = self.times(self.zero, a)
        right_result = self.times(a, self.zero)
        zero = self.zero

        if isinstance(left_result, (int, float)) and isinstance(zero, (int, float)):
            left_ok = abs(left_result - zero) < epsilon
            right_ok = abs(right_result - zero) < epsilon
            return left_ok and right_ok
        else:
            return left_result == zero and right_result == zero

    def verify_semiring_laws(self, a: T, b: T, c: T, epsilon: float = 1e-10) -> bool:
        """
        Verify core semiring laws for the given elements.

        Checks:
            1. Distributivity (left and right)
            2. Additive commutativity
            3. Zero annihilator property

        Note: This does not verify all monoid properties - use specialized
        verifiers for complete validation.

        Args:
            a: First test element
            b: Second test element
            c: Third test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if semiring laws hold, False otherwise

        Example:
            >>> sr = NaturalSemiring()
            >>> sr.verify_semiring_laws(2, 3, 4)
            True
        """
        return (
            self.verify_distributivity(a, b, c, epsilon) and
            self.verify_additive_commutativity(a, b, epsilon) and
            self.verify_additive_commutativity(b, c, epsilon) and
            self.verify_additive_commutativity(a, c, epsilon) and
            self.verify_zero_annihilator(a, epsilon) and
            self.verify_zero_annihilator(b, epsilon) and
            self.verify_zero_annihilator(c, epsilon)
        )

    def __repr__(self) -> str:
        """String representation of the semiring."""
        return f"{self.__class__.__name__}()"


class SemiringWrapper(Semiring[T]):
    """
    Wrapper class that allows creating semirings from functions.

    This is useful for quick prototyping and testing.

    Example:
        >>> nat_semiring = SemiringWrapper(
        ...     plus_fn=lambda a, b: a + b,
        ...     times_fn=lambda a, b: a * b,
        ...     zero_value=0,
        ...     one_value=1
        ... )
        >>> nat_semiring.plus(2, 3)
        5
        >>> nat_semiring.times(2, 3)
        6
    """

    def __init__(self, plus_fn, times_fn, zero_value: T, one_value: T):
        """
        Initialize a semiring from functions.

        Args:
            plus_fn: Function (a, b) -> a + b
            times_fn: Function (a, b) -> a * b
            zero_value: Additive identity
            one_value: Multiplicative identity
        """
        self._plus_fn = plus_fn
        self._times_fn = times_fn
        self._zero = zero_value
        self._one = one_value

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

    def __repr__(self) -> str:
        """String representation."""
        return f"SemiringWrapper(zero={self._zero}, one={self._one})"
