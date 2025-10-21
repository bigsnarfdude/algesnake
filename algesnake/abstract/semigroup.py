"""
Semigroup abstract base class.

A Semigroup is an algebraic structure consisting of a set with an associative binary operation.

Mathematical definition:
    A semigroup (S, •) satisfies:
    - Closure: For all a, b in S, a • b is in S
    - Associativity: For all a, b, c in S, (a • b) • c = a • (b • c)
"""

from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Optional
import sys

if sys.version_info >= (3, 10):
    from typing import Self
else:
    from typing_extensions import Self


T = TypeVar("T")


class Semigroup(ABC, Generic[T]):
    """
    Abstract base class for Semigroup.

    A Semigroup defines an associative binary operation (combine) over a type T.

    Subclasses must implement:
        - combine(a: T, b: T) -> T: The associative binary operation

    Example:
        >>> class MaxSemigroup(Semigroup[int]):
        ...     def combine(self, a: int, b: int) -> int:
        ...         return max(a, b)
        >>>
        >>> sg = MaxSemigroup()
        >>> sg.combine(5, 3)
        5
    """

    @abstractmethod
    def combine(self, a: T, b: T) -> T:
        """
        Combine two elements using the semigroup operation.

        This operation must be associative:
            combine(combine(a, b), c) == combine(a, combine(b, c))

        Args:
            a: First element
            b: Second element

        Returns:
            Combined element
        """
        pass

    def combine_all(self, elements: List[T]) -> Optional[T]:
        """
        Combine all elements in a list using the semigroup operation.

        This is a left-associative fold: combine(combine(a, b), c)

        Args:
            elements: List of elements to combine

        Returns:
            Combined result, or None if the list is empty

        Example:
            >>> sg = MaxSemigroup()
            >>> sg.combine_all([1, 5, 3, 2])
            5
        """
        if not elements:
            return None

        result = elements[0]
        for elem in elements[1:]:
            result = self.combine(result, elem)
        return result

    def verify_associativity(self, a: T, b: T, c: T, epsilon: float = 1e-10) -> bool:
        """
        Verify that the semigroup operation is associative for given elements.

        Checks: combine(combine(a, b), c) == combine(a, combine(b, c))

        Args:
            a: First test element
            b: Second test element
            c: Third test element
            epsilon: Tolerance for floating-point comparison

        Returns:
            True if associativity holds, False otherwise

        Example:
            >>> sg = MaxSemigroup()
            >>> sg.verify_associativity(1, 2, 3)
            True
        """
        left = self.combine(self.combine(a, b), c)
        right = self.combine(a, self.combine(b, c))

        # Handle different types for comparison
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return abs(left - right) < epsilon
        else:
            return left == right

    def __repr__(self) -> str:
        """String representation of the semigroup."""
        return f"{self.__class__.__name__}()"


class SemigroupWrapper(Semigroup[T]):
    """
    Wrapper class that allows creating semigroups from functions.

    This is useful for quick prototyping and testing.

    Example:
        >>> add_sg = SemigroupWrapper(lambda a, b: a + b)
        >>> add_sg.combine(2, 3)
        5
    """

    def __init__(self, combine_fn):
        """
        Initialize a semigroup from a combining function.

        Args:
            combine_fn: A function (a, b) -> result that implements the semigroup operation
        """
        self._combine_fn = combine_fn

    def combine(self, a: T, b: T) -> T:
        """Apply the wrapped combining function."""
        return self._combine_fn(a, b)

    def __repr__(self) -> str:
        """String representation."""
        return f"SemigroupWrapper({self._combine_fn.__name__})"
