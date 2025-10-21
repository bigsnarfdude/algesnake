"""
Option/Maybe monoid for handling optional values.

This module provides a monoid for optional values, similar to Scala's Option
or Haskell's Maybe. It allows safe composition of computations that may fail.
"""

from typing import TypeVar, Generic, Callable, Union, Any
from enum import Enum

T = TypeVar('T')


class OptionType(Enum):
    """Enum to distinguish between Some and None variants."""
    SOME = "some"
    NONE = "none"


class Option(Generic[T]):
    """
    Option/Maybe monoid for handling optional values.
    
    An Option can be either:
    - Some(value): contains a value
    - None_: represents absence of a value
    
    Identity element: None_
    Operation: Returns the first Some value, or None_ if both are None_
    
    Examples:
        >>> Some(5) + Some(3)
        Some(5)
        >>> Some(5) + None_()
        Some(5)
        >>> None_() + Some(3)
        Some(3)
        >>> None_() + None_()
        None_()
        >>> sum([Some(1), None_(), Some(3)])
        Some(1)
    """
    
    def __init__(self, value: T = None, option_type: OptionType = None):
        if option_type is None:
            # Auto-detect type
            if value is None:
                self._type = OptionType.NONE
                self._value = None
            else:
                self._type = OptionType.SOME
                self._value = value
        else:
            self._type = option_type
            self._value = value if option_type == OptionType.SOME else None
    
    @property
    def is_some(self) -> bool:
        """Check if this is a Some value."""
        return self._type == OptionType.SOME
    
    @property
    def is_none(self) -> bool:
        """Check if this is None."""
        return self._type == OptionType.NONE
    
    @property
    def value(self) -> T:
        """
        Get the contained value.
        
        Raises:
            ValueError: If this is None_
        """
        if self.is_none:
            raise ValueError("Cannot get value from None_")
        return self._value
    
    def get_or_else(self, default: T) -> T:
        """Get the value or return a default if None."""
        return self._value if self.is_some else default
    
    def map(self, f: Callable[[T], Any]) -> 'Option':
        """
        Apply a function to the contained value if Some.
        
        Args:
            f: Function to apply to the value
            
        Returns:
            Some(f(value)) if this is Some(value), None_ otherwise
        """
        if self.is_some:
            return Some(f(self._value))
        return None_()
    
    def flat_map(self, f: Callable[[T], 'Option']) -> 'Option':
        """
        Apply a function that returns an Option to the contained value.
        
        Args:
            f: Function that takes a value and returns an Option
            
        Returns:
            f(value) if this is Some(value), None_ otherwise
        """
        if self.is_some:
            return f(self._value)
        return None_()
    
    def filter(self, predicate: Callable[[T], bool]) -> 'Option':
        """
        Filter the value based on a predicate.
        
        Args:
            predicate: Function that returns True to keep the value
            
        Returns:
            This Option if predicate is True, None_ otherwise
        """
        if self.is_some and predicate(self._value):
            return self
        return None_()
    
    def combine(self, other: 'Option[T]') -> 'Option[T]':
        """
        Combine two Options using the first-wins strategy.
        
        Returns the first Some value, or None_ if both are None_.
        """
        if self.is_some:
            return self
        return other
    
    def __add__(self, other: 'Option[T]') -> 'Option[T]':
        """Enable + operator for Option."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support for sum() builtin."""
        if other == 0:
            return self
        return other.combine(self)
    
    def __repr__(self) -> str:
        if self.is_some:
            return f"Some({self._value!r})"
        return "None_()"
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, Option):
            return False
        if self.is_none and other.is_none:
            return True
        if self.is_some and other.is_some:
            return self._value == other._value
        return False
    
    def __hash__(self) -> int:
        if self.is_none:
            return hash(("None_", None))
        return hash(("Some", self._value))
    
    def __bool__(self) -> bool:
        """Options are truthy if they contain a value."""
        return self.is_some
    
    @classmethod
    def zero(cls) -> 'Option[T]':
        """Return the identity element (None_)."""
        return None_()


def Some(value: T) -> Option[T]:
    """
    Create an Option containing a value.
    
    Args:
        value: The value to wrap
        
    Returns:
        An Option containing the value
    """
    return Option(value, OptionType.SOME)


def None_() -> Option:
    """
    Create an empty Option.
    
    Returns:
        An empty Option
    """
    return Option(None, OptionType.NONE)


class OptionMonoid(Generic[T]):
    """
    Option monoid with custom value combination strategy.
    
    This variant allows you to specify how to combine values when both
    Options contain values, rather than just taking the first one.
    
    Examples:
        >>> # Sum the values if both present
        >>> m = OptionMonoid(lambda a, b: a + b)
        >>> m.combine(Some(5), Some(3))
        Some(8)
        >>> m.combine(Some(5), None_())
        Some(5)
        
        >>> # Take maximum value
        >>> m = OptionMonoid(max)
        >>> m.combine(Some(5), Some(3))
        Some(5)
    """
    
    def __init__(self, combine_values: Callable[[T, T], T]):
        """
        Create an OptionMonoid with a custom value combination function.
        
        Args:
            combine_values: Function to combine two values when both are Some
        """
        self.combine_values = combine_values
    
    def combine(self, a: Option[T], b: Option[T]) -> Option[T]:
        """Combine two Options using the value combination function."""
        if a.is_some and b.is_some:
            return Some(self.combine_values(a.value, b.value))
        elif a.is_some:
            return a
        else:
            return b
    
    def combine_all(self, options: list[Option[T]]) -> Option[T]:
        """Combine a list of Options."""
        result = None_()
        for opt in options:
            result = self.combine(result, opt)
        return result
    
    @property
    def zero(self) -> Option[T]:
        """Return the identity element (None_)."""
        return None_()


# Helper functions for common patterns
def option_or_else(*options: Option[T]) -> Option[T]:
    """
    Return the first Some value from a list of Options.
    
    Examples:
        >>> option_or_else(None_(), Some(5), Some(3))
        Some(5)
        >>> option_or_else(None_(), None_())
        None_()
    """
    for opt in options:
        if opt.is_some:
            return opt
    return None_()


def flatten_options(options: list[Option[T]]) -> list[T]:
    """
    Extract all Some values from a list of Options.
    
    Examples:
        >>> flatten_options([Some(1), None_(), Some(3), None_(), Some(5)])
        [1, 3, 5]
    """
    return [opt.value for opt in options if opt.is_some]


def sequence_options(options: list[Option[T]]) -> Option[list[T]]:
    """
    Convert a list of Options into an Option of a list.
    
    Returns Some(list) if all Options are Some, otherwise None_.
    
    Examples:
        >>> sequence_options([Some(1), Some(2), Some(3)])
        Some([1, 2, 3])
        >>> sequence_options([Some(1), None_(), Some(3)])
        None_()
    """
    values = []
    for opt in options:
        if opt.is_none:
            return None_()
        values.append(opt.value)
    return Some(values)
