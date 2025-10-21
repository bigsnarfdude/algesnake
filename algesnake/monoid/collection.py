"""
Collection monoids for aggregating collections.

This module provides concrete implementations of monoids for collection types,
including sets, lists, and dictionaries (maps).
"""

from typing import TypeVar, Generic, Set as PySet, List as PyList, Dict, Callable, Any
from abc import ABC

K = TypeVar('K')
V = TypeVar('V')
T = TypeVar('T')


class CollectionMonoid(ABC, Generic[T]):
    """Base class for collection monoids with operator overloading support."""
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value})"
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.value == other.value
    
    def __hash__(self) -> int:
        # Only hashable if the underlying value is hashable
        try:
            return hash((self.__class__.__name__, tuple(sorted(self.value))))
        except TypeError:
            return id(self)


class SetMonoid(CollectionMonoid[T], Generic[T]):
    """
    Set union monoid.
    
    Identity element: empty set
    Operation: set union (a ∪ b)
    
    Examples:
        >>> SetMonoid({1, 2}) + SetMonoid({2, 3})
        SetMonoid({1, 2, 3})
        >>> sum([SetMonoid({1}), SetMonoid({2}), SetMonoid({3})])
        SetMonoid({1, 2, 3})
        >>> SetMonoid.zero()
        SetMonoid(set())
    """
    
    def __init__(self, value: PySet[T] = None):
        self.value = value if value is not None else set()
    
    def combine(self, other: 'SetMonoid[T]') -> 'SetMonoid[T]':
        """Combine two SetMonoids by taking the union."""
        return SetMonoid(self.value | other.value)
    
    def __add__(self, other: 'SetMonoid[T]') -> 'SetMonoid[T]':
        """Enable + operator for SetMonoid."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support for sum() builtin."""
        if other == 0:
            return self
        return self.combine(other)
    
    @classmethod
    def zero(cls) -> 'SetMonoid[T]':
        """Return the identity element (empty set)."""
        return cls(set())
    
    @property
    def is_zero(self) -> bool:
        """Check if this is the identity element."""
        return len(self.value) == 0
    
    def __len__(self) -> int:
        """Return the size of the set."""
        return len(self.value)
    
    def __contains__(self, item: T) -> bool:
        """Check if an item is in the set."""
        return item in self.value


class ListMonoid(CollectionMonoid[T], Generic[T]):
    """
    List concatenation monoid.
    
    Identity element: empty list
    Operation: list concatenation (a ++ b)
    
    Examples:
        >>> ListMonoid([1, 2]) + ListMonoid([3, 4])
        ListMonoid([1, 2, 3, 4])
        >>> sum([ListMonoid([1]), ListMonoid([2]), ListMonoid([3])])
        ListMonoid([1, 2, 3])
        >>> ListMonoid.zero()
        ListMonoid([])
    """
    
    def __init__(self, value: PyList[T] = None):
        self.value = value if value is not None else []
    
    def combine(self, other: 'ListMonoid[T]') -> 'ListMonoid[T]':
        """Combine two ListMonoids by concatenation."""
        return ListMonoid(self.value + other.value)
    
    def __add__(self, other: 'ListMonoid[T]') -> 'ListMonoid[T]':
        """Enable + operator for ListMonoid."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support for sum() builtin."""
        if other == 0:
            return self
        return self.combine(other)
    
    @classmethod
    def zero(cls) -> 'ListMonoid[T]':
        """Return the identity element (empty list)."""
        return cls([])
    
    @property
    def is_zero(self) -> bool:
        """Check if this is the identity element."""
        return len(self.value) == 0
    
    def __len__(self) -> int:
        """Return the length of the list."""
        return len(self.value)
    
    def __getitem__(self, index):
        """Enable indexing."""
        return self.value[index]


class MapMonoid(CollectionMonoid[K], Generic[K, V]):
    """
    Map (dictionary) monoid with value combination.
    
    Identity element: empty dict
    Operation: merge dictionaries, combining values with a provided function
    
    Examples:
        >>> # Default behavior: last value wins
        >>> MapMonoid({'a': 1, 'b': 2}) + MapMonoid({'b': 3, 'c': 4})
        MapMonoid({'a': 1, 'b': 3, 'c': 4})
        
        >>> # Custom combination: sum values
        >>> m1 = MapMonoid({'a': 1, 'b': 2}, combine_values=lambda x, y: x + y)
        >>> m2 = MapMonoid({'b': 3, 'c': 4}, combine_values=lambda x, y: x + y)
        >>> m1 + m2
        MapMonoid({'a': 1, 'b': 5, 'c': 4})
    """
    
    def __init__(
        self, 
        value: Dict[K, V] = None,
        combine_values: Callable[[V, V], V] = None
    ):
        self.value = value if value is not None else {}
        # Default: last value wins
        self.combine_values = combine_values or (lambda x, y: y)
    
    def combine(self, other: 'MapMonoid[K, V]') -> 'MapMonoid[K, V]':
        """Combine two MapMonoids by merging keys and combining values."""
        result = dict(self.value)
        for key, value in other.value.items():
            if key in result:
                result[key] = self.combine_values(result[key], value)
            else:
                result[key] = value
        return MapMonoid(result, self.combine_values)
    
    def __add__(self, other: 'MapMonoid[K, V]') -> 'MapMonoid[K, V]':
        """Enable + operator for MapMonoid."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support for sum() builtin."""
        if other == 0:
            return self
        return self.combine(other)
    
    @classmethod
    def zero(cls) -> 'MapMonoid[K, V]':
        """Return the identity element (empty dict)."""
        return cls({})
    
    @property
    def is_zero(self) -> bool:
        """Check if this is the identity element."""
        return len(self.value) == 0
    
    def __len__(self) -> int:
        """Return the number of keys."""
        return len(self.value)
    
    def __getitem__(self, key: K) -> V:
        """Enable key access."""
        return self.value[key]
    
    def __contains__(self, key: K) -> bool:
        """Check if a key exists."""
        return key in self.value
    
    def get(self, key: K, default: V = None) -> V:
        """Get a value with a default."""
        return self.value.get(key, default)


class StringMonoid(CollectionMonoid[str]):
    """
    String concatenation monoid.
    
    Identity element: empty string
    Operation: string concatenation
    
    Examples:
        >>> StringMonoid("Hello") + StringMonoid(" World")
        StringMonoid('Hello World')
        >>> sum([StringMonoid("a"), StringMonoid("b"), StringMonoid("c")])
        StringMonoid('abc')
    """
    
    def __init__(self, value: str = ""):
        self.value = value
    
    def combine(self, other: 'StringMonoid') -> 'StringMonoid':
        """Combine two StringMonoids by concatenation."""
        return StringMonoid(self.value + other.value)
    
    def __add__(self, other: 'StringMonoid') -> 'StringMonoid':
        """Enable + operator for StringMonoid."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support for sum() builtin."""
        if other == 0:
            return self
        return self.combine(other)
    
    @classmethod
    def zero(cls) -> 'StringMonoid':
        """Return the identity element (empty string)."""
        return cls("")
    
    @property
    def is_zero(self) -> bool:
        """Check if this is the identity element."""
        return self.value == ""
    
    def __len__(self) -> int:
        """Return the length of the string."""
        return len(self.value)
    
    def __str__(self) -> str:
        """Return the string value."""
        return self.value


# Convenience functions
def set_union(*sets: PySet[T]) -> SetMonoid[T]:
    """Create a SetMonoid from multiple sets."""
    if not sets:
        return SetMonoid.zero()
    return sum(SetMonoid(s) for s in sets)


def concat_lists(*lists: PyList[T]) -> ListMonoid[T]:
    """Create a ListMonoid from multiple lists."""
    if not lists:
        return ListMonoid.zero()
    return sum(ListMonoid(lst) for lst in lists)


def merge_maps(
    *maps: Dict[K, V],
    combine_values: Callable[[V, V], V] = None
) -> MapMonoid[K, V]:
    """Create a MapMonoid from multiple dictionaries."""
    if not maps:
        return MapMonoid.zero()
    return sum(MapMonoid(m, combine_values) for m in maps)


def concat_strings(*strings: str) -> StringMonoid:
    """Create a StringMonoid from multiple strings."""
    if not strings:
        return StringMonoid.zero()
    return sum(StringMonoid(s) for s in strings)
