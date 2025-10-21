"""Concrete monoid implementations.

This module provides production-ready monoid implementations for:
- Numeric operations (Add, Multiply, Max, Min)
- Collection operations (Set, List, Map, String)
- Optional values (Some, None_, Option)

All implementations follow monoid laws:
- Associativity: (a • b) • c = a • (b • c)
- Identity: zero • a = a • zero = a
"""

from .numeric import Add, Multiply, Max, Min
from .collection import SetMonoid, ListMonoid, MapMonoid, StringMonoid
from .option import Some, None_, Option, OptionMonoid

__all__ = [
    # Numeric monoids
    'Add', 'Multiply', 'Max', 'Min',
    # Collection monoids
    'SetMonoid', 'ListMonoid', 'MapMonoid', 'StringMonoid',
    # Option monoid
    'Some', 'None_', 'Option', 'OptionMonoid',
]
