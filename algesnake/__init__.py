"""
Algesnake - Abstract algebra for Python data pipelines.

Algesnake provides algebraic abstractions (Monoids, Groups, Rings, Semirings)
for building aggregation systems, analytics pipelines, and approximation algorithms.
"""

# Phase 1: Abstract base classes
try:
    from .abstract import Semigroup, Monoid, Group, Ring, Semiring
except ImportError:
    # Abstract classes not yet implemented
    pass

# Phase 2: Concrete monoid implementations
from .monoid import (
    # Numeric monoids
    Add, Multiply, Max, Min,
    # Collection monoids
    SetMonoid, ListMonoid, MapMonoid, StringMonoid,
    # Option monoid
    Some, None_, Option, OptionMonoid,
)

# Phase 3: Approximation algorithms
from .approximate import (
    HyperLogLog,
    BloomFilter,
)

__version__ = "0.3.0"

__all__ = [
    # Numeric monoids
    'Add', 'Multiply', 'Max', 'Min',
    # Collection monoids
    'SetMonoid', 'ListMonoid', 'MapMonoid', 'StringMonoid',
    # Option monoid
    'Some', 'None_', 'Option', 'OptionMonoid',
    # Approximation algorithms
    'HyperLogLog', 'BloomFilter',
]

# Add abstract classes if available
try:
    __all__.extend(['Semigroup', 'Monoid', 'Group', 'Ring', 'Semiring'])
except NameError:
    pass
