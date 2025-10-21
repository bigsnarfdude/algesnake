"""
Algesnake: Abstract Algebra for Python

Algebra that slithers through your data pipelines! 🐍

A Python library providing abstract algebra abstractions (Monoids, Groups, Rings, Semirings)
for building aggregation systems, analytics pipelines, and approximation algorithms.

Example:
    >>> from algesnake import Max, Min
    >>> result = Max(5) + Max(3) + Max(1)
    >>> print(result)  # Max(5)
"""

__version__ = "0.1.0"

# Import abstract base classes
from algesnake.abstract.semigroup import Semigroup
from algesnake.abstract.monoid import Monoid
from algesnake.abstract.group import Group
from algesnake.abstract.ring import Ring
from algesnake.abstract.semiring import Semiring

__all__ = [
    "Semigroup",
    "Monoid",
    "Group",
    "Ring",
    "Semiring",
    "__version__",
]
