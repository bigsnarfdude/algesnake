"""
Abstract base classes for algebraic structures.

This module provides the foundational abstract base classes for:
- Semigroup: Associative binary operation
- Monoid: Semigroup with identity element
- Group: Monoid with inverse operation
- Ring: Two operations (addition and multiplication) with distribution
- Semiring: Like Ring but without additive inverses
"""

from algesnake.abstract.semigroup import Semigroup, SemigroupWrapper
from algesnake.abstract.monoid import Monoid, MonoidWrapper
from algesnake.abstract.group import Group, GroupWrapper
from algesnake.abstract.ring import Ring, RingWrapper
from algesnake.abstract.semiring import Semiring, SemiringWrapper

__all__ = [
    # Abstract base classes
    "Semigroup",
    "Monoid",
    "Group",
    "Ring",
    "Semiring",
    # Wrapper classes
    "SemigroupWrapper",
    "MonoidWrapper",
    "GroupWrapper",
    "RingWrapper",
    "SemiringWrapper",
]
