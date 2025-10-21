"""
Basic examples demonstrating Algesnake usage.
"""

from algesnake.operators import provides_monoid, provides_group, provides_ring
from algesnake.abstract import MonoidWrapper


# Example 1: Max Monoid with decorator
@provides_monoid
class Max:
    """Maximum value monoid."""

    def __init__(self, value):
        self.value = value

    def combine(self, other):
        return Max(max(self.value, other.value))

    @property
    def zero(self):
        return Max(float('-inf'))

    def __repr__(self):
        return f"Max({self.value})"


# Example 2: Integer addition group
@provides_group
class IntAdd:
    """Integer addition group."""

    def __init__(self, value):
        self.value = value

    def combine(self, other):
        return IntAdd(self.value + other.value)

    @property
    def zero(self):
        return IntAdd(0)

    def inverse(self):
        return IntAdd(-self.value)

    def __repr__(self):
        return f"IntAdd({self.value})"


# Example 3: Integer ring
@provides_ring
class IntRing:
    """Integer ring."""

    def __init__(self, value):
        self.value = value

    def plus(self, other):
        return IntRing(self.value + other.value)

    def times(self, other):
        return IntRing(self.value * other.value)

    @property
    def zero(self):
        return IntRing(0)

    @property
    def one(self):
        return IntRing(1)

    def negate(self):
        return IntRing(-self.value)

    def __repr__(self):
        return f"IntRing({self.value})"


def main():
    print("=" * 60)
    print("Algesnake 🐍 - Basic Examples")
    print("=" * 60)
    print()

    # Example 1: Max Monoid
    print("Example 1: Max Monoid")
    print("-" * 40)
    print("Finding maximum using monoid operations:")

    result = Max(5) + Max(3) + Max(1)
    print(f"Max(5) + Max(3) + Max(1) = {result}")

    values = [Max(1), Max(5), Max(3), Max(2), Max(4)]
    result = sum(values)
    print(f"sum([Max(1), Max(5), Max(3), Max(2), Max(4)]) = {result}")
    print()

    # Example 2: Integer Addition Group
    print("Example 2: Integer Addition Group")
    print("-" * 40)
    print("Group operations (addition and subtraction):")

    result = IntAdd(10) + IntAdd(5)
    print(f"IntAdd(10) + IntAdd(5) = {result}")

    result = IntAdd(10) - IntAdd(3)
    print(f"IntAdd(10) - IntAdd(3) = {result}")

    result = -IntAdd(5)
    print(f"-IntAdd(5) = {result}")

    result = IntAdd(10) + IntAdd(5) - IntAdd(3)
    print(f"IntAdd(10) + IntAdd(5) - IntAdd(3) = {result}")
    print()

    # Example 3: Ring Operations
    print("Example 3: Ring Operations")
    print("-" * 40)
    print("Ring distributivity: a × (b + c) = (a × b) + (a × c)")

    a = IntRing(2)
    b = IntRing(3)
    c = IntRing(4)

    left = a * (b + c)
    right = (a * b) + (a * c)
    print(f"{a} × ({b} + {c}) = {left}")
    print(f"({a} × {b}) + ({a} × {c}) = {right}")
    print(f"Distributivity holds: {left.value == right.value}")
    print()

    # Example 4: Complex Ring Expression
    print("Example 4: Complex Ring Expression")
    print("-" * 40)
    result = (IntRing(2) + IntRing(3)) * IntRing(4)
    print(f"(IntRing(2) + IntRing(3)) × IntRing(4) = {result}")

    result = IntRing(2) * IntRing(3) + IntRing(4) * IntRing(5)
    print(f"IntRing(2) × IntRing(3) + IntRing(4) × IntRing(5) = {result}")
    print()

    # Example 5: MonoidWrapper for quick prototyping
    print("Example 5: MonoidWrapper for Quick Prototyping")
    print("-" * 40)

    # Create a multiplication monoid using wrapper
    multiply_monoid = MonoidWrapper(
        combine_fn=lambda a, b: a * b,
        zero_value=1
    )

    result = multiply_monoid.combine_all([2, 3, 4, 5])
    print(f"Product of [2, 3, 4, 5] = {result}")

    # Create a min monoid using wrapper
    min_monoid = MonoidWrapper(
        combine_fn=lambda a, b: min(a, b),
        zero_value=float('inf')
    )

    result = min_monoid.combine_all([5, 2, 8, 1, 9])
    print(f"Minimum of [5, 2, 8, 1, 9] = {result}")
    print()

    # Example 6: Algebraic Law Verification
    print("Example 6: Algebraic Law Verification")
    print("-" * 40)

    from algesnake.abstract import Monoid

    class AddMonoid(Monoid[int]):
        @property
        def zero(self) -> int:
            return 0

        def combine(self, a: int, b: int) -> int:
            return a + b

    m = AddMonoid()
    print(f"Verify monoid laws for (1, 2, 3): {m.verify_monoid_laws(1, 2, 3)}")
    print(f"Verify associativity for (5, 10, 15): {m.verify_associativity(5, 10, 15)}")
    print(f"Verify identity for 42: {m.verify_identity(42)}")
    print()

    print("=" * 60)
    print("All examples completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
