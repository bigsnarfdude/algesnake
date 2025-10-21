# Algesnake 🐍 - Abstract Algebra for Python

> **Algebra that slithers through your data pipelines!**

A Python port of [Twitter's Algebird](https://github.com/twitter/algebird) library, providing abstract algebra abstractions (Monoids, Groups, Rings, Semirings) for building aggregation systems, analytics pipelines, and approximation algorithms.

## Overview

Algesnake makes it easy to build distributed aggregation systems by providing algebraic abstractions that are:
- **Composable**: Combine simple operations to build complex aggregations
- **Associative**: Safe for parallel and distributed computing
- **Type-safe**: Leverages Python's type system for correctness
- **Pythonic**: Natural operator overloading (use `+`, `*`, etc.)

## Features

### Phase 1 (Current) - Foundation

- ✅ **Abstract Base Classes**: Semigroup, Monoid, Group, Ring, Semiring
- ✅ **Operator Overloading**: Natural Python syntax with decorators and mixins
- ✅ **Law Verification**: Built-in helpers to verify algebraic properties
- ✅ **Comprehensive Tests**: Property-based testing with pytest

### Planned Features

- 🚧 **Approximation Algorithms**: HyperLogLog, CountMinSketch, Bloom Filter
- 🚧 **Concrete Implementations**: Numeric monoids, collection monoids, aggregators
- 🚧 **Distributed Integration**: PySpark and Dask support
- 🚧 **Performance**: Cython optimizations for hot paths

## Installation

```bash
# Clone the repository
git clone https://github.com/bigsnarfdude/algesnake.git
cd algesnake

# Install in development mode
pip install -e .

# Install with development dependencies
pip install -e ".[dev]"
```

## Quick Start

### Using the Decorator Approach

```python
from algesnake.operators import provides_monoid

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

# Use natural Python operators
result = Max(5) + Max(3) + Max(1)
print(result.value)  # 5

# Works with sum()
values = [Max(1), Max(5), Max(3), Max(2)]
print(sum(values).value)  # 5
```

### Using Abstract Base Classes

```python
from algesnake import Monoid

class AddMonoid(Monoid[int]):
    """Addition monoid for integers."""

    @property
    def zero(self) -> int:
        return 0

    def combine(self, a: int, b: int) -> int:
        return a + b

# Create instance and use
m = AddMonoid()
result = m.combine_all([1, 2, 3, 4, 5])
print(result)  # 15

# Verify algebraic laws
assert m.verify_monoid_laws(1, 2, 3)
```

### Using Wrapper Classes

```python
from algesnake.abstract import MonoidWrapper

# Create monoid from functions
max_monoid = MonoidWrapper(
    combine_fn=lambda a, b: max(a, b),
    zero_value=float('-inf')
)

result = max_monoid.combine_all([1, 5, 3, 2])
print(result)  # 5
```

## Core Concepts

### Semigroup

A **Semigroup** is a set with an associative binary operation.

```python
from algesnake import Semigroup

class MaxSemigroup(Semigroup[int]):
    def combine(self, a: int, b: int) -> int:
        return max(a, b)

sg = MaxSemigroup()
result = sg.combine_all([1, 5, 3, 2])  # 5
```

**Law**: Associativity - `(a • b) • c = a • (b • c)`

### Monoid

A **Monoid** is a Semigroup with an identity element (zero).

```python
from algesnake import Monoid

class AddMonoid(Monoid[int]):
    @property
    def zero(self) -> int:
        return 0

    def combine(self, a: int, b: int) -> int:
        return a + b

m = AddMonoid()
m.combine_all([])  # 0 (returns zero for empty list)
```

**Laws**:
- Associativity: `(a • b) • c = a • (b • c)`
- Identity: `zero • a = a • zero = a`

### Group

A **Group** is a Monoid with an inverse operation for every element.

```python
from algesnake import Group

class IntAddGroup(Group[int]):
    @property
    def zero(self) -> int:
        return 0

    def combine(self, a: int, b: int) -> int:
        return a + b

    def inverse(self, a: int) -> int:
        return -a

g = IntAddGroup()
g.subtract(5, 3)  # 2
```

**Laws**:
- Associativity, Identity (from Monoid)
- Inverse: `a • inverse(a) = inverse(a) • a = zero`

### Ring

A **Ring** has two operations: addition (forms a group) and multiplication (forms a monoid).

```python
from algesnake import Ring

class IntRing(Ring[int]):
    @property
    def zero(self) -> int:
        return 0

    @property
    def one(self) -> int:
        return 1

    def plus(self, a: int, b: int) -> int:
        return a + b

    def times(self, a: int, b: int) -> int:
        return a * b

    def negate(self, a: int) -> int:
        return -a

r = IntRing()
r.verify_distributivity(2, 3, 4)  # True
```

**Laws**:
- (R, +) is an abelian group
- (R, ×) is a monoid
- Distributivity: `a × (b + c) = (a × b) + (a × c)`

### Semiring

A **Semiring** is like a Ring but without requiring additive inverses (no subtraction).

```python
from algesnake import Semiring

class NaturalSemiring(Semiring[int]):
    @property
    def zero(self) -> int:
        return 0

    @property
    def one(self) -> int:
        return 1

    def plus(self, a: int, b: int) -> int:
        return a + b

    def times(self, a: int, b: int) -> int:
        return a * b
```

**Examples**: Natural numbers, Boolean algebra, Tropical semiring

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=algesnake --cov-report=html

# Run specific test file
pytest tests/unit/test_monoid.py

# Run with verbose output
pytest -v
```

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run type checking
mypy algesnake

# Format code
black algesnake tests

# Lint code
ruff algesnake tests
```

## Project Structure

```
algesnake/
├── algesnake/
│   ├── __init__.py
│   ├── abstract/           # Abstract base classes
│   │   ├── semigroup.py
│   │   ├── monoid.py
│   │   ├── group.py
│   │   ├── ring.py
│   │   └── semiring.py
│   ├── monoid/             # Concrete monoid implementations
│   ├── approximate/        # Approximation algorithms (HLL, CMS, etc.)
│   ├── operators.py        # Operator overloading support
│   └── utils.py
├── tests/
│   ├── unit/
│   ├── integration/
│   └── performance/
├── docs/
├── examples/
└── README.md
```

## Roadmap

### Phase 1: Foundation ✅ (Complete)
- Abstract base classes
- Operator overloading system
- Comprehensive tests
- Documentation

### Phase 2: Basic Implementations 🚧 (Next)
- Numeric monoids (Add, Multiply, Max, Min)
- Collection monoids (Set, Map, List)
- Option/Maybe monoid
- Basic aggregators

### Phase 3: Approximation Algorithms 🚧
- HyperLogLog (cardinality estimation)
- CountMinSketch (frequency estimation)
- Bloom Filter (membership testing)
- TopK, Quantiles

### Phase 4: Advanced Features
- Distributed computing integration (PySpark, Dask)
- Performance optimizations (Cython)
- Additional algebraic structures

## Why Algesnake?

**Problem**: Building aggregation systems is hard. You need to handle:
- Parallelism and distribution
- Partial failures and retries
- Memory constraints
- Approximate algorithms

**Solution**: Abstract algebra provides mathematical guarantees:
- **Associativity** → Safe to split work across machines
- **Identity** → Safe to handle empty data
- **Commutativity** → Order doesn't matter (where applicable)

**Example Use Cases**:
- **Analytics**: Count unique users across billions of events (HyperLogLog)
- **Monitoring**: Track top-K errors without storing everything
- **Machine Learning**: Distributed statistics computation
- **Data Pipelines**: Composable aggregations in MapReduce/Spark

## Comparison with Scala Algebird

| Feature | Python Algesnake | Scala Algebird |
|---------|----------------|----------------|
| Core abstractions | ✅ Semigroup, Monoid, Group, Ring | ✅ Same |
| Operator overloading | ✅ Decorators + mixins | ✅ Implicit classes |
| HyperLogLog | 🚧 Planned | ✅ |
| CountMinSketch | 🚧 Planned | ✅ |
| Bloom Filter | 🚧 Planned | ✅ |
| Spark integration | 🚧 Planned | ✅ |
| Type safety | ✅ Type hints + mypy | ✅ Scala type system |

## Contributing

Contributions are welcome! Please see our contributing guidelines.

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

Apache License 2.0 - See LICENSE file for details

## Acknowledgments

- Inspired by [Twitter's Algebird](https://github.com/twitter/algebird)
- Built with love for the Python data engineering community

## Resources

- [Original Scala Algebird](https://github.com/twitter/algebird)
- [Abstract Algebra Basics](https://en.wikipedia.org/wiki/Abstract_algebra)
- [Monoids in Category Theory](https://en.wikipedia.org/wiki/Monoid)
- [Approximation Algorithms](https://en.wikipedia.org/wiki/Approximation_algorithm)

---

**Status**: Phase 1 Complete | **Version**: 0.1.0 | **Python**: 3.8+
