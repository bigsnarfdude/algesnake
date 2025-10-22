# Concrete Monoid Implementations

Algesnake provides concrete monoid types for common operations in data processing and aggregation.

## Overview

Production-ready monoid implementations organized into three categories:

1. **Numeric Monoids** - Mathematical operations (Add, Multiply, Max, Min)
2. **Collection Monoids** - Data structure operations (Set, List, Map, String)
3. **Option Monoid** - Safe handling of optional/nullable values

All implementations follow monoid laws (associativity + identity) and provide natural Python operator overloading.

## Numeric Monoids

### Add
```python
from algesnake import Add

# Basic usage
result = Add(5) + Add(3) + Add(7)  # Add(15)

# Works with sum()
total = sum([Add(10), Add(20), Add(30)])  # Add(60)

# Identity element
Add.zero()  # Add(0)
```

### Multiply
```python
from algesnake import Multiply

# Basic usage
result = Multiply(2) * Multiply(3) * Multiply(4)  # Multiply(24)

# Identity element
Multiply.zero()  # Multiply(1)
```

### Max
```python
from algesnake import Max

# Find maximum
values = [Max(5), Max(2), Max(9), Max(1)]
maximum = sum(values)  # Max(9)

# Identity element (negative infinity)
Max.zero()  # Max(-inf)
```

### Min
```python
from algesnake import Min

# Find minimum
values = [Min(5), Min(2), Min(9), Min(1)]
minimum = sum(values)  # Min(1)

# Identity element (positive infinity)
Min.zero()  # Min(inf)
```

## Collection Monoids

### SetMonoid
```python
from algesnake import SetMonoid

# Set union
s1 = SetMonoid({1, 2, 3})
s2 = SetMonoid({3, 4, 5})
union = s1 + s2  # SetMonoid({1, 2, 3, 4, 5})

# Aggregate multiple sets
sets = [SetMonoid({1}), SetMonoid({2}), SetMonoid({3})]
all_values = sum(sets)  # SetMonoid({1, 2, 3})
```

### ListMonoid
```python
from algesnake import ListMonoid

# List concatenation
l1 = ListMonoid([1, 2, 3])
l2 = ListMonoid([4, 5, 6])
concatenated = l1 + l2  # ListMonoid([1, 2, 3, 4, 5, 6])

# Preserves order and duplicates
l3 = ListMonoid([1, 2, 2])
l4 = ListMonoid([2, 3])
result = l3 + l4  # ListMonoid([1, 2, 2, 2, 3])
```

### MapMonoid
```python
from algesnake import MapMonoid

# Default: last value wins
m1 = MapMonoid({'a': 1, 'b': 2})
m2 = MapMonoid({'b': 3, 'c': 4})
merged = m1 + m2  # MapMonoid({'a': 1, 'b': 3, 'c': 4})

# Custom value combination: sum values
m1 = MapMonoid({'a': 1, 'b': 2}, combine_values=lambda x, y: x + y)
m2 = MapMonoid({'b': 3, 'c': 4}, combine_values=lambda x, y: x + y)
merged = m1 + m2  # MapMonoid({'a': 1, 'b': 5, 'c': 4})

# Use with max
m1 = MapMonoid({'a': 5, 'b': 2}, combine_values=max)
m2 = MapMonoid({'b': 7, 'c': 3}, combine_values=max)
result = m1 + m2  # MapMonoid({'a': 5, 'b': 7, 'c': 3})
```

### StringMonoid
```python
from algesnake import StringMonoid

# String concatenation
words = [StringMonoid("Hello"), StringMonoid(" "), StringMonoid("World")]
sentence = sum(words)  # StringMonoid("Hello World")
```

## Option Monoid

The Option/Maybe monoid provides safe handling of optional values, similar to Scala's `Option` or Haskell's `Maybe`.

### Basic Usage
```python
from algesnake import Some, None_

# Create options
x = Some(5)      # Contains a value
y = None_()      # Empty option

# Check state
x.is_some        # True
x.is_none        # False
y.is_none        # True

# Access value
x.value          # 5
x.get_or_else(0) # 5
y.get_or_else(0) # 0
```

### Combination (First-Wins)
```python
# First Some wins
Some(5) + Some(3)     # Some(5)
Some(5) + None_()     # Some(5)
None_() + Some(3)     # Some(3)
None_() + None_()     # None_()

# Works with sum()
options = [None_(), Some(5), None_(), Some(3)]
sum(options)          # Some(5)
```

### Transformations
```python
# Map: transform the value if present
Some(5).map(lambda x: x * 2)              # Some(10)
None_().map(lambda x: x * 2)              # None_()

# FlatMap: chain operations that return Options
def safe_divide(x, y):
    return Some(x / y) if y != 0 else None_()

Some(10).flat_map(lambda x: safe_divide(x, 2))  # Some(5.0)
Some(10).flat_map(lambda x: safe_divide(x, 0))  # None_()

# Filter: keep value only if predicate is true
Some(5).filter(lambda x: x > 3)   # Some(5)
Some(5).filter(lambda x: x > 10)  # None_()
```

### Custom Value Combination
```python
from algesnake import OptionMonoid

# Combine values when both are Some
m = OptionMonoid(lambda a, b: a + b)
m.combine(Some(5), Some(3))     # Some(8)
m.combine(Some(5), None_())     # Some(5)

# Aggregate with custom combination
options = [Some(10), None_(), Some(20), Some(30)]
m.combine_all(options)          # Some(60)
```

## Real-World Examples

### Distributed Aggregation
```python
# Aggregate metrics across multiple servers/partitions
partition_1 = {
    'total_views': Add(1000),
    'unique_users': SetMonoid({'user1', 'user2', 'user3'}),
    'max_latency': Max(150.5),
}

partition_2 = {
    'total_views': Add(1500),
    'unique_users': SetMonoid({'user2', 'user4'}),
    'max_latency': Max(200.3),
}

# Combine (associative - order doesn't matter!)
aggregated = {
    'total_views': partition_1['total_views'] + partition_2['total_views'],
    'unique_users': partition_1['unique_users'] + partition_2['unique_users'],
    'max_latency': partition_1['max_latency'] + partition_2['max_latency'],
}
# Results: total_views=2500, unique_users=4, max_latency=200.3
```

### Word Count (MapReduce)
```python
from algesnake import MapMonoid

def count_words(text):
    words = text.split()
    counts = {word: 1 for word in words}
    return MapMonoid(counts, combine_values=lambda x, y: x + y)

# Process documents in parallel
doc1_counts = count_words("hello world hello")
doc2_counts = count_words("world of monoids")
doc3_counts = count_words("hello monoids")

# Combine all counts
total = doc1_counts + doc2_counts + doc3_counts
# Result: {'hello': 3, 'world': 2, 'monoids': 2, 'of': 1}
```

### Configuration with Fallback
```python
from algesnake import Some, None_

def get_env_config(key):
    # Check environment variables
    return Some(value) if key in env else None_()

def get_user_config(key):
    # Check user config file
    return Some(value) if key in user_config else None_()

def get_default_config(key):
    # Default values
    defaults = {'timeout': 60, 'retries': 3}
    return Some(defaults[key]) if key in defaults else None_()

# Try each source in order (first wins)
config_value = (get_env_config('timeout') + 
                get_user_config('timeout') + 
                get_default_config('timeout'))
```

## Monoid Properties

All implementations satisfy the monoid laws:

### Associativity
```python
# (a • b) • c = a • (b • c)
(a + b) + c == a + (b + c)
```

### Identity
```python
# zero • a = a • zero = a
MonoidType.zero() + a == a
a + MonoidType.zero() == a
```

## Testing

Comprehensive test suites verify:
- Monoid laws (associativity, identity)
- Operator overloading
- Edge cases
- Integration with Python builtins (`sum()`)

Run tests:
```bash
pytest test_numeric_monoids.py
pytest test_collection_monoids.py
pytest test_option_monoid.py
```

## Performance Considerations

- **Numeric monoids**: O(1) operations
- **SetMonoid**: O(n+m) for union of sets with n and m elements
- **ListMonoid**: O(n+m) for concatenation
- **MapMonoid**: O(n+m) with O(1) per key combination
- **StringMonoid**: O(n+m) for concatenation

## Next Steps (Phase 3)

The Phase 2 monoids provide the foundation for Phase 3 approximation algorithms:
- HyperLogLog (cardinality estimation) - built on numeric monoids
- CountMinSketch (frequency estimation) - built on map-like structures
- Bloom Filter (membership testing) - built on set-like operations

## API Reference

See individual module files for complete API documentation:
- `numeric_monoids.py` - Numeric operations
- `collection_monoids.py` - Collection operations
- `option_monoid.py` - Optional value handling

## Examples

Run the examples file to see all monoids in action:
```bash
python examples.py
```

## License

Apache License 2.0 - Same as the main Algesnake project
