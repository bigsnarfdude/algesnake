# Phase 2 Monoids - Quick Start Guide

## Installation (After Integration)

```bash
cd algesnake
pip install -e .
```

## 30-Second Tour

```python
from algesnake import Add, Max, SetMonoid, MapMonoid, Some, None_

# Numeric: Sum values
total = sum([Add(10), Add(20), Add(30)])
print(total.value)  # 60

# Numeric: Find maximum  
maximum = sum([Max(5), Max(2), Max(9), Max(1)])
print(maximum.value)  # 9

# Collection: Union sets
users = sum([
    SetMonoid({'alice', 'bob'}),
    SetMonoid({'bob', 'charlie'}),
    SetMonoid({'charlie', 'david'})
])
print(users.value)  # {'alice', 'bob', 'charlie', 'david'}

# Collection: Merge maps with value combination
word_counts = (
    MapMonoid({'hello': 2, 'world': 1}, lambda x, y: x + y) +
    MapMonoid({'world': 1, 'foo': 3}, lambda x, y: x + y)
)
print(word_counts.value)  # {'hello': 2, 'world': 2, 'foo': 3}

# Option: Safe value handling with fallback
config = Some('production') + None_() + Some('default')
print(config.value)  # 'production'
```

## Common Patterns

### Pattern 1: Distributed Aggregation

```python
from algesnake import Add, SetMonoid, Max

# Aggregate metrics from multiple servers
def aggregate_server_metrics(partitions):
    return {
        'total_requests': sum([p['requests'] for p in partitions]),
        'unique_users': sum([p['users'] for p in partitions]),
        'max_latency': sum([p['latency'] for p in partitions]),
    }

partition_1 = {
    'requests': Add(1000),
    'users': SetMonoid({'user1', 'user2'}),
    'latency': Max(120.5),
}

partition_2 = {
    'requests': Add(1500),
    'users': SetMonoid({'user2', 'user3'}),
    'latency': Max(95.3),
}

metrics = aggregate_server_metrics([partition_1, partition_2])
# Results: requests=2500, unique_users=3, max_latency=120.5
```

### Pattern 2: MapReduce Word Count

```python
from algesnake import MapMonoid

def word_count(documents):
    """Count word frequencies across multiple documents."""
    counts = [
        MapMonoid(
            {word: 1 for word in doc.split()},
            combine_values=lambda x, y: x + y
        )
        for doc in documents
    ]
    return sum(counts).value

docs = [
    "hello world hello",
    "world of monoids",
    "hello monoids"
]

frequencies = word_count(docs)
# {'hello': 3, 'world': 2, 'monoids': 2, 'of': 1}
```

### Pattern 3: Configuration with Fallback

```python
from algesnake import Some, None_

def get_config(key, env={}, user={}, defaults={}):
    """Get config with environment > user > default precedence."""
    env_val = Some(env[key]) if key in env else None_()
    user_val = Some(user[key]) if key in user else None_()
    default_val = Some(defaults[key]) if key in defaults else None_()
    
    return env_val + user_val + default_val

env = {'API_KEY': 'secret-123'}
user = {'TIMEOUT': 30}
defaults = {'API_KEY': 'default', 'TIMEOUT': 60, 'DEBUG': False}

api_key = get_config('API_KEY', env, user, defaults)
# Some('secret-123') - from environment

timeout = get_config('TIMEOUT', env, user, defaults)  
# Some(30) - from user config

debug = get_config('DEBUG', env, user, defaults)
# Some(False) - from defaults
```

### Pattern 4: Streaming Statistics

```python
from algesnake import Add, Max, Min

def compute_stats(stream):
    """Compute statistics in a single pass."""
    stats = {
        'count': Add(0),
        'sum': Add(0),
        'max': Max.zero(),
        'min': Min.zero(),
    }
    
    for value in stream:
        stats['count'] = stats['count'] + Add(1)
        stats['sum'] = stats['sum'] + Add(value)
        stats['max'] = stats['max'] + Max(value)
        stats['min'] = stats['min'] + Min(value)
    
    mean = stats['sum'].value / stats['count'].value
    return {
        'count': stats['count'].value,
        'mean': mean,
        'max': stats['max'].value,
        'min': stats['min'].value,
    }

data = [10, 25, 15, 30, 20, 35, 5, 40]
stats = compute_stats(data)
# {'count': 8, 'mean': 22.5, 'max': 40, 'min': 5}
```

### Pattern 5: Safe Operations with Option

```python
from algesnake import Some, None_

def safe_divide(a, b):
    """Division that returns Option instead of raising exception."""
    return Some(a / b) if b != 0 else None_()

def safe_sqrt(x):
    """Square root that returns Option for negative numbers."""
    return Some(x ** 0.5) if x >= 0 else None_()

# Chain operations safely
result = (
    Some(16)
    .flat_map(lambda x: safe_sqrt(x))      # Some(4.0)
    .flat_map(lambda x: safe_divide(x, 2)) # Some(2.0)
    .map(lambda x: x * 10)                  # Some(20.0)
)
print(result.value)  # 20.0

# Handles errors gracefully
result = (
    Some(-16)
    .flat_map(lambda x: safe_sqrt(x))      # None_() - negative number
    .flat_map(lambda x: safe_divide(x, 2)) # Still None_()
    .map(lambda x: x * 10)                  # Still None_()
)
print(result.is_none)  # True
```

## Why Monoids?

### Problem: Traditional Aggregation

```python
# Hard to parallelize, order-dependent
total = 0
users = set()
max_val = float('-inf')

for partition in partitions:
    total += partition['count']        # What if partitions processed in parallel?
    users.update(partition['users'])   # What if we need to retry?
    max_val = max(max_val, partition['max'])  # What about empty partitions?
```

### Solution: Monoid Aggregation

```python
# Easy to parallelize, order-independent, handles empty data
from algesnake import Add, SetMonoid, Max

metrics = sum([
    {
        'count': Add(partition['count']),
        'users': SetMonoid(partition['users']),
        'max': Max(partition['max']),
    }
    for partition in partitions
])

# Properties guaranteed by monoid laws:
# ✓ Associative: Can split work across machines
# ✓ Identity: Handles empty partitions correctly
# ✓ Composable: Easy to add more aggregations
```

## Key Benefits

1. **Parallel-Safe**: Associativity guarantees correct results regardless of processing order
2. **Composable**: Combine simple monoids to build complex aggregations
3. **Type-Safe**: Full type hints catch errors at development time
4. **Pythonic**: Natural `+` and `sum()` operators
5. **Production-Ready**: Handles edge cases, empty data, infinity values

## What's Next?

- See `PHASE2_README.md` for complete documentation
- Run `python examples.py` for more examples
- Check test files for comprehensive usage patterns
- Ready for Phase 3: HyperLogLog, CountMinSketch, Bloom Filter!

## Questions?

Each monoid type has detailed docstrings:
```python
help(Add)
help(SetMonoid)
help(Some)
```
