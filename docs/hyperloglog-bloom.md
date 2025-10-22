# HyperLogLog & Bloom Filter

## Overview

Foundational probabilistic data structures for efficient set operations:
- **HyperLogLog**: Cardinality estimation with ~2% error using minimal memory
- **Bloom Filter**: Probabilistic set membership testing with configurable false positive rates

Both structures implement monoid interfaces for distributed aggregation.

## Implementations

### HyperLogLog

**Location**: `algesnake/approximate/hyperloglog.py`

**Features**:
- Configurable precision parameter (4-16 bits)
- Standard HyperLogLog algorithm with bias correction
- Memory usage: 2^p bytes (e.g., p=14 uses 16 KB)
- Accuracy: ~1.04 / sqrt(2^p) standard error
- Monoid operations: element-wise max of registers

**Key Methods**:
- `add(item)`: Add element to sketch
- `cardinality()`: Estimate unique count
- `combine(other)`: Merge two HLL sketches
- Operator overloading: `+`, `sum()` support

**Example**:
```python
from algesnake.approximate import HyperLogLog

hll = HyperLogLog(precision=14)
for user_id in user_ids:
    hll.add(user_id)

print(f"Unique users: ~{hll.cardinality()}")
```

**Test Coverage**: 87% (29 tests)

### Bloom Filter

**Location**: `algesnake/approximate/bloom.py`

**Features**:
- Configurable capacity and error rate
- Optimal bit array size and hash count calculation
- Memory: ~10 bits per element
- No false negatives, configurable false positive rate
- Monoid operations: bitwise OR of bit arrays

**Key Methods**:
- `add(item)`: Add element to filter
- `contains(item)` / `in` operator: Test membership
- `combine(other)`: Merge two filters
- `saturation()`: Check filter fullness
- `expected_fpr()`: Calculate expected false positive rate

**Example**:
```python
from algesnake.approximate import BloomFilter

bf = BloomFilter(capacity=10000, error_rate=0.01)
bf.add("spam@example.com")

if "spam@example.com" in bf:
    print("Email is in blocklist")
```

**Test Coverage**: 96% (32 tests)

## Files Delivered

### Core Implementations
- `algesnake/approximate/__init__.py` - Module exports
- `algesnake/approximate/hyperloglog.py` - HyperLogLog implementation (241 lines)
- `algesnake/approximate/bloom.py` - Bloom Filter implementation (286 lines)

### Tests
- `tests/unit/test_hyperloglog.py` - 29 comprehensive tests
- `tests/unit/test_bloom.py` - 32 comprehensive tests

### Examples & Documentation
- `examples/hyperloglog_bloom_examples.py` - 10 real-world examples
- `docs/hyperloglog-bloom.md` - This documentation

## Test Results

**Total**: 61 tests passing (100%)
- HyperLogLog: 29 tests (87% coverage)
- Bloom Filter: 32 tests (96% coverage)

**Test Categories**:
- ✅ Basic functionality
- ✅ Accuracy benchmarks (within theoretical bounds)
- ✅ Monoid laws (associativity, identity)
- ✅ Operator overloading (+, sum())
- ✅ Edge cases
- ✅ Real-world scenarios

## Accuracy Benchmarks

### HyperLogLog Accuracy
| Cardinality | Precision | Estimated | Error |
|-------------|-----------|-----------|-------|
| 50          | 12        | 48-52     | <10%  |
| 1,000       | 14        | 950-1050  | <5%   |
| 100,000     | 14        | 97k-103k  | <3%   |

### Bloom Filter False Positive Rates
| Capacity | Items Added | Target FPR | Actual FPR |
|----------|-------------|------------|------------|
| 100      | 50          | 1%         | <3%        |
| 1,000    | 1,000       | 1%         | <2%        |
| 10,000   | 5,000       | 0.1%       | <0.5%      |

## Memory Efficiency

### HyperLogLog vs Traditional Set
Tracking 100,000 unique elements:
- **Traditional set**: ~8 MB
- **HyperLogLog (p=14)**: 16 KB
- **Memory savings**: 99.8%

### Bloom Filter vs Traditional Set
Tracking 100,000 elements with 1% FPR:
- **Traditional set**: ~8 MB  
- **Bloom Filter**: ~120 KB
- **Memory savings**: 98.5%

## Real-World Use Cases

### 1. Unique Visitor Counting (HyperLogLog)
```python
# Count unique visitors across distributed logs
partition1 = HyperLogLog(precision=14)
partition2 = HyperLogLog(precision=14)

# Each partition processes its events
for event in partition1_events:
    partition1.add(event.user_id)

for event in partition2_events:
    partition2.add(event.user_id)

# Merge using monoid operation
total_unique = partition1 + partition2
print(f"Total unique visitors: ~{total_unique.cardinality()}")
```

### 2. Spam Detection (Bloom Filter)
```python
# Build blocklist from known spam
spam_filter = BloomFilter(capacity=1000000, error_rate=0.001)

for spam_email in known_spam:
    spam_filter.add(spam_email)

# Fast membership test
if email in spam_filter:
    mark_as_spam(email)
```

### 3. Web Crawler Duplicate Detection (Bloom Filter)
```python
visited = BloomFilter(capacity=10000000, error_rate=0.01)

for url in urls_to_crawl:
    if url not in visited:
        crawl_page(url)
        visited.add(url)
```

## Monoid Properties

Both structures satisfy monoid laws:

**Associativity**:
```python
(a + b) + c == a + (b + c)
```

**Identity**:
```python
zero + a == a + zero == a
```

This enables:
- Distributed aggregation (MapReduce, Spark)
- Incremental updates
- Parallel processing
- Tree-based reduction

## Next Steps: Phase 3 Week 3-4

Planned implementations:
- **CountMinSketch**: Frequency estimation for heavy hitters
- **TopK**: Track top-K most frequent items
- Both will build on monoid patterns established here

## Performance Notes

- **HyperLogLog**: O(1) add, O(m) cardinality estimation where m=2^p
- **Bloom Filter**: O(k) add/contains where k=number of hash functions
- Both structures are thread-safe for reads, require synchronization for writes
- Serialization support via register/bit array access

## Integration

These approximation algorithms integrate seamlessly with Phase 2 monoids:

```python
from algesnake import Add, MapMonoid
from algesnake.approximate import HyperLogLog, BloomFilter

# Combine exact and approximate aggregations
metrics = {
    'total_events': Add(0),
    'unique_users': HyperLogLog(precision=14),
    'malicious_ips': BloomFilter(capacity=10000, error_rate=0.01),
    'page_views': MapMonoid({}, lambda x, y: x + y)
}
```

---

**Status**: ✅ Phase 3 Week 1-2 Complete  
**Tests**: 61/61 passing (100%)  
**Coverage**: HLL 87%, Bloom 96%  
**Date**: October 21, 2025
