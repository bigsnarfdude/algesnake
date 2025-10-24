# Algesnake 🐍 - Abstract Algebra for Python

> **Algebra that slithers through your data pipelines!**

A Python library providing abstract algebra abstractions (Monoids, Groups, Rings, Semirings, T-Digest) for building aggregation systems, analytics pipelines, and approximation algorithms. Inspired by [Twitter's Algebird](https://github.com/twitter/algebird).

[![Tests](https://img.shields.io/badge/tests-406%20passing-brightgreen)]()
[![Coverage](https://img.shields.io/badge/coverage-80%25-green)]()
[![Python](https://img.shields.io/badge/python-3.8%2B-blue)]()
[![Version](https://img.shields.io/badge/version-0.5.0-blue)]()

## What's New in 0.6.0

**NEW: Similarity Search & LSH Indexing!** 🚀

- **MinHash**: Jaccard similarity estimation for set comparison
- **Weighted MinHash**: Weighted Jaccard for TF-IDF and frequency-based similarity
- **MinHash LSH**: Fast similarity search with sub-linear query time
- **LSH Forest**: Top-K nearest neighbor queries without fixed thresholds
- **LSH Ensemble**: Optimized containment queries for subset/superset search
- **HNSW**: Approximate nearest neighbor search with logarithmic complexity

Previous features (0.5.0):
- **T-Digest**: High-accuracy quantile estimation (p50, p95, p99, p999)
- **CountMinSketch**: Conservative frequency estimation
- **TopK**: Track top-K most frequent items
- **HyperLogLog**: Cardinality estimation with ~2% error
- **Bloom Filter**: Membership testing with configurable false positives

All structures implement **monoid interfaces** for distributed aggregation!

## Features

### Abstract Algebra Foundation
- Abstract base classes: Semigroup, Monoid, Group, Ring, Semiring
- Operator overloading: Natural Python syntax (`+`, `-`, `*`)
- Law verification: Built-in helpers to verify algebraic properties
- Comprehensive tests: 406 tests, 80%+ coverage

### Concrete Monoids for Aggregation
- **Numeric monoids**: Add, Multiply, Max, Min
- **Collection monoids**: SetMonoid, ListMonoid, MapMonoid, StringMonoid
- **Option monoid**: Some, None_, Option type for safe null handling
- Full operator overloading and `sum()` support

### Probabilistic Data Structures
- **HyperLogLog**: Unique count estimation (2% error, O(log log n) space)
- **Bloom Filter**: Membership testing (configurable false positive rate)
- **CountMinSketch**: Frequency estimation (never underestimates)
- **TopK**: Heavy hitter detection (O(k) space)
- **T-Digest**: Percentile estimation (0.1-1% error for p99)

### Similarity Search & Indexing
- **MinHash**: Jaccard similarity estimation (O(k) space, ~1/√k error)
- **Weighted MinHash**: Weighted Jaccard for frequency-based similarity
- **MinHash LSH**: Sub-linear similarity search (O(n^ρ) where ρ < 1)
- **LSH Forest**: Top-K queries with adaptive thresholds
- **LSH Ensemble**: Size-aware containment search (subset/superset queries)
- **HNSW**: Approximate nearest neighbor (O(log n) query time)

## Installation

```bash
# Clone the repository
git clone https://github.com/bigsnarfdude/algesnake.git
cd algesnake
```

## Quick Start

### Concrete Monoids

```python
from algesnake import Add, Max, Min, SetMonoid, MapMonoid

# Numeric aggregation
numbers = [Add(1), Add(2), Add(3), Add(4), Add(5)]
total = sum(numbers)  # Add(15)
print(total.value)  # 15

# Find maximum
values = [Max(5), Max(3), Max(8), Max(1)]
maximum = sum(values)  # Max(8)
print(maximum.value)  # 8

# Set union across partitions
partition1 = SetMonoid({1, 2, 3})
partition2 = SetMonoid({3, 4, 5})
partition3 = SetMonoid({5, 6, 7})
all_items = partition1 + partition2 + partition3
print(all_items.items)  # {1, 2, 3, 4, 5, 6, 7}

# Merge dictionaries with custom combination
word_counts1 = MapMonoid({'hello': 2, 'world': 1}, lambda a, b: a + b)
word_counts2 = MapMonoid({'hello': 1, 'python': 3}, lambda a, b: a + b)
total_counts = word_counts1 + word_counts2
print(total_counts.items)  # {'hello': 3, 'world': 1, 'python': 3}
```

### Probabilistic Data Structures

#### HyperLogLog - Count Unique Users

```python
from algesnake.approximate import HyperLogLog

# Track unique users across billions of events
hll = HyperLogLog(precision=14)

for event in event_stream:
    hll.add(event.user_id)

print(f"Unique users: {hll.cardinality():.0f}")
# Accurate to ~2% with only 16KB memory!

# Distributed aggregation across servers
server1_hll = HyperLogLog(precision=14)
server2_hll = HyperLogLog(precision=14)
server3_hll = HyperLogLog(precision=14)

# ... add data to each ...

# Merge using monoid operation
global_hll = server1_hll + server2_hll + server3_hll
print(f"Total unique users: {global_hll.cardinality():.0f}")
```

#### Bloom Filter - Spam Detection

```python
from algesnake.approximate import BloomFilter

# Track 1 million known spam IPs with 1% false positive rate
spam_ips = BloomFilter(capacity=1_000_000, error_rate=0.01)

# Add known spam IPs
for ip in known_spam_ips:
    spam_ips.add(ip)

# Check incoming requests
if incoming_ip in spam_ips:
    # Might be spam (1% false positive rate)
    additional_verification_needed()
else:
    # Definitely not spam (0% false negative rate)
    allow_request()

# Distributed spam lists can be merged
global_spam = spam_list1 + spam_list2 + spam_list3
```

#### CountMinSketch - Error Frequency Tracking

```python
from algesnake.approximate import CountMinSketch

# Track error frequencies with 1% error, 99% confidence
cms = CountMinSketch.from_error_rate(epsilon=0.01, delta=0.01)

# Count errors across logs
for log_entry in log_stream:
    if log_entry.is_error:
        cms.add(log_entry.error_type)

# Query frequencies (conservative estimate, never underestimates)
print(f"ConnectionTimeout: {cms.estimate('ConnectionTimeout')}")
print(f"DatabaseError: {cms.estimate('DatabaseError')}")

# Merge logs from multiple servers
global_cms = server1_cms + server2_cms + server3_cms

# Find heavy hitters
from algesnake.approximate.countminsketch import heavy_hitters
top_errors = heavy_hitters(global_cms, error_types, threshold=100)
# Returns: [("ConnectionTimeout", 225), ("OutOfMemory", 150), ...]
```

#### TopK - Trending Hashtags

```python
from algesnake.approximate import TopK

# Track top 10 trending hashtags
topk = TopK(k=10)

for tweet in tweet_stream:
    for hashtag in extract_hashtags(tweet):
        topk.add(hashtag)

# Get top 5 trending
for tag, count in topk.top(n=5):
    print(f"{tag}: {count} mentions")

# Distributed trend tracking across regions
us_trends = TopK(k=20)
eu_trends = TopK(k=20)
asia_trends = TopK(k=20)

# Merge for global trends
global_trends = us_trends + eu_trends + asia_trends
```

#### T-Digest - API Latency Monitoring

```python
from algesnake.approximate import TDigest

# Monitor API response times
td = TDigest(compression=100)

for request in api_requests:
    td.add(request.latency_ms)

# Query percentiles for SLA tracking
print(f"Median (p50): {td.percentile(50):.1f}ms")
print(f"p95: {td.percentile(95):.1f}ms")
print(f"p99: {td.percentile(99):.1f}ms")
print(f"p999: {td.percentile(99.9):.1f}ms")

# Check SLA compliance
if td.percentile(95) < 200:
    print("✓ SLA PASS")
else:
    print("✗ SLA FAIL")

# Distributed latency tracking
server1 = TDigest(compression=100)
server2 = TDigest(compression=100)
server3 = TDigest(compression=100)

global_latency = server1 + server2 + server3
print(f"Global p95: {global_latency.percentile(95):.1f}ms")
```

## Real-World Use Cases

### 1. Distributed Analytics Pipeline

```python
from algesnake import Add, Max, Min, SetMonoid, MapMonoid
from algesnake.approximate import HyperLogLog, CountMinSketch, TopK

class EventAggregator:
    """Distributed event analytics using monoids."""

    def __init__(self):
        # Exact aggregations
        self.total_events = Add(0)
        self.max_latency = Max(0)
        self.min_latency = Min(float('inf'))
        self.unique_sessions = SetMonoid(set())

        # Approximate aggregations (memory-efficient)
        self.unique_users = HyperLogLog(precision=14)
        self.error_frequencies = CountMinSketch(width=1000, depth=5)
        self.top_pages = TopK(k=100)

    def process_event(self, event):
        self.total_events += Add(1)
        self.max_latency += Max(event.latency)
        self.min_latency += Min(event.latency)
        self.unique_sessions += SetMonoid({event.session_id})
        self.unique_users.add(event.user_id)

        if event.error:
            self.error_frequencies.add(event.error_type)

        self.top_pages.add(event.page_url)

    def merge(self, other):
        """Merge aggregations from another partition/server."""
        aggregator = EventAggregator()

        # All monoid operations - safe for distributed merging!
        aggregator.total_events = self.total_events + other.total_events
        aggregator.max_latency = self.max_latency + other.max_latency
        aggregator.min_latency = self.min_latency + other.min_latency
        aggregator.unique_sessions = self.unique_sessions + other.unique_sessions
        aggregator.unique_users = self.unique_users + other.unique_users
        aggregator.error_frequencies = self.error_frequencies + other.error_frequencies
        aggregator.top_pages = self.top_pages + other.top_pages

        return aggregator

    def report(self):
        print(f"Total Events: {self.total_events.value:,}")
        print(f"Unique Users: {self.unique_users.cardinality():,.0f}")
        print(f"Unique Sessions: {len(self.unique_sessions.items):,}")
        print(f"Latency Range: {self.min_latency.value:.1f}ms - {self.max_latency.value:.1f}ms")
        print(f"\nTop 5 Pages:")
        for url, count in self.top_pages.top(n=5):
            print(f"  {url}: {count:,} views")
```

### 2. SLA Monitoring Dashboard

```python
from algesnake.approximate import TDigest

class ServiceMonitor:
    """Multi-endpoint SLA monitoring."""

    def __init__(self, endpoints):
        self.latencies = {
            endpoint: TDigest(compression=100)
            for endpoint in endpoints
        }
        self.sla_threshold = 200  # 200ms p95 target

    def record_request(self, endpoint, latency_ms):
        self.latencies[endpoint].add(latency_ms)

    def check_sla(self):
        print(f"{'Endpoint':>30} | {'p50':>8} | {'p95':>8} | {'p99':>8} | {'SLA':>8}")
        print("-" * 71)

        for endpoint, td in self.latencies.items():
            if td.count == 0:
                continue

            p50 = td.percentile(50)
            p95 = td.percentile(95)
            p99 = td.percentile(99)
            sla_status = "✓ PASS" if p95 < self.sla_threshold else "✗ FAIL"

            print(f"{endpoint:>30} | {p50:>7.1f}ms | {p95:>7.1f}ms | {p99:>7.1f}ms | {sla_status:>8}")

# Usage
monitor = ServiceMonitor(['/api/users', '/api/products', '/api/orders'])

for request in requests:
    monitor.record_request(request.endpoint, request.latency_ms)

monitor.check_sla()
```

### 3. Web Crawler Deduplication

```python
from algesnake.approximate import BloomFilter

class SmartCrawler:
    """Web crawler with Bloom filter deduplication."""

    def __init__(self, expected_urls=10_000_000):
        # Track seen URLs with 0.1% false positive rate
        self.seen_urls = BloomFilter(
            capacity=expected_urls,
            error_rate=0.001
        )
        self.pages_crawled = 0
        self.pages_skipped = 0

    def should_crawl(self, url):
        if url in self.seen_urls:
            # Probably seen before (0.1% false positive)
            self.pages_skipped += 1
            return False
        else:
            # Definitely not seen before
            self.seen_urls.add(url)
            self.pages_crawled += 1
            return True

    def stats(self):
        # Memory usage: ~1.2MB for 10M URLs (vs ~320MB for set)
        print(f"Pages crawled: {self.pages_crawled:,}")
        print(f"Pages skipped: {self.pages_skipped:,}")
        print(f"Memory savings: 99.6%")
```

### 4. Distributed Log Analysis

```python
from algesnake.approximate import CountMinSketch, TopK
from algesnake.approximate.countminsketch import heavy_hitters

class LogAnalyzer:
    """Distributed log analysis with approximate structures."""

    def __init__(self):
        # Track all IP frequencies (approximate)
        self.ip_frequencies = CountMinSketch.from_error_rate(
            epsilon=0.01,  # 1% error
            delta=0.01     # 99% confidence
        )

        # Track exact top 100 IPs
        self.top_ips = TopK(k=100)

        # Track error codes
        self.error_codes = CountMinSketch(width=500, depth=7)

    def process_log_line(self, log_line):
        self.ip_frequencies.add(log_line.ip)
        self.top_ips.add(log_line.ip)

        if log_line.status >= 400:
            self.error_codes.add(str(log_line.status))

    def find_suspicious_ips(self, threshold=1000):
        """Find IPs with >threshold requests."""
        all_ips = [ip for ip, _ in self.top_ips.top()]
        return heavy_hitters(self.ip_frequencies, all_ips, threshold)

    def merge_from_servers(self, analyzers):
        """Merge logs from multiple servers."""
        merged = LogAnalyzer()
        merged.ip_frequencies = sum([a.ip_frequencies for a in analyzers])
        merged.top_ips = sum([a.top_ips for a in analyzers])
        merged.error_codes = sum([a.error_codes for a in analyzers])
        return merged

# Process logs across 3 servers
server1 = LogAnalyzer()
server2 = LogAnalyzer()
server3 = LogAnalyzer()

# ... process logs on each server ...

# Merge for global view
global_analyzer = LogAnalyzer().merge_from_servers([server1, server2, server3])

# Find suspicious IPs
suspicious = global_analyzer.find_suspicious_ips(threshold=10000)
for ip, count in suspicious:
    print(f"Suspicious IP: {ip} ({count:,} requests)")
```

## Core Concepts

### Monoid Pattern

All structures in Algesnake follow the **monoid pattern**:

1. **Identity (zero)**: Empty element that doesn't affect combination
2. **Combine**: Associative binary operation
3. **Associativity**: `(a + b) + c = a + (b + c)`

This enables:
- ✅ **Parallel processing**: Split data across machines
- ✅ **Incremental updates**: Add new data without recomputation
- ✅ **Fault tolerance**: Recompute failed partitions independently
- ✅ **Order independence**: Process data in any order

```python
# All these produce the same result!
result = (a + b) + (c + d)
result = ((a + b) + c) + d
result = a + (b + (c + d))
result = sum([a, b, c, d])
```

### Concrete Monoids

#### Numeric Monoids

```python
from algesnake import Add, Multiply, Max, Min

# Addition monoid (zero = 0)
Add(5) + Add(3)  # Add(8)
sum([Add(1), Add(2), Add(3)])  # Add(6)

# Multiplication monoid (zero = 1)
Multiply(5) * Multiply(3)  # Multiply(15) (future feature)

# Max monoid (zero = -∞)
Max(5) + Max(3)  # Max(5)
sum([Max(1), Max(5), Max(3)])  # Max(5)

# Min monoid (zero = +∞)
Min(5) + Min(3)  # Min(3)
sum([Min(1), Min(5), Min(3)])  # Min(1)
```

#### Collection Monoids

```python
from algesnake import SetMonoid, ListMonoid, MapMonoid, StringMonoid

# Set union monoid
s1 = SetMonoid({1, 2})
s2 = SetMonoid({2, 3})
s1 + s2  # SetMonoid({1, 2, 3})

# List concatenation monoid
l1 = ListMonoid([1, 2])
l2 = ListMonoid([3, 4])
l1 + l2  # ListMonoid([1, 2, 3, 4])

# Map merge monoid with custom combination
m1 = MapMonoid({'a': 1, 'b': 2}, lambda x, y: x + y)
m2 = MapMonoid({'b': 3, 'c': 4}, lambda x, y: x + y)
m1 + m2  # MapMonoid({'a': 1, 'b': 5, 'c': 4})

# String concatenation monoid
s1 = StringMonoid("Hello ")
s2 = StringMonoid("World")
s1 + s2  # StringMonoid("Hello World")
```

#### Option Monoid

```python
from algesnake import Some, None_, Option

# Safe null handling with monoids
def safe_divide(a, b):
    if b == 0:
        return None_()
    return Some(a / b)

result = safe_divide(10, 2)  # Some(5.0)
result = safe_divide(10, 0)  # None_()

# Combine options (first Some wins)
Some(5) + Some(3)  # Some(5)
Some(5) + None_()  # Some(5)
None_() + Some(3)  # Some(3)
None_() + None_()  # None_()

# Use with sum() for fallback chain
configs = [
    None_(),           # Primary config missing
    None_(),           # Secondary config missing
    Some('fallback')   # Fallback config
]
config = sum(configs)  # Some('fallback')
```

### Probabilistic Data Structures

All probabilistic data structures trade perfect accuracy for massive memory savings while maintaining monoid properties.

#### Accuracy vs Memory Trade-offs

| Structure | Use Case | Memory | Accuracy | Best For |
|-----------|----------|--------|----------|----------|
| **HyperLogLog** | Unique counts | O(log log n) | ~2% error | Billion-scale cardinality |
| **Bloom Filter** | Membership | O(n/ln²(1/ε)) | Configurable FP | Deduplication, caching |
| **CountMinSketch** | Frequencies | O(1/ε × log(1/δ)) | Never under | Heavy hitter detection |
| **TopK** | Top items | O(k) | Exact for top K | Trending, rankings |
| **T-Digest** | Percentiles | O(compression) | 0.1-1% (tails) | Latency, SLAs, monitoring |

#### Example: 10M Items Memory Comparison

| Structure | Full Storage | Approximate | Savings |
|-----------|--------------|-------------|---------|
| Unique count | 80 MB (set) | 16 KB (HLL) | **99.98%** |
| Membership | 80 MB (set) | 1.2 MB (Bloom) | **98.5%** |
| Frequencies | 80 MB (dict) | 40 KB (CMS) | **99.95%** |
| Top 100 | 80 MB (dict) | 2 KB (TopK) | **99.997%** |
| Percentiles | 80 MB (list) | 1.6 KB (T-Digest) | **99.998%** |

## Performance Benchmarks

### HyperLogLog Accuracy

```python
# Test: Count 1,000,000 unique items
true_count = 1_000_000
hll = HyperLogLog(precision=14)

for i in range(true_count):
    hll.add(f"user_{i}")

estimate = hll.cardinality()
error = abs(estimate - true_count) / true_count

print(f"True: {true_count:,}")
print(f"Estimate: {estimate:,.0f}")
print(f"Error: {error*100:.2f}%")  # Typically < 2%
```

### T-Digest Tail Accuracy

```python
# Test: 10,000 latency measurements
import random
td = TDigest(compression=100)
values = [random.gauss(100, 20) for _ in range(10000)]

for v in values:
    td.add(v)

# Compare to exact
values_sorted = sorted(values)
exact_p99 = values_sorted[int(len(values) * 0.99)]
approx_p99 = td.percentile(99)

print(f"Exact p99: {exact_p99:.2f}ms")
print(f"T-Digest p99: {approx_p99:.2f}ms")
print(f"Error: {abs(approx_p99 - exact_p99) / exact_p99 * 100:.2f}%")
# Typically < 1% error
```

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=algesnake --cov-report=html

# Run specific test file
pytest tests/unit/test_tdigest.py -v

# Run approximation algorithm tests
pytest tests/unit/test_hyperloglog.py tests/unit/test_bloom.py tests/unit/test_countminsketch.py tests/unit/test_topk.py tests/unit/test_tdigest.py -v
```

**Test Stats**:
- Total tests: 406
- Coverage: 80%+
- All structures: 85-96% individual coverage

## Project Structure

```
algesnake/
├── algesnake/
│   ├── __init__.py                 # Main exports
│   ├── abstract/                   # Abstract base classes
│   │   ├── semigroup.py
│   │   ├── monoid.py
│   │   ├── group.py
│   │   ├── ring.py
│   │   └── semiring.py
│   ├── monoid/                     # Concrete monoids
│   │   ├── numeric.py              # Add, Multiply, Max, Min
│   │   ├── collection.py           # Set, List, Map, String
│   │   └── option.py               # Some, None_, Option
│   ├── approximate/                # Probabilistic data structures
│   │   ├── hyperloglog.py          # Cardinality estimation
│   │   ├── bloom.py                # Membership testing
│   │   ├── countminsketch.py       # Frequency estimation
│   │   ├── topk.py                 # Heavy hitters
│   │   └── tdigest.py              # Quantile estimation
│   └── operators.py                # Operator overloading
├── tests/
│   ├── unit/                       # 406 comprehensive tests
│   └── integration/                # Integration tests
├── docs/                           # Detailed documentation
│   ├── QUICKSTART.md
│   ├── INTEGRATION_GUIDE.md
│   └── Feature-specific guides
├── examples/                       # Real-world examples
│   ├── Concrete monoid examples
│   ├── HyperLogLog & Bloom Filter examples
│   ├── CountMinSketch & TopK examples
│   └── T-Digest examples
└── README.md
```

## Roadmap

### ✅ Core Features (Complete)
- **Abstract algebra foundation**: Semigroup, Monoid, Group, Ring, Semiring (106 tests)
- **Concrete monoids**: Numeric, Collection, and Option types (193 tests)
- **Probabilistic data structures**: HyperLogLog, Bloom Filter, CountMinSketch, TopK, T-Digest (168 tests)
- **Similarity search & indexing**: MinHash, Weighted MinHash, LSH, LSH Forest, LSH Ensemble, HNSW (250+ tests)
- **Operator overloading**: Pythonic `+` and `sum()` support
- **Comprehensive testing**: 650+ tests with 80%+ coverage

### 🚧 Upcoming Features
- **HyperLogLog++**: Google's improved HLL with sparse representation
- **Distributed computing**: PySpark and Dask integration
- **Additional structures**: SimHash, QTree
- **Performance**: Cython optimizations for hot paths
- **Serialization**: Complete save/load support for all structures

## Why Algesnake?

### The Problem

Building distributed aggregation systems is hard:
- **Parallelism**: How to split work across machines?
- **Fault tolerance**: What if a machine fails?
- **Memory constraints**: Can't store everything
- **Approximate algorithms**: When exact is impossible

### The Solution

Abstract algebra provides mathematical guarantees:

**Associativity** → Split work arbitrarily across machines
```python
# These are equivalent:
result = ((server1 + server2) + server3)
result = (server1 + (server2 + server3))
# Order of merging doesn't matter!
```

**Identity** → Handle empty/missing data safely
```python
# Empty data doesn't break aggregation:
result = data + zero  # Same as data
```

**Commutativity** (when applicable) → Process in any order
```python
# For commutative monoids:
result = a + b + c
result = c + a + b  # Same result!
```

### Real-World Impact

**Example: Count unique users across 1 billion events**

❌ **Naive approach**:
- Store all user IDs in a set
- Memory: ~16 GB
- Can't distribute easily

✅ **Algesnake approach**:
```python
# Use HyperLogLog (monoid!)
hll = HyperLogLog(precision=14)
# Memory: 16 KB (99.9% savings!)

# Distribute across 100 servers
server_hlls = [HyperLogLog(precision=14) for _ in range(100)]

# Each server processes 1% of data independently
# Then merge (associative!)
global_hll = sum(server_hlls)

# Accurate to ~2%
unique_users = global_hll.cardinality()
```

## Comparison with Scala Algebird

| Feature | Python Algesnake | Scala Algebird |
|---------|-----------------|----------------|
| Core abstractions | ✅ Semigroup, Monoid, Group, Ring, Semiring | ✅ Same |
| Operator overloading | ✅ Pythonic (`+`, `sum()`) | ✅ Implicit classes |
| **Numeric monoids** | ✅ Add, Multiply, Max, Min | ✅ |
| **Collection monoids** | ✅ Set, List, Map, String | ✅ |
| **Option type** | ✅ Some, None_, Option | ✅ |
| **HyperLogLog** | ✅ Complete | ✅ |
| **Bloom Filter** | ✅ Complete | ✅ |
| **CountMinSketch** | ✅ Complete | ✅ |
| **TopK** | ✅ Complete | ✅ |
| **T-Digest** | ✅ Complete | ✅ |
| Spark integration | 🚧 Planned (PySpark) | ✅ Native Spark |
| Type safety | ✅ Type hints + mypy | ✅ Scala type system |
| Test coverage | ✅ 406 tests, 80%+ | ✅ Extensive |

## API Reference

### Quick Reference

```python
# Concrete Monoids
from algesnake import (
    Add, Multiply, Max, Min,           # Numeric
    SetMonoid, ListMonoid,              # Collections
    MapMonoid, StringMonoid,            # Collections
    Some, None_, Option,                # Option type
)

# Probabilistic Data Structures
from algesnake.approximate import (
    HyperLogLog,      # Cardinality estimation
    BloomFilter,      # Membership testing
    CountMinSketch,   # Frequency estimation
    TopK,             # Heavy hitters
    TDigest,          # Quantile estimation
)

# All structures support:
a + b              # Combine (monoid operation)
sum([a, b, c])     # Sum builtin
a.zero             # Identity element
a.combine(b)       # Explicit combine
```

### Detailed Documentation

See the `docs/` directory for detailed documentation:
- [QUICKSTART.md](docs/QUICKSTART.md) - Getting started guide
- [INTEGRATION_GUIDE.md](docs/INTEGRATION_GUIDE.md) - Integration patterns
- Concrete monoids reference - Numeric, Collection, and Option types
- HyperLogLog & Bloom Filter - Cardinality estimation and membership testing
- CountMinSketch & TopK - Frequency estimation and heavy hitters
- T-Digest - Quantile and percentile estimation

## Contributing

Contributions are welcome! Here's how to get started:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests (maintain >80% coverage)
5. Run tests (`pytest`)
6. Commit your changes
7. Push to the branch
8. Open a Pull Request

Please ensure:
- All tests pass
- Code follows Python style guidelines
- Documentation is updated
- Examples are provided for new features

## License

Apache License 2.0 - See LICENSE file for details

## Acknowledgments

- Inspired by [Twitter's Algebird](https://github.com/twitter/algebird)
- T-Digest based on [Ted Dunning's paper](https://arxiv.org/abs/1902.04023)
- Built for the Python data engineering community

## Resources

### Papers & Research
- [HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
- [Computing Extremely Accurate Quantiles Using t-Digests](https://arxiv.org/abs/1902.04023)
- [An Improved Data Stream Summary: The Count-Min Sketch and its Applications](https://dl.acm.org/doi/10.1016/j.jalgor.2003.12.001)
- [Network Applications of Bloom Filters: A Survey](https://www.eecs.harvard.edu/~michaelm/postscripts/im2005b.pdf)

### Related Projects
- [Twitter Algebird (Scala)](https://github.com/twitter/algebird)
- [Apache DataSketches (Java)](https://datasketches.apache.org/)
- [StreamLib (Java)](https://github.com/addthis/stream-lib)

### Learning Resources
- [Abstract Algebra Basics](https://en.wikipedia.org/wiki/Abstract_algebra)
- [Monoids in Category Theory](https://en.wikipedia.org/wiki/Monoid_(category_theory))
- [Probabilistic Data Structures](https://en.wikipedia.org/wiki/Category:Probabilistic_data_structures)

---

**Status**: 🎉 Production Ready | **Version**: 0.5.0 | **Python**: 3.8+ | **Tests**: 406 passing
