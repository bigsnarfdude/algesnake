# Data Structure Aggregation Guide

## Quick Answer: Which Structures Support `+` and `sum()`?

| Structure | Aggregatable? | Operation | Meaning |
|-----------|--------------|-----------|---------|
| **MinHash** | ✅ **YES** | `mh1 + mh2` | Set union |
| **Weighted MinHash** | ✅ **YES** | `wmh1 + wmh2` | Weighted union |
| **HyperLogLog** | ✅ **YES** | `hll1 + hll2` | Cardinality union |
| **Bloom Filter** | ✅ **YES** | `bf1 + bf2` | Set union (bitwise OR) |
| **CountMinSketch** | ✅ **YES** | `cms1 + cms2` | Frequency aggregation |
| **TopK** | ✅ **YES** | `tk1 + tk2` | Merge top items |
| **T-Digest** | ✅ **YES** | `td1 + td2` | Merge distributions |
| **MinHash LSH** | ✅ **YES** 🆕 | `lsh1 + lsh2` | Merge indexes |
| **LSH Forest** | ❌ **NO** | - | Index structure |
| **LSH Ensemble** | ❌ **NO** | - | Index structure |
| **HNSW** | ❌ **NO** | - | Graph structure |

---

## 🆕 NEW: MinHashLSH Now Supports Monoid Operations!

**MinHashLSH** can now be aggregated using `+` and `sum()` operations. This enables:
- Distributed LSH construction across multiple servers
- Merging independently-built indexes
- Incremental index updates

**Example:**
```python
# Build LSH indexes on different servers
lsh1 = MinHashLSH(threshold=0.7, num_perm=128)
lsh2 = MinHashLSH(threshold=0.7, num_perm=128)
# ... insert data ...

# Merge using monoid operations
merged = lsh1 + lsh2
# Or: combined = sum([lsh1, lsh2, lsh3])
```

**When to use:**
- ✅ Merging pre-built indexes from different sources
- ✅ Distributed LSH construction
- ✅ Incremental batch updates

**When NOT to use:**
- ❌ If you have the original MinHash sketches (aggregate those instead, then build LSH once)
- ❌ If parameters don't match (threshold, num_perm, b, r must be identical)

See `examples/minhash_lsh_monoid_examples.py` for detailed usage examples.

---

## Key Concept: Sketches vs. Indexes

### ✅ SKETCHES (Monoids - Aggregatable)

**What:** Compact data summaries that can be merged

**Examples:**
- MinHash, Weighted MinHash
- HyperLogLog, Bloom Filter
- CountMinSketch, TopK, T-Digest

**Properties:**
```python
# Associative: (a + b) + c = a + (b + c)
result1 = (sketch1 + sketch2) + sketch3
result2 = sketch1 + (sketch2 + sketch3)
assert result1 == result2

# Identity: sketch + zero = sketch
assert sketch + sketch.zero == sketch

# Supports sum()
combined = sum([sketch1, sketch2, sketch3])
```

**Use Cases:**
- ✅ Distributed computing (MapReduce, Spark, Dask)
- ✅ Streaming aggregation
- ✅ Multi-server statistics
- ✅ Incremental updates

### ❌ INDEXES (Not Monoids - Not Aggregatable)

**What:** Query structures built FROM sketches

**Examples:**
- MinHash LSH, LSH Forest, LSH Ensemble
- HNSW (graph-based ANN)

**Why No Aggregation?**
- Internal structure is complex (hash tables, trees, graphs)
- No meaningful "union" operation
- Like database indexes: can't "add" two B-trees

**Use Cases:**
- ✅ Fast similarity search (after data collected)
- ✅ Nearest neighbor queries
- ✅ Top-K retrieval

---

## Usage Patterns

### Pattern 1: Distributed Sketch Aggregation

```python
from algesnake.approximate import MinHash, HyperLogLog

# Each server computes sketches
server1_mh = MinHash(128)
server1_hll = HyperLogLog(14)
for user in server1_users:
    server1_mh.update(user)
    server1_hll.add(user)

server2_mh = MinHash(128)
server2_hll = HyperLogLog(14)
for user in server2_users:
    server2_mh.update(user)
    server2_hll.add(user)

# Aggregate using monoid operations
global_mh = server1_mh + server2_mh
global_hll = server1_hll + server2_hll

# Or use sum()
all_servers = [server1_mh, server2_mh, server3_mh]
global_mh = sum(all_servers)

print(f"Global unique count: {global_hll.cardinality()}")
```

### Pattern 2: Sketch Then Index

```python
from algesnake.approximate import MinHash, MinHashLSH

# Step 1: Create sketches (aggregatable)
doc_sketches = {}
for doc_id, words in documents.items():
    mh = MinHash(128)
    mh.update_batch(words)
    doc_sketches[doc_id] = mh

# Step 2: Can aggregate sketches
category_sketch = doc_sketches["doc1"] + doc_sketches["doc2"]

# Step 3: Build index for queries (NOT aggregatable)
lsh = MinHashLSH(threshold=0.7, num_perm=128)
for doc_id, mh in doc_sketches.items():
    lsh.insert(doc_id, mh)

# Step 4: Query
query_mh = MinHash(128)
query_mh.update_batch(query_words)
results = lsh.query(query_mh)
```

### Pattern 3: Multi-Stage Aggregation

```python
# Stage 1: 1000 servers → 1000 sketches
server_sketches = [create_sketch(server_data) for server in servers]

# Stage 2: Aggregate to 10 regions
region_sketches = []
for i in range(10):
    region = sum(server_sketches[i*100:(i+1)*100])
    region_sketches.append(region)

# Stage 3: Final global aggregate
global_sketch = sum(region_sketches)

# Stage 4: Build index for queries
lsh = MinHashLSH(threshold=0.5, num_perm=128)
for region_id, sketch in enumerate(region_sketches):
    lsh.insert(f"region{region_id}", sketch)
```

### Pattern 4: Combining Multiple Sketch Types

```python
class AggregateStats:
    """Combine multiple sketch types for comprehensive stats."""

    def __init__(self):
        self.similarity = MinHash(128)       # User similarity
        self.cardinality = HyperLogLog(14)   # Unique count
        self.membership = BloomFilter(10000, 0.01)  # Deduplication
        self.frequencies = CountMinSketch(1000, 5)  # Frequency

    def __add__(self, other):
        """Monoid operation - combine all sketches."""
        result = AggregateStats()
        result.similarity = self.similarity + other.similarity
        result.cardinality = self.cardinality + other.cardinality
        result.membership = self.membership + other.membership
        result.frequencies = self.frequencies + other.frequencies
        return result

# Each server tracks stats
server1 = AggregateStats()
server2 = AggregateStats()
server3 = AggregateStats()

# Aggregate
global_stats = server1 + server2 + server3
# Or: global_stats = sum([server1, server2, server3])
```

---

## Why This Matters

### Distributed Computing Benefits

Monoid properties enable:

1. **Parallelization**: Process data partitions independently
2. **Fault Tolerance**: Recompute failed partitions without affecting others
3. **Order Independence**: Process in any order (associativity)
4. **Incremental Updates**: Add new data without full recomputation
5. **MapReduce/Spark**: Framework assumes monoid operations

### Example: MapReduce with MinHash

```python
# Map phase: Each mapper creates local sketch
def mapper(partition):
    mh = MinHash(128)
    for item in partition:
        mh.update(item)
    return mh

# Reduce phase: Combine using monoid operation
def reducer(sketches):
    return sum(sketches)  # Works because MinHash is a monoid!

# Framework handles distribution
results = mapreduce(data, mapper, reducer)
```

---

## Performance Comparison

### Sketch Aggregation (Constant Time)

```python
# O(k) where k = num_perm
mh1 = MinHash(128)  # 128 hash values
mh2 = MinHash(128)
combined = mh1 + mh2  # O(128) - just element-wise min

# Same regardless of input size!
# 1 million items or 1 billion items - same merge time
```

### Index Building (Linear Time)

```python
# O(n * log n) where n = number of items
lsh = MinHashLSH(threshold=0.5, num_perm=128)
for i in range(n):
    lsh.insert(f"item{i}", minhash)  # O(log n) per insert

# Can't merge two LSH indexes!
# lsh1 + lsh2  # ❌ Not supported
```

---

## Summary Table

| Operation | Sketches | Indexes |
|-----------|----------|---------|
| **Create from data** | ✅ Fast (O(n)) | ✅ Fast (O(n log n)) |
| **Merge/Combine** | ✅ **YES** (O(k)) | ❌ **NO** |
| **Distributed** | ✅ **YES** | ❌ Build after aggregation |
| **Incremental** | ✅ **YES** | ⚠️ Rebuild required |
| **Query** | ⚠️ Linear scan | ✅ **Sub-linear** |
| **Use `sum()`** | ✅ **YES** | ❌ **NO** |

---

## Common Questions

**Q: Can I merge two LSH indexes?**
A: No. LSH is an index, not a sketch. Build the index AFTER aggregating sketches.

**Q: How do I aggregate across 1000 servers?**
A: Use sketches (MinHash, HLL, etc.), aggregate with `sum()`, THEN build index if needed.

**Q: Why does MinHash support `+` but LSH doesn't?**
A: MinHash is a data summary (monoid). LSH is a query structure (not a monoid).

**Q: Can I update an LSH index incrementally?**
A: Not efficiently. For streaming data, use sketches or rebuild the index periodically.

**Q: Which is faster: sketch aggregation or index queries?**
A: Aggregation is O(k), queries are O(log n) or O(n^ρ). Use sketches for aggregation, indexes for queries.

---

## See Also

- `examples/distributed_aggregation_guide.py` - Full working examples
- `examples/similarity_search_examples.py` - Real-world use cases
- Algebird paper: "Abstract Algebra for Analytics" (Twitter Engineering)
