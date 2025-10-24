"""
Distributed Aggregation Patterns with Similarity Search Structures
===================================================================

This guide explains which similarity search structures support monoid operations
for distributed aggregation, and how to use them.

TLDR - Monoid Support:
✅ MinHash - YES (combines = set union)
✅ Weighted MinHash - YES (combines = weighted union)
❌ MinHash LSH - NO (it's an index, not a sketch)
❌ LSH Forest - NO (it's an index, not a sketch)
❌ LSH Ensemble - NO (it's an index, not a sketch)
❌ HNSW - NO (it's a graph, not a sketch)

Key Insight: SKETCHES are monoids, INDEXES are not.
"""

from algesnake.approximate import (
    MinHash, WeightedMinHash, MinHashLSH, HyperLogLog, BloomFilter
)
from algesnake.approximate.minhash import create_minhash


print("=" * 80)
print("Part 1: Sketches vs Indexes - What's the Difference?")
print("=" * 80)

print("""
SKETCHES (Monoids - support aggregation):
- MinHash, Weighted MinHash
- HyperLogLog, Bloom Filter, CountMinSketch, TopK, T-Digest
- These are SUMMARIES of data that can be merged
- Use case: Distributed aggregation across servers

INDEXES (NOT Monoids - don't support aggregation):
- MinHash LSH, LSH Forest, LSH Ensemble, HNSW
- These are QUERY STRUCTURES built from sketches
- Use case: Fast search after data is collected
""")


print("\n" + "=" * 80)
print("Part 2: MinHash - Distributed Set Union Aggregation")
print("=" * 80)

print("\n✅ YES - MinHash supports + and sum() operations")
print("Semantic meaning: Combines = UNION of sets\n")

# Example: 3 servers tracking user activity
server1_users = ["user1", "user2", "user3"]
server2_users = ["user2", "user3", "user4"]  # overlap
server3_users = ["user5", "user6"]

# Each server computes MinHash locally
mh1 = create_minhash(server1_users, num_perm=128)
mh2 = create_minhash(server2_users, num_perm=128)
mh3 = create_minhash(server3_users, num_perm=128)

print("Server 1:", server1_users)
print("Server 2:", server2_users)
print("Server 3:", server3_users)

# Method 1: Using + operator
combined_plus = mh1 + mh2 + mh3
print(f"\nCombined using +: {combined_plus}")

# Method 2: Using sum() builtin
combined_sum = sum([mh1, mh2, mh3])
print(f"Combined using sum(): {combined_sum}")

# Both produce same result
assert combined_plus == combined_sum
print("✓ Both methods produce identical results (monoid property)")

# Verify union semantics
all_users = set(server1_users + server2_users + server3_users)
direct_mh = create_minhash(all_users, num_perm=128)
print(f"\nDirect from union: {direct_mh}")
print(f"Matches combined: {combined_sum == direct_mh}")


print("\n" + "=" * 80)
print("Part 3: Monoid Properties Enable Distributed Computing")
print("=" * 80)

print("""
Why monoid operations matter for distributed systems:

1. ASSOCIATIVITY: (a + b) + c = a + (b + c)
   → Process in any order, any grouping
   → MapReduce, Spark, Dask all rely on this

2. IDENTITY: a + zero = a
   → Handle empty partitions safely
   → No special cases needed

3. COMMUTATIVITY (for MinHash): a + b = b + a
   → Process in any order
   → Shuffle partitions freely
""")

# Demonstrate associativity
print("\nDemonstrating associativity:")
result1 = (mh1 + mh2) + mh3
result2 = mh1 + (mh2 + mh3)
result3 = (mh1 + mh3) + mh2  # different order
print(f"(mh1 + mh2) + mh3 == mh1 + (mh2 + mh3): {result1 == result2}")
print(f"(mh1 + mh3) + mh2 == mh1 + (mh2 + mh3): {result3 == result2}")

# Demonstrate identity
zero = mh1.zero
print(f"\nDemonstrating identity:")
print(f"mh1 + zero == mh1: {(mh1 + zero) == mh1}")
print(f"zero.is_zero(): {zero.is_zero()}")


print("\n" + "=" * 80)
print("Part 4: Weighted MinHash - Distributed Weighted Aggregation")
print("=" * 80)

print("\n✅ YES - Weighted MinHash also supports + and sum()")
print("Semantic meaning: Combines = weighted union\n")

from algesnake.approximate.weighted_minhash import create_weighted_minhash

# Example: 3 data centers tracking search terms with frequencies
dc1_terms = {"python": 100, "java": 50}
dc2_terms = {"python": 80, "javascript": 70}
dc3_terms = {"rust": 30, "go": 40}

wmh1 = create_weighted_minhash(dc1_terms, num_perm=128)
wmh2 = create_weighted_minhash(dc2_terms, num_perm=128)
wmh3 = create_weighted_minhash(dc3_terms, num_perm=128)

print("DC 1 terms:", dc1_terms)
print("DC 2 terms:", dc2_terms)
print("DC 3 terms:", dc3_terms)

# Combine across data centers
global_wmh = wmh1 + wmh2 + wmh3
print(f"\nGlobal weighted MinHash: {global_wmh}")

# Can also use sum()
global_wmh2 = sum([wmh1, wmh2, wmh3])
assert global_wmh == global_wmh2
print("✓ Addition and sum() produce same result")


print("\n" + "=" * 80)
print("Part 5: Why LSH Indexes DON'T Support Aggregation")
print("=" * 80)

print("""
❌ LSH, LSH Forest, LSH Ensemble, HNSW are NOT monoids

Reason: They are INDEXES, not SKETCHES
- Indexes have internal structure (hash tables, trees, graphs)
- Combining two indexes is not well-defined
- No meaningful "union" operation

Think of it like database indexes:
- You can't "add" two B-trees together
- You must rebuild the index from data

Workflow:
1. Collect sketches using monoid ops (MinHash, HLL, etc.)
2. THEN build index once for querying
""")

# Example: Correct workflow
print("\n📊 Correct Pattern: Sketch → Aggregate → Index")

# Step 1: Multiple sources create sketches
documents = {
    "doc1": ["machine", "learning", "python"],
    "doc2": ["data", "science", "python"],
    "doc3": ["web", "development", "javascript"],
}

doc_sketches = {
    doc_id: create_minhash(words, num_perm=128)
    for doc_id, words in documents.items()
}

print("\nStep 1: Created MinHash sketches for each document")
print(f"  Total: {len(doc_sketches)} sketches")

# Step 2: Sketches CAN be combined (monoid)
# For example, combine doc1 and doc2 into a single "ML category"
ml_category = doc_sketches["doc1"] + doc_sketches["doc2"]
print("\nStep 2: Combined doc1 + doc2 into ML category (monoid operation)")
print(f"  Result: {ml_category}")

# Step 3: Build LSH index from final sketches
lsh = MinHashLSH(threshold=0.5, num_perm=128)
for doc_id, mh in doc_sketches.items():
    lsh.insert(doc_id, mh)

print("\nStep 3: Built LSH index for fast queries")
print(f"  Index contains: {len(lsh)} documents")

# Step 4: Query (NOT aggregation)
query = create_minhash(["python", "programming"], num_perm=128)
results = lsh.query(query)
print(f"\nStep 4: Query for similar docs to ['python', 'programming']")
print(f"  Results: {results}")


print("\n" + "=" * 80)
print("Part 6: Combining Multiple Sketch Types for Stats")
print("=" * 80)

print("""
You CAN combine different sketch types together for comprehensive stats!

Pattern: Each server computes multiple sketches, then aggregates
""")

# Example: Log analysis across 3 servers
class ServerStats:
    """Aggregatable statistics using multiple sketches."""

    def __init__(self):
        # Different sketches for different purposes
        self.unique_users = MinHash(num_perm=128)  # User similarity
        self.unique_count = HyperLogLog(precision=14)  # Exact count
        self.seen_ips = BloomFilter(capacity=10000, error_rate=0.01)  # Dedup

    def add_event(self, user_id, ip):
        """Process one event."""
        self.unique_users.update(user_id)
        self.unique_count.add(user_id)
        self.seen_ips.add(ip)

    def __add__(self, other):
        """Combine stats from two servers (monoid!)."""
        result = ServerStats()
        result.unique_users = self.unique_users + other.unique_users
        result.unique_count = self.unique_count + other.unique_count
        result.seen_ips = self.seen_ips + other.seen_ips
        return result

# Each server tracks events
server1 = ServerStats()
server1.add_event("user1", "192.168.1.1")
server1.add_event("user2", "192.168.1.2")

server2 = ServerStats()
server2.add_event("user2", "192.168.1.2")  # overlap
server2.add_event("user3", "192.168.1.3")

server3 = ServerStats()
server3.add_event("user4", "192.168.1.4")

print("Server 1: 2 events (user1, user2)")
print("Server 2: 2 events (user2, user3)")
print("Server 3: 1 event (user4)")

# Aggregate using monoid operations
global_stats = server1 + server2 + server3

print(f"\n✓ Global unique user count: {global_stats.unique_count.cardinality():.0f}")
print(f"✓ Global MinHash ready for similarity queries")
print(f"✓ Global Bloom filter size: {len(global_stats.seen_ips.bits)} bits")


print("\n" + "=" * 80)
print("Part 7: Summary - When to Use What")
print("=" * 80)

print("""
USE CASE: Distributed aggregation (MapReduce, Spark, streaming)
✅ USE: MinHash, Weighted MinHash, HyperLogLog, Bloom Filter, etc.
   - Combine with + or sum()
   - Process in parallel
   - Merge results from multiple sources

USE CASE: Fast similarity search / nearest neighbor
❌ DON'T aggregate: LSH, LSH Forest, LSH Ensemble, HNSW
✅ DO build once: After data is collected/aggregated

PATTERN:
┌─────────────┐
│ Server 1    │──→ MinHash ──┐
├─────────────┤              │
│ Server 2    │──→ MinHash ──┼──→ sum() ──→ Combined MinHash
├─────────────┤              │                    ↓
│ Server 3    │──→ MinHash ──┘              Build LSH Index
└─────────────┘                                   ↓
                                            Fast Queries!

KEY INSIGHT:
- Sketches = Data structures (monoids, aggregatable)
- Indexes = Query structures (not monoids, build once)
""")


print("\n" + "=" * 80)
print("Bonus: Real-World Example - Multi-Stage Aggregation")
print("=" * 80)

print("""
Scenario: Analyze user behavior across 1000 servers
Goal: Find similar user cohorts

Stage 1: Each server computes MinHash (1000 sketches)
Stage 2: MapReduce - Aggregate to 10 region sketches
Stage 3: Final reduce - Single global sketch
Stage 4: Build LSH index for cohort queries
""")

# Simulate multi-stage aggregation
print("\nSimulating 100 servers with 10 regions...")

regions = {}
for region_id in range(10):
    # Simulate 10 servers per region
    region_sketches = []
    for server_id in range(10):
        # Each server sees different users
        users = [f"user{region_id * 100 + server_id * 10 + i}" for i in range(5)]
        mh = create_minhash(users, num_perm=128)
        region_sketches.append(mh)

    # Aggregate region (Stage 2)
    regions[f"region{region_id}"] = sum(region_sketches)
    print(f"  Region {region_id}: aggregated 10 servers")

# Final aggregation (Stage 3)
global_sketch = sum(regions.values())
print(f"\n✓ Global sketch: combined {len(regions)} regions")

# Stage 4: Build LSH for queries
lsh_final = MinHashLSH(threshold=0.3, num_perm=128)
for region_id, mh in regions.items():
    lsh_final.insert(region_id, mh)

print(f"✓ Built LSH index: {len(lsh_final)} regions indexed")
print(f"✓ Ready for fast similarity queries!")

print("\n" + "=" * 80)
print("Summary: Sketch = Aggregate, Index = Query")
print("=" * 80)
