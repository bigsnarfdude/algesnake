"""
MinHashLSH Monoid Operations - Distributed Index Construction
==============================================================

This example demonstrates how to use MinHashLSH as a monoid for
distributed index construction.
"""

from algesnake.approximate import MinHash, MinHashLSH
from algesnake.approximate.minhash import create_minhash

print("=" * 80)
print("MinHashLSH Monoid Operations Demo")
print("=" * 80)

# Example 1: Basic combine operation
print("\n" + "=" * 80)
print("Example 1: Combining Two LSH Indexes")
print("=" * 80)

lsh1 = MinHashLSH(threshold=0.6, num_perm=128)
lsh2 = MinHashLSH(threshold=0.6, num_perm=128)

# Server 1 indexes documents 1-5
print("\nServer 1 indexing documents 1-5...")
for i in range(1, 6):
    words = [f"doc{i}", f"word{i}", "common"]
    mh = create_minhash(words, num_perm=128)
    lsh1.insert(f"doc{i}", mh)

print(f"  Server 1: {len(lsh1)} documents indexed")

# Server 2 indexes documents 6-10
print("\nServer 2 indexing documents 6-10...")
for i in range(6, 11):
    words = [f"doc{i}", f"word{i}", "common"]
    mh = create_minhash(words, num_perm=128)
    lsh2.insert(f"doc{i}", mh)

print(f"  Server 2: {len(lsh2)} documents indexed")

# Combine using + operator
print("\nCombining indexes...")
combined = lsh1 + lsh2

print(f"✓ Combined index: {len(combined)} documents")
print(f"✓ Contains: {sorted(combined.keys)[:5]}... and {len(combined) - 5} more")


# Example 2: Using sum() for multiple indexes
print("\n" + "=" * 80)
print("Example 2: Aggregating Multiple Servers with sum()")
print("=" * 80)

# Simulate 5 servers, each indexing 10 documents
print("\nSimulating 5 servers building LSH indexes...")
server_indexes = []

for server_id in range(5):
    lsh = MinHashLSH(threshold=0.6, num_perm=128)

    for doc_id in range(10):
        words = [f"server{server_id}", f"doc{doc_id}", "keyword"]
        mh = create_minhash(words, num_perm=128)
        lsh.insert(f"s{server_id}_doc{doc_id}", mh)

    server_indexes.append(lsh)
    print(f"  Server {server_id}: {len(lsh)} documents indexed")

# Aggregate using sum()
print("\nAggregating all servers using sum()...")
global_lsh = sum(server_indexes)

print(f"✓ Global LSH index: {len(global_lsh)} total documents")
print(f"✓ Parameters: threshold={global_lsh.threshold}, num_perm={global_lsh.num_perm}")

# Query the combined index
print("\nQuerying combined index...")
query = create_minhash(["server2", "doc5", "keyword"], num_perm=128)
results = global_lsh.query(query)
print(f"✓ Query found {len(results)} similar documents")
if results:
    print(f"  Results: {results[:3]}...")


# Example 3: Monoid properties demonstration
print("\n" + "=" * 80)
print("Example 3: Demonstrating Monoid Properties")
print("=" * 80)

# Create three indexes
lsh_a = MinHashLSH(threshold=0.5, num_perm=128)
lsh_b = MinHashLSH(threshold=0.5, num_perm=128)
lsh_c = MinHashLSH(threshold=0.5, num_perm=128)

lsh_a.insert("doc_a", create_minhash(["a"], num_perm=128))
lsh_b.insert("doc_b", create_minhash(["b"], num_perm=128))
lsh_c.insert("doc_c", create_minhash(["c"], num_perm=128))

# 1. Associativity: (a + b) + c = a + (b + c)
print("\n1. Associativity Test:")
result1 = (lsh_a + lsh_b) + lsh_c
result2 = lsh_a + (lsh_b + lsh_c)
print(f"   (a + b) + c has {len(result1)} items: {sorted(result1.keys)}")
print(f"   a + (b + c) has {len(result2)} items: {sorted(result2.keys)}")
print(f"   ✓ Associative: {result1.keys == result2.keys}")

# 2. Identity: lsh + zero = lsh
print("\n2. Identity Test:")
zero = lsh_a.zero
result_with_zero = lsh_a + zero
print(f"   lsh_a has {len(lsh_a)} items")
print(f"   zero has {len(zero)} items (is_zero={zero.is_zero()})")
print(f"   lsh_a + zero has {len(result_with_zero)} items")
print(f"   ✓ Identity: {result_with_zero.keys == lsh_a.keys}")

# 3. Closure: result is same type
print("\n3. Closure Test:")
result = lsh_a + lsh_b
print(f"   lsh_a + lsh_b returns: {type(result).__name__}")
print(f"   ✓ Closure: {isinstance(result, MinHashLSH)}")


# Example 4: Distributed MapReduce pattern
print("\n" + "=" * 80)
print("Example 4: MapReduce Pattern with MinHashLSH")
print("=" * 80)

def map_phase(partition_id, documents):
    """Map: Build LSH index for a partition."""
    lsh = MinHashLSH(threshold=0.7, num_perm=128)
    for doc_id, words in documents.items():
        mh = create_minhash(words, num_perm=128)
        lsh.insert(doc_id, mh)
    return lsh

def reduce_phase(indexes):
    """Reduce: Combine LSH indexes using monoid operation."""
    return sum(indexes)

# Simulate distributed processing
print("\nMap Phase: Processing 4 partitions in parallel...")
partitions = [
    {f"p0_doc{i}": [f"partition0", f"doc{i}"] for i in range(5)},
    {f"p1_doc{i}": [f"partition1", f"doc{i}"] for i in range(5)},
    {f"p2_doc{i}": [f"partition2", f"doc{i}"] for i in range(5)},
    {f"p3_doc{i}": [f"partition3", f"doc{i}"] for i in range(5)},
]

# Map
partition_indexes = []
for partition_id, docs in enumerate(partitions):
    lsh = map_phase(partition_id, docs)
    partition_indexes.append(lsh)
    print(f"  Partition {partition_id}: {len(lsh)} docs indexed")

# Reduce
print("\nReduce Phase: Combining all partitions...")
final_lsh = reduce_phase(partition_indexes)

print(f"✓ Final LSH index: {len(final_lsh)} total documents")
print(f"✓ All partitions merged successfully")


# Example 5: Incremental updates
print("\n" + "=" * 80)
print("Example 5: Incremental Updates with Monoid Operations")
print("=" * 80)

# Start with base index
base_lsh = MinHashLSH(threshold=0.6, num_perm=128)
for i in range(5):
    mh = create_minhash([f"base_doc{i}"], num_perm=128)
    base_lsh.insert(f"base{i}", mh)

print(f"Base index: {len(base_lsh)} documents")

# Add new batch 1
batch1_lsh = MinHashLSH(threshold=0.6, num_perm=128)
for i in range(3):
    mh = create_minhash([f"batch1_doc{i}"], num_perm=128)
    batch1_lsh.insert(f"batch1_{i}", mh)

print(f"Batch 1: {len(batch1_lsh)} new documents")

# Add new batch 2
batch2_lsh = MinHashLSH(threshold=0.6, num_perm=128)
for i in range(2):
    mh = create_minhash([f"batch2_doc{i}"], num_perm=128)
    batch2_lsh.insert(f"batch2_{i}", mh)

print(f"Batch 2: {len(batch2_lsh)} new documents")

# Incrementally combine
updated_lsh = base_lsh + batch1_lsh + batch2_lsh

print(f"\n✓ Updated index: {len(updated_lsh)} total documents")
print(f"✓ Contains: {sorted(updated_lsh.keys)[:10]}...")


print("\n" + "=" * 80)
print("Summary")
print("=" * 80)
print("""
MinHashLSH now supports full monoid operations:

✅ combine(other) - Explicit monoid operation
✅ + operator - Pythonic syntax
✅ sum([lsh1, lsh2, ...]) - Standard Python aggregation
✅ zero property - Monoid identity
✅ is_zero() - Check for empty index

Use cases:
• Distributed LSH construction across multiple servers
• MapReduce/Spark integration
• Incremental index updates
• Merging independently-built indexes

Note: MinHash and Weighted MinHash are also monoids!
Pattern: Aggregate sketches → Build/merge indexes → Query
""")
