"""Real-world examples of similarity search with MinHash, LSH, and HNSW.

This module demonstrates practical applications of:
- MinHash: Jaccard similarity estimation
- Weighted MinHash: Weighted similarity
- MinHash LSH: Fast similarity search
- LSH Forest: Top-K nearest neighbors
- LSH Ensemble: Containment queries
- HNSW: Approximate nearest neighbor search
"""

from algesnake.approximate import (
    MinHash,
    WeightedMinHash,
    MinHashLSH,
    MinHashLSHForest,
    MinHashLSHEnsemble,
    HNSW
)
from algesnake.approximate.minhash import create_minhash, estimate_jaccard
from algesnake.approximate.weighted_minhash import create_weighted_minhash
from algesnake.approximate.minhash_lsh import build_lsh_index
from algesnake.approximate.minhash_lsh_forest import build_lsh_forest
import math


# Example 1: Document Similarity with MinHash
print("=" * 70)
print("Example 1: Document Similarity Detection")
print("=" * 70)

# Sample documents
documents = {
    "doc1": "the quick brown fox jumps over the lazy dog",
    "doc2": "the quick brown dog jumps over the lazy fox",
    "doc3": "a fast brown fox leaps over a sleepy dog",
    "doc4": "hello world this is completely different",
}

# Convert documents to word sets and create MinHash signatures
doc_minhashes = {}
for doc_id, text in documents.items():
    words = text.split()
    mh = create_minhash(words, num_perm=128)
    doc_minhashes[doc_id] = mh

# Compare all pairs
print("\nPairwise Jaccard similarities:")
for i, (doc1_id, mh1) in enumerate(doc_minhashes.items()):
    for doc2_id, mh2 in list(doc_minhashes.items())[i+1:]:
        similarity = mh1.jaccard(mh2)
        print(f"  {doc1_id} vs {doc2_id}: {similarity:.3f}")

print("\nObservation: doc1 and doc2 are very similar (many common words)")
print("             doc4 is different from all others")


# Example 2: Weighted Document Similarity (TF-based)
print("\n" + "=" * 70)
print("Example 2: Weighted Similarity with Term Frequencies")
print("=" * 70)

# Documents with term frequencies
doc_term_freq = {
    "article1": {"machine": 10, "learning": 10, "data": 5, "science": 5},
    "article2": {"machine": 8, "learning": 8, "neural": 6, "network": 6},
    "article3": {"cooking": 10, "recipe": 8, "food": 6, "kitchen": 4},
}

# Create weighted MinHash signatures
weighted_minhashes = {}
for doc_id, term_freq in doc_term_freq.items():
    wmh = create_weighted_minhash(term_freq, num_perm=128)
    weighted_minhashes[doc_id] = wmh

print("\nWeighted Jaccard similarities:")
for i, (doc1_id, wmh1) in enumerate(weighted_minhashes.items()):
    for doc2_id, wmh2 in list(weighted_minhashes.items())[i+1:]:
        similarity = wmh1.jaccard(wmh2)
        print(f"  {doc1_id} vs {doc2_id}: {similarity:.3f}")

print("\nObservation: article1 and article2 are similar (ML/AI topics)")
print("             article3 is different (cooking topic)")


# Example 3: Fast Similarity Search with LSH
print("\n" + "=" * 70)
print("Example 3: Fast Duplicate Detection with LSH")
print("=" * 70)

# Simulate a large document collection
print("\nIndexing 1000 documents...")

# Create LSH index with threshold 0.7
lsh = MinHashLSH(threshold=0.7, num_perm=128)

# Insert documents
for i in range(1000):
    # Generate pseudo-random documents
    words = [f"word{j % 100}" for j in range(i, i + 20)]
    mh = create_minhash(words, num_perm=128)
    lsh.insert(f"doc{i}", mh)

print(f"Indexed {len(lsh)} documents")
print(f"Index statistics: {lsh.get_counts()}")

# Query for duplicates
print("\nQuerying for similar documents...")
query_words = [f"word{j}" for j in range(10, 30)]
query_mh = create_minhash(query_words, num_perm=128)

results = lsh.query_with_similarity(query_mh)
print(f"\nFound {len(results)} documents with similarity >= 0.7:")
for doc_id, similarity in results[:5]:
    print(f"  {doc_id}: {similarity:.3f}")

print("\nTime complexity: O(n^0.6) instead of O(n) brute force!")


# Example 4: Top-K Recommendation with LSH Forest
print("\n" + "=" * 70)
print("Example 4: Content Recommendation with LSH Forest")
print("=" * 70)

# Movie descriptions (simplified as word sets)
movies = {
    "movie1": ["action", "adventure", "thriller", "hero", "villain"],
    "movie2": ["action", "adventure", "comedy", "hero", "sidekick"],
    "movie3": ["romance", "drama", "love", "relationship", "emotional"],
    "movie4": ["action", "sci-fi", "future", "robots", "technology"],
    "movie5": ["comedy", "romance", "funny", "love", "happy"],
}

# Build LSH Forest
movie_minhashes = {
    movie_id: create_minhash(tags, num_perm=128)
    for movie_id, tags in movies.items()
}

forest = build_lsh_forest(movie_minhashes)
print(f"Built LSH Forest with {len(forest)} movies")

# User watches movie1 (action/adventure), find similar movies
print("\nUser watched: movie1 (action adventure thriller)")
query = create_minhash(["action", "adventure", "thriller"], num_perm=128)

recommendations = forest.query_with_similarity(query, k=3)
print("\nTop 3 recommendations:")
for movie_id, similarity in recommendations:
    print(f"  {movie_id}: {similarity:.3f} - {movies[movie_id][:3]}")


# Example 5: Subset Search with LSH Ensemble
print("\n" + "=" * 70)
print("Example 5: Tag-based Search with LSH Ensemble")
print("=" * 70)

# Products with tags of varying sizes
products = {
    "prod1": (["laptop", "computer"], 2),
    "prod2": (["laptop", "computer", "gaming"], 3),
    "prod3": (["laptop", "computer", "gaming", "rgb", "high-end"], 5),
    "prod4": (["phone", "mobile"], 2),
    "prod5": (["phone", "mobile", "smartphone", "android"], 4),
}

# Build LSH Ensemble for containment queries
ensemble = MinHashLSHEnsemble(threshold=0.6, num_perm=128, num_part=4)

for prod_id, (tags, size) in products.items():
    mh = create_minhash(tags, num_perm=128)
    ensemble.insert(prod_id, mh, size)

ensemble.index()
print(f"Indexed {len(ensemble)} products")

# Search for products containing ["laptop", "gaming"]
print("\nSearching for products matching: ['laptop', 'gaming']")
query_tags = ["laptop", "gaming"]
query_mh = create_minhash(query_tags, num_perm=128)

results = ensemble.query_with_containment(query_mh, size=len(query_tags))
print(f"\nFound {len(results)} products:")
for prod_id, containment in results:
    tags, _ = products[prod_id]
    print(f"  {prod_id}: {containment:.3f} - {tags}")


# Example 6: Image Feature Search with HNSW
print("\n" + "=" * 70)
print("Example 6: Visual Search with HNSW")
print("=" * 70)


def euclidean_distance(v1, v2):
    """Euclidean distance for vectors."""
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(v1, v2)))


# Simulate image feature vectors (in reality these would be from CNN)
image_features = {
    "img1": [0.1, 0.2, 0.3, 0.1, 0.5],  # "cat" features
    "img2": [0.15, 0.25, 0.25, 0.15, 0.45],  # similar to cat
    "img3": [0.9, 0.1, 0.05, 0.8, 0.1],  # "dog" features
    "img4": [0.85, 0.15, 0.1, 0.75, 0.15],  # similar to dog
    "img5": [0.5, 0.5, 0.5, 0.5, 0.5],  # generic/unclear
}

# Build HNSW index
hnsw = HNSW(distance_func=euclidean_distance, m=16, ef_construction=200, ef=50)

for img_id, features in image_features.items():
    hnsw.insert(img_id, features)

print(f"Indexed {len(hnsw)} images")
print(f"HNSW stats: {hnsw.get_stats()}")

# Query with a "cat-like" image
print("\nQuerying with cat-like image features: [0.12, 0.22, 0.28, ...]")
query_features = [0.12, 0.22, 0.28, 0.12, 0.48]

similar_images = hnsw.query_with_distances(query_features, k=3)
print("\nTop 3 similar images:")
for img_id, distance in similar_images:
    print(f"  {img_id}: distance = {distance:.4f}")

print("\nTime complexity: O(log N) instead of O(N) brute force!")


# Example 7: Distributed Aggregation with MinHash (Monoid Property)
print("\n" + "=" * 70)
print("Example 7: Distributed Log Analysis (Monoid Aggregation)")
print("=" * 70)

# Simulate logs from 3 servers
server1_logs = ["error", "warning", "info", "debug", "error"]
server2_logs = ["warning", "info", "error", "critical"]
server3_logs = ["info", "debug", "warning"]

# Each server computes its own MinHash
server1_mh = create_minhash(server1_logs, num_perm=128)
server2_mh = create_minhash(server2_logs, num_perm=128)
server3_mh = create_minhash(server3_logs, num_perm=128)

# Aggregate using monoid operation (union)
global_mh = server1_mh + server2_mh + server3_mh

# Compare individual server vs global
print("\nServer similarities to global log:")
for i, mh in enumerate([server1_mh, server2_mh, server3_mh], 1):
    similarity = mh.jaccard(global_mh)
    print(f"  Server {i}: {similarity:.3f}")

# Using sum() builtin
all_servers = sum([server1_mh, server2_mh, server3_mh])
print(f"\nCombined using sum(): {all_servers == global_mh}")

print("\nMonoid properties enable:")
print("  ✓ Parallel processing across servers")
print("  ✓ Incremental updates")
print("  ✓ Fault tolerance")


# Example 8: Plagiarism Detection
print("\n" + "=" * 70)
print("Example 8: Plagiarism Detection System")
print("=" * 70)

# Student submissions
submissions = {
    "student1": "the algorithm uses dynamic programming to solve the problem efficiently",
    "student2": "the algorithm utilizes dynamic programming to solve the problem effectively",
    "student3": "recursive approach with memoization is used for this algorithmic problem",
    "student4": "my solution involves a greedy approach with sorting",
}

# Create shingles (3-grams) for better plagiarism detection
def create_shingles(text, k=3):
    """Create k-grams from text."""
    words = text.split()
    return [" ".join(words[i:i+k]) for i in range(len(words) - k + 1)]


# Build plagiarism detector with LSH
plagiarism_lsh = MinHashLSH(threshold=0.5, num_perm=256)  # Higher precision

for student_id, text in submissions.items():
    shingles = create_shingles(text, k=3)
    mh = create_minhash(shingles, num_perm=256)
    plagiarism_lsh.insert(student_id, mh)

print(f"Analyzed {len(plagiarism_lsh)} submissions")

# Check each submission for plagiarism
print("\nPlagiarism detection results:")
for student_id, text in submissions.items():
    shingles = create_shingles(text, k=3)
    query_mh = create_minhash(shingles, num_perm=256)

    matches = plagiarism_lsh.query_with_similarity(query_mh)

    # Filter out self-matches
    matches = [(sid, sim) for sid, sim in matches if sid != student_id]

    if matches:
        print(f"\n{student_id}: ⚠️  Potential plagiarism detected!")
        for match_id, similarity in matches:
            print(f"  Similar to {match_id}: {similarity:.3f}")
    else:
        print(f"\n{student_id}: ✓ No plagiarism detected")


print("\n" + "=" * 70)
print("Examples complete! Summary:")
print("=" * 70)
print("""
✓ MinHash: Fast Jaccard similarity estimation (O(k) space)
✓ Weighted MinHash: Handles weighted sets (term frequencies)
✓ MinHash LSH: Sub-linear similarity search (O(n^ρ) time)
✓ LSH Forest: Top-K queries without threshold
✓ LSH Ensemble: Optimized for containment queries
✓ HNSW: Approximate nearest neighbor (O(log n) time)

All structures support monoid operations for distributed processing!
""")
