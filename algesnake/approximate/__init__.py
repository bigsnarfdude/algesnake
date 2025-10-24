"""Probabilistic data structures and approximation algorithms.

This module provides memory-efficient approximation algorithms for:
- Cardinality estimation (HyperLogLog)
- Membership testing (Bloom Filter)
- Frequency estimation (CountMinSketch)
- Heavy hitters (TopK)
- Quantile estimation (T-Digest)
- Similarity search (MinHash, Weighted MinHash)
- LSH indexing (MinHash LSH, LSH Forest, LSH Ensemble)
- Approximate nearest neighbor search (HNSW)

All structures implement monoid interfaces for distributed aggregation.
"""

from .hyperloglog import HyperLogLog
from .bloom import BloomFilter
from .countminsketch import CountMinSketch
from .topk import TopK
from .tdigest import TDigest
from .minhash import MinHash
from .weighted_minhash import WeightedMinHash
from .minhash_lsh import MinHashLSH
from .minhash_lsh_forest import MinHashLSHForest
from .minhash_lsh_ensemble import MinHashLSHEnsemble
from .hnsw import HNSW

__all__ = [
    'HyperLogLog',
    'BloomFilter',
    'CountMinSketch',
    'TopK',
    'TDigest',
    'MinHash',
    'WeightedMinHash',
    'MinHashLSH',
    'MinHashLSHForest',
    'MinHashLSHEnsemble',
    'HNSW',
]
