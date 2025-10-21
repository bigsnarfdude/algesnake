"""Probabilistic data structures and approximation algorithms.

This module provides memory-efficient approximation algorithms for:
- Cardinality estimation (HyperLogLog)
- Membership testing (Bloom Filter)
- Frequency estimation (CountMinSketch)
- Heavy hitters (TopK)
- Quantile estimation (T-Digest)

All structures implement monoid interfaces for distributed aggregation.
"""

from .hyperloglog import HyperLogLog
from .bloom import BloomFilter
from .countminsketch import CountMinSketch
from .topk import TopK
from .tdigest import TDigest

__all__ = [
    'HyperLogLog',
    'BloomFilter',
    'CountMinSketch',
    'TopK',
    'TDigest',
]
