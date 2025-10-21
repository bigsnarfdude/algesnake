"""Probabilistic data structures and approximation algorithms.

This module provides memory-efficient approximation algorithms for:
- Cardinality estimation (HyperLogLog)
- Membership testing (Bloom Filter)
- Frequency estimation (CountMinSketch)
- Heavy hitters (TopK)
- Quantile estimation (T-Digest) - Phase 3 Week 5-6

All structures implement monoid interfaces for distributed aggregation.
"""

from .hyperloglog import HyperLogLog
from .bloom import BloomFilter
from .countminsketch import CountMinSketch
from .topk import TopK

__all__ = [
    'HyperLogLog',
    'BloomFilter',
    'CountMinSketch',
    'TopK',
]
