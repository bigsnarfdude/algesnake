"""Probabilistic data structures and approximation algorithms.

This module provides memory-efficient approximation algorithms for:
- Cardinality estimation (HyperLogLog)
- Membership testing (Bloom Filter)
- Frequency estimation (CountMinSketch) - Phase 3 Week 3-4
- Heavy hitters (TopK) - Phase 3 Week 3-4
- Quantile estimation (T-Digest) - Phase 3 Week 5-6

All structures implement monoid interfaces for distributed aggregation.
"""

from .hyperloglog import HyperLogLog
from .bloom import BloomFilter

__all__ = [
    'HyperLogLog',
    'BloomFilter',
]
