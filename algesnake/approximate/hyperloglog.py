"""HyperLogLog: Probabilistic cardinality estimation.

HyperLogLog (HLL) estimates the number of distinct elements in a multiset
using logarithmic space. It's perfect for counting unique users, IPs, or 
any distinct values in massive datasets.

Typical accuracy: ~2% error with 1-2 KB memory for billions of unique items.

References:
- Flajolet et al. "HyperLogLog: the analysis of a near-optimal cardinality 
  estimation algorithm" (2007)
- http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
"""

import hashlib
import math
from typing import Any, Hashable, List, Optional


class HyperLogLog:
    """HyperLogLog probabilistic cardinality counter.
    
    A HyperLogLog uses m = 2^p registers to estimate cardinality.
    Higher p values give better accuracy but use more memory.
    
    Memory usage: 2^p bytes (e.g., p=14 uses 16 KB)
    Standard error: 1.04 / sqrt(2^p)
    
    This implementation forms a monoid:
    - Identity: Empty HLL (all registers = 0)
    - Combine: Element-wise max of registers
    
    Examples:
        >>> hll = HyperLogLog(precision=14)
        >>> for user_id in user_ids:
        ...     hll.add(user_id)
        >>> print(hll.cardinality())  # Estimated unique users
        
        >>> # Distributed aggregation
        >>> hll1 = HyperLogLog(14)
        >>> hll2 = HyperLogLog(14)
        >>> combined = hll1 + hll2  # Monoid merge
    """
    
    # Bias correction constants from the paper
    _ALPHA = {
        4: 0.673,
        5: 0.697,
        6: 0.709,
        7: 0.7152,
        8: 0.7182,
    }
    
    def __init__(self, precision: int = 14):
        """Initialize HyperLogLog counter.
        
        Args:
            precision: Number of bits for register indexing (4-16).
                      Higher = more accurate but more memory.
                      Default 14 gives ~1.6% error with 16 KB.
        
        Raises:
            ValueError: If precision not in range [4, 16]
        """
        if not 4 <= precision <= 16:
            raise ValueError("Precision must be between 4 and 16")
        
        self.precision = precision
        self.m = 1 << precision  # 2^precision registers
        self.registers = [0] * self.m
        
        # Alpha constant for bias correction
        if precision <= 8:
            self.alpha = self._ALPHA.get(precision, 0.7213 / (1 + 1.079 / self.m))
        else:
            self.alpha = 0.7213 / (1 + 1.079 / self.m)
    
    def add(self, item: Hashable) -> None:
        """Add an item to the HyperLogLog.
        
        Args:
            item: Hashable value to add (user_id, IP, product_id, etc.)
        """
        # Hash the item to 64 bits
        h = self._hash(item)
        
        # Use first p bits as register index
        register_index = h & ((1 << self.precision) - 1)
        
        # Count leading zeros in remaining bits + 1
        remaining_bits = h >> self.precision
        leading_zeros = self._count_leading_zeros(remaining_bits) + 1
        
        # Update register with max value
        self.registers[register_index] = max(
            self.registers[register_index],
            leading_zeros
        )
    
    def cardinality(self) -> int:
        """Estimate the number of distinct elements.
        
        Returns:
            Estimated cardinality (may have ~2% error)
        """
        # Raw estimate using harmonic mean
        raw_estimate = self.alpha * (self.m ** 2) / sum(
            2 ** (-reg) for reg in self.registers
        )
        
        # Apply bias corrections for small/large cardinalities
        if raw_estimate <= 2.5 * self.m:
            # Small range correction
            zeros = self.registers.count(0)
            if zeros != 0:
                return int(self.m * math.log(self.m / zeros))
        
        if raw_estimate <= (1/30) * (1 << 32):
            # No correction needed
            return int(raw_estimate)
        else:
            # Large range correction
            # Avoid math domain error if raw_estimate is too large
            ratio = raw_estimate / (1 << 32)
            if ratio >= 1:
                return int(raw_estimate)
            return int(-1 * (1 << 32) * math.log(1 - ratio))
    
    def combine(self, other: 'HyperLogLog') -> 'HyperLogLog':
        """Combine two HyperLogLogs (monoid operation).
        
        Args:
            other: Another HyperLogLog with same precision
        
        Returns:
            New HyperLogLog with merged registers
        
        Raises:
            ValueError: If precisions don't match
        """
        if self.precision != other.precision:
            raise ValueError(
                f"Cannot combine HLLs with different precisions: "
                f"{self.precision} vs {other.precision}"
            )
        
        result = HyperLogLog(self.precision)
        result.registers = [
            max(a, b) for a, b in zip(self.registers, other.registers)
        ]
        return result
    
    def __add__(self, other: 'HyperLogLog') -> 'HyperLogLog':
        """Combine using + operator (monoid operation)."""
        return self.combine(other)
    
    def __radd__(self, other):
        """Support sum() builtin."""
        if other == 0:
            return self
        return self.__add__(other)
    
    def __eq__(self, other: Any) -> bool:
        """Check equality based on registers."""
        if not isinstance(other, HyperLogLog):
            return False
        return (
            self.precision == other.precision and
            self.registers == other.registers
        )
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"HyperLogLog(precision={self.precision}, "
            f"cardinality≈{self.cardinality()})"
        )
    
    @property
    def zero(self) -> 'HyperLogLog':
        """Monoid identity: empty HyperLogLog."""
        return HyperLogLog(self.precision)
    
    def is_zero(self) -> bool:
        """Check if this is the zero element."""
        return all(reg == 0 for reg in self.registers)
    
    def _hash(self, item: Hashable) -> int:
        """Hash an item to 64-bit integer.

        Uses SHA-256 for good distribution across all types.
        """
        # Always hash to ensure good distribution
        h = hashlib.sha256(str(item).encode('utf-8')).digest()
        return int.from_bytes(h[:8], byteorder='big')
    
    def _count_leading_zeros(self, bits: int) -> int:
        """Count leading zeros in a 64-bit integer.
        
        Args:
            bits: Integer to count leading zeros in
        
        Returns:
            Number of leading zeros (0-64)
        """
        if bits == 0:
            return 64 - self.precision
        
        count = 0
        # Check 64 bits (minus precision bits already used)
        max_bits = 64 - self.precision
        
        for i in range(max_bits - 1, -1, -1):
            if bits & (1 << i):
                break
            count += 1
        
        return count
    
    def merge_many(self, others: List['HyperLogLog']) -> 'HyperLogLog':
        """Merge multiple HyperLogLogs efficiently.
        
        Args:
            others: List of HyperLogLogs to merge
        
        Returns:
            Merged HyperLogLog
        """
        result = self
        for other in others:
            result = result.combine(other)
        return result


# Convenience functions
def estimate_cardinality(items: List[Hashable], precision: int = 14) -> int:
    """Estimate cardinality of a list using HyperLogLog.
    
    Args:
        items: List of hashable items
        precision: HLL precision parameter (4-16)
    
    Returns:
        Estimated number of unique items
    
    Examples:
        >>> users = ['user1', 'user2', 'user1', 'user3']
        >>> estimate_cardinality(users)
        3
    """
    hll = HyperLogLog(precision)
    for item in items:
        hll.add(item)
    return hll.cardinality()


def merge_hlls(hlls: List[HyperLogLog]) -> HyperLogLog:
    """Merge multiple HyperLogLogs using sum().
    
    Args:
        hlls: List of HyperLogLogs with same precision
    
    Returns:
        Merged HyperLogLog
    
    Examples:
        >>> hll1, hll2, hll3 = [HyperLogLog(14) for _ in range(3)]
        >>> merged = merge_hlls([hll1, hll2, hll3])
    """
    if not hlls:
        raise ValueError("Cannot merge empty list of HLLs")
    
    return sum(hlls)
