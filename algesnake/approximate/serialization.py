"""
HyperLogLog Serialization - Algebird-compatible format
=======================================================

Python implementation matching Algebird's HLLSerializer pattern:
- Binary serialization (struct-based, similar to Kryo)
- Base64 encoding for string storage
- Magic prefix for format identification
- Compatible with Redis, HTTP, Kafka, etc.

Based on Twitter's Algebird pattern:
https://github.com/twitter/algebird
"""

import struct
import base64
from typing import Optional
from algesnake.approximate.hyperloglog import HyperLogLog


class HLLSerializer:
    """
    Serializer for HyperLogLog matching Algebird pattern.

    Algebird (Scala):
        val bytes = kryo.toBytesWithClass(hll)
        val encoded = Base64.encodeBase64(bytes)
        "%%%" + new String(encoded)

    AlgeSNake (Python):
        bytes_data = struct.pack(...)
        encoded = base64.b64encode(bytes_data)
        "%%%" + encoded.decode('ascii')
    """

    MAGIC = "%%%"  # Magic prefix to identify serialized HLL

    @classmethod
    def to_magic_string(cls, hll: HyperLogLog) -> str:
        """
        Serialize HLL to magic string (Algebird-compatible).

        Equivalent to Algebird's:
            def toMagicString(hll: HLL): String

        Args:
            hll: HyperLogLog instance

        Returns:
            String like "%%%AgMBAAQDAQIA..." (magic prefix + base64 data)

        Example:
            >>> hll = HyperLogLog(precision=14)
            >>> hll.add("user_123")
            >>> serialized = HLLSerializer.to_magic_string(hll)
            >>> print(serialized)
            %%%AgMBAAQDAQIA...
        """
        # Step 1: Convert HLL to bytes (like kryo.toBytesWithClass)
        bytes_data = cls._hll_to_bytes(hll)

        # Step 2: Encode to Base64 (like Base64.encodeBase64)
        encoded = base64.b64encode(bytes_data)

        # Step 3: Create string with magic prefix
        serialized = encoded.decode('ascii')

        return cls.MAGIC + serialized

    @classmethod
    def from_magic_string(cls, hll_hash: str) -> Optional[HyperLogLog]:
        """
        Deserialize HLL from magic string (Algebird-compatible).

        Equivalent to Algebird's:
            def fromMagicString(hllHash: String): HLL

        Args:
            hll_hash: Serialized string like "%%%AgMBAAQDAQIA..."

        Returns:
            Reconstructed HyperLogLog instance, or None if invalid

        Example:
            >>> serialized = "%%%AgMBAAQDAQIA..."
            >>> hll = HLLSerializer.from_magic_string(serialized)
            >>> print(hll.cardinality())
            1
        """
        # Step 1: Remove magic prefix
        trimmed = cls._unmagic(hll_hash)

        if trimmed == "None":
            return None

        try:
            # Step 2: Decode from Base64 (like Base64.decodeBase64)
            bytes_data = base64.b64decode(trimmed)

            # Step 3: Convert bytes to HLL (like kryo.fromBytes)
            hll = cls._bytes_to_hll(bytes_data)

            return hll

        except Exception:
            return None

    @classmethod
    def _unmagic(cls, serialized: str) -> str:
        """
        Remove magic prefix from serialized string.

        Equivalent to Algebird's:
            def unMAGIC(serialized: String): String
        """
        if serialized.startswith(cls.MAGIC):
            return serialized[len(cls.MAGIC):]
        else:
            return "None"

    @classmethod
    def _hll_to_bytes(cls, hll: HyperLogLog) -> bytes:
        """
        Convert HLL to binary format (similar to Kryo serialization).

        Binary format:
            [precision: 1 byte][register_0: 1 byte][register_1: 1 byte]...

        This is more compact than Kryo but achieves the same goal.
        """
        # Pack: precision (1 byte) + all registers (1 byte each)
        fmt = f'<B{hll.m}B'  # Little-endian, unsigned bytes
        return struct.pack(fmt, hll.precision, *hll.registers)

    @classmethod
    def _bytes_to_hll(cls, data: bytes) -> HyperLogLog:
        """
        Convert binary data to HLL (similar to Kryo deserialization).

        Reconstructs HLL from packed binary format.
        """
        # Extract precision from first byte
        precision = data[0]
        m = 1 << precision

        # Unpack all bytes
        fmt = f'<B{m}B'
        unpacked = struct.unpack(fmt, data)

        # Reconstruct HLL
        hll = HyperLogLog(precision=precision)
        hll.registers = list(unpacked[1:])  # Skip first byte (precision)

        return hll

    @classmethod
    def to_bytes(cls, hll: HyperLogLog) -> bytes:
        """
        Serialize HLL directly to bytes (without Base64 encoding).

        Use this for:
        - Binary storage (Redis BYTEA, PostgreSQL)
        - Network protocols (gRPC, Thrift)
        - File storage

        Example:
            >>> hll = HyperLogLog(precision=14)
            >>> bytes_data = HLLSerializer.to_bytes(hll)
            >>> redis.set('hll:key', bytes_data)
        """
        return cls._hll_to_bytes(hll)

    @classmethod
    def from_bytes(cls, data: bytes) -> Optional[HyperLogLog]:
        """
        Deserialize HLL from raw bytes (without Base64 decoding).

        Example:
            >>> bytes_data = redis.get('hll:key')
            >>> hll = HLLSerializer.from_bytes(bytes_data)
        """
        try:
            return cls._bytes_to_hll(data)
        except Exception:
            return None


# Convenience functions matching Algebird style

def serialize_hll(hll: HyperLogLog, format: str = "magic") -> str:
    """
    Serialize HLL to string (Algebird-style API).

    Args:
        hll: HyperLogLog instance
        format: "magic" (default) or "base64"

    Returns:
        Serialized string

    Example:
        >>> hll = HyperLogLog(precision=14)
        >>> serialized = serialize_hll(hll)
        >>> print(serialized[:10])
        %%%AgMBAAQ
    """
    if format == "magic":
        return HLLSerializer.to_magic_string(hll)
    elif format == "base64":
        bytes_data = HLLSerializer.to_bytes(hll)
        return base64.b64encode(bytes_data).decode('ascii')
    else:
        raise ValueError(f"Unknown format: {format}")


def deserialize_hll(serialized: str) -> Optional[HyperLogLog]:
    """
    Deserialize HLL from string (Algebird-style API).

    Automatically detects magic prefix or plain base64.

    Example:
        >>> serialized = "%%%AgMBAAQ..."
        >>> hll = deserialize_hll(serialized)
        >>> print(hll.cardinality())
    """
    if serialized.startswith(HLLSerializer.MAGIC):
        return HLLSerializer.from_magic_string(serialized)
    else:
        # Try plain base64
        try:
            bytes_data = base64.b64decode(serialized)
            return HLLSerializer.from_bytes(bytes_data)
        except Exception:
            return None
