"""
HyperLogLog Storage and Serialization
======================================

This example shows:
1. What's inside an HLL object (internal structure)
2. How to serialize HLL to disk (save/load)
3. How to store HLL in databases (Redis, PostgreSQL, etc.)
4. How to send HLL between servers (JSON, msgpack, etc.)
"""

from algesnake.approximate import HyperLogLog
import pickle
import json
import base64
import struct
from datetime import datetime


def example_1_inspect_hll_internals():
    """Example 1: What's inside an HLL object?"""
    print("=" * 70)
    print("EXAMPLE 1: HyperLogLog Internal Structure")
    print("=" * 70 + "\n")

    hll = HyperLogLog(precision=14)

    # Add some users
    for i in range(1000):
        hll.add(f"user_{i}")

    # Inspect internal structure
    print("HyperLogLog Object Attributes:")
    print(f"  precision:     {hll.precision}")
    print(f"  m (registers): {hll.m}")
    print(f"  alpha:         {hll.alpha}")
    print(f"  registers:     [list of {len(hll.registers)} integers]")
    print()

    # Show what the registers look like
    print("Register Values (first 20):")
    print(f"  {hll.registers[:20]}")
    print()

    # Check register value distribution
    non_zero = sum(1 for r in hll.registers if r > 0)
    print(f"Non-zero registers: {non_zero} / {hll.m} ({non_zero/hll.m*100:.1f}%)")
    print()

    # Memory calculation
    import sys
    object_size = sys.getsizeof(hll)
    registers_size = sys.getsizeof(hll.registers)
    print(f"Memory Usage:")
    print(f"  HLL object:    {object_size} bytes")
    print(f"  Registers:     {registers_size} bytes")
    print(f"  Total:         {object_size + registers_size} bytes (~{(object_size + registers_size)/1024:.1f} KB)")
    print()


def example_2_pickle_serialization():
    """Example 2: Save/load HLL using pickle."""
    print("=" * 70)
    print("EXAMPLE 2: Pickle Serialization (Python-native)")
    print("=" * 70 + "\n")

    # Create HLL with data
    hll = HyperLogLog(precision=14)
    for i in range(10000):
        hll.add(f"user_{i}")

    original_count = hll.cardinality()
    print(f"Original HLL: {original_count} unique users")
    print()

    # Save to disk using pickle
    filename = "/tmp/hll_pickle_test.pkl"
    print(f"💾 Saving HLL to {filename}...")

    with open(filename, 'wb') as f:
        pickle.dump(hll, f)

    import os
    file_size = os.path.getsize(filename)
    print(f"   File size: {file_size} bytes (~{file_size/1024:.1f} KB)")
    print()

    # Load from disk
    print(f"📂 Loading HLL from {filename}...")
    with open(filename, 'rb') as f:
        loaded_hll = pickle.load(f)

    loaded_count = loaded_hll.cardinality()
    print(f"   Loaded HLL: {loaded_count} unique users")
    print(f"   Match: {'✓ YES' if loaded_count == original_count else '✗ NO'}")
    print()


def example_3_json_serialization():
    """Example 3: Serialize HLL to JSON (for APIs/databases)."""
    print("=" * 70)
    print("EXAMPLE 3: JSON Serialization (for APIs/databases)")
    print("=" * 70 + "\n")

    # Create HLL
    hll = HyperLogLog(precision=12)  # Smaller for demo
    for i in range(5000):
        hll.add(f"user_{i}")

    original_count = hll.cardinality()
    print(f"Original HLL: {original_count} unique users")
    print()

    # Serialize to JSON-compatible dict
    def hll_to_dict(hll):
        """Convert HLL to JSON-serializable dict."""
        return {
            'precision': hll.precision,
            'm': hll.m,
            'alpha': hll.alpha,
            'registers': hll.registers,  # List of integers
            'created_at': datetime.now().isoformat()
        }

    hll_dict = hll_to_dict(hll)
    json_str = json.dumps(hll_dict)

    print(f"💾 Serialized to JSON:")
    print(f"   JSON size: {len(json_str)} bytes (~{len(json_str)/1024:.1f} KB)")
    print(f"   Sample: {json_str[:100]}...")
    print()

    # Deserialize from JSON
    def dict_to_hll(data):
        """Reconstruct HLL from dict."""
        hll = HyperLogLog(precision=data['precision'])
        hll.registers = data['registers']
        return hll

    loaded_dict = json.loads(json_str)
    loaded_hll = dict_to_hll(loaded_dict)

    loaded_count = loaded_hll.cardinality()
    print(f"📂 Deserialized from JSON:")
    print(f"   Loaded HLL: {loaded_count} unique users")
    print(f"   Match: {'✓ YES' if loaded_count == original_count else '✗ NO'}")
    print()


def example_4_compact_binary():
    """Example 4: Compact binary format (most efficient)."""
    print("=" * 70)
    print("EXAMPLE 4: Compact Binary Format (smallest size)")
    print("=" * 70 + "\n")

    # Create HLL
    hll = HyperLogLog(precision=14)
    for i in range(10000):
        hll.add(f"user_{i}")

    original_count = hll.cardinality()
    print(f"Original HLL: {original_count} unique users")
    print()

    # Serialize to compact binary
    def hll_to_bytes(hll):
        """Convert HLL to compact binary format."""
        # Pack: precision (1 byte) + registers (each 1 byte)
        fmt = f'<B{hll.m}B'  # '<' = little-endian, 'B' = unsigned byte
        data = struct.pack(fmt, hll.precision, *hll.registers)
        return data

    binary_data = hll_to_bytes(hll)

    print(f"💾 Binary serialization:")
    print(f"   Size: {len(binary_data)} bytes ({len(binary_data)/1024:.1f} KB)")
    print(f"   First 20 bytes: {binary_data[:20].hex()}")
    print()

    # Deserialize from binary
    def bytes_to_hll(data):
        """Reconstruct HLL from binary data."""
        precision = data[0]
        m = 1 << precision
        fmt = f'<B{m}B'
        unpacked = struct.unpack(fmt, data)

        hll = HyperLogLog(precision=precision)
        hll.registers = list(unpacked[1:])  # Skip first byte (precision)
        return hll

    loaded_hll = bytes_to_hll(binary_data)

    loaded_count = loaded_hll.cardinality()
    print(f"📂 Binary deserialization:")
    print(f"   Loaded HLL: {loaded_count} unique users")
    print(f"   Match: {'✓ YES' if loaded_count == original_count else '✗ NO'}")
    print()


def example_5_redis_storage():
    """Example 5: Store HLL in Redis (pseudo-code)."""
    print("=" * 70)
    print("EXAMPLE 5: Redis Storage Pattern (pseudo-code)")
    print("=" * 70 + "\n")

    hll = HyperLogLog(precision=14)
    for i in range(5000):
        hll.add(f"user_{i}")

    # Convert to base64 for Redis string storage
    def hll_to_base64(hll):
        """Encode HLL as base64 string for Redis."""
        fmt = f'<B{hll.m}B'
        binary_data = struct.pack(fmt, hll.precision, *hll.registers)
        return base64.b64encode(binary_data).decode('ascii')

    base64_str = hll_to_base64(hll)

    print("Redis Storage Pattern:")
    print()
    print("# Save HLL to Redis")
    print(f"redis_client.set('hll:daily:2025-10-23', '{base64_str[:50]}...')")
    print()
    print("# Load HLL from Redis")
    print("encoded_hll = redis_client.get('hll:daily:2025-10-23')")
    print("hll = base64_decode_to_hll(encoded_hll)")
    print()
    print(f"Encoded size: {len(base64_str)} characters")
    print()


def example_6_database_storage():
    """Example 6: Store HLL in PostgreSQL/MySQL."""
    print("=" * 70)
    print("EXAMPLE 6: Database Storage Pattern (SQL)")
    print("=" * 70 + "\n")

    hll = HyperLogLog(precision=14)
    for i in range(10000):
        hll.add(f"user_{i}")

    # Convert to binary for BYTEA/BLOB storage
    def hll_to_bytes(hll):
        fmt = f'<B{hll.m}B'
        return struct.pack(fmt, hll.precision, *hll.registers)

    binary_data = hll_to_bytes(hll)

    print("PostgreSQL Storage Pattern:")
    print()
    print("-- Create table")
    print("CREATE TABLE daily_user_stats (")
    print("    date DATE PRIMARY KEY,")
    print("    hll_data BYTEA NOT NULL,")
    print("    cardinality INTEGER")
    print(");")
    print()
    print("-- Insert HLL")
    print("INSERT INTO daily_user_stats (date, hll_data, cardinality)")
    print("VALUES (")
    print("    '2025-10-23',")
    print(f"    %s,  -- binary data ({len(binary_data)} bytes)")
    print(f"    {int(hll.cardinality())}")
    print(");")
    print()
    print("-- Retrieve HLL")
    print("SELECT hll_data, cardinality")
    print("FROM daily_user_stats")
    print("WHERE date = '2025-10-23';")
    print()


def example_7_multi_day_storage():
    """Example 7: Store multiple days and reconstruct monthly."""
    print("=" * 70)
    print("EXAMPLE 7: Multi-Day Storage & Monthly Reconstruction")
    print("=" * 70 + "\n")

    # Simulate 7 days of HLLs
    print("Creating 7 days of HLL data...")
    daily_hlls = {}

    from datetime import timedelta
    base_date = datetime.now()

    for day_offset in range(7):
        date = (base_date - timedelta(days=day_offset)).date()
        hll = HyperLogLog(precision=14)

        # Add random users
        import random
        for _ in range(5000):
            user_id = f"user_{random.randint(1, 20000)}"
            hll.add(user_id)

        daily_hlls[date] = hll
        print(f"  {date}: {int(hll.cardinality())} unique users")

    print()

    # Save all to disk
    storage_dir = "/tmp/hll_storage"
    import os
    os.makedirs(storage_dir, exist_ok=True)

    print(f"💾 Saving HLLs to {storage_dir}/...")
    total_size = 0

    for date, hll in daily_hlls.items():
        filename = f"{storage_dir}/hll_{date}.pkl"
        with open(filename, 'wb') as f:
            pickle.dump(hll, f)
        file_size = os.path.getsize(filename)
        total_size += file_size

    print(f"   Saved 7 files, total: {total_size/1024:.1f} KB")
    print()

    # Load and merge for weekly stats
    print(f"📂 Loading HLLs from {storage_dir}/...")
    loaded_hlls = []

    for date in daily_hlls.keys():
        filename = f"{storage_dir}/hll_{date}.pkl"
        with open(filename, 'rb') as f:
            hll = pickle.load(f)
            loaded_hlls.append(hll)

    print(f"   Loaded 7 HLLs")
    print()

    # Merge using monoid operation
    weekly_hll = sum(loaded_hlls)

    print("📊 Weekly Stats (merged from 7 files):")
    print(f"   Weekly unique users: {int(weekly_hll.cardinality())}")
    print()


def example_8_format_comparison():
    """Example 8: Compare different serialization formats."""
    print("=" * 70)
    print("EXAMPLE 8: Serialization Format Comparison")
    print("=" * 70 + "\n")

    hll = HyperLogLog(precision=14)
    for i in range(10000):
        hll.add(f"user_{i}")

    print("Serialization Size Comparison:")
    print()

    # Pickle
    import pickle
    pickle_data = pickle.dumps(hll)
    print(f"  Pickle:         {len(pickle_data):>6} bytes ({len(pickle_data)/1024:.1f} KB)")

    # JSON
    hll_dict = {
        'precision': hll.precision,
        'registers': hll.registers
    }
    json_data = json.dumps(hll_dict).encode()
    print(f"  JSON:           {len(json_data):>6} bytes ({len(json_data)/1024:.1f} KB)")

    # Binary (compact)
    fmt = f'<B{hll.m}B'
    binary_data = struct.pack(fmt, hll.precision, *hll.registers)
    print(f"  Binary:         {len(binary_data):>6} bytes ({len(binary_data)/1024:.1f} KB)")

    # Base64 (for strings)
    base64_data = base64.b64encode(binary_data)
    print(f"  Base64:         {len(base64_data):>6} bytes ({len(base64_data)/1024:.1f} KB)")

    print()
    print(f"✅ Most efficient: Binary format ({len(binary_data)} bytes)")
    print(f"   Just stores: precision (1 byte) + {hll.m} register values (1 byte each)")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("HYPERLOGLOG STORAGE & SERIALIZATION")
    print("=" * 70 + "\n")

    example_1_inspect_hll_internals()
    print("\n")

    example_2_pickle_serialization()
    print("\n")

    example_3_json_serialization()
    print("\n")

    example_4_compact_binary()
    print("\n")

    example_5_redis_storage()
    print("\n")

    example_6_database_storage()
    print("\n")

    example_7_multi_day_storage()
    print("\n")

    example_8_format_comparison()

    print("=" * 70)
    print("Examples complete!")
    print("=" * 70)
