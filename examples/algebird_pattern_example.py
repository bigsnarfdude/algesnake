"""
Algebird-Pattern Serialization Example
=======================================

This example matches the exact pattern from:
- https://github.com/bigsnarfdude/addifier
- https://github.com/bigsnarfdude/akka-http-algebird

Scala (Algebird):
    val kryo = KryoPool.withByteArrayOutputStream(...)
    val bytes = kryo.toBytesWithClass(hll)
    val encoded = Base64.encodeBase64(bytes)
    "%%%" + new String(encoded)

Python (AlgeSNake):
    bytes_data = struct.pack(...)
    encoded = base64.b64encode(bytes_data)
    "%%%" + encoded.decode('ascii')
"""

from algesnake.approximate import HyperLogLog
from algesnake.approximate.serialization import (
    HLLSerializer,
    serialize_hll,
    deserialize_hll
)

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None


def example_1_basic_serialization():
    """Example 1: Basic magic string serialization."""
    print("=" * 70)
    print("EXAMPLE 1: Magic String Serialization (Algebird Pattern)")
    print("=" * 70 + "\n")

    # Create HLL
    hll = HyperLogLog(precision=14)
    for i in range(10000):
        hll.add(f"user_{i}")

    original_count = hll.cardinality()
    print(f"Original HLL: {original_count} unique users")
    print()

    # Serialize to magic string (matching Algebird's toMagicString)
    print("📝 Serializing with HLLSerializer.to_magic_string()...")
    magic_string = HLLSerializer.to_magic_string(hll)

    print(f"   Magic prefix: '{magic_string[:3]}'")
    print(f"   Total length: {len(magic_string)} characters")
    print(f"   Sample: {magic_string[:50]}...")
    print()

    # Deserialize from magic string (matching Algebird's fromMagicString)
    print("📂 Deserializing with HLLSerializer.from_magic_string()...")
    loaded_hll = HLLSerializer.from_magic_string(magic_string)

    loaded_count = loaded_hll.cardinality()
    print(f"   Loaded HLL: {loaded_count} unique users")
    print(f"   Match: {'✓ YES' if abs(loaded_count - original_count) < 1 else '✗ NO'}")
    print()


def example_2_redis_storage():
    """Example 2: Redis storage (Addifier pattern)."""
    print("=" * 70)
    print("EXAMPLE 2: Redis Storage (Addifier Pattern)")
    print("=" * 70 + "\n")

    # Simulate the Addifier pattern
    print("Simulating Addifier-style Redis aggregation...")
    print()

    # Create Redis client (or mock if not available)
    redis_available = False
    if REDIS_AVAILABLE:
        try:
            r = redis.Redis(decode_responses=False)
            r.ping()
            redis_available = True
            print("✓ Redis connection successful")
        except Exception:
            print("⚠ Redis not available, showing pattern only")
    else:
        print("⚠ Redis module not installed, showing pattern only")

    print()

    # Pattern 1: Store daily HLL
    date = "2025-10-23"
    key = f"hll:daily:{date}"

    hll = HyperLogLog(precision=14)
    for i in range(5000):
        hll.add(f"user_{i}")

    # Serialize to magic string
    magic_string = HLLSerializer.to_magic_string(hll)

    print(f"Storing HLL for {date}:")
    print(f"  Key: {key}")
    print(f"  Value: {magic_string[:50]}...")
    print(f"  Size: {len(magic_string)} chars")

    if redis_available:
        # Store in Redis (as string)
        r.set(key, magic_string)
        print(f"  ✓ Stored in Redis")
    else:
        print(f"  (Would store: r.set('{key}', magic_string))")

    print()

    # Pattern 2: Retrieve and query
    print(f"Retrieving HLL from Redis:")

    if redis_available:
        stored_string = r.get(key).decode('ascii')
        retrieved_hll = HLLSerializer.from_magic_string(stored_string)
        count = retrieved_hll.cardinality()
        print(f"  ✓ Retrieved from Redis")
        print(f"  Count: {count} unique users")
    else:
        print(f"  (Would retrieve: stored = r.get('{key}'))")
        print(f"  (Then deserialize: hll = from_magic_string(stored))")

    print()


def example_3_addifier_pipeline():
    """Example 3: Complete Addifier-style pipeline."""
    print("=" * 70)
    print("EXAMPLE 3: Complete Addifier Pipeline")
    print("=" * 70 + "\n")

    class AddifierPipeline:
        """
        Python equivalent to Addifier's HLL aggregation pipeline.

        Matches the pattern from:
        https://github.com/bigsnarfdude/addifier
        """

        def __init__(self, precision=14):
            self.precision = precision
            self.redis_available = False
            self._mock_storage = {}

            if REDIS_AVAILABLE:
                try:
                    self.redis = redis.Redis(decode_responses=False)
                    self.redis.ping()
                    self.redis_available = True
                except Exception:
                    pass

        def add_event(self, date: str, user_id: str):
            """
            Add user event to daily HLL.

            Scala equivalent:
                def addEvent(date: String, userId: String): Unit
            """
            key = f"hll:daily:{date}"

            # Get existing HLL or create new (like Algebird's monoid.zero)
            if self.redis_available:
                existing = self.redis.get(key)
                if existing:
                    hll = HLLSerializer.from_magic_string(existing.decode('ascii'))
                else:
                    hll = HyperLogLog(precision=self.precision)
            else:
                existing = self._mock_storage.get(key)
                if existing:
                    hll = HLLSerializer.from_magic_string(existing)
                else:
                    hll = HyperLogLog(precision=self.precision)

            # Add new user (like monoid combine)
            hll.add(user_id)

            # Serialize and store
            magic_string = HLLSerializer.to_magic_string(hll)

            if self.redis_available:
                self.redis.set(key, magic_string)
            else:
                self._mock_storage[key] = magic_string

        def get_cardinality(self, date: str) -> int:
            """
            Get unique user count for date.

            Scala equivalent:
                def getCardinality(date: String): Long
            """
            key = f"hll:daily:{date}"

            if self.redis_available:
                existing = self.redis.get(key)
                if not existing:
                    return 0
                hll = HLLSerializer.from_magic_string(existing.decode('ascii'))
            else:
                existing = self._mock_storage.get(key)
                if not existing:
                    return 0
                hll = HLLSerializer.from_magic_string(existing)

            return int(hll.cardinality())

        def merge_week(self, dates: list) -> int:
            """
            Merge multiple days for weekly count.

            Scala equivalent:
                def mergeWeek(dates: Seq[String]): Long
            """
            hlls = []

            for date in dates:
                key = f"hll:daily:{date}"

                if self.redis_available:
                    existing = self.redis.get(key)
                    if existing:
                        hll = HLLSerializer.from_magic_string(existing.decode('ascii'))
                        hlls.append(hll)
                else:
                    existing = self._mock_storage.get(key)
                    if existing:
                        hll = HLLSerializer.from_magic_string(existing)
                        hlls.append(hll)

            if not hlls:
                return 0

            # Merge using monoid sum (like Algebird's monoid.sum)
            merged = sum(hlls)
            return int(merged.cardinality())

    # Usage example
    pipeline = AddifierPipeline(precision=14)

    storage_mode = "Redis" if pipeline.redis_available else "Mock storage"
    print(f"Using: {storage_mode}\n")

    # Simulate events over 7 days
    print("Simulating 7 days of user events...")

    import random
    from datetime import datetime, timedelta

    base_date = datetime.now()
    dates = []

    for day_offset in range(7):
        date = (base_date - timedelta(days=day_offset)).date().isoformat()
        dates.append(date)

        # Add 1000 events per day
        for _ in range(1000):
            user_id = f"user_{random.randint(1, 5000)}"
            pipeline.add_event(date, user_id)

    print(f"  ✓ Added ~7000 events across 7 days\n")

    # Query daily counts
    print("Daily unique user counts:")
    for date in reversed(dates):
        count = pipeline.get_cardinality(date)
        print(f"  {date}: {count:>5,} unique users")

    print()

    # Query weekly aggregate
    weekly_count = pipeline.merge_week(dates)
    print(f"Weekly unique users (merged): {weekly_count:,}")
    print()


def example_4_http_api_pattern():
    """Example 4: HTTP API pattern (akka-http-algebird style)."""
    print("=" * 70)
    print("EXAMPLE 4: HTTP API Pattern (akka-http-algebird)")
    print("=" * 70 + "\n")

    print("Flask API matching akka-http-algebird pattern:\n")

    # Show the API pattern
    api_code = '''
from flask import Flask, request, jsonify
from algesnake.approximate import HyperLogLog
from algesnake.approximate.serialization import HLLSerializer
import redis

app = Flask(__name__)
r = redis.Redis(decode_responses=False)

@app.route('/add', methods=['POST'])
def add_event():
    """Add user event - matches Akka HTTP POST /add"""
    data = request.json
    date = data['date']
    user_id = data['user_id']

    key = f'hll:daily:{date}'

    # Get existing or create new
    existing = r.get(key)
    if existing:
        hll = HLLSerializer.from_magic_string(existing.decode('ascii'))
    else:
        hll = HyperLogLog(precision=14)

    # Add user
    hll.add(user_id)

    # Store
    magic_string = HLLSerializer.to_magic_string(hll)
    r.set(key, magic_string)

    return jsonify({'status': 'OK'})

@app.route('/count/<date>', methods=['GET'])
def get_count(date):
    """Get count - matches Akka HTTP GET /count/:date"""
    key = f'hll:daily:{date}'

    existing = r.get(key)
    if not existing:
        return jsonify({'count': 0})

    hll = HLLSerializer.from_magic_string(existing.decode('ascii'))
    return jsonify({'count': int(hll.cardinality())})

@app.route('/merge', methods=['POST'])
def merge_dates():
    """Merge multiple dates - matches Akka HTTP POST /merge"""
    dates = request.json['dates']
    hlls = []

    for date in dates:
        key = f'hll:daily:{date}'
        existing = r.get(key)

        if existing:
            hll = HLLSerializer.from_magic_string(existing.decode('ascii'))
            hlls.append(hll)

    if not hlls:
        return jsonify({'count': 0})

    merged = sum(hlls)
    return jsonify({'count': int(merged.cardinality())})

if __name__ == '__main__':
    app.run(port=8080)
'''

    print(api_code)

    print("\nUsage:")
    print("  curl -X POST http://localhost:8080/add \\")
    print("       -d '{\"date\": \"2025-10-23\", \"user_id\": \"user_123\"}'")
    print()
    print("  curl http://localhost:8080/count/2025-10-23")
    print()
    print("  curl -X POST http://localhost:8080/merge \\")
    print("       -d '{\"dates\": [\"2025-10-17\", \"2025-10-18\", ..., \"2025-10-23\"]}'")
    print()


def example_5_format_comparison():
    """Example 5: Compare Scala vs Python serialization."""
    print("=" * 70)
    print("EXAMPLE 5: Serialization Format Comparison")
    print("=" * 70 + "\n")

    hll = HyperLogLog(precision=14)
    for i in range(10000):
        hll.add(f"user_{i}")

    print("Format Comparison (precision=14, 10k unique items):\n")

    # Magic string (Algebird pattern)
    magic_string = HLLSerializer.to_magic_string(hll)

    # Raw bytes
    bytes_data = HLLSerializer.to_bytes(hll)

    print(f"  Algebird (Scala):")
    print(f"    Kryo binary:     ~16-18 KB")
    print(f"    + Base64:        ~22-24 KB (base64 overhead)")
    print(f"    + Magic:         ~22-24 KB (%%% prefix)")
    print()

    print(f"  AlgeSNake (Python):")
    print(f"    Struct binary:   {len(bytes_data):>7} bytes ({len(bytes_data)/1024:.1f} KB)")
    print(f"    + Base64:        {len(magic_string)-3:>7} bytes ({(len(magic_string)-3)/1024:.1f} KB)")
    print(f"    + Magic:         {len(magic_string):>7} bytes ({len(magic_string)/1024:.1f} KB)")
    print()

    print(f"✅ Both achieve ~16-22 KB for 10k unique items")
    print(f"✅ Both use magic prefix for format identification")
    print(f"✅ Both use Base64 for string storage (Redis, HTTP)")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("ALGEBIRD-PATTERN SERIALIZATION EXAMPLES")
    print("Matching: addifier + akka-http-algebird")
    print("=" * 70 + "\n")

    example_1_basic_serialization()
    print("\n")

    example_2_redis_storage()
    print("\n")

    example_3_addifier_pipeline()
    print("\n")

    example_4_http_api_pattern()
    print("\n")

    example_5_format_comparison()

    print("=" * 70)
    print("Examples complete!")
    print("=" * 70)
