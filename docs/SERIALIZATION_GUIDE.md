# HyperLogLog Serialization Guide

## AlgeSNake vs Algebird Serialization

This guide compares serialization approaches between:
- **Algebird** (Scala): Twitter's implementation
- **AlgeSNake** (Python): Python port

---

## Serialization Step-by-Step

### What is Serialization?

**Serialization** = Converting in-memory object → bytes (for storage/network)
**Deserialization** = Converting bytes → in-memory object

```
Memory Object          Bytes               Storage/Network
─────────────         ─────────           ────────────────
HyperLogLog    -->    [0x0e, 0x03...]  -->  Redis/Disk/HTTP
   (Python)           (binary data)         (persistent)
```

---

## Algebird (Scala) Approach

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Serialization | **Chill** (Kryo wrapper) | Fast binary serialization |
| Compression | Snappy/LZ4 | Optional compression |
| Wire Format | Kryo binary | Compact binary format |
| Type Safety | Scala case classes | Compile-time safety |
| Redis Integration | Jedis/Lettuce | Redis client |

### Algebird Serialization Code

```scala
// Algebird HLL serialization example
import com.twitter.algebird._
import com.twitter.chill._

// 1. Create HLL
val hll = HyperLogLogMonoid(bits = 14)
var aggregate = hll.zero

// 2. Add items
(0 until 10000).foreach { i =>
  aggregate = aggregate + hll.create(s"user_$i".getBytes)
}

// 3. Serialize using Chill (Kryo wrapper)
val serializer = KryoSerializer.registered
val bytes: Array[Byte] = serializer.toBinary(aggregate)

// 4. Store in Redis
jedis.set("hll:daily:2025-10-23".getBytes, bytes)

// 5. Deserialize from Redis
val storedBytes = jedis.get("hll:daily:2025-10-23".getBytes)
val loadedHLL = serializer.fromBinary(storedBytes).asInstanceOf[HLL]

// 6. Query
println(s"Cardinality: ${hll.estimateSize(loadedHLL)}")
```

### Algebird Internal Format (Simplified)

```
Algebird HLL Internal Structure:
┌─────────────────────────────────────┐
│ bits: Int (precision)               │
│ size: Int (number of registers)     │
│ registers: Vector[Byte]             │  ← Sparse or dense
│ + optional compression metadata     │
└─────────────────────────────────────┘
```

---

## AlgeSNake (Python) Approach

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Serialization | **Pickle/Struct** | Python native + binary |
| Compression | gzip/zlib | Optional compression |
| Wire Format | Custom binary | Flexible formats |
| Type Safety | Type hints | Runtime checking |
| Redis Integration | redis-py | Redis client |

### AlgeSNake Serialization Code

```python
# AlgeSNake HLL serialization example
from algesnake.approximate import HyperLogLog
import struct
import redis

# 1. Create HLL
hll = HyperLogLog(precision=14)

# 2. Add items
for i in range(10000):
    hll.add(f"user_{i}")

# 3. Serialize to binary (most efficient)
def hll_to_bytes(hll):
    """Pack: precision (1 byte) + registers (m bytes)"""
    fmt = f'<B{hll.m}B'  # Little-endian, unsigned bytes
    return struct.pack(fmt, hll.precision, *hll.registers)

bytes_data = hll_to_bytes(hll)

# 4. Store in Redis
r = redis.Redis()
r.set('hll:daily:2025-10-23', bytes_data)

# 5. Deserialize from Redis
def bytes_to_hll(data):
    """Unpack binary data to HLL"""
    precision = data[0]
    m = 1 << precision
    fmt = f'<B{m}B'
    unpacked = struct.unpack(fmt, data)

    hll = HyperLogLog(precision=precision)
    hll.registers = list(unpacked[1:])
    return hll

stored_bytes = r.get('hll:daily:2025-10-23')
loaded_hll = bytes_to_hll(stored_bytes)

# 6. Query
print(f"Cardinality: {loaded_hll.cardinality()}")
```

### AlgeSNake Internal Format

```
AlgeSNake HLL Internal Structure:
┌─────────────────────────────────────┐
│ precision: int                      │
│ m: int (2^precision)                │
│ alpha: float                        │
│ registers: List[int]                │  ← Always dense array
└─────────────────────────────────────┘
```

---

## Detailed Serialization Steps

### 1. **Object → Bytes** (Serialization)

#### Algebird (Scala + Kryo)
```scala
// Step 1: Get Kryo serializer
val kryo = KryoSerializer.registered

// Step 2: Write object to byte array
val output = new Output(4096)  // Buffer
kryo.writeObject(output, hll)

// Step 3: Get bytes
val bytes: Array[Byte] = output.toBytes
```

**Kryo Format:**
- Writes class metadata
- Writes field values in order
- Uses variable-length encoding
- Optimized for JVM objects

#### AlgeSNake (Python + Struct)
```python
# Step 1: Define binary format
# '<'  = little-endian byte order
# 'B'  = unsigned byte (0-255)
# '{m}B' = m unsigned bytes for registers
fmt = f'<B{hll.m}B'

# Step 2: Pack object fields into bytes
bytes_data = struct.pack(
    fmt,
    hll.precision,      # 1 byte
    *hll.registers      # m bytes (one per register)
)

# Step 3: bytes_data is ready!
```

**Struct Format:**
- Manual field selection
- Fixed binary layout
- No metadata overhead
- Compact representation

### 2. **Bytes → Network/Storage**

Both use the same approach:

```python
# Redis
redis.set(key, bytes_data)

# HTTP
requests.post(url, data=bytes_data)

# Disk
with open('file.bin', 'wb') as f:
    f.write(bytes_data)

# Kafka
producer.send(topic, bytes_data)
```

### 3. **Network/Storage → Bytes**

```python
# Redis
bytes_data = redis.get(key)

# HTTP
bytes_data = response.content

# Disk
with open('file.bin', 'rb') as f:
    bytes_data = f.read()

# Kafka
bytes_data = message.value
```

### 4. **Bytes → Object** (Deserialization)

#### Algebird (Scala + Kryo)
```scala
// Step 1: Create input from bytes
val input = new Input(bytes)

// Step 2: Read object
val hll = kryo.readObject(input, classOf[HLL])
```

#### AlgeSNake (Python + Struct)
```python
# Step 1: Extract precision from first byte
precision = bytes_data[0]
m = 1 << precision

# Step 2: Define unpack format
fmt = f'<B{m}B'

# Step 3: Unpack bytes into values
unpacked = struct.unpack(fmt, bytes_data)

# Step 4: Reconstruct HLL
hll = HyperLogLog(precision=precision)
hll.registers = list(unpacked[1:])  # Skip first byte
```

---

## Complete Example: Addifier-style Redis Pipeline

### Algebird Version (Scala)

```scala
// From your addifier repo pattern
import com.twitter.algebird._
import com.twitter.chill._
import redis.clients.jedis._

object HLLPipeline {
  val hllMonoid = new HyperLogLogMonoid(14)
  val serializer = KryoSerializer.registered

  def addEvent(jedis: Jedis, date: String, userId: String): Unit = {
    val key = s"hll:daily:$date"

    // Get existing HLL or create new
    val existing = Option(jedis.get(key.getBytes))
      .map(bytes => serializer.fromBinary(bytes).asInstanceOf[HLL])
      .getOrElse(hllMonoid.zero)

    // Add new user
    val updated = existing + hllMonoid.create(userId.getBytes)

    // Serialize and store
    val bytes = serializer.toBinary(updated)
    jedis.set(key.getBytes, bytes)
  }

  def getCardinality(jedis: Jedis, date: String): Long = {
    val key = s"hll:daily:$date"
    Option(jedis.get(key.getBytes))
      .map { bytes =>
        val hll = serializer.fromBinary(bytes).asInstanceOf[HLL]
        hllMonoid.estimateSize(hll).toLong
      }
      .getOrElse(0L)
  }

  def mergeWeek(jedis: Jedis, dates: Seq[String]): Long = {
    val hlls = dates.flatMap { date =>
      val key = s"hll:daily:$date"
      Option(jedis.get(key.getBytes))
        .map(bytes => serializer.fromBinary(bytes).asInstanceOf[HLL])
    }

    val merged = hllMonoid.sum(hlls)
    hllMonoid.estimateSize(merged).toLong
  }
}
```

### AlgeSNake Version (Python)

```python
# AlgeSNake equivalent to addifier
from algesnake.approximate import HyperLogLog
import struct
import redis

class HLLPipeline:
    def __init__(self, precision=14):
        self.precision = precision
        self.redis = redis.Redis()

    def _hll_to_bytes(self, hll):
        """Serialize HLL to bytes."""
        fmt = f'<B{hll.m}B'
        return struct.pack(fmt, hll.precision, *hll.registers)

    def _bytes_to_hll(self, data):
        """Deserialize bytes to HLL."""
        precision = data[0]
        m = 1 << precision
        fmt = f'<B{m}B'
        unpacked = struct.unpack(fmt, data)

        hll = HyperLogLog(precision=precision)
        hll.registers = list(unpacked[1:])
        return hll

    def add_event(self, date, user_id):
        """Add user to daily HLL."""
        key = f'hll:daily:{date}'

        # Get existing HLL or create new
        existing_bytes = self.redis.get(key)
        if existing_bytes:
            hll = self._bytes_to_hll(existing_bytes)
        else:
            hll = HyperLogLog(precision=self.precision)

        # Add new user
        hll.add(user_id)

        # Serialize and store
        bytes_data = self._hll_to_bytes(hll)
        self.redis.set(key, bytes_data)

    def get_cardinality(self, date):
        """Get unique user count for date."""
        key = f'hll:daily:{date}'
        existing_bytes = self.redis.get(key)

        if not existing_bytes:
            return 0

        hll = self._bytes_to_hll(existing_bytes)
        return int(hll.cardinality())

    def merge_week(self, dates):
        """Merge 7 days for weekly unique count."""
        hlls = []

        for date in dates:
            key = f'hll:daily:{date}'
            existing_bytes = self.redis.get(key)

            if existing_bytes:
                hll = self._bytes_to_hll(existing_bytes)
                hlls.append(hll)

        if not hlls:
            return 0

        # Merge using monoid sum
        merged = sum(hlls)
        return int(merged.cardinality())


# Usage
pipeline = HLLPipeline(precision=14)

# Add events
pipeline.add_event('2025-10-23', 'user_123')
pipeline.add_event('2025-10-23', 'user_456')
pipeline.add_event('2025-10-22', 'user_123')  # Same user, different day

# Query
print(f"Daily: {pipeline.get_cardinality('2025-10-23')}")
print(f"Weekly: {pipeline.merge_week(['2025-10-17', '2025-10-18', ..., '2025-10-23'])}")
```

---

## Technology Comparison

### Serialization Libraries

| Feature | Algebird (Kryo) | AlgeSNake (Struct) |
|---------|-----------------|-------------------|
| Speed | Very fast | Fast |
| Size | Very compact | Compact |
| Format | Binary (Kryo) | Binary (custom) |
| Type safety | Strong (Scala) | Runtime (Python) |
| Compatibility | JVM only | Cross-platform |
| Setup | Requires Chill | Built-in |

### Binary Format Size (Precision=14)

```
Algebird (Kryo):    ~16-18 KB  (with Kryo metadata)
AlgeSNake (Struct):  16,385 bytes (1 + 16,384)
```

### Performance Comparison

```
Operation         Algebird (Scala)    AlgeSNake (Python)
─────────────────────────────────────────────────────────
Serialize         ~0.1ms              ~0.5ms
Deserialize       ~0.1ms              ~0.5ms
Add item          ~0.001ms            ~0.005ms
Cardinality       ~0.01ms             ~0.1ms
```

*Note: Scala/JVM is generally faster, but Python is "fast enough" for most use cases*

---

## Advanced: Akka HTTP Integration

### Algebird + Akka HTTP (from akka-http-algebird repo)

```scala
// REST API for HLL aggregation
import akka.http.scaladsl.server.Directives._
import com.twitter.algebird._

object HLLService {
  val hllMonoid = new HyperLogLogMonoid(14)

  val routes =
    path("add") {
      post {
        entity(as[String]) { userId =>
          // Add to HLL
          val hll = hllMonoid.create(userId.getBytes)
          // Store...
          complete("OK")
        }
      }
    } ~
    path("count") {
      get {
        // Get HLL from storage
        // Return cardinality
        complete(cardinality.toString)
      }
    }
}
```

### AlgeSNake + Flask/FastAPI Equivalent

```python
from flask import Flask, request, jsonify
from algesnake.approximate import HyperLogLog
import redis
import struct

app = Flask(__name__)
r = redis.Redis()

def hll_to_bytes(hll):
    fmt = f'<B{hll.m}B'
    return struct.pack(fmt, hll.precision, *hll.registers)

def bytes_to_hll(data):
    precision = data[0]
    m = 1 << precision
    fmt = f'<B{m}B'
    unpacked = struct.unpack(fmt, data)
    hll = HyperLogLog(precision=precision)
    hll.registers = list(unpacked[1:])
    return hll

@app.route('/add', methods=['POST'])
def add_user():
    user_id = request.json['user_id']
    date = request.json['date']

    key = f'hll:daily:{date}'
    existing = r.get(key)

    if existing:
        hll = bytes_to_hll(existing)
    else:
        hll = HyperLogLog(precision=14)

    hll.add(user_id)
    r.set(key, hll_to_bytes(hll))

    return jsonify({'status': 'OK'})

@app.route('/count/<date>', methods=['GET'])
def get_count(date):
    key = f'hll:daily:{date}'
    existing = r.get(key)

    if not existing:
        return jsonify({'count': 0})

    hll = bytes_to_hll(existing)
    return jsonify({'count': int(hll.cardinality())})

if __name__ == '__main__':
    app.run(port=8080)
```

---

## Best Practices

### 1. **Choose the Right Format**

```python
# For Python-only systems
pickle.dumps(hll)  # Easiest

# For cross-language systems
struct.pack(...)   # Most compatible

# For human debugging
json.dumps(...)    # Readable

# For minimal size
gzip.compress(struct.pack(...))  # Smallest
```

### 2. **Add Metadata**

```python
def serialize_with_metadata(hll):
    """Add version and timestamp for future compatibility."""
    return {
        'version': 1,
        'timestamp': datetime.now().isoformat(),
        'precision': hll.precision,
        'registers': hll.registers
    }
```

### 3. **Handle Errors**

```python
def safe_deserialize(data):
    try:
        return bytes_to_hll(data)
    except struct.error:
        # Handle corrupt data
        return HyperLogLog(precision=14)  # Fresh HLL
```

---

## Summary

| Aspect | Algebird | AlgeSNake |
|--------|----------|-----------|
| Language | Scala | Python |
| Serializer | Kryo (via Chill) | Struct/Pickle |
| Speed | Very fast | Fast enough |
| Size | ~16-18 KB | ~16 KB |
| Setup | More complex | Simple |
| Type safety | Compile-time | Runtime |
| Best for | JVM ecosystems | Python ecosystems |

Both achieve the same goal: **compact, efficient serialization of HLL for storage and network transfer**.

The choice depends on your tech stack!
