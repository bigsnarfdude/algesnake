# HyperLogLog Serialization: Step-by-Step

## Matching Your Algebird Implementation

Based on your repos:
- https://github.com/bigsnarfdude/addifier
- https://github.com/bigsnarfdude/akka-http-algebird

---

## The Complete Flow

```
┌──────────────────┐
│  HLL Object      │  In-memory Python/Scala object
│  (precision=14)  │
└────────┬─────────┘
         │
         │ STEP 1: Serialize to Binary
         ▼
┌──────────────────┐
│  Binary Data     │  16,385 bytes (precision + registers)
│  [0x0e, 0x03...] │
└────────┬─────────┘
         │
         │ STEP 2: Base64 Encode
         ▼
┌──────────────────┐
│  Base64 String   │  21,848 characters
│  "DgMBAAQDAQ..." │
└────────┬─────────┘
         │
         │ STEP 3: Add Magic Prefix
         ▼
┌──────────────────┐
│  Magic String    │  21,851 characters
│  "%%%DgMBAAQ..." │  ← Ready for Redis/HTTP/Kafka
└────────┬─────────┘
         │
         │ STEP 4: Store/Transmit
         ▼
┌──────────────────┐
│  Redis/HTTP/DB   │  Persistent storage
│  Key: hll:daily  │
└──────────────────┘
```

---

## Step-by-Step Code Comparison

### STEP 1: Serialize to Binary

#### Algebird (Scala + Kryo)
```scala
// Your code from addifier/HLLSerializer.scala
val kryo = KryoSerializer.kryo
val bytes: Array[Byte] = kryo.toBytesWithClass(hll)

// Kryo writes:
// - Class metadata (com.twitter.algebird.HLL)
// - Field values (bits, size, registers)
// - Optimized binary format
```

#### AlgeSNake (Python + Struct)
```python
# algesnake/approximate/serialization.py
import struct

def hll_to_bytes(hll):
    """Convert HLL to binary (similar to Kryo)"""
    # Pack: precision (1 byte) + registers (m bytes)
    fmt = f'<B{hll.m}B'  # Little-endian, unsigned bytes
    return struct.pack(fmt, hll.precision, *hll.registers)

bytes_data = hll_to_bytes(hll)
# Result: 16,385 bytes
```

**Output:**
```
Algebird: [class_metadata][bits][size][registers...]  (~16-18 KB)
AlgeSNake: [precision][register_0][register_1]...     (16,385 bytes)
```

---

### STEP 2: Base64 Encode

#### Algebird (Scala)
```scala
// Your code from addifier/HLLSerializer.scala
import org.apache.commons.codec.binary.Base64

val bytes: Array[Byte] = kryo.toBytesWithClass(hll)
val encoded = Base64.encodeBase64(bytes)
val serialized = new String(encoded)

// Result: ~22-24 KB string
```

#### AlgeSNake (Python)
```python
# algesnake/approximate/serialization.py
import base64

bytes_data = hll_to_bytes(hll)
encoded = base64.b64encode(bytes_data)
serialized = encoded.decode('ascii')

# Result: 21,848 characters
```

**Why Base64?**
- Converts binary → text (safe for Redis strings, HTTP, JSON)
- ~33% size increase (3 bytes → 4 characters)
- Universal compatibility

---

### STEP 3: Add Magic Prefix

#### Algebird (Scala)
```scala
// Your code from addifier/HLLSerializer.scala
private val MAGIC = "%%%"

def toMagicString(hll: HLL): String = {
  val bytes: Array[Byte] = kryo.toBytesWithClass(hll)
  val encoded = Base64.encodeBase64(bytes)
  val serialized = new String(encoded)
  MAGIC + serialized  // ← Magic prefix!
}

// Result: "%%%DgMBAAQDAQIA..."
```

#### AlgeSNake (Python)
```python
# algesnake/approximate/serialization.py
class HLLSerializer:
    MAGIC = "%%%"  # Same magic prefix!

    @classmethod
    def to_magic_string(cls, hll):
        """Serialize HLL to magic string (Algebird-compatible)"""
        bytes_data = cls._hll_to_bytes(hll)
        encoded = base64.b64encode(bytes_data)
        serialized = encoded.decode('ascii')
        return cls.MAGIC + serialized  # ← Magic prefix!

magic_string = HLLSerializer.to_magic_string(hll)
# Result: "%%%DgMBAAQDAQIA..."
```

**Why Magic Prefix?**
- Format identification (know it's an HLL)
- Version detection (can change magic for new formats)
- Corruption detection (if magic is wrong, data is bad)

---

### STEP 4: Store/Transmit

#### Algebird (Scala + Redis)
```scala
// Store in Redis
import redis.clients.jedis.Jedis

val jedis = new Jedis("localhost")
val key = "hll:daily:2025-10-23"
val magicString = HLLSerializer.toMagicString(hll)

jedis.set(key, magicString)
// Stored as Redis STRING: "%%%DgMBAAQ..."
```

#### AlgeSNake (Python + Redis)
```python
# Store in Redis
import redis

r = redis.Redis()
key = "hll:daily:2025-10-23"
magic_string = HLLSerializer.to_magic_string(hll)

r.set(key, magic_string)
# Stored as Redis STRING: "%%%DgMBAAQ..."
```

**Same Result!** Both store identical format in Redis.

---

## Deserialization (Reverse Process)

### Algebird (Scala)
```scala
// Your code from addifier/HLLSerializer.scala
def fromMagicString(hllHash: String): HLL = {
  // STEP 1: Remove magic prefix
  val trimmed = unMAGIC(hllHash)

  // STEP 2: Decode from Base64
  val bytes = Base64.decodeBase64(trimmed)

  // STEP 3: Deserialize from binary
  val hyperll = kryo.fromBytes(bytes).asInstanceOf[HLL]

  hyperll
}

def unMAGIC(serialized: String): String = {
  if(serialized.startsWith(MAGIC))
    serialized.drop(MAGIC.size)
  else
    "None"
}
```

### AlgeSNake (Python)
```python
# algesnake/approximate/serialization.py
class HLLSerializer:
    @classmethod
    def from_magic_string(cls, hll_hash):
        """Deserialize HLL from magic string"""
        # STEP 1: Remove magic prefix
        trimmed = cls._unmagic(hll_hash)
        if trimmed == "None":
            return None

        # STEP 2: Decode from Base64
        bytes_data = base64.b64decode(trimmed)

        # STEP 3: Deserialize from binary
        hll = cls._bytes_to_hll(bytes_data)

        return hll

    @classmethod
    def _unmagic(cls, serialized):
        """Remove magic prefix (exact same logic as Scala)"""
        if serialized.startswith(cls.MAGIC):
            return serialized[len(cls.MAGIC):]
        else:
            return "None"
```

---

## Complete Example: Addifier Pattern

### Scala (Your Code)
```scala
// From addifier repo
object HLLPipeline {
  val hllMonoid = new HyperLogLogMonoid(14)
  val jedis = new Jedis("localhost")

  def addEvent(date: String, userId: String): Unit = {
    val key = s"hll:daily:$date"

    // Get existing HLL or create new
    val existing = Option(jedis.get(key))
      .map(str => HLLSerializer.fromMagicString(str))
      .getOrElse(hllMonoid.zero)

    // Add new user (monoid combine)
    val updated = existing + hllMonoid.create(userId.getBytes)

    // Serialize and store
    val magicString = HLLSerializer.toMagicString(updated)
    jedis.set(key, magicString)
  }

  def getCardinality(date: String): Long = {
    val key = s"hll:daily:$date"
    Option(jedis.get(key))
      .map(str => hllMonoid.estimateSize(HLLSerializer.fromMagicString(str)))
      .getOrElse(0L)
  }
}

// Usage
HLLPipeline.addEvent("2025-10-23", "user_123")
HLLPipeline.addEvent("2025-10-23", "user_456")
println(HLLPipeline.getCardinality("2025-10-23"))  // 2
```

### Python (AlgeSNake)
```python
# Python equivalent
from algesnake.approximate import HyperLogLog
from algesnake.approximate.serialization import HLLSerializer
import redis

class HLLPipeline:
    def __init__(self):
        self.redis = redis.Redis(decode_responses=False)

    def add_event(self, date, user_id):
        key = f"hll:daily:{date}"

        # Get existing HLL or create new
        existing = self.redis.get(key)
        if existing:
            hll = HLLSerializer.from_magic_string(existing.decode('ascii'))
        else:
            hll = HyperLogLog(precision=14)

        # Add new user (monoid operation)
        hll.add(user_id)

        # Serialize and store
        magic_string = HLLSerializer.to_magic_string(hll)
        self.redis.set(key, magic_string)

    def get_cardinality(self, date):
        key = f"hll:daily:{date}"
        existing = self.redis.get(key)

        if not existing:
            return 0

        hll = HLLSerializer.from_magic_string(existing.decode('ascii'))
        return int(hll.cardinality())

# Usage
pipeline = HLLPipeline()
pipeline.add_event("2025-10-23", "user_123")
pipeline.add_event("2025-10-23", "user_456")
print(pipeline.get_cardinality("2025-10-23"))  # ~2
```

**Same pattern! Same behavior! Same Redis format!**

---

## Technology Summary

| Step | Algebird (Scala) | AlgeSNake (Python) | Purpose |
|------|------------------|-------------------|---------|
| **Binary** | Kryo (via Chill) | struct.pack() | HLL → bytes |
| **Encoding** | Apache Commons Base64 | base64.b64encode() | bytes → text |
| **Magic** | "%%%" prefix | "%%%" prefix | Format ID |
| **Storage** | Jedis (Redis client) | redis-py (Redis client) | Persistence |
| **Size** | ~22-24 KB | ~21.3 KB | Nearly identical! |

---

## Key Takeaways

1. **Same Magic Prefix**: Both use `"%%%"` for format identification
2. **Same Base64**: Both encode binary → text for Redis strings
3. **Same Pattern**: Get existing → add user → store back
4. **Same Size**: ~21-24 KB for precision=14
5. **Cross-Compatible**: Could theoretically read each other's format (with minor adjustments)

The main difference is **binary serialization**:
- **Kryo**: JVM-optimized, includes class metadata
- **Struct**: Python-native, manual packing, slightly more compact

Both achieve the goal: **compact, efficient serialization for distributed HLL aggregation!**
