# Twitter Analytics Examples

Real-world Twitter analytics use cases inspired by Twitter's Algebird library.

These examples demonstrate how Algesnake's probabilistic data structures and monoids enable memory-efficient, distributed analytics at Twitter scale.

## Examples Overview

| Example | Use Case | Structures Used | Complexity |
|---------|----------|----------------|------------|
| `01_unique_users_streaming.py` | Count unique users in real-time streams | HyperLogLog, Add | ⭐ Beginner |
| `02_trending_hashtags.py` | Track trending hashtags (TopK) | CountMinSketch, TopK | ⭐ Beginner |
| `03_spam_detection.py` | Detect spam accounts with Bloom filters | BloomFilter | ⭐ Beginner |
| `04_tweet_embedding_tracker.py` | Track which sites embed tweets | CountMinSketch | ⭐⭐ Intermediate |
| `05_user_engagement_metrics.py` | Aggregate user engagement stats | Add, Max, Min, MapMonoid | ⭐ Beginner |
| `06_timeline_builder.py` | Build user timelines (ordered lists) | ListMonoid (non-commutative) | ⭐⭐ Intermediate |
| `07_ad_campaign_analytics.py` | Monitor ad campaign performance | HLL, TDigest, Add, Max | ⭐⭐ Intermediate |
| `08_retweet_network.py` | Analyze retweet networks | SetMonoid, HLL, MapMonoid | ⭐⭐ Intermediate |
| `09_engagement_percentiles.py` | Track engagement rate distributions | TDigest | ⭐⭐ Intermediate |
| `10_url_shortener_analytics.py` | Track URL click-through rates | CountMinSketch, HLL | ⭐⭐ Intermediate |
| `11_rate_limiting.py` | Detect abuse with sliding windows | CountMinSketch, MapMonoid | ⭐⭐⭐ Advanced |
| `12_content_recommendation.py` | Recommend content based on history | BloomFilter, SetMonoid | ⭐⭐ Intermediate |
| `13_ab_testing.py` | A/B test analytics with aggregations | Add, TDigest, MapMonoid | ⭐⭐ Intermediate |
| `14_user_growth_cohorts.py` | Track user growth cohorts | HLL, MapMonoid | ⭐⭐ Intermediate |
| `15_distributed_log_analysis.py` | Analyze logs across distributed servers | CMS, TopK, HLL | ⭐⭐⭐ Advanced |

## Twitter's Original Use Cases

These examples are based on **actual Twitter engineering uses** of Algebird:

### Production Systems
- **Summingbird**: Twitter's streaming MapReduce framework uses Algebird for real-time analytics
- **Scalding**: Twitter's Hadoop framework leverages Algebird monoids for batch processing
- **Advertising Platform**: Built on Summingbird + Algebird for campaign analytics
- **Timeline Service**: Uses non-commutative monoids for ordered tweet lists

### Key Insights from Twitter Engineering

From [Twitter's blog post](https://blog.twitter.com/engineering/en_us/a/2013/streaming-mapreduce-with-summingbird):
> "Algebird provides a number of data structures that are Monoids and thus easy to use in
> aggregations: HyperLogLog for cardinality estimation, Count-Min Sketch for frequency,
> and Bloom Filters for set membership."

From [Michael Noll's analysis](https://www.michael-noll.com/blog/2013/12/02/twitter-algebird-monoid-monad-for-large-scala-data-analytics/):
> "Twitter advertising is built on Summingbird. Various campaigns can be built by building
> a backend using a monoid that expresses the needs."

## Running the Examples

```bash
# Run individual examples
python examples/twitter_analytics/01_unique_users_streaming.py
python examples/twitter_analytics/02_trending_hashtags.py

# Run all examples
for f in examples/twitter_analytics/*.py; do
    echo "Running $f..."
    python "$f"
done
```

## Common Patterns

### 1. Distributed Aggregation (Monoid Pattern)
```python
# Process data in parallel across servers
server1_result = process_partition(data_partition_1)
server2_result = process_partition(data_partition_2)
server3_result = process_partition(data_partition_3)

# Merge results (associative!)
global_result = server1_result + server2_result + server3_result
```

### 2. Streaming Analytics (Incremental Updates)
```python
# Start with empty state
global_state = HyperLogLog(precision=14)

# Process batches as they arrive
for batch in stream:
    batch_state = process_batch(batch)
    global_state = global_state + batch_state  # Incremental merge!
```

### 3. Memory-Efficient Approximation
```python
# Exact: Store 10M user IDs = 80 MB
exact_users = set()  # Memory: O(n)

# Approximate: HyperLogLog = 16 KB (99.98% savings!)
approx_users = HyperLogLog(precision=14)  # Memory: O(log log n)
```

## Why These Structures Matter at Twitter Scale

| Structure | Twitter Use Case | Memory Savings | Accuracy |
|-----------|------------------|----------------|----------|
| **HyperLogLog** | Unique users, unique tweets | 99.98% | ~2% error |
| **CountMinSketch** | Hashtag frequencies, error rates | 99.95% | Never underestimates |
| **TopK** | Trending topics, influencers | 99.997% | Exact for top K |
| **TDigest** | API latency, engagement percentiles | 99.998% | 0.1-1% error (tails) |
| **BloomFilter** | Spam detection, seen content | 98.5% | Configurable false positive |

## Architecture Patterns

### Batch + Streaming (Lambda Architecture)
```python
# Batch: Process historical data (Hadoop/Spark)
batch_result = process_historical_data()

# Streaming: Process real-time data (Storm/Flink)
streaming_result = process_realtime_stream()

# Merge: Combine batch + streaming (Monoid!)
complete_view = batch_result + streaming_result
```

### Multi-Level Aggregation
```python
# User-level aggregation
user_metrics = {
    'tweets': Add(0),
    'followers': HyperLogLog(precision=14),
    'engagement': TDigest(compression=100)
}

# Campaign-level aggregation (aggregate users)
campaign_metrics = sum([user_metrics for user in campaign_users])

# Global-level aggregation (aggregate campaigns)
global_metrics = sum([campaign_metrics for campaign in campaigns])
```

## Performance Characteristics

### HyperLogLog
- **Space**: O(log log n) = 16 KB for billions of items
- **Time**: O(1) per insertion, O(1) for cardinality query
- **Error**: ~2% standard error with precision=14

### CountMinSketch
- **Space**: O(w × d) = configurable based on ε and δ
- **Time**: O(d) per insertion/query (d = # hash functions)
- **Error**: ε with probability 1-δ, never underestimates

### TopK (SpaceSaver)
- **Space**: O(k) = only store top K items
- **Time**: O(log k) per insertion
- **Error**: Guaranteed exact for top K

### TDigest
- **Space**: O(compression) = ~1-2 KB typical
- **Time**: O(log compression) per insertion
- **Error**: 0.1-1% for extreme percentiles (p99, p999)

### BloomFilter
- **Space**: O(n × bits_per_element) = configurable
- **Time**: O(k) per operation (k = # hash functions)
- **Error**: Configurable false positive rate, 0% false negatives

## Learning Path

### Beginner (Start Here)
1. `01_unique_users_streaming.py` - Learn HyperLogLog basics
2. `02_trending_hashtags.py` - Understand TopK for rankings
3. `05_user_engagement_metrics.py` - Master basic monoids (Add, Max, Min)

### Intermediate
4. `07_ad_campaign_analytics.py` - Combine multiple structures
5. `09_engagement_percentiles.py` - Learn TDigest for percentiles
6. `08_retweet_network.py` - Network analysis with monoids

### Advanced
7. `11_rate_limiting.py` - Complex sliding window patterns
8. `15_distributed_log_analysis.py` - Full distributed system simulation
9. `06_timeline_builder.py` - Non-commutative monoids

## Resources

### Twitter Engineering Blog
- [Streaming MapReduce with Summingbird](https://blog.twitter.com/engineering/en_us/a/2013/streaming-mapreduce-with-summingbird)
- [Scalding 0.8.0 and Algebird](https://blog.twitter.com/engineering/en_us/a/2012/scalding-080-and-algebird)

### Technical Deep Dives
- [Of Algebirds, Monoids, Monads, and other Bestiary](https://www.michael-noll.com/blog/2013/12/02/twitter-algebird-monoid-monad-for-large-scala-data-analytics/)
- [Algebird Official Docs](https://twitter.github.io/algebird/)

### Academic Papers
- [HyperLogLog: Analysis of a Near-Optimal Cardinality Estimation Algorithm](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
- [Computing Extremely Accurate Quantiles Using t-Digests](https://arxiv.org/abs/1902.04023)
- [An Improved Data Stream Summary: The Count-Min Sketch](https://dl.acm.org/doi/10.1016/j.jalgor.2003.12.001)

## Contributing

Have a Twitter analytics use case we missed? PRs welcome! Please ensure:
- Example includes docstring explaining the use case
- Code demonstrates monoid properties (associativity, zero element)
- Includes comparison with exact computation when feasible
- Follows existing example structure

---

**Built with Algesnake** - Abstract algebra for Python, inspired by Twitter's Algebird.
