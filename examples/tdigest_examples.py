"""Phase 3 Week 5-6: T-Digest - Examples

This file demonstrates T-Digest usage for:
- Quantile/percentile estimation (p50, p95, p99)
- Latency monitoring and SLA tracking
- Distributed aggregation scenarios
"""

from algesnake.approximate import TDigest
from algesnake.approximate.tdigest import estimate_quantiles, merge_tdigests
import random


def example_tdigest_basics():
    """Basic T-Digest usage."""
    print("=" * 60)
    print("Example 1: T-Digest Basics")
    print("=" * 60)
    
    # Create T-Digest with compression=100
    td = TDigest(compression=100)
    
    # Add values (e.g., response times in ms)
    for i in range(1000):
        td.add(float(i))
    
    # Query percentiles
    print(f"Added {td.count:.0f} values")
    print(f"\nPercentiles:")
    print(f"  p50 (median): {td.percentile(50):.2f}")
    print(f"  p95: {td.percentile(95):.2f}")
    print(f"  p99: {td.percentile(99):.2f}")
    print(f"  p999: {td.percentile(99.9):.2f}")
    
    print(f"\nMin: {td.min():.2f}, Max: {td.max():.2f}")
    print()


def example_latency_monitoring():
    """Latency monitoring with SLA tracking."""
    print("=" * 60)
    print("Example 2: API Latency Monitoring")
    print("=" * 60)
    
    td = TDigest(compression=100)
    
    # Simulate API request latencies (ms)
    # Most requests are fast, some are slow
    for _ in range(950):
        latency = random.gauss(50, 10)  # Fast requests
        td.add(max(0, latency))
    
    for _ in range(50):
        latency = random.gauss(500, 100)  # Slow requests
        td.add(max(0, latency))
    
    # Check SLA compliance
    print(f"Total requests: {td.count:.0f}")
    print(f"\nLatency distribution:")
    print(f"  Median (p50): {td.percentile(50):.1f}ms")
    print(f"  p90: {td.percentile(90):.1f}ms")
    print(f"  p95: {td.percentile(95):.1f}ms")
    print(f"  p99: {td.percentile(99):.1f}ms")
    print(f"  p999: {td.percentile(99.9):.1f}ms")
    
    # Check SLA (e.g., p95 < 200ms)
    p95 = td.percentile(95)
    sla_threshold = 200
    status = "✓ PASS" if p95 < sla_threshold else "✗ FAIL"
    print(f"\nSLA Check (p95 < {sla_threshold}ms): {status}")
    print()


def example_distributed_aggregation():
    """Distributed latency aggregation across servers."""
    print("=" * 60)
    print("Example 3: Distributed Latency Aggregation")
    print("=" * 60)
    
    # Three API servers with latency data
    server1 = TDigest(compression=100)
    server2 = TDigest(compression=100)
    server3 = TDigest(compression=100)
    
    # Server 1: US East (fast)
    print("Collecting metrics from Server 1 (US East)...")
    for _ in range(1000):
        server1.add(random.gauss(30, 5))
    
    # Server 2: US West (medium)
    print("Collecting metrics from Server 2 (US West)...")
    for _ in range(1000):
        server2.add(random.gauss(60, 10))
    
    # Server 3: EU (slower)
    print("Collecting metrics from Server 3 (EU)...")
    for _ in range(1000):
        server3.add(random.gauss(100, 15))
    
    # Merge using monoid operation
    global_td = server1 + server2 + server3
    
    print(f"\nPer-server p95 latency:")
    print(f"  Server 1: {server1.percentile(95):.1f}ms")
    print(f"  Server 2: {server2.percentile(95):.1f}ms")
    print(f"  Server 3: {server3.percentile(95):.1f}ms")
    
    print(f"\nGlobal p95 latency: {global_td.percentile(95):.1f}ms")
    print(f"Global p99 latency: {global_td.percentile(99):.1f}ms")
    print()


def example_streaming_percentiles():
    """Streaming percentile calculation."""
    print("=" * 60)
    print("Example 4: Streaming Percentile Tracking")
    print("=" * 60)
    
    td = TDigest(compression=100)
    
    # Simulate streaming data in batches
    batches = [
        [random.gauss(100, 20) for _ in range(100)]
        for _ in range(10)
    ]
    
    print("Processing streaming batches:")
    for i, batch in enumerate(batches, 1):
        for value in batch:
            td.add(max(0, value))
        
        if i % 2 == 0:  # Report every 2 batches
            print(f"\n  After {i} batches ({td.count:.0f} values):")
            print(f"    p50: {td.percentile(50):.1f}")
            print(f"    p95: {td.percentile(95):.1f}")
            print(f"    p99: {td.percentile(99):.1f}")
    
    print()


def example_cdf_analysis():
    """Cumulative distribution function analysis."""
    print("=" * 60)
    print("Example 5: CDF Analysis")
    print("=" * 60)
    
    td = TDigest(compression=100)
    
    # Add response times
    for _ in range(1000):
        td.add(random.gauss(100, 30))
    
    # Calculate CDF at various points
    thresholds = [50, 100, 150, 200, 250]
    
    print("Cumulative Distribution Function:")
    print(f"{'Threshold (ms)':>15} | {'% of requests':>15}")
    print("-" * 33)
    
    for threshold in thresholds:
        cdf = td.cdf(float(threshold))
        print(f"{threshold:>15} | {cdf*100:>14.1f}%")
    
    print(f"\n{td.cdf(100)*100:.1f}% of requests complete within 100ms")
    print()


def example_sla_monitoring():
    """SLA monitoring dashboard."""
    print("=" * 60)
    print("Example 6: SLA Monitoring Dashboard")
    print("=" * 60)
    
    # Different service endpoints
    endpoints = {
        '/api/users': TDigest(compression=100),
        '/api/products': TDigest(compression=100),
        '/api/orders': TDigest(compression=100),
    }
    
    # Simulate traffic
    for endpoint, td in endpoints.items():
        base_latency = {'users': 50, 'products': 80, 'orders': 120}[endpoint.split('/')[-1]]
        
        for _ in range(1000):
            latency = random.gauss(base_latency, base_latency * 0.2)
            td.add(max(0, latency))
    
    # Dashboard output
    print(f"{'Endpoint':>20} | {'p50':>8} | {'p95':>8} | {'p99':>8} | {'SLA':>8}")
    print("-" * 61)
    
    sla_threshold = 150
    for endpoint, td in endpoints.items():
        p50 = td.percentile(50)
        p95 = td.percentile(95)
        p99 = td.percentile(99)
        sla_status = "✓" if p95 < sla_threshold else "✗"
        
        print(f"{endpoint:>20} | {p50:>7.1f}ms | {p95:>7.1f}ms | {p99:>7.1f}ms | {sla_status:>8}")
    
    print()


def example_error_vs_success():
    """Compare distributions: successful vs error responses."""
    print("=" * 60)
    print("Example 7: Success vs Error Response Times")
    print("=" * 60)
    
    success_td = TDigest(compression=100)
    error_td = TDigest(compression=100)
    
    # Successful requests: fast
    for _ in range(950):
        success_td.add(random.gauss(50, 10))
    
    # Error requests: slow (timeouts, retries)
    for _ in range(50):
        error_td.add(random.gauss(5000, 500))
    
    print("Successful Responses:")
    print(f"  Count: {success_td.count:.0f}")
    print(f"  p50: {success_td.percentile(50):.1f}ms")
    print(f"  p95: {success_td.percentile(95):.1f}ms")
    print(f"  p99: {success_td.percentile(99):.1f}ms")
    
    print("\nError Responses:")
    print(f"  Count: {error_td.count:.0f}")
    print(f"  p50: {error_td.percentile(50):.1f}ms")
    print(f"  p95: {error_td.percentile(95):.1f}ms")
    
    print(f"\n💡 Errors are {error_td.percentile(50)/success_td.percentile(50):.1f}x slower!")
    print()


def example_quantile_queries():
    """Multiple quantile queries."""
    print("=" * 60)
    print("Example 8: Quantile Queries")
    print("=" * 60)
    
    td = TDigest(compression=100)
    
    # Add database query times (ms)
    for _ in range(10000):
        query_time = random.expovariate(1/50)  # Exponential distribution
        td.add(query_time)
    
    # Query multiple quantiles
    quantiles = [0.5, 0.75, 0.90, 0.95, 0.99, 0.999]
    
    print("Database Query Time Distribution:")
    print(f"{'Percentile':>12} | {'Time (ms)':>12}")
    print("-" * 26)
    
    for q in quantiles:
        time_ms = td.quantile(q)
        print(f"p{q*100:>6.1f} | {time_ms:>11.2f}ms")
    
    print()


def example_memory_efficiency():
    """Memory efficiency comparison."""
    print("=" * 60)
    print("Example 9: Memory Efficiency")
    print("=" * 60)
    
    import sys
    
    # Scenario: Track 100,000 latency measurements
    
    # Traditional approach: store all values
    traditional_list = []
    for _ in range(10000):  # Smaller for memory reasons
        traditional_list.append(random.gauss(100, 20))
    
    list_memory_kb = sys.getsizeof(traditional_list) / 1024
    
    # T-Digest approach
    td = TDigest(compression=100)
    for _ in range(10000):
        td.add(random.gauss(100, 20))
    
    # Approximate T-Digest memory
    td_memory_kb = (len(td.centroids) * 16) / 1024  # Rough estimate
    
    print("Tracking 10,000 latency measurements:")
    
    print(f"\nTraditional list:")
    print(f"  Memory: {list_memory_kb:.2f} KB")
    print(f"  Can calculate: Exact percentiles")
    
    print(f"\nT-Digest (compression=100):")
    print(f"  Memory: {td_memory_kb:.2f} KB")
    print(f"  Centroids: {len(td.centroids)}")
    print(f"  Can calculate: Approximate percentiles")
    print(f"  Memory savings: {(1 - td_memory_kb / list_memory_kb) * 100:.1f}%")
    
    # Accuracy check
    traditional_list.sort()
    exact_p95 = traditional_list[int(len(traditional_list) * 0.95)]
    approx_p95 = td.percentile(95)
    error = abs(approx_p95 - exact_p95) / exact_p95
    
    print(f"\nAccuracy check (p95):")
    print(f"  Exact: {exact_p95:.2f}ms")
    print(f"  T-Digest: {approx_p95:.2f}ms")
    print(f"  Error: {error*100:.2f}%")
    print()


def example_convenience_functions():
    """Using convenience functions."""
    print("=" * 60)
    print("Example 10: Convenience Functions")
    print("=" * 60)
    
    # Generate latency data
    latencies = [random.gauss(100, 25) for _ in range(1000)]
    
    # estimate_quantiles - quick multi-quantile estimation
    p50, p90, p95, p99 = estimate_quantiles(latencies, [0.50, 0.90, 0.95, 0.99])
    
    print("Using estimate_quantiles():")
    print(f"  p50: {p50:.2f}ms")
    print(f"  p90: {p90:.2f}ms")
    print(f"  p95: {p95:.2f}ms")
    print(f"  p99: {p99:.2f}ms")
    
    # merge_tdigests - merge multiple digests
    digests = [TDigest(compression=100) for _ in range(3)]
    
    for i, td in enumerate(digests):
        for _ in range(100):
            td.add(random.gauss(100 + i*50, 20))
    
    merged = merge_tdigests(digests)
    
    print(f"\nUsing merge_tdigests():")
    print(f"  Merged {len(digests)} digests")
    print(f"  Total count: {merged.count:.0f}")
    print(f"  Global p95: {merged.percentile(95):.2f}ms")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("ALGESNAKE PHASE 3 WEEK 5-6")
    print("T-Digest for Quantile Estimation")
    print("=" * 60 + "\n")
    
    # Basic examples
    example_tdigest_basics()
    example_latency_monitoring()
    
    # Distributed examples
    example_distributed_aggregation()
    example_streaming_percentiles()
    
    # Analysis examples
    example_cdf_analysis()
    example_sla_monitoring()
    example_error_vs_success()
    example_quantile_queries()
    
    # Utility examples
    example_memory_efficiency()
    example_convenience_functions()
    
    print("=" * 60)
    print("All examples completed successfully!")
    print("=" * 60)
