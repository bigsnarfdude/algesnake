"""
Example 10: Distributed Log Analysis

Real-world use case: Analyze logs across distributed Twitter services
Inspired by: Spark + Algebird examples for log processing

Problem:
- Logs from thousands of servers (API, database, cache, etc.)
- Need to find: error frequencies, suspicious IPs, performance issues
- Can't collect all logs centrally (too much data)

Solution:
- Process logs on each server using monoids
- Merge results from all servers  
- CountMinSketch for frequencies, HLL for cardinality, TopK for heavy hitters
"""

from algesnake.approximate import CountMinSketch, HyperLogLog, TopK
from algesnake.approximate.countminsketch import heavy_hitters
from algesnake import Add, Max, Min, MapMonoid
import random
from datetime import datetime, timedelta


class LogAnalyzer:
    """
    Distributed log analyzer using monoids.
    
    Can run on each server independently, then merge results.
    """
    
    def __init__(self):
        # Error frequency tracking
        self.error_frequencies = CountMinSketch.from_error_rate(epsilon=0.01, delta=0.01)
        
        # Top errors
        self.top_errors = TopK(k=20)
        
        # IP address tracking
        self.unique_ips = HyperLogLog(precision=14)
        self.ip_frequencies = CountMinSketch.from_error_rate(epsilon=0.01, delta=0.01)
        
        # HTTP status codes
        self.status_codes = CountMinSketch(width=100, depth=5)
        
        # Response time metrics
        self.total_requests = Add(0)
        self.max_response_time = Max(0)
        self.min_response_time = Min(float('inf'))
        
        # Error count
        self.error_count = Add(0)
        self.success_count = Add(0)
    
    def process_log_line(self, log):
        """
        Process a single log line.
        
        Log format: {
            'timestamp': datetime,
            'ip': str,
            'method': str,
            'path': str,
            'status': int,
            'response_time_ms': float,
            'error_type': str (if error)
        }
        """
        self.total_requests += Add(1)
        
        # Track IP addresses
        self.unique_ips.add(log['ip'])
        self.ip_frequencies.add(log['ip'])
        
        # Track status codes
        self.status_codes.add(str(log['status']))
        
        # Track response times
        self.max_response_time += Max(log['response_time_ms'])
        if log['response_time_ms'] > 0:
            self.min_response_time += Min(log['response_time_ms'])
        
        # Track errors
        if log['status'] >= 400:
            self.error_count += Add(1)
            
            if 'error_type' in log:
                self.error_frequencies.add(log['error_type'])
                self.top_errors.add(log['error_type'])
        else:
            self.success_count += Add(1)
    
    def find_suspicious_ips(self, threshold=1000):
        """Find IPs with >threshold requests (potential abuse)."""
        # Get all unique IPs (approximate)
        # In production, you'd maintain a separate list of candidate IPs
        all_ips = []  # Would come from separate tracking
        return heavy_hitters(self.ip_frequencies, all_ips, threshold)
    
    def get_error_report(self):
        """Generate error analysis report."""
        return {
            'total_requests': self.total_requests.value,
            'errors': self.error_count.value,
            'successes': self.success_count.value,
            'error_rate': (self.error_count.value / self.total_requests.value * 100) if self.total_requests.value > 0 else 0,
            'unique_ips': int(self.unique_ips.cardinality()),
            'max_response_time': self.max_response_time.value,
            'min_response_time': self.min_response_time.value if self.min_response_time.value != float('inf') else 0,
            'top_errors': self.top_errors.top(10)
        }
    
    def merge(self, other):
        """Merge log analyzers (MONOID OPERATION!)."""
        merged = LogAnalyzer()
        
        # Merge all monoids
        merged.error_frequencies = self.error_frequencies + other.error_frequencies
        merged.top_errors = self.top_errors + other.top_errors
        merged.unique_ips = self.unique_ips + other.unique_ips
        merged.ip_frequencies = self.ip_frequencies + other.ip_frequencies
        merged.status_codes = self.status_codes + other.status_codes
        merged.total_requests = self.total_requests + other.total_requests
        merged.max_response_time = self.max_response_time + other.max_response_time
        merged.min_response_time = self.min_response_time + other.min_response_time
        merged.error_count = self.error_count + other.error_count
        merged.success_count = self.success_count + other.success_count
        
        return merged
    
    def __add__(self, other):
        return self.merge(other)


def generate_log_data(num_logs, error_rate=0.05, server_name="server"):
    """Generate synthetic log data."""
    error_types = [
        "InternalServerError",
        "DatabaseTimeout",
        "CacheConnectionError",
        "OutOfMemoryError",
        "NullPointerException",
        "AuthenticationFailure",
        "RateLimitExceeded",
    ]
    
    paths = ["/api/tweets", "/api/users", "/api/timeline", "/api/search"]
    
    logs = []
    base_time = datetime.now()
    
    for i in range(num_logs):
        is_error = random.random() < error_rate
        
        if is_error:
            status = random.choice([500, 503, 502, 504, 400, 401, 403, 404])
            response_time = random.gauss(2000, 500)  # Slower for errors
            error_type = random.choice(error_types)
        else:
            status = random.choice([200, 201, 204])
            response_time = random.gauss(100, 30)  # Fast for success
            error_type = None
        
        log = {
            'timestamp': base_time + timedelta(seconds=i),
            'ip': f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
            'method': 'GET',
            'path': random.choice(paths),
            'status': status,
            'response_time_ms': max(response_time, 0),
        }
        
        if error_type:
            log['error_type'] = error_type
        
        logs.append(log)
    
    return logs


def simulate_distributed_logging():
    """Simulate distributed log analysis across multiple servers."""
    print("\n" + "="*70)
    print("DISTRIBUTED LOG ANALYSIS (Spark-Style)")
    print("="*70)
    
    # Simulate 5 servers
    servers = ["api-server-1", "api-server-2", "api-server-3", 
               "db-server-1", "cache-server-1"]
    
    server_analyzers = {}
    
    print("\nProcessing logs on each server...")
    for server in servers:
        analyzer = LogAnalyzer()
        
        # Each server has different load
        num_logs = random.randint(5000, 15000)
        error_rate = random.uniform(0.03, 0.08)  # 3-8% error rate
        
        logs = generate_log_data(num_logs, error_rate, server)
        
        # Process logs on this server
        for log in logs:
            analyzer.process_log_line(log)
        
        server_analyzers[server] = analyzer
        
        report = analyzer.get_error_report()
        print(f"  {server:15} {report['total_requests']:>7,} requests, "
              f"{report['errors']:>5,} errors ({report['error_rate']:>4.1f}%)")
    
    # MERGE all servers (MONOID OPERATION!)
    print("\nMerging logs from all servers...")
    global_analyzer = server_analyzers[servers[0]]
    for server in servers[1:]:
        global_analyzer = global_analyzer + server_analyzers[server]
    
    # Global report
    global_report = global_analyzer.get_error_report()
    
    print("\n" + "="*70)
    print("GLOBAL LOG ANALYSIS")
    print("="*70)
    print(f"\nTotal requests: {global_report['total_requests']:,}")
    print(f"Successes: {global_report['successes']:,}")
    print(f"Errors: {global_report['errors']:,}")
    print(f"Error rate: {global_report['error_rate']:.2f}%")
    print(f"\nUnique IPs: {global_report['unique_ips']:,}")
    print(f"\nResponse times:")
    print(f"  Min: {global_report['min_response_time']:.1f}ms")
    print(f"  Max: {global_report['max_response_time']:.1f}ms")
    
    print(f"\n{'='*70}")
    print("TOP 10 ERROR TYPES")
    print("="*70)
    print(f"{'Rank':<6} {'Error Type':<30} {'Count':<10}")
    print("-" * 70)
    
    for i, (error, count) in enumerate(global_report['top_errors'], 1):
        print(f"{i:<6} {error:<30} {count:<10,}")
    
    print("\n✓ Merged logs from 5 servers using monoid operations!")
    print("✓ This is how Spark + Algebird processes logs at scale!")


def time_window_analysis():
    """Analyze logs across time windows."""
    print("\n" + "="*70)
    print("TIME WINDOW ANALYSIS (5-minute windows)")
    print("="*70)
    
    # 6 five-minute windows (30 minutes total)
    window_analyzers = []
    
    for window in range(6):
        analyzer = LogAnalyzer()
        
        # Generate logs for this window
        logs = generate_log_data(2000, error_rate=0.05)
        
        for log in logs:
            analyzer.process_log_line(log)
        
        window_analyzers.append(analyzer)
        
        report = analyzer.get_error_report()
        print(f"  Window {window+1}: {report['total_requests']:>5,} requests, "
              f"{report['error_rate']:>4.1f}% errors")
    
    # Merge all windows
    total_analyzer = window_analyzers[0]
    for analyzer in window_analyzers[1:]:
        total_analyzer = total_analyzer + analyzer
    
    total_report = total_analyzer.get_error_report()
    print(f"\n  Total (30 min): {total_report['total_requests']:>5,} requests, "
          f"{total_report['error_rate']:>4.1f}% errors")
    
    print("\n✓ Merged 6 time windows using monoid operations!")


if __name__ == "__main__":
    print("="*70)
    print("Distributed Log Analysis with Monoids")
    print("Based on Spark + Algebird Log Processing Examples")
    print("="*70)
    
    # Simulate single server analysis
    print("\nSingle server log analysis...")
    analyzer = LogAnalyzer()
    
    logs = generate_log_data(10000, error_rate=0.06)
    
    for log in logs:
        analyzer.process_log_line(log)
    
    report = analyzer.get_error_report()
    
    print("\n" + "="*70)
    print("SINGLE SERVER REPORT")
    print("="*70)
    print(f"Total requests: {report['total_requests']:,}")
    print(f"Error rate: {report['error_rate']:.2f}%")
    print(f"Unique IPs: {report['unique_ips']:,}")
    
    print(f"\nTop 5 errors:")
    for i, (error, count) in enumerate(report['top_errors'][:5], 1):
        print(f"  {i}. {error}: {count:,}")
    
    # Distributed analysis
    simulate_distributed_logging()
    
    # Time window analysis
    time_window_analysis()
    
    print("\n" + "="*70)
    print("KEY TAKEAWAYS")
    print("="*70)
    print("✓ Process logs locally on each server using monoids")
    print("✓ Merge results from all servers (no central bottleneck)")
    print("✓ CountMinSketch + TopK for error frequency analysis")
    print("✓ HyperLogLog for unique IP counting")
    print("✓ Works for both server-based and time-based aggregation")
    print("✓ This is exactly how Spark + Algebird processes logs")
    print("="*70)
