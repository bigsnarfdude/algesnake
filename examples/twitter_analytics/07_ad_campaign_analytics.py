"""
Example 07: Ad Campaign Analytics

Real-world use case: Twitter advertising platform analytics
Inspired by: Twitter's advertising infrastructure built on Summingbird + Algebird

Twitter's Original Problem:
- Monitor ad campaigns across millions of impressions
- Track: CTR, reach, spend, latency, conversion metrics
- Must aggregate in real-time across distributed servers
- Need percentile metrics for SLA monitoring

Solution:
- Combination of multiple monoid structures
- HyperLogLog for unique reach
- TDigest for latency percentiles  
- Add for counters
- Works in both batch and streaming modes

Reference: Michael Noll's blog - "Twitter advertising is also built on Summingbird"
"""

from algesnake import Add, Max, Min
from algesnake.approximate import HyperLogLog, TDigest
import random


class AdCampaignMonitor:
    """
    Monitor ad campaign performance like Twitter's advertising platform.
    
    Combines multiple monoids for comprehensive campaign analytics.
    """
    
    def __init__(self, campaign_id):
        self.campaign_id = campaign_id
        
        # Count metrics (Add monoid)
        self.impressions = Add(0)
        self.clicks = Add(0)
        self.conversions = Add(0)
        self.spend = Add(0.0)
        
        # Unique metrics (HyperLogLog)
        self.unique_users = HyperLogLog(precision=14)
        self.unique_clickers = HyperLogLog(precision=14)
        
        # Latency metrics (TDigest for percentiles)
        self.ad_load_latency = TDigest(compression=100)
        self.click_latency = TDigest(compression=100)
        
        # Max/Min metrics
        self.max_ctr_hour = Max(0.0)
        self.min_ctr_hour = Min(float('inf'))
        
    def record_impression(self, user_id, latency_ms):
        """Record an ad impression."""
        self.impressions += Add(1)
        self.unique_users.add(user_id)
        self.ad_load_latency.add(latency_ms)
        
    def record_click(self, user_id, latency_ms, cost):
        """Record an ad click."""
        self.clicks += Add(1)
        self.unique_clickers.add(user_id)
        self.click_latency.add(latency_ms)
        self.spend += Add(cost)
        
    def record_conversion(self, revenue):
        """Record a conversion (purchase, signup, etc.)."""
        self.conversions += Add(1)
        
    def merge(self, other):
        """Merge campaign metrics (MONOID OPERATION!)."""
        merged = AdCampaignMonitor(self.campaign_id)
        
        merged.impressions = self.impressions + other.impressions
        merged.clicks = self.clicks + other.clicks
        merged.conversions = self.conversions + other.conversions
        merged.spend = self.spend + other.spend
        
        merged.unique_users = self.unique_users + other.unique_users
        merged.unique_clickers = self.unique_clickers + other.unique_clickers
        
        merged.ad_load_latency = self.ad_load_latency + other.ad_load_latency
        merged.click_latency = self.click_latency + other.click_latency
        
        merged.max_ctr_hour = self.max_ctr_hour + other.max_ctr_hour
        merged.min_ctr_hour = self.min_ctr_hour + other.min_ctr_hour
        
        return merged
    
    def __add__(self, other):
        return self.merge(other)
    
    def get_metrics(self):
        """Calculate campaign metrics."""
        ctr = (self.clicks.value / self.impressions.value * 100) if self.impressions.value > 0 else 0
        cvr = (self.conversions.value / self.clicks.value * 100) if self.clicks.value > 0 else 0
        cpc = (self.spend.value / self.clicks.value) if self.clicks.value > 0 else 0
        cpm = (self.spend.value / self.impressions.value * 1000) if self.impressions.value > 0 else 0
        
        return {
            'campaign_id': self.campaign_id,
            'impressions': self.impressions.value,
            'clicks': self.clicks.value,
            'conversions': self.conversions.value,
            'ctr': ctr,
            'cvr': cvr,
            'unique_reach': int(self.unique_users.cardinality()),
            'unique_clickers': int(self.unique_clickers.cardinality()),
            'frequency': self.impressions.value / self.unique_users.cardinality() if self.unique_users.cardinality() > 0 else 0,
            'spend': self.spend.value,
            'cpc': cpc,
            'cpm': cpm,
            'latency': {
                'ad_load_p50': self.ad_load_latency.percentile(50) if self.ad_load_latency.count > 0 else 0,
                'ad_load_p95': self.ad_load_latency.percentile(95) if self.ad_load_latency.count > 0 else 0,
                'ad_load_p99': self.ad_load_latency.percentile(99) if self.ad_load_latency.count > 0 else 0,
                'click_p50': self.click_latency.percentile(50) if self.click_latency.count > 0 else 0,
                'click_p95': self.click_latency.percentile(95) if self.click_latency.count > 0 else 0,
            }
        }


def run_campaign_simulation():
    """Simulate a Twitter ad campaign."""
    campaign = AdCampaignMonitor("summer_sale_2024")
    
    # Simulate 100K impressions
    for i in range(100000):
        user_id = f"user_{random.randint(1, 50000)}"  # 50K unique users
        latency = random.gauss(50, 15)  # 50ms avg latency
        campaign.record_impression(user_id, max(latency, 0))
        
        # 3% CTR
        if random.random() < 0.03:
            click_latency = random.gauss(100, 30)
            cost = 0.50  # $0.50 per click
            campaign.record_click(user_id, max(click_latency, 0), cost)
            
            # 5% conversion rate (of clicks)
            if random.random() < 0.05:
                campaign.record_conversion(revenue=50.0)
    
    return campaign


def multi_campaign_analysis():
    """Analyze multiple campaigns."""
    print("\n" + "="*70)
    print("MULTI-CAMPAIGN ANALYSIS")
    print("="*70)
    
    campaigns = {}
    
    for campaign_id, impressions, ctr in [
        ("campaign_a", 50000, 0.04),  # High CTR
        ("campaign_b", 80000, 0.02),  # Low CTR
        ("campaign_c", 60000, 0.035), # Medium CTR
    ]:
        monitor = AdCampaignMonitor(campaign_id)
        
        for _ in range(impressions):
            user_id = f"user_{random.randint(1, 30000)}"
            latency = random.gauss(50, 10)
            monitor.record_impression(user_id, max(latency, 0))
            
            if random.random() < ctr:
                click_latency = random.gauss(100, 20)
                monitor.record_click(user_id, max(click_latency, 0), 0.50)
                
                if random.random() < 0.05:
                    monitor.record_conversion(50.0)
        
        campaigns[campaign_id] = monitor
    
    # Print individual campaign metrics
    print(f"\n{'Campaign':<15} {'Impressions':>12} {'Clicks':>8} {'CTR':>8} {'Spend':>10} {'CPC':>8}")
    print("-" * 70)
    
    for campaign_id, monitor in campaigns.items():
        metrics = monitor.get_metrics()
        print(f"{campaign_id:<15} {metrics['impressions']:>12,} {metrics['clicks']:>8,} "
              f"{metrics['ctr']:>7.2f}% ${metrics['spend']:>9.2f} ${metrics['cpc']:>7.2f}")
    
    # Aggregate all campaigns
    global_monitor = campaigns["campaign_a"]
    for campaign_id in ["campaign_b", "campaign_c"]:
        global_monitor = global_monitor + campaigns[campaign_id]
    
    global_metrics = global_monitor.get_metrics()
    print("-" * 70)
    print(f"{'TOTAL':<15} {global_metrics['impressions']:>12,} {global_metrics['clicks']:>8,} "
          f"{global_metrics['ctr']:>7.2f}% ${global_metrics['spend']:>9.2f} ${global_metrics['cpc']:>7.2f}")


def distributed_campaign_monitoring():
    """Monitor campaign across distributed servers."""
    print("\n" + "="*70)
    print("DISTRIBUTED CAMPAIGN MONITORING")
    print("="*70)
    
    campaign_id = "global_campaign"
    
    # 3 servers in different regions
    us_monitor = AdCampaignMonitor(campaign_id)
    eu_monitor = AdCampaignMonitor(campaign_id)
    asia_monitor = AdCampaignMonitor(campaign_id)
    
    # Each server processes impressions in its region
    for _ in range(30000):
        user_id = f"us_user_{random.randint(1, 20000)}"
        us_monitor.record_impression(user_id, random.gauss(40, 10))
        if random.random() < 0.03:
            us_monitor.record_click(user_id, random.gauss(90, 20), 0.50)
    
    for _ in range(40000):
        user_id = f"eu_user_{random.randint(1, 25000)}"
        eu_monitor.record_impression(user_id, random.gauss(60, 15))
        if random.random() < 0.025:
            eu_monitor.record_click(user_id, random.gauss(110, 25), 0.50)
    
    for _ in range(35000):
        user_id = f"asia_user_{random.randint(1, 22000)}"
        asia_monitor.record_impression(user_id, random.gauss(80, 20))
        if random.random() < 0.028:
            asia_monitor.record_click(user_id, random.gauss(130, 30), 0.50)
    
    # Regional reports
    print("\nRegional Performance:")
    for region, monitor in [("US", us_monitor), ("EU", eu_monitor), ("ASIA", asia_monitor)]:
        metrics = monitor.get_metrics()
        print(f"\n{region}:")
        print(f"  Impressions: {metrics['impressions']:,}")
        print(f"  Clicks: {metrics['clicks']:,}")
        print(f"  CTR: {metrics['ctr']:.2f}%")
        print(f"  Latency p95: {metrics['latency']['ad_load_p95']:.1f}ms")
    
    # MERGE all regions (MONOID OPERATION!)
    global_monitor = us_monitor + eu_monitor + asia_monitor
    global_metrics = global_monitor.get_metrics()
    
    print("\n" + "-"*70)
    print("GLOBAL (Merged):")
    print(f"  Total impressions: {global_metrics['impressions']:,}")
    print(f"  Total clicks: {global_metrics['clicks']:,}")
    print(f"  Global CTR: {global_metrics['ctr']:.2f}%")
    print(f"  Unique reach: {global_metrics['unique_reach']:,}")
    print(f"  Global latency p95: {global_metrics['latency']['ad_load_p95']:.1f}ms")
    print("\n✓ Merged metrics from 3 regions using monoid operations!")


if __name__ == "__main__":
    print("="*70)
    print("Twitter Ad Campaign Analytics")
    print("Based on Twitter's Advertising Platform (Summingbird + Algebird)")
    print("="*70)
    
    # Run simulation
    print("\nRunning campaign simulation...")
    campaign = run_campaign_simulation()
    
    # Get metrics
    metrics = campaign.get_metrics()
    
    print("\n" + "="*70)
    print(f"CAMPAIGN REPORT: {metrics['campaign_id']}")
    print("="*70)
    print(f"\nPerformance:")
    print(f"  Impressions: {metrics['impressions']:,}")
    print(f"  Clicks: {metrics['clicks']:,}")
    print(f"  Conversions: {metrics['conversions']:,}")
    print(f"  CTR: {metrics['ctr']:.2f}%")
    print(f"  CVR: {metrics['cvr']:.2f}%")
    
    print(f"\nReach:")
    print(f"  Unique users reached: {metrics['unique_reach']:,}")
    print(f"  Unique clickers: {metrics['unique_clickers']:,}")
    print(f"  Frequency: {metrics['frequency']:.2f} impressions/user")
    
    print(f"\nCost:")
    print(f"  Total spend: ${metrics['spend']:.2f}")
    print(f"  CPC: ${metrics['cpc']:.2f}")
    print(f"  CPM: ${metrics['cpm']:.2f}")
    
    print(f"\nLatency (SLA Monitoring):")
    print(f"  Ad load p50: {metrics['latency']['ad_load_p50']:.1f}ms")
    print(f"  Ad load p95: {metrics['latency']['ad_load_p95']:.1f}ms")
    print(f"  Ad load p99: {metrics['latency']['ad_load_p99']:.1f}ms")
    print(f"  Click p50: {metrics['latency']['click_p50']:.1f}ms")
    print(f"  Click p95: {metrics['latency']['click_p95']:.1f}ms")
    
    # Multi-campaign analysis
    multi_campaign_analysis()
    
    # Distributed monitoring
    distributed_campaign_monitoring()
    
    print("\n" + "="*70)
    print("KEY TAKEAWAYS")
    print("="*70)
    print("✓ Combines multiple monoids: Add, HLL, TDigest, Max, Min")
    print("✓ TDigest provides SLA monitoring with percentiles")
    print("✓ HyperLogLog tracks unique reach efficiently")
    print("✓ All metrics merge via monoid operations")
    print("✓ This is how Twitter's advertising platform works at scale")
    print("="*70)
