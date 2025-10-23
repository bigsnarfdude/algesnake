"""
HyperLogLog + Bloom Filter for Complete User Tracking
======================================================

This example shows:
1. HLL for counting unique users per day/month
2. Bloom Filter for checking "did specific user log in?"
3. Combining both for complete analytics
"""

from algesnake.approximate import HyperLogLog, BloomFilter
from datetime import datetime, timedelta
from collections import OrderedDict
import random


class CompleteUserTracker:
    """Track both counts AND specific user membership."""

    def __init__(self, precision=14, max_days=30, expected_monthly_users=100_000):
        """
        Args:
            precision: HyperLogLog precision
            max_days: Days to keep in rotation
            expected_monthly_users: Expected unique users per month (for Bloom sizing)
        """
        self.precision = precision
        self.max_days = max_days

        # HLL for COUNTING (per day)
        self.daily_user_counts = OrderedDict()  # date -> HLL

        # Bloom Filter for MEMBERSHIP (per day)
        self.daily_user_membership = OrderedDict()  # date -> BloomFilter

        self.expected_daily_users = expected_monthly_users // 10

    def add_login(self, user_id, timestamp=None):
        """Record a login event."""
        if timestamp is None:
            timestamp = datetime.now()

        date_key = timestamp.date()

        # Create structures for this day if needed
        if date_key not in self.daily_user_counts:
            self.daily_user_counts[date_key] = HyperLogLog(precision=self.precision)
            self.daily_user_membership[date_key] = BloomFilter(
                capacity=self.expected_daily_users,
                error_rate=0.01  # 1% false positive
            )

        # Add to HLL (for counting)
        self.daily_user_counts[date_key].add(user_id)

        # Add to Bloom Filter (for membership)
        self.daily_user_membership[date_key].add(user_id)

        # Rotate old data
        self._rotate_old_days()

    def _rotate_old_days(self):
        """Remove data older than max_days."""
        if len(self.daily_user_counts) <= self.max_days:
            return

        cutoff_date = datetime.now().date() - timedelta(days=self.max_days)

        dates_to_remove = [
            date for date in self.daily_user_counts.keys()
            if date < cutoff_date
        ]

        for date in dates_to_remove:
            del self.daily_user_counts[date]
            del self.daily_user_membership[date]

    # ============================================================
    # COUNTING QUERIES (using HyperLogLog)
    # ============================================================

    def get_daily_user_count(self, date):
        """Get unique user count for a specific day."""
        if date not in self.daily_user_counts:
            return 0
        return int(self.daily_user_counts[date].cardinality())

    def get_weekly_user_count(self, end_date=None):
        """Get unique user count for last 7 days (merged)."""
        if end_date is None:
            end_date = datetime.now().date()

        last_7_days = [
            end_date - timedelta(days=i)
            for i in range(7)
            if (end_date - timedelta(days=i)) in self.daily_user_counts
        ]

        if not last_7_days:
            return 0

        # MERGE 7 days of HLLs using sum()
        weekly_hll = sum([
            self.daily_user_counts[date]
            for date in last_7_days
        ])

        return int(weekly_hll.cardinality())

    def get_monthly_user_count(self):
        """Get unique user count for all tracked days (merged)."""
        if not self.daily_user_counts:
            return 0

        # MERGE all days using sum()
        monthly_hll = sum(self.daily_user_counts.values())

        return int(monthly_hll.cardinality())

    # ============================================================
    # MEMBERSHIP QUERIES (using Bloom Filter)
    # ============================================================

    def did_user_login_on_date(self, user_id, date):
        """Check if specific user logged in on specific date."""
        if date not in self.daily_user_membership:
            return False

        # Check Bloom Filter for this day
        return user_id in self.daily_user_membership[date]

    def did_user_login_this_week(self, user_id, end_date=None):
        """Check if specific user logged in during last 7 days."""
        if end_date is None:
            end_date = datetime.now().date()

        last_7_days = [
            end_date - timedelta(days=i)
            for i in range(7)
            if (end_date - timedelta(days=i)) in self.daily_user_membership
        ]

        # Check any day in the week
        for date in last_7_days:
            if user_id in self.daily_user_membership[date]:
                return True

        return False

    def did_user_login_this_month(self, user_id):
        """Check if specific user logged in during tracked period."""
        # Check all tracked days
        for bloom_filter in self.daily_user_membership.values():
            if user_id in bloom_filter:
                return True

        return False

    def get_user_login_dates(self, user_id):
        """Get dates when user logged in (may have 1% false positives)."""
        login_dates = []

        for date, bloom_filter in self.daily_user_membership.items():
            if user_id in bloom_filter:
                login_dates.append(date)

        return login_dates

    # ============================================================
    # REPORTING
    # ============================================================

    def print_report(self):
        """Print complete tracking report."""
        print("\n" + "=" * 70)
        print("COMPLETE USER TRACKING REPORT")
        print("=" * 70)

        # Monthly counts (HLL)
        monthly_count = self.get_monthly_user_count()
        print(f"\n📊 UNIQUE USER COUNTS (via HyperLogLog)")
        print(f"   Monthly: {monthly_count:,} unique users")
        print(f"   Weekly:  {self.get_weekly_user_count():,} unique users")

        # Recent daily counts
        print(f"\n📆 DAILY USER COUNTS (Last 7 days)")
        recent_dates = sorted(self.daily_user_counts.keys(), reverse=True)[:7]

        for date in reversed(recent_dates):
            count = self.get_daily_user_count(date)
            print(f"   {date}: {count:,} unique users")

        # Memory usage
        hll_memory = len(self.daily_user_counts) * (2 ** self.precision) / 1024
        bloom_memory = sum([
            bf.size / 8 / 1024
            for bf in self.daily_user_membership.values()
        ])

        print(f"\n💾 MEMORY USAGE")
        print(f"   HyperLogLog: {hll_memory:.2f} KB")
        print(f"   Bloom Filter: {bloom_memory:.2f} KB")
        print(f"   Total: {hll_memory + bloom_memory:.2f} KB")

        print("=" * 70 + "\n")


def example_1_basic_tracking():
    """Example 1: Basic counting vs membership queries."""
    print("=" * 70)
    print("EXAMPLE 1: Counting vs Membership Queries")
    print("=" * 70 + "\n")

    tracker = CompleteUserTracker(precision=14, expected_monthly_users=10_000)

    # Simulate 7 days of logins
    print("Simulating 7 days of login data...\n")

    base_date = datetime.now() - timedelta(days=7)
    all_users = []

    for day_offset in range(7):
        current_date = base_date + timedelta(days=day_offset)

        # 1000 users per day, some overlap
        daily_users = [
            f"user_{random.randint(1, 5000)}"
            for _ in range(1000)
        ]

        for user_id in daily_users:
            tracker.add_login(user_id, current_date)
            all_users.append(user_id)

    # QUERY 1: How many unique users this week? (HLL)
    weekly_count = tracker.get_weekly_user_count()
    print(f"✅ COUNTING QUERY (HLL):")
    print(f"   Weekly unique users: ~{weekly_count:,}")

    # QUERY 2: Did specific users log in? (Bloom Filter)
    test_users = ["user_1", "user_100", "user_5000", "user_9999"]

    print(f"\n✅ MEMBERSHIP QUERIES (Bloom Filter):")
    for user_id in test_users:
        logged_in = tracker.did_user_login_this_week(user_id)
        status = "✓ YES" if logged_in else "✗ NO"
        print(f"   Did {user_id} log in this week? {status}")

    # QUERY 3: When did user_1 log in? (Bloom Filter)
    print(f"\n✅ USER LOGIN HISTORY (Bloom Filter):")
    login_dates = tracker.get_user_login_dates("user_1")
    if login_dates:
        print(f"   user_1 logged in on {len(login_dates)} days:")
        for date in login_dates:
            print(f"      - {date}")
    else:
        print(f"   user_1 did not log in (or wasn't tracked)")

    print()


def example_2_specific_user_queries():
    """Example 2: Checking specific users across different time periods."""
    print("=" * 70)
    print("EXAMPLE 2: Specific User Queries Across Time")
    print("=" * 70 + "\n")

    tracker = CompleteUserTracker(precision=14, expected_monthly_users=50_000)

    # Simulate 30 days
    print("Simulating 30 days of login data...\n")

    base_date = datetime.now() - timedelta(days=30)

    # Create some known users with specific patterns
    power_user = "user_power"  # Logs in every day
    weekly_user = "user_weekly"  # Logs in once a week
    monthly_user = "user_monthly"  # Logged in once, 20 days ago
    inactive_user = "user_inactive"  # Never logged in

    for day_offset in range(30):
        current_date = base_date + timedelta(days=day_offset)

        # Power user: every day
        tracker.add_login(power_user, current_date)

        # Weekly user: every 7 days
        if day_offset % 7 == 0:
            tracker.add_login(weekly_user, current_date)

        # Monthly user: only day 10
        if day_offset == 10:
            tracker.add_login(monthly_user, current_date)

        # Add random users
        for _ in range(1000):
            user_id = f"user_{random.randint(1, 20000)}"
            tracker.add_login(user_id, current_date)

    # Query patterns
    print("USER LOGIN PATTERNS:")
    print()

    for user_id, expected_pattern in [
        (power_user, "daily"),
        (weekly_user, "weekly"),
        (monthly_user, "monthly"),
        (inactive_user, "never")
    ]:
        print(f"🔍 Checking: {user_id} (expected: {expected_pattern})")

        # Check this month
        logged_in_month = tracker.did_user_login_this_month(user_id)
        print(f"   This month: {'✓ YES' if logged_in_month else '✗ NO'}")

        # Check this week
        logged_in_week = tracker.did_user_login_this_week(user_id)
        print(f"   This week:  {'✓ YES' if logged_in_week else '✗ NO'}")

        # Check today
        today = datetime.now().date()
        logged_in_today = tracker.did_user_login_on_date(user_id, today)
        print(f"   Today:      {'✓ YES' if logged_in_today else '✗ NO'}")

        # Get all login dates
        login_dates = tracker.get_user_login_dates(user_id)
        print(f"   Login days: {len(login_dates)} days")
        print()

    # Show overall stats
    print("📊 OVERALL STATISTICS:")
    print(f"   Monthly unique users: ~{tracker.get_monthly_user_count():,}")
    print(f"   Weekly unique users:  ~{tracker.get_weekly_user_count():,}")
    print()


def example_3_memory_comparison():
    """Example 3: Compare memory usage."""
    print("=" * 70)
    print("EXAMPLE 3: Memory Comparison")
    print("=" * 70 + "\n")

    tracker = CompleteUserTracker(precision=14, expected_monthly_users=100_000)

    # Simulate realistic usage
    print("Simulating 30 days with 100k users/day...\n")

    base_date = datetime.now() - timedelta(days=30)

    for day_offset in range(30):
        current_date = base_date + timedelta(days=day_offset)

        # 100k events per day
        for _ in range(100_000):
            user_id = f"user_{random.randint(1, 500_000)}"
            tracker.add_login(user_id, current_date)

        if (day_offset + 1) % 10 == 0:
            print(f"   Processed day {day_offset + 1}/30")

    print("\n✅ Simulation complete!\n")

    # Calculate memory usage
    hll_memory = len(tracker.daily_user_counts) * (2 ** tracker.precision) / 1024
    bloom_memory = sum([
        bf.size / 8 / 1024
        for bf in tracker.daily_user_membership.values()
    ])
    total_memory = hll_memory + bloom_memory

    # Traditional approach memory estimate
    # 30 days × 100k users × 20 bytes per user_id average
    traditional_memory = 30 * 100_000 * 20 / 1024

    print("💾 MEMORY COMPARISON:")
    print(f"   Traditional (storing all user IDs): {traditional_memory:,.0f} KB")
    print(f"   HyperLogLog (counting):              {hll_memory:,.2f} KB")
    print(f"   Bloom Filter (membership):           {bloom_memory:,.2f} KB")
    print(f"   Total (HLL + Bloom):                 {total_memory:,.2f} KB")
    print(f"   Memory savings:                      {(1 - total_memory / traditional_memory) * 100:.2f}%")

    print("\n📊 CAPABILITIES:")
    print("   Traditional:")
    print("      ✅ Count unique users (exact)")
    print("      ✅ Check specific user (exact)")
    print("      ✅ List all users")
    print("      ❌ Uses massive memory")
    print()
    print("   HLL + Bloom Filter:")
    print("      ✅ Count unique users (~2% error)")
    print("      ✅ Check specific user (~1% false positive)")
    print("      ❌ Cannot list all users")
    print("      ✅ 99%+ memory savings!")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("HYPERLOGLOG + BLOOM FILTER USER TRACKING")
    print("=" * 70 + "\n")

    example_1_basic_tracking()
    print("\n")

    example_2_specific_user_queries()
    print("\n")

    example_3_memory_comparison()

    print("=" * 70)
    print("Examples complete!")
    print("=" * 70)
