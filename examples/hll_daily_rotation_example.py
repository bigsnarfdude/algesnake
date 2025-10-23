"""
HyperLogLog Daily Rotation Example
===================================

This example shows how to:
1. Track unique users per day using HyperLogLog
2. Rotate daily HLLs (keep only last 30 days)
3. Merge 30 days of HLLs to get monthly unique users
4. Generate daily, weekly, and monthly reports
"""

from algesnake.approximate import HyperLogLog
from datetime import datetime, timedelta
from collections import OrderedDict
import random


class MonthlyUserTracker:
    """Track daily unique users with 30-day rotation."""

    def __init__(self, precision=14, max_days=30):
        """
        Args:
            precision: HyperLogLog precision (14 = ~1.6% error, 16 KB)
            max_days: Number of days to keep in rotation (default 30)
        """
        self.precision = precision
        self.max_days = max_days

        # Store one HLL per day (ordered by date)
        self.daily_hlls = OrderedDict()

        # Cache for weekly/monthly aggregates (optional optimization)
        self._monthly_cache = None
        self._cache_date = None

    def add_login(self, user_id, ip_address, timestamp=None):
        """Process a login event."""
        if timestamp is None:
            timestamp = datetime.now()

        date_key = timestamp.date()

        # Create HLL for this day if it doesn't exist
        if date_key not in self.daily_hlls:
            self.daily_hlls[date_key] = {
                'users': HyperLogLog(precision=self.precision),
                'ips': HyperLogLog(precision=self.precision),
                'login_count': 0
            }

        # Add to daily HLLs
        self.daily_hlls[date_key]['users'].add(user_id)
        self.daily_hlls[date_key]['ips'].add(ip_address)
        self.daily_hlls[date_key]['login_count'] += 1

        # Invalidate cache
        self._monthly_cache = None

        # Rotate old data (keep only last 30 days)
        self._rotate_old_days()

    def _rotate_old_days(self):
        """Remove HLLs older than max_days."""
        if len(self.daily_hlls) <= self.max_days:
            return

        # Get cutoff date
        cutoff_date = datetime.now().date() - timedelta(days=self.max_days)

        # Remove old days
        dates_to_remove = [
            date for date in self.daily_hlls.keys()
            if date < cutoff_date
        ]

        for date in dates_to_remove:
            del self.daily_hlls[date]
            print(f"[ROTATION] Removed data for {date} (older than {self.max_days} days)")

    def get_daily_stats(self, date):
        """Get stats for a specific day."""
        if date not in self.daily_hlls:
            return None

        day_data = self.daily_hlls[date]
        return {
            'date': str(date),
            'unique_users': int(day_data['users'].cardinality()),
            'unique_ips': int(day_data['ips'].cardinality()),
            'total_logins': day_data['login_count']
        }

    def get_weekly_stats(self, end_date=None):
        """Get 7-day unique users by merging daily HLLs."""
        if end_date is None:
            end_date = datetime.now().date()

        # Get last 7 days
        last_7_days = [
            end_date - timedelta(days=i)
            for i in range(7)
            if (end_date - timedelta(days=i)) in self.daily_hlls
        ]

        if not last_7_days:
            return None

        # MERGE using sum() - This is the monoid magic!
        weekly_users_hll = sum([
            self.daily_hlls[date]['users']
            for date in last_7_days
        ])

        weekly_ips_hll = sum([
            self.daily_hlls[date]['ips']
            for date in last_7_days
        ])

        total_logins = sum([
            self.daily_hlls[date]['login_count']
            for date in last_7_days
        ])

        return {
            'period': f'{min(last_7_days)} to {max(last_7_days)}',
            'days_included': len(last_7_days),
            'unique_users': int(weekly_users_hll.cardinality()),
            'unique_ips': int(weekly_ips_hll.cardinality()),
            'total_logins': total_logins
        }

    def get_monthly_stats(self):
        """Get 30-day unique users by merging all daily HLLs."""
        if not self.daily_hlls:
            return None

        # MERGE all 30 days using sum() - Monoid operation!
        monthly_users_hll = sum([
            day_data['users']
            for day_data in self.daily_hlls.values()
        ])

        monthly_ips_hll = sum([
            day_data['ips']
            for day_data in self.daily_hlls.values()
        ])

        total_logins = sum([
            day_data['login_count']
            for day_data in self.daily_hlls.values()
        ])

        dates = list(self.daily_hlls.keys())

        return {
            'period': f'{min(dates)} to {max(dates)}',
            'days_included': len(dates),
            'unique_users': int(monthly_users_hll.cardinality()),
            'unique_ips': int(monthly_ips_hll.cardinality()),
            'total_logins': total_logins,
            'avg_daily_logins': total_logins / len(dates),
            'memory_used_kb': self.estimate_memory_usage()
        }

    def estimate_memory_usage(self):
        """Estimate total memory usage in KB."""
        # Each HLL uses m registers * 8 bits
        bytes_per_hll = (2 ** self.precision) * 1  # 1 byte per register
        hlls_per_day = 2  # users + ips
        total_days = len(self.daily_hlls)

        total_bytes = bytes_per_hll * hlls_per_day * total_days
        return total_bytes / 1024

    def print_summary_report(self):
        """Print a complete summary report."""
        print("\n" + "=" * 70)
        print("MONTHLY USER TRACKING REPORT")
        print("=" * 70)

        # Monthly stats
        monthly = self.get_monthly_stats()
        if monthly:
            print(f"\n📊 MONTHLY STATS ({monthly['period']})")
            print(f"   Days tracked: {monthly['days_included']}")
            print(f"   Unique users: {monthly['unique_users']:,}")
            print(f"   Unique IPs: {monthly['unique_ips']:,}")
            print(f"   Total logins: {monthly['total_logins']:,}")
            print(f"   Avg daily logins: {monthly['avg_daily_logins']:.0f}")
            print(f"   Memory used: {monthly['memory_used_kb']:.2f} KB")

        # Weekly stats
        weekly = self.get_weekly_stats()
        if weekly:
            print(f"\n📅 WEEKLY STATS (Last 7 days)")
            print(f"   Period: {weekly['period']}")
            print(f"   Unique users: {weekly['unique_users']:,}")
            print(f"   Unique IPs: {weekly['unique_ips']:,}")
            print(f"   Total logins: {weekly['total_logins']:,}")

        # Recent daily breakdown (last 7 days)
        print(f"\n📆 DAILY BREAKDOWN (Last 7 days)")
        recent_dates = sorted(self.daily_hlls.keys(), reverse=True)[:7]

        print(f"   {'Date':<12} | {'Unique Users':>13} | {'Unique IPs':>11} | {'Total Logins':>13}")
        print(f"   {'-'*12}-+-{'-'*13}-+-{'-'*11}-+-{'-'*13}")

        for date in reversed(recent_dates):
            stats = self.get_daily_stats(date)
            print(f"   {stats['date']:<12} | {stats['unique_users']:>13,} | "
                  f"{stats['unique_ips']:>11,} | {stats['total_logins']:>13,}")

        print("=" * 70 + "\n")


def simulate_realistic_usage():
    """Simulate realistic login patterns over 35 days."""

    tracker = MonthlyUserTracker(precision=14, max_days=30)

    # Base date (35 days ago, so we can see rotation)
    base_date = datetime.now() - timedelta(days=35)

    # User pool
    total_users = 50_000  # 50k registered users

    print("Simulating 35 days of login data...")
    print("(This will trigger rotation after 30 days)\n")

    # Simulate 35 days
    for day_offset in range(35):
        current_date = base_date + timedelta(days=day_offset)

        # Weekday vs weekend patterns
        is_weekend = current_date.weekday() >= 5
        daily_active_users = 15_000 if not is_weekend else 8_000

        # Generate logins for this day
        active_users = random.sample(range(total_users), daily_active_users)

        for user_idx in active_users:
            user_id = f"user_{user_idx}"

            # Each user logs in 1-5 times per day
            login_count = random.randint(1, 5)

            for _ in range(login_count):
                # Simulate IP (some users change IPs)
                ip_base = user_idx % 256
                ip_variant = random.randint(0, 3)  # Occasional IP changes
                ip_address = f"10.{ip_base // 256}.{ip_base % 256}.{ip_variant}"

                # Add timestamp variation (throughout the day)
                timestamp = current_date + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                )

                tracker.add_login(user_id, ip_address, timestamp)

        # Print progress every 7 days
        if (day_offset + 1) % 7 == 0:
            print(f"✓ Processed day {day_offset + 1}/35 ({current_date.date()})")

    print(f"\n✓ Simulation complete!\n")

    # Print final report
    tracker.print_summary_report()

    # Show what happens with rotation
    print("📝 NOTE: Days 1-5 were rotated out (older than 30 days)")
    print(f"   Currently tracking: {len(tracker.daily_hlls)} days")
    print(f"   Memory efficiency: Using only {tracker.estimate_memory_usage():.2f} KB")
    print(f"   vs ~{len(tracker.daily_hlls) * 15_000 * 50 / 1024:.0f} MB for exact tracking\n")


def example_manual_monthly_merge():
    """Show manual merging of 30 daily HLLs."""
    print("=" * 70)
    print("EXAMPLE: Manual Monthly Merge")
    print("=" * 70 + "\n")

    # Create 30 daily HLLs
    daily_hlls = []

    print("Creating 30 daily HLLs...")
    for day in range(30):
        hll = HyperLogLog(precision=14)

        # Each day has 5,000 active users from a pool of 20,000
        active_users = random.sample(range(20_000), 5_000)

        for user_id in active_users:
            hll.add(f"user_{user_id}")

        daily_hlls.append(hll)
        print(f"  Day {day + 1}: {int(hll.cardinality()):,} unique users")

    # Merge all 30 days using + operator
    print("\nMerging 30 days using sum()...")
    monthly_hll = sum(daily_hlls)

    print(f"\n✅ Monthly unique users: {int(monthly_hll.cardinality()):,}")
    print(f"   (Expected ~20,000 since pool has 20k users)")
    print(f"   Memory: {(monthly_hll.m * 1) / 1024:.2f} KB for merged result")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("HYPERLOGLOG DAILY ROTATION & MONTHLY STATS")
    print("=" * 70 + "\n")

    # Example 1: Manual merge
    example_manual_monthly_merge()

    print("\n")

    # Example 2: Realistic usage with rotation
    simulate_realistic_usage()

    print("=" * 70)
    print("Examples complete!")
    print("=" * 70)
