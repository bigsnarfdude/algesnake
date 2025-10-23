"""
Example 06: Timeline Builder (Non-Commutative Monoid)

Real-world use case: Build user timelines with ordered tweet lists
Inspired by: Twitter's actual use of ListMonoid for timeline construction

Twitter's Original Problem:
- Build user timelines: (user_id, timestamp) -> [tweets]
- Order matters! Tweets must appear in chronological order
- Must aggregate across distributed servers
- Can't rely on processing order (distributed systems)

Solution:
- ListMonoid with append operation (non-commutative but associative)
- Deterministic ordering using timestamps
- Works in distributed environments

Reference: Michael Noll's blog - "List monoid for Twitter timelines"
Quote: "the key is userid,time and the value is the list of tweets over that timeline
(so ordering matters here)"
"""

from algesnake import ListMonoid
from algesnake.approximate import HyperLogLog
import random
from datetime import datetime, timedelta


class Tweet:
    """Represents a single tweet."""

    def __init__(self, tweet_id, user_id, text, timestamp, likes=0):
        self.tweet_id = tweet_id
        self.user_id = user_id
        self.text = text
        self.timestamp = timestamp
        self.likes = likes

    def __repr__(self):
        return f"Tweet({self.tweet_id}, @{self.user_id}, {self.timestamp.strftime('%H:%M:%S')})"

    def to_dict(self):
        return {
            'tweet_id': self.tweet_id,
            'user_id': self.user_id,
            'text': self.text,
            'timestamp': self.timestamp.isoformat(),
            'likes': self.likes
        }


class TimelineBuilder:
    """
    Build user timelines using ListMonoid.

    Key insight: Lists are associative (can group operations) but
    NOT commutative (order matters). This is perfect for timelines!

    Associative: (a + b) + c = a + (b + c) ✓
    Commutative: a + b = b + a ✗ (for timelines)
    """

    def __init__(self, user_id):
        self.user_id = user_id
        # Timeline is a list of tweets (ordered!)
        self.timeline = ListMonoid([])

    def add_tweet(self, tweet):
        """Add a tweet to timeline."""
        # Convert tweet to list and append
        tweet_list = ListMonoid([tweet])
        self.timeline = self.timeline + tweet_list

    def add_tweets(self, tweets):
        """Add multiple tweets (preserving order)."""
        tweets_list = ListMonoid(tweets)
        self.timeline = self.timeline + tweets_list

    def merge(self, other):
        """
        Merge timelines from different sources.

        IMPORTANT: This concatenates lists, so pre-sorting by timestamp
        is required for correct chronological order.
        """
        merged = TimelineBuilder(self.user_id)
        merged.timeline = self.timeline + other.timeline
        return merged

    def __add__(self, other):
        """Enable + operator for merging."""
        return self.merge(other)

    def get_timeline(self, limit=None):
        """Get timeline tweets."""
        tweets = self.timeline.value
        if limit:
            return tweets[:limit]
        return tweets

    def get_sorted_timeline(self, limit=None):
        """Get timeline sorted by timestamp (most recent first)."""
        sorted_tweets = sorted(self.timeline.value,
                             key=lambda t: t.timestamp,
                             reverse=True)
        if limit:
            return sorted_tweets[:limit]
        return sorted_tweets


def demonstrate_associativity():
    """
    Demonstrate that ListMonoid is associative.

    This is why it works in distributed systems!
    """
    print("\n" + "="*70)
    print("DEMONSTRATING ASSOCIATIVITY")
    print("="*70)

    # Create tweets at different times
    now = datetime.now()
    tweet1 = Tweet("t1", "alice", "First tweet", now)
    tweet2 = Tweet("t2", "alice", "Second tweet", now + timedelta(minutes=1))
    tweet3 = Tweet("t3", "alice", "Third tweet", now + timedelta(minutes=2))
    tweet4 = Tweet("t4", "alice", "Fourth tweet", now + timedelta(minutes=3))

    # Create lists
    list1 = ListMonoid([tweet1, tweet2])
    list2 = ListMonoid([tweet3])
    list3 = ListMonoid([tweet4])

    # Different groupings (associativity test)
    result1 = (list1 + list2) + list3  # Group (1,2) first, then add 3
    result2 = list1 + (list2 + list3)  # Group (2,3) first, then prepend 1

    print("\nGrouping 1: (list1 + list2) + list3")
    print(f"  Result: {[t.tweet_id for t in result1.value]}")

    print("\nGrouping 2: list1 + (list2 + list3)")
    print(f"  Result: {[t.tweet_id for t in result2.value]}")

    print("\nAssociativity check:", result1.value == result2.value, "✓")
    print("\nThis property allows flexible grouping in distributed systems!")


def demonstrate_non_commutativity():
    """
    Demonstrate that ListMonoid is NOT commutative.

    Order matters for timelines!
    """
    print("\n" + "="*70)
    print("DEMONSTRATING NON-COMMUTATIVITY (Order Matters!)")
    print("="*70)

    now = datetime.now()
    tweet1 = Tweet("t1", "bob", "Morning tweet", now)
    tweet2 = Tweet("t2", "bob", "Evening tweet", now + timedelta(hours=12))

    list1 = ListMonoid([tweet1])
    list2 = ListMonoid([tweet2])

    # Different orders
    result1 = list1 + list2  # Morning then evening
    result2 = list2 + list1  # Evening then morning (WRONG ORDER!)

    print("\nOrder 1 (correct): list1 + list2")
    print(f"  Result: {[f'{t.tweet_id} ({t.timestamp.strftime('%H:%M')})' for t in result1.value]}")

    print("\nOrder 2 (wrong): list2 + list1")
    print(f"  Result: {[f'{t.tweet_id} ({t.timestamp.strftime('%H:%M')})' for t in result2.value]}")

    print("\nCommutativity check:", result1.value == result2.value, "✗")
    print("\nLists are NOT commutative - order matters for timelines!")


def distributed_timeline_construction():
    """
    Demonstrate building timelines across distributed servers.

    Key insight: Even though lists aren't commutative, we can still
    use them in distributed systems by ensuring each server processes
    time-ordered data.
    """
    print("\n" + "="*70)
    print("DISTRIBUTED TIMELINE CONSTRUCTION")
    print("="*70)

    user_id = "alice"

    # Simulate 3 servers, each processing different time windows
    # Server 1: Hour 0-8 (morning)
    # Server 2: Hour 9-17 (afternoon)
    # Server 3: Hour 18-23 (evening)

    server1_timeline = TimelineBuilder(user_id)
    server2_timeline = TimelineBuilder(user_id)
    server3_timeline = TimelineBuilder(user_id)

    base_time = datetime.now().replace(hour=0, minute=0, second=0)

    # Server 1: Morning tweets (0-8am)
    print("\nServer 1 processing morning tweets (0-8am)...")
    for hour in range(9):
        for _ in range(random.randint(0, 3)):
            timestamp = base_time + timedelta(hours=hour, minutes=random.randint(0, 59))
            tweet = Tweet(
                f"tweet_{hour}_{_}",
                user_id,
                f"Morning update at {timestamp.strftime('%H:%M')}",
                timestamp,
                likes=random.randint(0, 100)
            )
            server1_timeline.add_tweet(tweet)

    # Server 2: Afternoon tweets (9-17pm)
    print("Server 2 processing afternoon tweets (9am-5pm)...")
    for hour in range(9, 18):
        for _ in range(random.randint(0, 3)):
            timestamp = base_time + timedelta(hours=hour, minutes=random.randint(0, 59))
            tweet = Tweet(
                f"tweet_{hour}_{_}",
                user_id,
                f"Afternoon update at {timestamp.strftime('%H:%M')}",
                timestamp,
                likes=random.randint(0, 100)
            )
            server2_timeline.add_tweet(tweet)

    # Server 3: Evening tweets (18-23pm)
    print("Server 3 processing evening tweets (6pm-11pm)...")
    for hour in range(18, 24):
        for _ in range(random.randint(0, 3)):
            timestamp = base_time + timedelta(hours=hour, minutes=random.randint(0, 59))
            tweet = Tweet(
                f"tweet_{hour}_{_}",
                user_id,
                f"Evening update at {timestamp.strftime('%H:%M')}",
                timestamp,
                likes=random.randint(0, 100)
            )
            server3_timeline.add_tweet(tweet)

    print(f"\nServer 1: {len(server1_timeline.get_timeline())} tweets")
    print(f"Server 2: {len(server2_timeline.get_timeline())} tweets")
    print(f"Server 3: {len(server3_timeline.get_timeline())} tweets")

    # MERGE timelines (MONOID OPERATION!)
    # Since each server handles distinct time windows, concatenation works
    global_timeline = server1_timeline + server2_timeline + server3_timeline

    print(f"\nGlobal timeline: {len(global_timeline.get_timeline())} tweets")

    # Display timeline (sorted by timestamp)
    print("\nTimeline Preview (10 most recent):")
    print("-" * 70)
    for tweet in global_timeline.get_sorted_timeline(10):
        print(f"  {tweet.timestamp.strftime('%H:%M:%S')} - {tweet.text}")

    print("\n✓ Merged 3 timelines using ListMonoid + operation!")


def home_timeline_example():
    """
    Build a home timeline (aggregating followed users).

    This simulates Twitter's "Home" feed.
    """
    print("\n" + "="*70)
    print("HOME TIMELINE (Following Multiple Users)")
    print("="*70)

    following = ["alice", "bob", "charlie"]
    timelines = {}

    base_time = datetime.now()

    # Each followed user has their own timeline
    for user in following:
        timeline = TimelineBuilder(user)

        # Generate tweets for this user
        for i in range(random.randint(5, 15)):
            timestamp = base_time + timedelta(minutes=random.randint(0, 1440))  # Random time today
            tweet = Tweet(
                f"{user}_tweet_{i}",
                user,
                f"Update from @{user}",
                timestamp,
                likes=random.randint(0, 500)
            )
            timeline.add_tweet(tweet)

        timelines[user] = timeline

    # Build home timeline (merge all followed users)
    home_timeline = TimelineBuilder("viewer")

    for user, timeline in timelines.items():
        home_timeline.timeline = home_timeline.timeline + timeline.timeline

    print(f"\nHome timeline: {len(home_timeline.get_timeline())} tweets from {len(following)} users")

    # Show recent tweets
    print("\nRecent tweets:")
    print("-" * 70)
    for tweet in home_timeline.get_sorted_timeline(15):
        print(f"  @{tweet.user_id:10} {tweet.timestamp.strftime('%H:%M')} - {tweet.text}")


if __name__ == "__main__":
    print("="*70)
    print("Twitter Timeline Builder")
    print("Based on Twitter's Algebird ListMonoid Implementation")
    print("="*70)
    print("\nKey concept: ListMonoid is associative but NOT commutative")
    print("Perfect for ordered timelines in distributed systems!")

    # Demonstrate properties
    demonstrate_associativity()
    demonstrate_non_commutativity()

    # Distributed construction
    distributed_timeline_construction()

    # Home timeline example
    home_timeline_example()

    print("\n" + "="*70)
    print("KEY TAKEAWAYS")
    print("="*70)
    print("✓ ListMonoid is associative: (a+b)+c = a+(b+c)")
    print("✓ ListMonoid is NOT commutative: a+b ≠ b+a (order matters!)")
    print("✓ Associativity enables distributed processing")
    print("✓ Each server handles time-ordered segments")
    print("✓ This is how Twitter builds timelines at scale")
    print("="*70)
