"""
Integration tests for monoid aggregation patterns.

Tests combining multiple monoids and real-world aggregation scenarios.
"""

import pytest
from algesnake.monoid.numeric import Add, Multiply, Max, Min
from algesnake.monoid.collection import SetMonoid, ListMonoid, MapMonoid
from algesnake.monoid.option import Some, None_, Option


class TestDistributedAggregation:
    """Test monoid aggregation in distributed computing scenarios."""

    def test_word_count_across_partitions(self):
        """Simulate word count aggregation across multiple data partitions."""
        # Partition 1: First chunk of text
        partition1 = MapMonoid({'hello': 2, 'world': 1, 'python': 1}, lambda a, b: a + b)

        # Partition 2: Second chunk of text
        partition2 = MapMonoid({'hello': 1, 'world': 3, 'data': 2}, lambda a, b: a + b)

        # Partition 3: Third chunk of text
        partition3 = MapMonoid({'python': 2, 'data': 1, 'science': 4}, lambda a, b: a + b)

        # Combine all partitions
        total_counts = partition1 + partition2 + partition3

        assert total_counts.value == {
            'hello': 3,
            'world': 4,
            'python': 3,
            'data': 3,
            'science': 4
        }

    def test_unique_users_across_servers(self):
        """Track unique users across multiple servers using SetMonoid."""
        # Server 1: Users who logged in
        server1_users = SetMonoid({'alice', 'bob', 'charlie'})

        # Server 2: Users who logged in
        server2_users = SetMonoid({'bob', 'david', 'eve'})

        # Server 3: Users who logged in
        server3_users = SetMonoid({'alice', 'eve', 'frank'})

        # Union across all servers
        all_users = server1_users + server2_users + server3_users

        assert all_users.value == {'alice', 'bob', 'charlie', 'david', 'eve', 'frank'}
        assert len(all_users.value) == 6

    def test_sum_builtin_with_monoids(self):
        """Test that sum() builtin works with monoids."""
        # Sum numeric values
        numbers = [Add(1), Add(2), Add(3), Add(4), Add(5)]
        total = sum(numbers)
        assert total.value == 15

        # Max across values
        maxes = [Max(5), Max(3), Max(8), Max(1)]
        maximum = sum(maxes)
        assert maximum.value == 8

    def test_nested_aggregation(self):
        """Test aggregating aggregations (nested monoid operations)."""
        # Each partition has local aggregates
        partition1_total = sum([Add(1), Add(2), Add(3)])  # 6
        partition2_total = sum([Add(4), Add(5)])  # 9
        partition3_total = sum([Add(6), Add(7), Add(8)])  # 21

        # Global aggregate across partitions
        global_total = sum([partition1_total, partition2_total, partition3_total])

        assert global_total.value == 36

    def test_multiple_aggregations_same_data(self):
        """Test running multiple different aggregations on same data."""
        data = [1, 2, 3, 4, 5]

        # Sum
        total = sum([Add(x) for x in data])
        assert total.value == 15

        # Max
        maximum = sum([Max(x) for x in data])
        assert maximum.value == 5

        # Min
        minimum = sum([Min(x) for x in data])
        assert minimum.value == 1

        # Product (using * operator for Multiply)
        product = Multiply(1)
        for x in data:
            product = product * Multiply(x)
        assert product.value == 120


class TestComplexAggregations:
    """Test complex real-world aggregation patterns."""

    def test_event_analytics_pipeline(self):
        """Simulate event analytics aggregation pipeline."""
        # Event stream from different sources
        events_source1 = [
            {'user': 'alice', 'action': 'login'},
            {'user': 'bob', 'action': 'purchase'},
            {'user': 'alice', 'action': 'view'},
        ]

        events_source2 = [
            {'user': 'charlie', 'action': 'login'},
            {'user': 'bob', 'action': 'view'},
            {'user': 'alice', 'action': 'purchase'},
        ]

        # Aggregate: Count unique users
        users_1 = SetMonoid({e['user'] for e in events_source1})
        users_2 = SetMonoid({e['user'] for e in events_source2})
        all_users = users_1 + users_2

        assert len(all_users.value) == 3  # alice, bob, charlie

        # Aggregate: Count events by type
        def count_actions(events):
            counts = {}
            for e in events:
                counts[e['action']] = counts.get(e['action'], 0) + 1
            return MapMonoid(counts, lambda a, b: a + b)

        actions_1 = count_actions(events_source1)
        actions_2 = count_actions(events_source2)
        all_actions = actions_1 + actions_2

        assert all_actions.value == {'login': 2, 'purchase': 2, 'view': 2}

    def test_merge_configuration_maps(self):
        """Test merging configuration dictionaries with max values."""
        # Default config
        default_config = MapMonoid(
            {'timeout': 30, 'retries': 3, 'buffer_size': 1024},
            lambda a, b: max(a, b)  # Take max value for conflicts
        )

        # User config (override some values)
        user_config = MapMonoid(
            {'timeout': 60, 'buffer_size': 2048},
            lambda a, b: max(a, b)
        )

        # Merged config
        final_config = default_config + user_config

        assert final_config.value == {
            'timeout': 60,  # User override
            'retries': 3,  # From default
            'buffer_size': 2048  # User override
        }

    def test_option_monoid_for_nullable_aggregation(self):
        """Test Option monoid for handling nullable values in aggregation."""
        # Sparse data with missing values
        values = [Some(5), None_(), Some(3), None_(), Some(7), Some(2)]

        # Option monoid returns first Some value, not sum
        # (It's a "first wins" monoid, not additive)
        result = sum(values)

        assert result.is_some
        assert result.value == 5  # First Some value

        # All None values
        all_none = sum([None_(), None_(), None_()])
        assert all_none.is_none

    def test_combining_different_collection_types(self):
        """Test combining different collection monoids."""
        # Collect items from multiple sources
        list1 = ListMonoid([1, 2, 3])
        list2 = ListMonoid([4, 5])
        list3 = ListMonoid([6, 7, 8])

        combined = list1 + list2 + list3
        assert combined.value == [1, 2, 3, 4, 5, 6, 7, 8]

        # Set union
        set1 = SetMonoid({1, 2, 3})
        set2 = SetMonoid({3, 4, 5})
        set3 = SetMonoid({5, 6, 7})

        union = set1 + set2 + set3
        assert union.value == {1, 2, 3, 4, 5, 6, 7}


class TestAssociativityInPractice:
    """Test that associativity property enables flexible computation order."""

    def test_different_groupings_same_result(self):
        """Test that different grouping orders produce same result."""
        m1 = Add(1)
        m2 = Add(2)
        m3 = Add(3)
        m4 = Add(4)

        # Left-associative: ((1 + 2) + 3) + 4
        left = ((m1 + m2) + m3) + m4

        # Right-associative: 1 + (2 + (3 + 4))
        right = m1 + (m2 + (m3 + m4))

        # Balanced: (1 + 2) + (3 + 4)
        balanced = (m1 + m2) + (m3 + m4)

        assert left.value == right.value == balanced.value == 10

    def test_parallel_aggregation_simulation(self):
        """Simulate parallel aggregation with different groupings."""
        # 8 values to aggregate
        values = [Add(i) for i in range(1, 9)]

        # Sequential: fold from left
        sequential = values[0]
        for v in values[1:]:
            sequential = sequential + v

        # Parallel simulation: group in pairs, then combine
        # Level 1: [(1+2), (3+4), (5+6), (7+8)]
        level1 = [
            values[0] + values[1],  # 3
            values[2] + values[3],  # 7
            values[4] + values[5],  # 11
            values[6] + values[7],  # 15
        ]

        # Level 2: [(3+7), (11+15)]
        level2 = [
            level1[0] + level1[1],  # 10
            level1[2] + level1[3],  # 26
        ]

        # Level 3: [10+26]
        parallel = level2[0] + level2[1]

        # Both approaches yield same result
        assert sequential.value == parallel.value == 36

    def test_out_of_order_aggregation(self):
        """Test that order of combining doesn't matter (commutativity for commutative monoids)."""
        # For commutative monoids like Add
        values = [Add(5), Add(3), Add(8), Add(1)]

        # Forward order
        forward = sum(values)

        # Reverse order
        reverse = sum(reversed(values))

        # Random order
        random_order = sum([values[2], values[0], values[3], values[1]])

        assert forward.value == reverse.value == random_order.value == 17


class TestRealWorldScenarios:
    """Test real-world usage scenarios."""

    def test_log_aggregation(self):
        """Test aggregating log data from multiple sources."""
        # Log entries from server 1
        server1_errors = MapMonoid({'404': 5, '500': 2}, lambda a, b: a + b)
        server1_requests = Add(1500)

        # Log entries from server 2
        server2_errors = MapMonoid({'404': 3, '503': 1}, lambda a, b: a + b)
        server2_requests = Add(2000)

        # Aggregate errors
        total_errors = server1_errors + server2_errors
        assert total_errors.value == {'404': 8, '500': 2, '503': 1}

        # Aggregate requests
        total_requests = server1_requests + server2_requests
        assert total_requests.value == 3500

    def test_metrics_rollup(self):
        """Test rolling up metrics across time windows."""
        # Minute-level metrics
        minute_1 = Add(100)  # requests
        minute_2 = Add(150)
        minute_3 = Add(120)

        # 3-minute window
        three_min = minute_1 + minute_2 + minute_3
        assert three_min.value == 370

        # Can combine windows
        minute_4 = Add(130)
        minute_5 = Add(140)
        minute_6 = Add(110)
        next_three_min = minute_4 + minute_5 + minute_6

        # 6-minute window
        six_min = three_min + next_three_min
        assert six_min.value == 750

    def test_incremental_computation(self):
        """Test incremental updates to aggregations."""
        # Initial aggregate
        current_total = Add(100)

        # New data arrives
        new_value = Add(25)
        current_total = current_total + new_value
        assert current_total.value == 125

        # More data
        another_value = Add(50)
        current_total = current_total + another_value
        assert current_total.value == 175

        # Can add batches
        batch = sum([Add(10), Add(20), Add(30)])
        current_total = current_total + batch
        assert current_total.value == 235
