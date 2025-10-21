"""Tests for TopK heavy hitter tracker."""

import pytest
from algesnake.approximate import TopK
from algesnake.approximate.topk import find_top_k, merge_topk, streaming_top_k


class TestTopKBasics:
    """Tests for basic TopK functionality."""
    
    def test_initialization(self):
        """Test TopK initialization."""
        topk = TopK(k=10)
        
        assert topk.k == 10
        assert topk.is_zero()
        assert len(topk.top()) == 0
    
    def test_initialization_validation(self):
        """Test parameter validation."""
        # Valid k
        topk = TopK(k=5)
        assert topk.k == 5
        
        # Invalid k
        with pytest.raises(ValueError, match="K must be positive"):
            TopK(k=0)
        
        with pytest.raises(ValueError, match="K must be positive"):
            TopK(k=-5)
    
    def test_add_single_item(self):
        """Test adding single item."""
        topk = TopK(k=5)
        
        topk.add("apple")
        
        assert not topk.is_zero()
        assert "apple" in topk
        assert topk.estimate("apple") == 1
    
    def test_add_multiple_items(self):
        """Test adding multiple items."""
        topk = TopK(k=3)
        
        topk.add("apple")
        topk.add("banana")
        topk.add("cherry")
        
        top_items = topk.top()
        assert len(top_items) == 3
    
    def test_add_with_count(self):
        """Test adding item with count."""
        topk = TopK(k=5)
        
        topk.add("apple", count=10)
        
        assert topk.estimate("apple") == 10
    
    def test_top_returns_sorted(self):
        """Test that top() returns items sorted by frequency."""
        topk = TopK(k=5)
        
        topk.add("rare", count=1)
        topk.add("uncommon", count=5)
        topk.add("common", count=10)
        topk.add("very_common", count=20)
        
        top_items = topk.top()
        
        # Should be sorted descending by frequency
        assert top_items[0][0] == "very_common"
        assert top_items[1][0] == "common"
        assert top_items[2][0] == "uncommon"
        assert top_items[3][0] == "rare"
    
    def test_top_with_limit(self):
        """Test top(n) with limit."""
        topk = TopK(k=10)
        
        for i in range(10):
            topk.add(f"item{i}", count=i + 1)
        
        top_3 = topk.top(n=3)
        
        assert len(top_3) == 3
        assert top_3[0][1] == 10  # Highest frequency
    
    def test_contains(self):
        """Test contains / 'in' operator."""
        topk = TopK(k=3)
        
        topk.add("apple")
        topk.add("banana")
        
        assert "apple" in topk
        assert "banana" in topk
        assert "cherry" not in topk
        assert topk.contains("apple")
    
    def test_zero_element(self):
        """Test monoid zero property."""
        topk = TopK(k=10)
        zero = topk.zero
        
        assert isinstance(zero, TopK)
        assert zero.k == topk.k
        assert zero.is_zero()
    
    def test_is_zero(self):
        """Test is_zero method."""
        topk = TopK(k=5)
        assert topk.is_zero()
        
        topk.add("item")
        assert not topk.is_zero()


class TestTopKTracking:
    """Tests for TopK tracking behavior."""
    
    def test_tracks_exactly_k_items(self):
        """Test that TopK tracks exactly K items when given more."""
        topk = TopK(k=3)
        
        # Add 5 items
        topk.add("item1", count=10)
        topk.add("item2", count=20)
        topk.add("item3", count=5)
        topk.add("item4", count=15)
        topk.add("item5", count=1)
        
        top_items = topk.top()
        
        # Should only track top 3
        assert len(top_items) <= 3
        
        # Should have the 3 highest: 20, 15, 10
        frequencies = [freq for _, freq in top_items]
        assert 20 in frequencies
        assert 15 in frequencies
        assert 10 in frequencies
    
    def test_evicts_low_frequency_items(self):
        """Test that low frequency items are evicted."""
        topk = TopK(k=2)
        
        topk.add("low", count=1)
        topk.add("high1", count=100)
        topk.add("high2", count=200)
        
        # "low" should not be in top 2
        top_items = topk.top()
        items = [item for item, _ in top_items]
        
        assert "high1" in items
        assert "high2" in items
        assert len(top_items) == 2
    
    def test_updates_existing_item_frequency(self):
        """Test updating frequency of existing item."""
        topk = TopK(k=3)
        
        topk.add("apple", count=5)
        topk.add("apple", count=10)  # Update
        
        assert topk.estimate("apple") == 15
    
    def test_maintains_top_k_after_updates(self):
        """Test that TopK maintains correct items after frequency updates."""
        topk = TopK(k=2)
        
        topk.add("a", count=10)
        topk.add("b", count=5)
        topk.add("c", count=1)
        
        # Now update "c" to make it top item
        topk.add("c", count=20)  # Now c=21, a=10, b=5
        
        top_items = topk.top()
        items = [item for item, _ in top_items]
        
        # Should have c and a
        assert "c" in items
        assert "a" in items


class TestTopKMonoid:
    """Tests for TopK monoid properties."""
    
    def test_combine_basic(self):
        """Test basic combine operation."""
        topk1 = TopK(k=5)
        topk2 = TopK(k=5)
        
        topk1.add("apple", count=10)
        topk2.add("apple", count=5)
        
        combined = topk1.combine(topk2)
        
        assert combined.estimate("apple") == 15
    
    def test_combine_disjoint(self):
        """Test combining TopK with disjoint items."""
        topk1 = TopK(k=5)
        topk2 = TopK(k=5)
        
        topk1.add("apple", count=10)
        topk2.add("banana", count=20)
        
        combined = topk1.combine(topk2)
        
        assert combined.estimate("apple") == 10
        assert combined.estimate("banana") == 20
    
    def test_combine_merges_top_k(self):
        """Test that combine selects top K from merged items."""
        topk1 = TopK(k=2)
        topk2 = TopK(k=2)
        
        topk1.add("a", count=100)
        topk1.add("b", count=50)
        
        topk2.add("c", count=200)
        topk2.add("d", count=10)
        
        combined = topk1 + topk2
        
        # Combined should have top 2: c(200), a(100)
        top_items = combined.top()
        items = [item for item, _ in top_items]
        
        assert len(top_items) == 2
        assert "c" in items
        assert "a" in items
    
    def test_operator_overload(self):
        """Test + operator for combine."""
        topk1 = TopK(k=5)
        topk2 = TopK(k=5)
        
        topk1.add("item", count=5)
        topk2.add("item", count=3)
        
        combined = topk1 + topk2
        
        assert isinstance(combined, TopK)
        assert combined.estimate("item") == 8
    
    def test_sum_builtin(self):
        """Test sum() builtin support."""
        trackers = [TopK(k=5) for _ in range(3)]
        
        trackers[0].add("item", count=10)
        trackers[1].add("item", count=20)
        trackers[2].add("item", count=30)
        
        combined = sum(trackers)
        
        assert combined.estimate("item") == 60
    
    def test_associativity(self):
        """Test (a + b) + c = a + (b + c)."""
        topk_a = TopK(k=5)
        topk_b = TopK(k=5)
        topk_c = TopK(k=5)
        
        topk_a.add("item", count=10)
        topk_b.add("item", count=20)
        topk_c.add("item", count=30)
        
        left = (topk_a + topk_b) + topk_c
        right = topk_a + (topk_b + topk_c)
        
        # Should have same frequency
        assert left.estimate("item") == right.estimate("item")
    
    def test_identity(self):
        """Test zero + a = a + zero = a."""
        topk = TopK(k=5)
        topk.add("item", count=42)
        
        zero = topk.zero
        
        left = zero + topk
        right = topk + zero
        
        # All should have same estimate
        assert left.estimate("item") == 42
        assert right.estimate("item") == 42
    
    def test_combine_different_k_fails(self):
        """Test that combining different K values raises error."""
        topk1 = TopK(k=5)
        topk2 = TopK(k=10)
        
        with pytest.raises(ValueError, match="different K values"):
            topk1.combine(topk2)


class TestTopKHelpers:
    """Tests for helper functions."""
    
    def test_find_top_k(self):
        """Test find_top_k convenience function."""
        items = ["a"]*10 + ["b"]*5 + ["c"]*3 + ["d"]*1
        
        top_items = find_top_k(items, k=3)
        
        assert len(top_items) == 3
        assert top_items[0] == ("a", 10)
        assert top_items[1] == ("b", 5)
        assert top_items[2] == ("c", 3)
    
    def test_merge_topk(self):
        """Test merge_topk convenience function."""
        trackers = [TopK(k=5) for _ in range(3)]
        
        trackers[0].add("item", count=10)
        trackers[1].add("item", count=20)
        trackers[2].add("item", count=30)
        
        merged = merge_topk(trackers)
        
        assert merged.estimate("item") == 60
    
    def test_merge_topk_empty_list_fails(self):
        """Test that merging empty list raises error."""
        with pytest.raises(ValueError, match="empty list"):
            merge_topk([])
    
    def test_streaming_top_k(self):
        """Test streaming_top_k function."""
        batches = [
            ["a", "b", "a"],
            ["a", "c", "b"],
            ["c", "c", "a"]
        ]
        
        topk = streaming_top_k(batches, k=3)
        
        top_items = topk.top()
        
        # "a" appears 4 times, "c" appears 3 times, "b" appears 2 times
        assert top_items[0][0] == "a"
        assert top_items[0][1] == 4


class TestTopKEdgeCases:
    """Tests for edge cases."""
    
    def test_k_equals_one(self):
        """Test TopK with k=1."""
        topk = TopK(k=1)
        
        topk.add("a", count=10)
        topk.add("b", count=5)
        topk.add("c", count=20)
        
        top_items = topk.top()
        
        assert len(top_items) == 1
        assert top_items[0][0] == "c"
        assert top_items[0][1] == 20
    
    def test_fewer_items_than_k(self):
        """Test when fewer than K items added."""
        topk = TopK(k=10)
        
        topk.add("a", count=5)
        topk.add("b", count=3)
        
        top_items = topk.top()
        
        assert len(top_items) == 2
    
    def test_all_same_frequency(self):
        """Test items with same frequency."""
        topk = TopK(k=3)
        
        topk.add("a", count=10)
        topk.add("b", count=10)
        topk.add("c", count=10)
        topk.add("d", count=10)
        
        top_items = topk.top()
        
        # Should have 3 items with freq 10
        assert len(top_items) == 3
        assert all(freq == 10 for _, freq in top_items)
    
    def test_equality(self):
        """Test equality operator."""
        topk1 = TopK(k=5)
        topk2 = TopK(k=5)
        
        # Empty should be equal
        assert topk1 == topk2
        
        # Add same items
        topk1.add("item", count=10)
        topk2.add("item", count=10)
        
        assert topk1 == topk2
        
        # Different items should not be equal
        topk3 = TopK(k=5)
        topk3.add("other", count=10)
        
        assert topk1 != topk3
    
    def test_repr(self):
        """Test string representation."""
        topk = TopK(k=5)
        topk.add("apple", count=10)
        topk.add("banana", count=5)
        
        repr_str = repr(topk)
        
        assert "TopK" in repr_str
        assert "k=5" in repr_str


class TestTopKRealWorld:
    """Real-world use case tests."""
    
    def test_trending_hashtags(self):
        """Test trending hashtag detection."""
        topk = TopK(k=5)
        
        # Simulate tweets with hashtags
        tweets = [
            "#python", "#ai", "#machinelearning",
            "#python", "#datascience", "#python",
            "#ai", "#python", "#coding",
            "#python", "#ai", "#machinelearning"
        ]
        
        for hashtag in tweets:
            topk.add(hashtag)
        
        top_hashtags = topk.top(n=3)
        
        # "#python" should be #1 (5 occurrences)
        assert top_hashtags[0][0] == "#python"
        assert top_hashtags[0][1] == 5
        
        # "#ai" should be #2 (3 occurrences)
        assert top_hashtags[1][0] == "#ai"
        assert top_hashtags[1][1] == 3
    
    def test_error_log_analysis(self):
        """Test finding most common errors in logs."""
        topk = TopK(k=3)
        
        # Simulate error logs
        errors = {
            "ConnectionTimeout": 100,
            "NullPointerException": 50,
            "OutOfMemory": 75,
            "FileNotFound": 10,
            "PermissionDenied": 5,
        }
        
        for error, count in errors.items():
            topk.add(error, count=count)
        
        top_errors = topk.top()
        
        # Should have top 3: ConnectionTimeout(100), OutOfMemory(75), NullPointerException(50)
        assert len(top_errors) == 3
        assert top_errors[0][0] == "ConnectionTimeout"
        assert top_errors[1][0] == "OutOfMemory"
        assert top_errors[2][0] == "NullPointerException"
    
    def test_product_popularity(self):
        """Test tracking popular products."""
        topk = TopK(k=5)
        
        # Simulate product views
        products = ["laptop"]*50 + ["phone"]*45 + ["tablet"]*40 + \
                   ["watch"]*20 + ["headphones"]*15 + ["mouse"]*5
        
        for product in products:
            topk.add(product)
        
        top_products = topk.top()
        
        # Should have correct order
        assert top_products[0][0] == "laptop"
        assert top_products[1][0] == "phone"
        assert top_products[2][0] == "tablet"
        assert len(top_products) == 5
    
    def test_distributed_analytics(self):
        """Test distributed analytics scenario."""
        # Three analytics servers tracking page views
        server1 = TopK(k=5)
        server2 = TopK(k=5)
        server3 = TopK(k=5)
        
        # Server 1
        for _ in range(100):
            server1.add("/home")
        for _ in range(50):
            server1.add("/products")
        
        # Server 2
        for _ in range(75):
            server2.add("/home")
        for _ in range(60):
            server2.add("/about")
        
        # Server 3
        for _ in range(50):
            server3.add("/products")
        for _ in range(40):
            server3.add("/contact")
        
        # Merge all servers
        global_topk = server1 + server2 + server3
        
        top_pages = global_topk.top()
        
        # "/home" should be #1 (175 views)
        assert top_pages[0][0] == "/home"
        assert top_pages[0][1] == 175
        
        # "/products" should be #2 (100 views)
        assert top_pages[1][0] == "/products"
        assert top_pages[1][1] == 100
