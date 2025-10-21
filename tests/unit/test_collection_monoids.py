"""
Tests for collection monoids.
"""

import pytest
from algesnake.monoid.collection import (
    SetMonoid, ListMonoid, MapMonoid, StringMonoid,
    set_union, concat_lists, merge_maps, concat_strings
)


class TestSetMonoid:
    """Tests for SetMonoid."""
    
    def test_combine(self):
        s1 = SetMonoid({1, 2, 3})
        s2 = SetMonoid({3, 4, 5})
        assert s1.combine(s2) == SetMonoid({1, 2, 3, 4, 5})
    
    def test_operator_overload(self):
        s1 = SetMonoid({1, 2})
        s2 = SetMonoid({2, 3})
        assert s1 + s2 == SetMonoid({1, 2, 3})
    
    def test_sum_builtin(self):
        sets = [SetMonoid({1}), SetMonoid({2}), SetMonoid({3})]
        assert sum(sets) == SetMonoid({1, 2, 3})
    
    def test_zero(self):
        assert SetMonoid.zero() == SetMonoid(set())
        s = SetMonoid({1, 2, 3})
        assert s + SetMonoid.zero() == s
        assert SetMonoid.zero() + s == s
    
    def test_is_zero(self):
        assert SetMonoid(set()).is_zero
        assert SetMonoid.zero().is_zero
        assert not SetMonoid({1}).is_zero
    
    def test_len(self):
        assert len(SetMonoid({1, 2, 3})) == 3
        assert len(SetMonoid.zero()) == 0
    
    def test_contains(self):
        s = SetMonoid({1, 2, 3})
        assert 1 in s
        assert 4 not in s
    
    def test_associativity(self):
        a = SetMonoid({1, 2})
        b = SetMonoid({2, 3})
        c = SetMonoid({3, 4})
        assert (a + b) + c == a + (b + c)
    
    def test_commutativity(self):
        a = SetMonoid({1, 2})
        b = SetMonoid({3, 4})
        assert a + b == b + a
    
    def test_idempotency(self):
        s = SetMonoid({1, 2, 3})
        assert s + s == s
    
    def test_convenience_function(self):
        result = set_union({1, 2}, {2, 3}, {3, 4})
        assert result == SetMonoid({1, 2, 3, 4})
        assert set_union() == SetMonoid.zero()


class TestListMonoid:
    """Tests for ListMonoid."""
    
    def test_combine(self):
        l1 = ListMonoid([1, 2, 3])
        l2 = ListMonoid([4, 5, 6])
        assert l1.combine(l2) == ListMonoid([1, 2, 3, 4, 5, 6])
    
    def test_operator_overload(self):
        l1 = ListMonoid([1, 2])
        l2 = ListMonoid([3, 4])
        assert l1 + l2 == ListMonoid([1, 2, 3, 4])
    
    def test_sum_builtin(self):
        lists = [ListMonoid([1]), ListMonoid([2]), ListMonoid([3])]
        assert sum(lists) == ListMonoid([1, 2, 3])
    
    def test_zero(self):
        assert ListMonoid.zero() == ListMonoid([])
        l = ListMonoid([1, 2, 3])
        assert l + ListMonoid.zero() == l
        assert ListMonoid.zero() + l == l
    
    def test_is_zero(self):
        assert ListMonoid([]).is_zero
        assert ListMonoid.zero().is_zero
        assert not ListMonoid([1]).is_zero
    
    def test_len(self):
        assert len(ListMonoid([1, 2, 3])) == 3
        assert len(ListMonoid.zero()) == 0
    
    def test_indexing(self):
        l = ListMonoid([1, 2, 3, 4, 5])
        assert l[0] == 1
        assert l[-1] == 5
        assert l[1:3] == [2, 3]
    
    def test_associativity(self):
        a = ListMonoid([1, 2])
        b = ListMonoid([3, 4])
        c = ListMonoid([5, 6])
        assert (a + b) + c == a + (b + c)
    
    def test_non_commutative(self):
        """Lists are not commutative - order matters."""
        a = ListMonoid([1, 2])
        b = ListMonoid([3, 4])
        assert a + b != b + a
        assert a + b == ListMonoid([1, 2, 3, 4])
        assert b + a == ListMonoid([3, 4, 1, 2])
    
    def test_duplicates_preserved(self):
        l1 = ListMonoid([1, 2, 2])
        l2 = ListMonoid([2, 3])
        assert l1 + l2 == ListMonoid([1, 2, 2, 2, 3])
    
    def test_convenience_function(self):
        result = concat_lists([1, 2], [3, 4], [5, 6])
        assert result == ListMonoid([1, 2, 3, 4, 5, 6])
        assert concat_lists() == ListMonoid.zero()


class TestMapMonoid:
    """Tests for MapMonoid."""
    
    def test_combine_default(self):
        """Default behavior: last value wins."""
        m1 = MapMonoid({'a': 1, 'b': 2})
        m2 = MapMonoid({'b': 3, 'c': 4})
        result = m1.combine(m2)
        assert result == MapMonoid({'a': 1, 'b': 3, 'c': 4})
    
    def test_combine_with_function(self):
        """Custom combination: sum values."""
        m1 = MapMonoid({'a': 1, 'b': 2}, combine_values=lambda x, y: x + y)
        m2 = MapMonoid({'b': 3, 'c': 4}, combine_values=lambda x, y: x + y)
        result = m1.combine(m2)
        assert result == MapMonoid({'a': 1, 'b': 5, 'c': 4}, combine_values=lambda x, y: x + y)
    
    def test_operator_overload(self):
        m1 = MapMonoid({'a': 1})
        m2 = MapMonoid({'b': 2})
        assert m1 + m2 == MapMonoid({'a': 1, 'b': 2})
    
    def test_sum_builtin(self):
        maps = [
            MapMonoid({'a': 1}),
            MapMonoid({'b': 2}),
            MapMonoid({'c': 3})
        ]
        assert sum(maps) == MapMonoid({'a': 1, 'b': 2, 'c': 3})
    
    def test_zero(self):
        assert MapMonoid.zero() == MapMonoid({})
        m = MapMonoid({'a': 1, 'b': 2})
        assert m + MapMonoid.zero() == m
        assert MapMonoid.zero() + m == m
    
    def test_is_zero(self):
        assert MapMonoid({}).is_zero
        assert MapMonoid.zero().is_zero
        assert not MapMonoid({'a': 1}).is_zero
    
    def test_len(self):
        assert len(MapMonoid({'a': 1, 'b': 2, 'c': 3})) == 3
        assert len(MapMonoid.zero()) == 0
    
    def test_getitem(self):
        m = MapMonoid({'a': 1, 'b': 2})
        assert m['a'] == 1
        assert m['b'] == 2
    
    def test_contains(self):
        m = MapMonoid({'a': 1, 'b': 2})
        assert 'a' in m
        assert 'c' not in m
    
    def test_get_method(self):
        m = MapMonoid({'a': 1, 'b': 2})
        assert m.get('a') == 1
        assert m.get('c') is None
        assert m.get('c', 99) == 99
    
    def test_associativity(self):
        a = MapMonoid({'a': 1})
        b = MapMonoid({'b': 2})
        c = MapMonoid({'c': 3})
        assert (a + b) + c == a + (b + c)
    
    def test_max_combination(self):
        """Combine with max function."""
        m1 = MapMonoid({'a': 5, 'b': 2}, combine_values=max)
        m2 = MapMonoid({'b': 7, 'c': 3}, combine_values=max)
        result = m1 + m2
        assert result.value == {'a': 5, 'b': 7, 'c': 3}
    
    def test_list_concatenation(self):
        """Combine with list concatenation."""
        m1 = MapMonoid({'a': [1, 2], 'b': [3]}, combine_values=lambda x, y: x + y)
        m2 = MapMonoid({'b': [4, 5], 'c': [6]}, combine_values=lambda x, y: x + y)
        result = m1 + m2
        assert result.value == {'a': [1, 2], 'b': [3, 4, 5], 'c': [6]}
    
    def test_convenience_function(self):
        result = merge_maps({'a': 1}, {'b': 2}, {'c': 3})
        assert result == MapMonoid({'a': 1, 'b': 2, 'c': 3})
        
        result_sum = merge_maps(
            {'a': 1, 'b': 2},
            {'b': 3, 'c': 4},
            combine_values=lambda x, y: x + y
        )
        assert result_sum.value == {'a': 1, 'b': 5, 'c': 4}


class TestStringMonoid:
    """Tests for StringMonoid."""
    
    def test_combine(self):
        s1 = StringMonoid("Hello")
        s2 = StringMonoid(" World")
        assert s1.combine(s2) == StringMonoid("Hello World")
    
    def test_operator_overload(self):
        s1 = StringMonoid("Hello")
        s2 = StringMonoid(" World")
        assert s1 + s2 == StringMonoid("Hello World")
    
    def test_sum_builtin(self):
        strings = [StringMonoid("a"), StringMonoid("b"), StringMonoid("c")]
        assert sum(strings) == StringMonoid("abc")
    
    def test_zero(self):
        assert StringMonoid.zero() == StringMonoid("")
        s = StringMonoid("test")
        assert s + StringMonoid.zero() == s
        assert StringMonoid.zero() + s == s
    
    def test_is_zero(self):
        assert StringMonoid("").is_zero
        assert StringMonoid.zero().is_zero
        assert not StringMonoid("a").is_zero
    
    def test_len(self):
        assert len(StringMonoid("hello")) == 5
        assert len(StringMonoid.zero()) == 0
    
    def test_str(self):
        s = StringMonoid("test")
        assert str(s) == "test"
    
    def test_associativity(self):
        a = StringMonoid("a")
        b = StringMonoid("b")
        c = StringMonoid("c")
        assert (a + b) + c == a + (b + c)
    
    def test_non_commutative(self):
        a = StringMonoid("Hello")
        b = StringMonoid(" World")
        assert a + b != b + a
    
    def test_convenience_function(self):
        result = concat_strings("Hello", " ", "World", "!")
        assert result == StringMonoid("Hello World!")
        assert concat_strings() == StringMonoid.zero()
    
    def test_multiline_strings(self):
        s1 = StringMonoid("Line 1\n")
        s2 = StringMonoid("Line 2\n")
        s3 = StringMonoid("Line 3")
        assert s1 + s2 + s3 == StringMonoid("Line 1\nLine 2\nLine 3")


class TestMonoidLaws:
    """Comprehensive tests for monoid laws across all collection monoids."""
    
    @pytest.mark.parametrize("create_monoid,values", [
        (lambda x: SetMonoid(x[0]), [{1, 2}, {2, 3}, {3, 4}]),
        (lambda x: ListMonoid(list(x[0])), [[1, 2], [3, 4], [5, 6]]),
        (lambda x: MapMonoid(x[0]), [{'a': 1}, {'b': 2}, {'c': 3}]),
        (lambda x: StringMonoid(x), ["a", "b", "c"]),
    ])
    def test_associativity(self, create_monoid, values):
        """Test (a • b) • c = a • (b • c)"""
        a, b, c = [create_monoid([v]) for v in values]
        assert (a + b) + c == a + (b + c)
    
    @pytest.mark.parametrize("monoid_class,value", [
        (SetMonoid, {1, 2, 3}),
        (ListMonoid, [1, 2, 3]),
        (MapMonoid, {'a': 1, 'b': 2}),
        (StringMonoid, "test"),
    ])
    def test_identity(self, monoid_class, value):
        """Test zero • a = a • zero = a"""
        a = monoid_class(value)
        zero = monoid_class.zero()
        assert zero + a == a
        assert a + zero == a


class TestEdgeCases:
    """Tests for edge cases and special values."""
    
    def test_empty_collections(self):
        assert SetMonoid(set()) + SetMonoid(set()) == SetMonoid(set())
        assert ListMonoid([]) + ListMonoid([]) == ListMonoid([])
        assert MapMonoid({}) + MapMonoid({}) == MapMonoid({})
        assert StringMonoid("") + StringMonoid("") == StringMonoid("")
    
    def test_nested_structures(self):
        # Lists of lists
        l1 = ListMonoid([[1, 2], [3, 4]])
        l2 = ListMonoid([[5, 6]])
        assert l1 + l2 == ListMonoid([[1, 2], [3, 4], [5, 6]])
        
        # Maps with list values
        m1 = MapMonoid({'a': [1, 2]}, combine_values=lambda x, y: x + y)
        m2 = MapMonoid({'a': [3, 4]}, combine_values=lambda x, y: x + y)
        assert (m1 + m2).value == {'a': [1, 2, 3, 4]}
    
    def test_large_collections(self):
        # Large set
        s1 = SetMonoid(set(range(1000)))
        s2 = SetMonoid(set(range(500, 1500)))
        result = s1 + s2
        assert len(result) == 1500
        
        # Large list
        l1 = ListMonoid(list(range(1000)))
        l2 = ListMonoid(list(range(1000)))
        result = l1 + l2
        assert len(result) == 2000
