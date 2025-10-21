"""
Tests for Option/Maybe monoid.
"""

import pytest
from algesnake.monoid.option import (
    Option, Some, None_, OptionMonoid,
    option_or_else, flatten_options, sequence_options
)


class TestOption:
    """Tests for basic Option functionality."""
    
    def test_some_creation(self):
        opt = Some(5)
        assert opt.is_some
        assert not opt.is_none
        assert opt.value == 5
    
    def test_none_creation(self):
        opt = None_()
        assert opt.is_none
        assert not opt.is_some
    
    def test_value_access(self):
        assert Some(42).value == 42
        with pytest.raises(ValueError):
            None_().value
    
    def test_get_or_else(self):
        assert Some(5).get_or_else(10) == 5
        assert None_().get_or_else(10) == 10
    
    def test_equality(self):
        assert Some(5) == Some(5)
        assert Some(5) != Some(3)
        assert None_() == None_()
        assert Some(5) != None_()
    
    def test_hash(self):
        # Should be able to use in sets/dicts
        s = {Some(1), Some(2), None_()}
        assert len(s) == 3
        assert Some(1) in s
        assert None_() in s
    
    def test_bool(self):
        assert bool(Some(5))
        assert bool(Some(0))  # Even Some(0) is truthy
        assert not bool(None_())
    
    def test_repr(self):
        assert repr(Some(5)) == "Some(5)"
        assert repr(None_()) == "None_()"


class TestOptionMap:
    """Tests for Option.map()."""
    
    def test_map_some(self):
        opt = Some(5)
        result = opt.map(lambda x: x * 2)
        assert result == Some(10)
    
    def test_map_none(self):
        opt = None_()
        result = opt.map(lambda x: x * 2)
        assert result == None_()
    
    def test_map_chain(self):
        result = Some(5).map(lambda x: x * 2).map(lambda x: x + 1)
        assert result == Some(11)
    
    def test_map_type_change(self):
        result = Some(5).map(str)
        assert result == Some("5")


class TestOptionFlatMap:
    """Tests for Option.flat_map()."""
    
    def test_flatmap_some_to_some(self):
        opt = Some(5)
        result = opt.flat_map(lambda x: Some(x * 2))
        assert result == Some(10)
    
    def test_flatmap_some_to_none(self):
        opt = Some(5)
        result = opt.flat_map(lambda x: None_())
        assert result == None_()
    
    def test_flatmap_none(self):
        opt = None_()
        result = opt.flat_map(lambda x: Some(x * 2))
        assert result == None_()
    
    def test_flatmap_chain(self):
        def safe_divide(x):
            return Some(10 / x) if x != 0 else None_()
        
        result = Some(2).flat_map(safe_divide)
        assert result == Some(5.0)
        
        result = Some(0).flat_map(safe_divide)
        assert result == None_()


class TestOptionFilter:
    """Tests for Option.filter()."""
    
    def test_filter_passes(self):
        opt = Some(5)
        result = opt.filter(lambda x: x > 3)
        assert result == Some(5)
    
    def test_filter_fails(self):
        opt = Some(5)
        result = opt.filter(lambda x: x > 10)
        assert result == None_()
    
    def test_filter_none(self):
        opt = None_()
        result = opt.filter(lambda x: True)
        assert result == None_()
    
    def test_filter_chain(self):
        result = (Some(10)
                 .filter(lambda x: x > 5)
                 .filter(lambda x: x < 20)
                 .filter(lambda x: x % 2 == 0))
        assert result == Some(10)


class TestOptionCombine:
    """Tests for Option combine (monoid operation)."""
    
    def test_some_combine_some(self):
        """First Some wins."""
        assert Some(5).combine(Some(3)) == Some(5)
    
    def test_some_combine_none(self):
        assert Some(5).combine(None_()) == Some(5)
    
    def test_none_combine_some(self):
        assert None_().combine(Some(3)) == Some(3)
    
    def test_none_combine_none(self):
        assert None_().combine(None_()) == None_()
    
    def test_operator_overload(self):
        assert Some(5) + Some(3) == Some(5)
        assert Some(5) + None_() == Some(5)
        assert None_() + Some(3) == Some(3)
        assert None_() + None_() == None_()
    
    def test_sum_builtin(self):
        options = [None_(), Some(5), None_(), Some(3)]
        assert sum(options) == Some(5)
        
        options = [None_(), None_(), None_()]
        assert sum(options) == None_()
    
    def test_zero(self):
        assert Option.zero() == None_()
        opt = Some(5)
        assert opt + Option.zero() == opt
        assert Option.zero() + opt == opt


class TestOptionMonoidWithCombination:
    """Tests for OptionMonoid with custom value combination."""
    
    def test_combine_with_addition(self):
        m = OptionMonoid(lambda a, b: a + b)
        result = m.combine(Some(5), Some(3))
        assert result == Some(8)
    
    def test_combine_with_max(self):
        m = OptionMonoid(max)
        result = m.combine(Some(5), Some(3))
        assert result == Some(5)
    
    def test_combine_some_none(self):
        m = OptionMonoid(lambda a, b: a + b)
        assert m.combine(Some(5), None_()) == Some(5)
        assert m.combine(None_(), Some(3)) == Some(3)
    
    def test_combine_none_none(self):
        m = OptionMonoid(lambda a, b: a + b)
        assert m.combine(None_(), None_()) == None_()
    
    def test_combine_all(self):
        m = OptionMonoid(lambda a, b: a + b)
        options = [Some(1), Some(2), Some(3), Some(4)]
        assert m.combine_all(options) == Some(10)
    
    def test_combine_all_with_nones(self):
        m = OptionMonoid(lambda a, b: a + b)
        options = [None_(), Some(5), None_(), Some(3), None_()]
        assert m.combine_all(options) == Some(8)
    
    def test_combine_all_empty(self):
        m = OptionMonoid(lambda a, b: a + b)
        assert m.combine_all([]) == None_()
    
    def test_zero_property(self):
        m = OptionMonoid(lambda a, b: a + b)
        assert m.zero == None_()


class TestOptionHelperFunctions:
    """Tests for helper functions."""
    
    def test_option_or_else(self):
        assert option_or_else(None_(), Some(5), Some(3)) == Some(5)
        assert option_or_else(Some(1), Some(2), Some(3)) == Some(1)
        assert option_or_else(None_(), None_(), None_()) == None_()
    
    def test_flatten_options(self):
        options = [Some(1), None_(), Some(3), None_(), Some(5)]
        assert flatten_options(options) == [1, 3, 5]
        
        assert flatten_options([None_(), None_()]) == []
        assert flatten_options([Some(1), Some(2), Some(3)]) == [1, 2, 3]
    
    def test_sequence_options_all_some(self):
        options = [Some(1), Some(2), Some(3)]
        result = sequence_options(options)
        assert result == Some([1, 2, 3])
    
    def test_sequence_options_with_none(self):
        options = [Some(1), None_(), Some(3)]
        result = sequence_options(options)
        assert result == None_()
    
    def test_sequence_options_empty(self):
        result = sequence_options([])
        assert result == Some([])


class TestMonoidLaws:
    """Verify monoid laws for Option."""
    
    def test_associativity(self):
        """Test (a • b) • c = a • (b • c)"""
        a = Some(1)
        b = Some(2)
        c = Some(3)
        assert (a + b) + c == a + (b + c)
        
        a = None_()
        b = Some(2)
        c = Some(3)
        assert (a + b) + c == a + (b + c)
    
    def test_left_identity(self):
        """Test zero • a = a"""
        a = Some(5)
        zero = Option.zero()
        assert zero + a == a
    
    def test_right_identity(self):
        """Test a • zero = a"""
        a = Some(5)
        zero = Option.zero()
        assert a + zero == a
    
    def test_identity_with_none(self):
        """Test that None_ is the identity."""
        a = None_()
        zero = Option.zero()
        assert zero + a == a
        assert a + zero == a


class TestRealWorldUseCases:
    """Tests demonstrating real-world usage patterns."""
    
    def test_safe_division(self):
        def safe_divide(a, b):
            return Some(a / b) if b != 0 else None_()
        
        assert safe_divide(10, 2) == Some(5.0)
        assert safe_divide(10, 0) == None_()
    
    def test_config_fallback(self):
        """Simulate config with fallbacks."""
        config_override = None_()
        config_user = Some("user-value")
        config_default = Some("default-value")
        
        result = config_override + config_user + config_default
        assert result == Some("user-value")
    
    def test_parsing_chain(self):
        """Simulate a parsing pipeline."""
        def parse_int(s):
            try:
                return Some(int(s))
            except ValueError:
                return None_()
        
        def validate_positive(x):
            return Some(x) if x > 0 else None_()
        
        # Valid input
        result = parse_int("42").flat_map(validate_positive)
        assert result == Some(42)
        
        # Invalid parse
        result = parse_int("abc").flat_map(validate_positive)
        assert result == None_()
        
        # Invalid validation
        result = parse_int("-5").flat_map(validate_positive)
        assert result == None_()
    
    def test_database_lookup_pattern(self):
        """Simulate looking up data from multiple sources."""
        cache = None_()  # Cache miss
        database = Some("db-value")  # Found in DB
        api = Some("api-value")  # Could fetch from API
        
        result = cache + database + api
        assert result == Some("db-value")
        
        # All sources empty
        result = None_() + None_() + None_()
        assert result == None_()
    
    def test_aggregating_optional_values(self):
        """Aggregate optional values with custom combination."""
        m = OptionMonoid(lambda a, b: a + b)
        
        # Simulating collecting scores from different sources
        scores = [
            Some(10),  # Test 1
            None_(),   # Test 2 (not taken)
            Some(15),  # Test 3
            None_(),   # Test 4 (not taken)
            Some(20),  # Test 5
        ]
        
        total = m.combine_all(scores)
        assert total == Some(45)


class TestEdgeCases:
    """Tests for edge cases and special values."""
    
    def test_none_with_zero_value(self):
        """Some(0) is different from None_."""
        assert Some(0) != None_()
        assert Some(0).is_some
        assert Some(0).value == 0
    
    def test_none_with_false_value(self):
        """Some(False) is different from None_."""
        assert Some(False) != None_()
        assert Some(False).is_some
        assert Some(False).value is False
    
    def test_none_with_empty_string(self):
        """Some("") is different from None_."""
        assert Some("") != None_()
        assert Some("").is_some
        assert Some("").value == ""
    
    def test_option_of_option(self):
        """Options can contain Options."""
        opt = Some(Some(5))
        assert opt.is_some
        assert opt.value == Some(5)
        assert opt.value.value == 5
        
        # Flattening
        opt = Some(Some(5))
        flattened = opt.flat_map(lambda x: x)
        assert flattened == Some(5)
