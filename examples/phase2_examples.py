"""
Examples demonstrating Phase 2 Monoids for Algesnake.

This module shows practical usage of all the concrete monoid implementations.
"""

from numeric_monoids import Add, Multiply, Max, Min, add, multiply, max_of, min_of
from collection_monoids import (
    SetMonoid, ListMonoid, MapMonoid, StringMonoid,
    set_union, concat_lists, merge_maps, concat_strings
)
from option_monoid import Some, None_, OptionMonoid, option_or_else, sequence_options


def numeric_examples():
    """Examples using numeric monoids."""
    print("=== Numeric Monoids Examples ===\n")
    
    # Addition
    print("Addition:")
    result = Add(5) + Add(3) + Add(7)
    print(f"  5 + 3 + 7 = {result.value}")
    
    total = sum([Add(10), Add(20), Add(30), Add(40)])
    print(f"  Sum of [10, 20, 30, 40] = {total.value}")
    
    # Multiplication
    print("\nMultiplication:")
    result = Multiply(2) * Multiply(3) * Multiply(4)
    print(f"  2 × 3 × 4 = {result.value}")
    
    # Max
    print("\nMaximum:")
    values = [Max(5), Max(2), Max(9), Max(1), Max(7)]
    maximum = sum(values)
    print(f"  Max of [5, 2, 9, 1, 7] = {maximum.value}")
    
    # Min
    print("\nMinimum:")
    values = [Min(5), Min(2), Min(9), Min(1), Min(7)]
    minimum = sum(values)
    print(f"  Min of [5, 2, 9, 1, 7] = {minimum.value}")
    
    print()


def collection_examples():
    """Examples using collection monoids."""
    print("=== Collection Monoids Examples ===\n")
    
    # Sets
    print("Set Union:")
    s1 = SetMonoid({1, 2, 3})
    s2 = SetMonoid({3, 4, 5})
    s3 = SetMonoid({5, 6, 7})
    union = s1 + s2 + s3
    print(f"  {{1,2,3}} ∪ {{3,4,5}} ∪ {{5,6,7}} = {union.value}")
    
    # Lists
    print("\nList Concatenation:")
    l1 = ListMonoid([1, 2, 3])
    l2 = ListMonoid([4, 5, 6])
    l3 = ListMonoid([7, 8, 9])
    concat = l1 + l2 + l3
    print(f"  [1,2,3] ++ [4,5,6] ++ [7,8,9] = {concat.value}")
    
    # Maps with sum
    print("\nMap Merge (sum values):")
    m1 = MapMonoid({'a': 1, 'b': 2}, combine_values=lambda x, y: x + y)
    m2 = MapMonoid({'b': 3, 'c': 4}, combine_values=lambda x, y: x + y)
    m3 = MapMonoid({'a': 5, 'c': 6}, combine_values=lambda x, y: x + y)
    merged = m1 + m2 + m3
    print(f"  Merged map: {merged.value}")
    
    # Strings
    print("\nString Concatenation:")
    words = [StringMonoid("Hello"), StringMonoid(" "), StringMonoid("World"), StringMonoid("!")]
    sentence = sum(words)
    print(f"  Result: '{sentence.value}'")
    
    print()


def option_examples():
    """Examples using Option monoid."""
    print("=== Option Monoid Examples ===\n")
    
    # Basic combination
    print("Option Combination (first-wins):")
    result = Some(5) + None_() + Some(3)
    print(f"  Some(5) + None_() + Some(3) = {result}")
    
    result = None_() + None_() + Some(10)
    print(f"  None_() + None_() + Some(10) = {result}")
    
    # Map and flatMap
    print("\nOption Transformations:")
    result = Some(5).map(lambda x: x * 2).map(lambda x: x + 1)
    print(f"  Some(5).map(*2).map(+1) = {result}")
    
    # Safe division with flatMap
    def safe_divide(a, b):
        return Some(a / b) if b != 0 else None_()
    
    result = Some(10).flat_map(lambda x: safe_divide(x, 2))
    print(f"  Some(10).flatMap(÷2) = {result}")
    
    result = Some(10).flat_map(lambda x: safe_divide(x, 0))
    print(f"  Some(10).flatMap(÷0) = {result}")
    
    # Custom combination
    print("\nOption with custom value combination:")
    m = OptionMonoid(lambda a, b: a + b)
    options = [Some(10), None_(), Some(20), Some(30)]
    total = m.combine_all(options)
    print(f"  Sum of [Some(10), None_(), Some(20), Some(30)] = {total}")
    
    print()


def distributed_aggregation_example():
    """
    Example: Distributed aggregation using monoids.
    
    Simulates aggregating data across multiple partitions/nodes,
    demonstrating how monoid properties enable safe parallel processing.
    """
    print("=== Distributed Aggregation Example ===\n")
    print("Simulating parallel processing of user events...\n")
    
    # Partition 1: User events from server 1
    partition_1 = {
        'total_views': Add(1000),
        'unique_users': SetMonoid({'user1', 'user2', 'user3'}),
        'errors': ListMonoid(['error1', 'error2']),
        'max_latency': Max(150.5),
        'min_latency': Min(5.2),
    }
    
    # Partition 2: User events from server 2
    partition_2 = {
        'total_views': Add(1500),
        'unique_users': SetMonoid({'user2', 'user4', 'user5'}),
        'errors': ListMonoid(['error3']),
        'max_latency': Max(200.3),
        'min_latency': Min(3.8),
    }
    
    # Partition 3: User events from server 3
    partition_3 = {
        'total_views': Add(800),
        'unique_users': SetMonoid({'user1', 'user5', 'user6'}),
        'errors': ListMonoid([]),
        'max_latency': Max(120.0),
        'min_latency': Min(4.5),
    }
    
    # Aggregate across partitions (can be done in any order!)
    aggregated = {
        'total_views': sum([partition_1['total_views'], 
                           partition_2['total_views'], 
                           partition_3['total_views']]),
        'unique_users': sum([partition_1['unique_users'], 
                            partition_2['unique_users'], 
                            partition_3['unique_users']]),
        'errors': sum([partition_1['errors'], 
                      partition_2['errors'], 
                      partition_3['errors']]),
        'max_latency': sum([partition_1['max_latency'], 
                           partition_2['max_latency'], 
                           partition_3['max_latency']]),
        'min_latency': sum([partition_1['min_latency'], 
                           partition_2['min_latency'], 
                           partition_3['min_latency']]),
    }
    
    print(f"Total Views: {aggregated['total_views'].value}")
    print(f"Unique Users: {len(aggregated['unique_users'])} users")
    print(f"  Users: {aggregated['unique_users'].value}")
    print(f"All Errors: {aggregated['errors'].value}")
    print(f"Max Latency: {aggregated['max_latency'].value}ms")
    print(f"Min Latency: {aggregated['min_latency'].value}ms")
    
    print()


def word_count_example():
    """
    Example: Distributed word count using MapMonoid.
    
    Classic MapReduce example demonstrating composable aggregation.
    """
    print("=== Word Count Example ===\n")
    
    # Document partitions
    doc1 = "hello world hello"
    doc2 = "world of monoids"
    doc3 = "hello monoids"
    
    # Map phase: count words in each partition
    def count_words(text):
        words = text.split()
        counts = {}
        for word in words:
            counts[word] = counts.get(word, 0) + 1
        return MapMonoid(counts, combine_values=lambda x, y: x + y)
    
    partition_1 = count_words(doc1)
    partition_2 = count_words(doc2)
    partition_3 = count_words(doc3)
    
    # Reduce phase: combine all counts
    total_counts = partition_1 + partition_2 + partition_3
    
    print("Word counts:")
    for word, count in sorted(total_counts.value.items()):
        print(f"  {word}: {count}")
    
    print()


def statistics_example():
    """
    Example: Composable statistics using multiple monoids.
    
    Shows how to compute multiple statistics in a single pass.
    """
    print("=== Composable Statistics Example ===\n")
    
    data = [10, 25, 15, 30, 20, 35, 5, 40]
    
    stats = {
        'count': sum([Add(1) for _ in data]),
        'sum': sum([Add(x) for x in data]),
        'max': sum([Max(x) for x in data]),
        'min': sum([Min(x) for x in data]),
    }
    
    mean = stats['sum'].value / stats['count'].value
    
    print(f"Data: {data}")
    print(f"Count: {stats['count'].value}")
    print(f"Sum: {stats['sum'].value}")
    print(f"Mean: {mean:.2f}")
    print(f"Max: {stats['max'].value}")
    print(f"Min: {stats['min'].value}")
    
    print()


def config_fallback_example():
    """
    Example: Configuration with fallback using Option monoid.
    
    Shows how Option enables clean fallback chains.
    """
    print("=== Configuration Fallback Example ===\n")
    
    # Simulate different config sources
    def get_env_config(key):
        env_vars = {'API_KEY': 'env-key-123'}
        return Some(env_vars[key]) if key in env_vars else None_()
    
    def get_user_config(key):
        user_config = {'TIMEOUT': 30}
        return Some(user_config[key]) if key in user_config else None_()
    
    def get_default_config(key):
        defaults = {'API_KEY': 'default-key', 'TIMEOUT': 60, 'DEBUG': False}
        return Some(defaults[key]) if key in defaults else None_()
    
    # Lookup with fallback chain
    keys = ['API_KEY', 'TIMEOUT', 'DEBUG', 'MISSING']
    
    for key in keys:
        value = get_env_config(key) + get_user_config(key) + get_default_config(key)
        if value.is_some:
            print(f"{key}: {value.value}")
        else:
            print(f"{key}: Not found")
    
    print()


def main():
    """Run all examples."""
    numeric_examples()
    collection_examples()
    option_examples()
    print()
    distributed_aggregation_example()
    word_count_example()
    statistics_example()
    config_fallback_example()


if __name__ == '__main__':
    main()
