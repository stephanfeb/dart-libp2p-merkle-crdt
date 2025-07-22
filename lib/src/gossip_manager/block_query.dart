// lib/src/gossip_manager/block_query.dart
import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:merkledag/merkledag.dart' show MerkleNode, CRDTPayload;
import 'block_store_entry.dart';
import 'field_extractor.dart';

/// Comparison operators for query conditions
enum ComparisonOperator {
  equals,
  notEquals,
  greaterThan,
  greaterThanOrEqual,
  lessThan,
  lessThanOrEqual,
  contains,
  startsWith,
  endsWith,
  isNull,
  isNotNull,
}

/// Logical operators for combining query conditions
enum LogicalOperator {
  and,
  or,
  not,
}

/// Sort direction for ordering results
enum SortDirection {
  ascending,
  descending,
}

/// Represents a single query condition
class QueryCondition {
  final String fieldPath;
  final ComparisonOperator operator;
  final dynamic value;

  const QueryCondition({
    required this.fieldPath,
    required this.operator,
    this.value,
  });

  /// Evaluates this condition against a node using the provided field extractor
  bool evaluate<P extends CRDTPayload<V>, V>(MerkleNode<P> node, FieldExtractor<P, V> fieldExtractor) {
    final fieldValue = fieldExtractor.extractField(node, fieldPath);
    
    switch (operator) {
      case ComparisonOperator.equals:
        return fieldValue == value;
      case ComparisonOperator.notEquals:
        return fieldValue != value;
      case ComparisonOperator.greaterThan:
        return _compareValues(fieldValue, value) > 0;
      case ComparisonOperator.greaterThanOrEqual:
        return _compareValues(fieldValue, value) >= 0;
      case ComparisonOperator.lessThan:
        return _compareValues(fieldValue, value) < 0;
      case ComparisonOperator.lessThanOrEqual:
        return _compareValues(fieldValue, value) <= 0;
      case ComparisonOperator.contains:
        return fieldValue?.toString().contains(value?.toString() ?? '') ?? false;
      case ComparisonOperator.startsWith:
        return fieldValue?.toString().startsWith(value?.toString() ?? '') ?? false;
      case ComparisonOperator.endsWith:
        return fieldValue?.toString().endsWith(value?.toString() ?? '') ?? false;
      case ComparisonOperator.isNull:
        return fieldValue == null;
      case ComparisonOperator.isNotNull:
        return fieldValue != null;
    }
  }

  /// Helper method to compare values of different types
  int _compareValues(dynamic a, dynamic b) {
    if (a == null && b == null) return 0;
    if (a == null) return -1;
    if (b == null) return 1;

    if (a is Comparable && b is Comparable) {
      try {
        return a.compareTo(b);
      } catch (e) {
        // Fall back to string comparison if direct comparison fails
        return a.toString().compareTo(b.toString());
      }
    }

    return a.toString().compareTo(b.toString());
  }

  @override
  String toString() {
    return 'QueryCondition{fieldPath: $fieldPath, operator: $operator, value: $value}';
  }
}

/// Represents a logical combination of query conditions
class LogicalCondition {
  final LogicalOperator operator;
  final List<dynamic> operands; // Can contain QueryCondition or LogicalCondition

  LogicalCondition({
    required this.operator,
    required this.operands,
  });

  /// Evaluates this logical condition against a node
  bool evaluate<P extends CRDTPayload<V>, V>(MerkleNode<P> node, FieldExtractor<P, V> fieldExtractor) {
    switch (operator) {
      case LogicalOperator.and:
        return operands.every((operand) => _evaluateOperand<P, V>(operand, node, fieldExtractor));
      case LogicalOperator.or:
        return operands.any((operand) => _evaluateOperand<P, V>(operand, node, fieldExtractor));
      case LogicalOperator.not:
        if (operands.length != 1) {
          throw ArgumentError('NOT operator must have exactly one operand');
        }
        return !_evaluateOperand<P, V>(operands.first, node, fieldExtractor);
    }
  }

  bool _evaluateOperand<P extends CRDTPayload<V>, V>(dynamic operand, MerkleNode<P> node, FieldExtractor<P, V> fieldExtractor) {
    if (operand is QueryCondition) {
      return operand.evaluate<P, V>(node, fieldExtractor);
    } else if (operand is LogicalCondition) {
      return operand.evaluate<P, V>(node, fieldExtractor);
    } else {
      throw ArgumentError('Invalid operand type: ${operand.runtimeType}');
    }
  }

  @override
  String toString() {
    return 'LogicalCondition{operator: $operator, operands: $operands}';
  }
}

/// Represents a sort specification
class SortSpec {
  final String fieldPath;
  final SortDirection direction;

  const SortSpec({
    required this.fieldPath,
    this.direction = SortDirection.ascending,
  });

  @override
  String toString() {
    return 'SortSpec{fieldPath: $fieldPath, direction: $direction}';
  }
}

/// Custom predicate function type for advanced queries
typedef CustomPredicate<P extends CRDTPayload<V>, V> = bool Function(BlockStoreEntry<P, V> entry);

/// JPA-inspired query builder interface for BlockStore queries
abstract class BlockQuery<P extends CRDTPayload<V>, V> {
  /// Adds an equality condition
  BlockQuery<P, V> where(String fieldPath, dynamic value);

  /// Adds a condition with a specific operator
  BlockQuery<P, V> whereOperator(String fieldPath, ComparisonOperator operator, [dynamic value]);

  /// Adds an IN condition (value must be in the provided list)
  BlockQuery<P, V> whereIn(String fieldPath, List<dynamic> values);

  /// Adds a NOT IN condition
  BlockQuery<P, V> whereNotIn(String fieldPath, List<dynamic> values);

  /// Adds a greater than condition
  BlockQuery<P, V> whereGreaterThan(String fieldPath, dynamic value);

  /// Adds a greater than or equal condition
  BlockQuery<P, V> whereGreaterThanOrEqual(String fieldPath, dynamic value);

  /// Adds a less than condition
  BlockQuery<P, V> whereLessThan(String fieldPath, dynamic value);

  /// Adds a less than or equal condition
  BlockQuery<P, V> whereLessThanOrEqual(String fieldPath, dynamic value);

  /// Adds a contains condition (for string fields)
  BlockQuery<P, V> whereContains(String fieldPath, String value);

  /// Adds a starts with condition (for string fields)
  BlockQuery<P, V> whereStartsWith(String fieldPath, String value);

  /// Adds a ends with condition (for string fields)
  BlockQuery<P, V> whereEndsWith(String fieldPath, String value);

  /// Adds a null check condition
  BlockQuery<P, V> whereNull(String fieldPath);

  /// Adds a not null check condition
  BlockQuery<P, V> whereNotNull(String fieldPath);

  /// Adds a custom predicate condition
  BlockQuery<P, V> whereCustom(CustomPredicate<P, V> predicate);

  /// Adds a condition based on the payload directly
  BlockQuery<P, V> wherePayload(bool Function(P payload) predicate);

  /// Adds a condition based on the value directly
  BlockQuery<P, V> whereValue(bool Function(V value) predicate);

  /// Adds a condition to filter by collection
  BlockQuery<P, V> whereCollection(String collection);

  /// Adds a condition to filter by multiple collections
  BlockQuery<P, V> whereCollectionIn(List<String> collections);

  /// Adds a condition to exclude specific collections
  BlockQuery<P, V> whereCollectionNotIn(List<String> collections);

  /// Adds a condition to filter entries with any collection assigned
  BlockQuery<P, V> whereHasCollection();

  /// Adds a condition to filter entries with no collection assigned
  BlockQuery<P, V> whereNoCollection();

  /// Starts an AND group
  BlockQuery<P, V> and();

  /// Starts an OR group
  BlockQuery<P, V> or();

  /// Starts a NOT group
  BlockQuery<P, V> not();

  /// Ends the current logical group
  BlockQuery<P, V> endGroup();

  /// Adds ordering by field
  BlockQuery<P, V> orderBy(String fieldPath, [SortDirection direction = SortDirection.ascending]);

  /// Adds ordering by multiple fields
  BlockQuery<P, V> orderByMultiple(List<SortSpec> sortSpecs);

  /// Sets the maximum number of results to return
  BlockQuery<P, V> limit(int count);

  /// Sets the number of results to skip
  BlockQuery<P, V> offset(int count);

  /// Executes the query and returns matching entries
  Future<List<BlockStoreEntry<P, V>>> execute();

  /// Executes the query and returns only the first result
  Future<BlockStoreEntry<P, V>?> executeFirst();

  /// Executes the query and returns the count of matching entries
  Future<int> executeCount();

  /// Executes the query and returns whether any entries match
  Future<bool> executeExists();
}

/// Implementation of BlockQuery with fluent API
class BlockQueryImpl<P extends CRDTPayload<V>, V> implements BlockQuery<P, V> {
  final Stream<BlockStoreEntry<P, V>> _entryStream;
  final FieldExtractor<P, V> _fieldExtractor;

  // Query state
  dynamic _rootCondition;
  final List<dynamic> _conditionStack = [];
  final List<SortSpec> _sortSpecs = [];
  int? _limitCount;
  int? _offsetCount;
  final List<CustomPredicate<P, V>> _customPredicates = [];

  BlockQueryImpl({
    required Stream<BlockStoreEntry<P, V>> entryStream,
    required FieldExtractor<P, V> fieldExtractor,
  }) : _entryStream = entryStream, _fieldExtractor = fieldExtractor;

  @override
  BlockQuery<P, V> where(String fieldPath, dynamic value) {
    return whereOperator(fieldPath, ComparisonOperator.equals, value);
  }

  @override
  BlockQuery<P, V> whereOperator(String fieldPath, ComparisonOperator operator, [dynamic value]) {
    final condition = QueryCondition(
      fieldPath: fieldPath,
      operator: operator,
      value: value,
    );
    _addCondition(condition);
    return this;
  }

  @override
  BlockQuery<P, V> whereIn(String fieldPath, List<dynamic> values) {
    // Create an OR condition for all values
    final conditions = values.map((value) => 
      QueryCondition(fieldPath: fieldPath, operator: ComparisonOperator.equals, value: value)
    ).toList();
    
    final orCondition = LogicalCondition(
      operator: LogicalOperator.or,
      operands: conditions,
    );
    
    _addCondition(orCondition);
    return this;
  }

  @override
  BlockQuery<P, V> whereNotIn(String fieldPath, List<dynamic> values) {
    // Create an AND condition for all NOT EQUALS values
    final conditions = values.map((value) => 
      QueryCondition(fieldPath: fieldPath, operator: ComparisonOperator.notEquals, value: value)
    ).toList();
    
    final andCondition = LogicalCondition(
      operator: LogicalOperator.and,
      operands: conditions,
    );
    
    _addCondition(andCondition);
    return this;
  }

  @override
  BlockQuery<P, V> whereGreaterThan(String fieldPath, dynamic value) {
    return whereOperator(fieldPath, ComparisonOperator.greaterThan, value);
  }

  @override
  BlockQuery<P, V> whereGreaterThanOrEqual(String fieldPath, dynamic value) {
    return whereOperator(fieldPath, ComparisonOperator.greaterThanOrEqual, value);
  }

  @override
  BlockQuery<P, V> whereLessThan(String fieldPath, dynamic value) {
    return whereOperator(fieldPath, ComparisonOperator.lessThan, value);
  }

  @override
  BlockQuery<P, V> whereLessThanOrEqual(String fieldPath, dynamic value) {
    return whereOperator(fieldPath, ComparisonOperator.lessThanOrEqual, value);
  }

  @override
  BlockQuery<P, V> whereContains(String fieldPath, String value) {
    return whereOperator(fieldPath, ComparisonOperator.contains, value);
  }

  @override
  BlockQuery<P, V> whereStartsWith(String fieldPath, String value) {
    return whereOperator(fieldPath, ComparisonOperator.startsWith, value);
  }

  @override
  BlockQuery<P, V> whereEndsWith(String fieldPath, String value) {
    return whereOperator(fieldPath, ComparisonOperator.endsWith, value);
  }

  @override
  BlockQuery<P, V> whereNull(String fieldPath) {
    return whereOperator(fieldPath, ComparisonOperator.isNull);
  }

  @override
  BlockQuery<P, V> whereNotNull(String fieldPath) {
    return whereOperator(fieldPath, ComparisonOperator.isNotNull);
  }

  @override
  BlockQuery<P, V> whereCustom(CustomPredicate<P, V> predicate) {
    _customPredicates.add(predicate);
    return this;
  }

  @override
  BlockQuery<P, V> wherePayload(bool Function(P payload) predicate) {
    return whereCustom((entry) => entry.node != null ? predicate(entry.node!.payload) : false);
  }

  @override
  BlockQuery<P, V> whereValue(bool Function(V value) predicate) {
    return whereCustom((entry) => entry.node != null ? predicate(entry.node!.payload.value) : false);
  }

  @override
  BlockQuery<P, V> whereCollection(String collection) {
    return whereCustom((entry) => entry.collection == collection);
  }

  @override
  BlockQuery<P, V> whereCollectionIn(List<String> collections) {
    return whereCustom((entry) => entry.collection != null && collections.contains(entry.collection));
  }

  @override
  BlockQuery<P, V> whereCollectionNotIn(List<String> collections) {
    return whereCustom((entry) => entry.collection == null || !collections.contains(entry.collection));
  }

  @override
  BlockQuery<P, V> whereHasCollection() {
    return whereCustom((entry) => entry.collection != null);
  }

  @override
  BlockQuery<P, V> whereNoCollection() {
    return whereCustom((entry) => entry.collection == null);
  }

  @override
  BlockQuery<P, V> and() {
    _conditionStack.add(LogicalOperator.and);
    return this;
  }

  @override
  BlockQuery<P, V> or() {
    _conditionStack.add(LogicalOperator.or);
    return this;
  }

  @override
  BlockQuery<P, V> not() {
    _conditionStack.add(LogicalOperator.not);
    return this;
  }

  @override
  BlockQuery<P, V> endGroup() {
    // This would need more sophisticated implementation for proper grouping
    // For now, just pop from the stack
    if (_conditionStack.isNotEmpty) {
      _conditionStack.removeLast();
    }
    return this;
  }

  @override
  BlockQuery<P, V> orderBy(String fieldPath, [SortDirection direction = SortDirection.ascending]) {
    _sortSpecs.add(SortSpec(fieldPath: fieldPath, direction: direction));
    return this;
  }

  @override
  BlockQuery<P, V> orderByMultiple(List<SortSpec> sortSpecs) {
    _sortSpecs.addAll(sortSpecs);
    return this;
  }

  @override
  BlockQuery<P, V> limit(int count) {
    _limitCount = count;
    return this;
  }

  @override
  BlockQuery<P, V> offset(int count) {
    _offsetCount = count;
    return this;
  }

  @override
  Future<List<BlockStoreEntry<P, V>>> execute() async {
    final results = <BlockStoreEntry<P, V>>[];
    
    await for (final entry in _entryStream) {
      if (_matchesQuery(entry)) {
        results.add(entry);
      }
    }

    // Apply sorting
    if (_sortSpecs.isNotEmpty) {
      results.sort((a, b) => _compareEntries(a, b));
    }

    // Apply offset and limit
    int startIndex = _offsetCount ?? 0;
    int? endIndex = _limitCount != null ? startIndex + _limitCount! : null;
    
    if (startIndex > 0 || endIndex != null) {
      return results.sublist(
        startIndex.clamp(0, results.length),
        endIndex?.clamp(0, results.length),
      );
    }

    return results;
  }

  @override
  Future<BlockStoreEntry<P, V>?> executeFirst() async {
    await for (final entry in _entryStream) {
      if (_matchesQuery(entry)) {
        return entry;
      }
    }
    return null;
  }

  @override
  Future<int> executeCount() async {
    int count = 0;
    await for (final entry in _entryStream) {
      if (_matchesQuery(entry)) {
        count++;
      }
    }
    return count;
  }

  @override
  Future<bool> executeExists() async {
    await for (final entry in _entryStream) {
      if (_matchesQuery(entry)) {
        return true;
      }
    }
    return false;
  }

  /// Helper method to add a condition to the query
  void _addCondition(dynamic condition) {
    if (_rootCondition == null) {
      _rootCondition = condition;
    } else {
      // Combine with existing condition using AND by default
      _rootCondition = LogicalCondition(
        operator: LogicalOperator.and,
        operands: [_rootCondition, condition],
      );
    }
  }

  /// Helper method to check if an entry matches the query
  bool _matchesQuery(BlockStoreEntry<P, V> entry) {
    // Skip tombstoned entries unless they have data
    if (entry.isTombstoned || entry.node == null) {
      return false;
    }

    // Check custom predicates
    for (final predicate in _customPredicates) {
      if (!predicate(entry)) {
        return false;
      }
    }

    // Check field-based conditions
    if (_rootCondition != null) {
      return _evaluateCondition(_rootCondition, entry.node!);
    }

    return true;
  }

  /// Helper method to evaluate a condition against a node
  bool _evaluateCondition(dynamic condition, MerkleNode<P> node) {
    if (condition is QueryCondition) {
      return condition.evaluate<P, V>(node, _fieldExtractor);
    } else if (condition is LogicalCondition) {
      return condition.evaluate<P, V>(node, _fieldExtractor);
    } else {
      throw ArgumentError('Invalid condition type: ${condition.runtimeType}');
    }
  }

  /// Helper method to compare entries for sorting
  int _compareEntries(BlockStoreEntry<P, V> a, BlockStoreEntry<P, V> b) {
    for (final sortSpec in _sortSpecs) {
      final aValue = a.node != null ? _fieldExtractor.extractField(a.node!, sortSpec.fieldPath) : null;
      final bValue = b.node != null ? _fieldExtractor.extractField(b.node!, sortSpec.fieldPath) : null;

      int comparison = _compareValues(aValue, bValue);
      
      if (sortSpec.direction == SortDirection.descending) {
        comparison = -comparison;
      }

      if (comparison != 0) {
        return comparison;
      }
    }

    return 0;
  }

  /// Helper method to compare values for sorting
  int _compareValues(dynamic a, dynamic b) {
    if (a == null && b == null) return 0;
    if (a == null) return -1;
    if (b == null) return 1;

    if (a is Comparable && b is Comparable) {
      try {
        return a.compareTo(b);
      } catch (e) {
        return a.toString().compareTo(b.toString());
      }
    }

    return a.toString().compareTo(b.toString());
  }
}
