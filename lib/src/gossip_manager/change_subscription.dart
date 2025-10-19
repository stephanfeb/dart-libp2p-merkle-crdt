// lib/src/gossip_manager/change_subscription.dart
import 'dart:async';
import 'package:dcid/dcid.dart' show CID;
import 'package:merkledag/merkledag.dart' show CRDTPayload;

import 'block_change_event.dart';
import 'field_extractor.dart';
import 'block_query.dart';

/// Represents a subscription to block change events with optional filtering
class ChangeSubscription<P extends CRDTPayload<V>, V> {
  final String _id;
  final StreamController<BlockChangeEvent<P, V>> _controller;
  final ChangeFilter<P, V>? _filter;
  final DateTime _createdAt;
  bool _isActive = true;

  ChangeSubscription._(this._id, this._filter) 
      : _controller = StreamController<BlockChangeEvent<P, V>>.broadcast(),
        _createdAt = DateTime.now();

  /// Creates a new subscription with optional filtering
  factory ChangeSubscription.create({
    String? id,
    ChangeFilter<P, V>? filter,
  }) {
    return ChangeSubscription._(
      id ?? DateTime.now().millisecondsSinceEpoch.toString(),
      filter,
    );
  }

  /// The unique identifier for this subscription
  String get id => _id;

  /// The stream of change events for this subscription
  Stream<BlockChangeEvent<P, V>> get stream => _controller.stream;

  /// Whether this subscription is currently active
  bool get isActive => _isActive;

  /// When this subscription was created
  DateTime get createdAt => _createdAt;

  /// The filter applied to this subscription (if any)
  ChangeFilter<P, V>? get filter => _filter;

  /// Emits an event to this subscription if it passes the filter
  void _emit(BlockChangeEvent<P, V> event) {
    print('ChangeSubscription._emit called for subscription ${_id}');
    print('  - isActive: ${_isActive}');
    print('  - hasFilter: ${_filter != null}');
    print('  - eventType: ${event.type}');
    
    if (!_isActive) {
      print('  - Subscription not active, returning');
      return;
    }
    
    if (_filter == null) {
      print('  - No filter, adding event to controller');
      _controller.add(event);
    } else {
      final matches = _filter!.matches(event);
      print('  - Filter matches: ${matches}');
      if (matches) {
        print('  - Adding event to controller');
        _controller.add(event);
      } else {
        print('  - Event filtered out');
      }
    }
  }

  /// Closes this subscription and stops emitting events
  Future<void> close() async {
    if (!_isActive) return;
    
    _isActive = false;
    await _controller.close();
  }
}

/// Abstract base class for filtering change events
abstract class ChangeFilter<P extends CRDTPayload<V>, V> {
  /// Tests whether an event matches this filter
  bool matches(BlockChangeEvent<P, V> event);

  /// Combines this filter with another using AND logic
  ChangeFilter<P, V> and(ChangeFilter<P, V> other) {
    return AndFilter<P, V>([this, other]);
  }

  /// Combines this filter with another using OR logic
  ChangeFilter<P, V> or(ChangeFilter<P, V> other) {
    return OrFilter<P, V>([this, other]);
  }

  /// Negates this filter
  ChangeFilter<P, V> not() {
    return NotFilter<P, V>(this);
  }
}

/// Filter that matches events by change type
class ChangeTypeFilter<P extends CRDTPayload<V>, V> extends ChangeFilter<P, V> {
  final Set<BlockChangeType> _types;

  ChangeTypeFilter(BlockChangeType type) : _types = {type};
  ChangeTypeFilter.multiple(Iterable<BlockChangeType> types) : _types = Set.from(types);

  @override
  bool matches(BlockChangeEvent<P, V> event) {
    return _types.contains(event.type);
  }
}

/// Filter that matches events by CID
class CidFilter<P extends CRDTPayload<V>, V> extends ChangeFilter<P, V> {
  final Set<CID> _cids;

  CidFilter(CID cid) : _cids = {cid};
  CidFilter.multiple(Iterable<CID> cids) : _cids = Set.from(cids);

  @override
  bool matches(BlockChangeEvent<P, V> event) {
    return _cids.contains(event.cid);
  }
}

/// Filter that matches events by collection
class CollectionFilter<P extends CRDTPayload<V>, V> extends ChangeFilter<P, V> {
  final Set<String> _collections;
  final bool _includeNullCollection;

  CollectionFilter(String collection) 
      : _collections = {collection}, 
        _includeNullCollection = false;
        
  CollectionFilter.multiple(Iterable<String> collections) 
      : _collections = Set.from(collections),
        _includeNullCollection = false;

  CollectionFilter.includeUncategorized(Iterable<String> collections)
      : _collections = Set.from(collections),
        _includeNullCollection = true;

  @override
  bool matches(BlockChangeEvent<P, V> event) {
    if (event.collection == null) {
      return _includeNullCollection;
    }
    return _collections.contains(event.collection);
  }
}

/// Filter that matches events using a custom predicate
class PredicateFilter<P extends CRDTPayload<V>, V> extends ChangeFilter<P, V> {
  final bool Function(BlockChangeEvent<P, V>) _predicate;

  PredicateFilter(this._predicate);

  @override
  bool matches(BlockChangeEvent<P, V> event) {
    return _predicate(event);
  }
}

/// Filter that matches events based on field values using a field extractor
class FieldFilter<P extends CRDTPayload<V>, V> extends ChangeFilter<P, V> {
  final FieldExtractor<P, V> _fieldExtractor;
  final String _fieldPath;
  final dynamic _expectedValue;
  final bool Function(dynamic, dynamic) _comparator;

  FieldFilter(
    this._fieldExtractor,
    this._fieldPath,
    this._expectedValue,
    [this._comparator = _defaultEquals]
  );

  static bool _defaultEquals(dynamic a, dynamic b) => a == b;

  @override
  bool matches(BlockChangeEvent<P, V> event) {
    if (event.node == null) return false;
    
    try {
      final actualValue = _fieldExtractor.extractField(event.node!, _fieldPath);
      return _comparator(actualValue, _expectedValue);
    } catch (e) {
      return false;
    }
  }
}

/// Filter that matches events within a time range
class TimeRangeFilter<P extends CRDTPayload<V>, V> extends ChangeFilter<P, V> {
  final DateTime? _start;
  final DateTime? _end;

  TimeRangeFilter({DateTime? start, DateTime? end}) 
      : _start = start,
        _end = end;

  @override
  bool matches(BlockChangeEvent<P, V> event) {
    if (_start != null && event.timestamp.isBefore(_start!)) {
      return false;
    }
    if (_end != null && event.timestamp.isAfter(_end!)) {
      return false;
    }
    return true;
  }
}

/// Combines multiple filters with AND logic
class AndFilter<P extends CRDTPayload<V>, V> extends ChangeFilter<P, V> {
  final List<ChangeFilter<P, V>> _filters;

  AndFilter(this._filters);

  @override
  bool matches(BlockChangeEvent<P, V> event) {
    return _filters.every((filter) => filter.matches(event));
  }
}

/// Combines multiple filters with OR logic
class OrFilter<P extends CRDTPayload<V>, V> extends ChangeFilter<P, V> {
  final List<ChangeFilter<P, V>> _filters;

  OrFilter(this._filters);

  @override
  bool matches(BlockChangeEvent<P, V> event) {
    return _filters.any((filter) => filter.matches(event));
  }
}

/// Negates another filter
class NotFilter<P extends CRDTPayload<V>, V> extends ChangeFilter<P, V> {
  final ChangeFilter<P, V> _filter;

  NotFilter(this._filter);

  @override
  bool matches(BlockChangeEvent<P, V> event) {
    return !_filter.matches(event);
  }
}

/// Manages multiple change subscriptions
class ChangeSubscriptionManager<P extends CRDTPayload<V>, V> {
  final Map<String, ChangeSubscription<P, V>> _subscriptions = {};
  static int _nextId = 1;

  /// Creates a new subscription with optional filtering
  ChangeSubscription<P, V> subscribe({
    String? id,
    ChangeFilter<P, V>? filter,
  }) {
    final subscriptionId = id ?? 'sub_${_nextId++}';
    final subscription = ChangeSubscription<P, V>.create(id: subscriptionId, filter: filter);
    _subscriptions[subscription.id] = subscription;
    print('ChangeSubscriptionManager.subscribe: Created subscription ${subscription.id} with filter: ${filter != null}');
    print('  Total subscriptions: ${_subscriptions.length}');
    return subscription;
  }

  /// Removes a subscription by ID
  Future<bool> unsubscribe(String id) async {
    final subscription = _subscriptions.remove(id);
    if (subscription != null) {
      await subscription.close();
      return true;
    }
    return false;
  }

  /// Gets a subscription by ID
  ChangeSubscription<P, V>? getSubscription(String id) {
    return _subscriptions[id];
  }

  /// Gets all active subscriptions
  List<ChangeSubscription<P, V>> get activeSubscriptions {
    return _subscriptions.values.where((s) => s.isActive).toList();
  }

  /// Emits an event to all matching subscriptions
  void emit(BlockChangeEvent<P, V> event) {
    print('ChangeSubscriptionManager.emit: Emitting event ${event.type} to ${_subscriptions.length} subscriptions');
    for (final subscription in _subscriptions.values) {
      print('  Processing subscription ${subscription.id}');
      subscription._emit(event);
    }
  }

  /// Closes all subscriptions
  Future<void> closeAll() async {
    final futures = _subscriptions.values.map((s) => s.close());
    await Future.wait(futures);
    _subscriptions.clear();
  }

  /// Gets the number of active subscriptions
  int get subscriptionCount => _subscriptions.values.where((s) => s.isActive).length;
}
