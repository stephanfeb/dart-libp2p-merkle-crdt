import 'dart:convert' show jsonDecode, utf8;
import 'dart:typed_data';

import 'package:merkledag/merkledag.dart' show CRDTPayload;
import 'canonical_json.dart';
import 'vector_clock.dart';

/// Represents a single entry in a DocumentCRDT.
/// Each entry has a value, timestamp, deletion status, and actor ID.
class DocumentEntry {
  final dynamic value;
  final VectorClock timestamp;
  final bool isDeleted;
  final String actorId;

  DocumentEntry({
    required this.value,
    required this.timestamp,
    required this.isDeleted,
    required this.actorId,
  });

  /// Creates a new entry with a value.
  factory DocumentEntry.create(
    dynamic value,
    VectorClock timestamp,
    String actorId,
  ) {
    return DocumentEntry(
      value: value,
      timestamp: timestamp,
      isDeleted: false,
      actorId: actorId,
    );
  }

  /// Creates a tombstone entry (deleted).
  factory DocumentEntry.tombstone(
    VectorClock timestamp,
    String actorId,
  ) {
    return DocumentEntry(
      value: null,
      timestamp: timestamp,
      isDeleted: true,
      actorId: actorId,
    );
  }

  /// Creates a copy of this entry with updated fields.
  DocumentEntry copyWith({
    dynamic value,
    VectorClock? timestamp,
    bool? isDeleted,
    String? actorId,
  }) {
    return DocumentEntry(
      value: value ?? this.value,
      timestamp: timestamp ?? this.timestamp,
      isDeleted: isDeleted ?? this.isDeleted,
      actorId: actorId ?? this.actorId,
    );
  }

  /// Serializes the entry to JSON.
  Map<String, dynamic> toJson() {
    return {
      'value': _serializeValue(value),
      'timestamp': timestamp.toJson(),
      'isDeleted': isDeleted,
      'actorId': actorId,
    };
  }

  /// Deserializes an entry from JSON.
  factory DocumentEntry.fromJson(Map<String, dynamic> json) {
    return DocumentEntry(
      value: _deserializeValue(json['value']),
      timestamp: VectorClock.fromJson(json['timestamp'] as Map<String, dynamic>),
      isDeleted: json['isDeleted'] as bool,
      actorId: json['actorId'] as String,
    );
  }

  /// Helper to serialize values that might be complex types.
  static dynamic _serializeValue(dynamic value) {
    if (value == null) return null;
    if (value is String || value is num || value is bool) return value;
    if (value is List) return value.map(_serializeValue).toList();
    if (value is Map) {
      return value.map((k, v) => MapEntry(k.toString(), _serializeValue(v)));
    }
    // For other types, convert to string representation
    return value.toString();
  }

  /// Helper to deserialize values.
  static dynamic _deserializeValue(dynamic value) {
    // JSON deserialization handles basic types correctly
    return value;
  }

  /// Deep equality comparison for values that may contain nested structures.
  static bool _deepEquals(dynamic a, dynamic b) {
    if (identical(a, b)) return true;
    if (a == null || b == null) return a == b;
    
    // Handle Maps (don't check exact runtime type, just that both are Maps)
    if (a is Map && b is Map) {
      if (a.length != b.length) return false;
      for (final key in a.keys) {
        if (!b.containsKey(key) || !_deepEquals(a[key], b[key])) {
          return false;
        }
      }
      return true;
    }
    
    // Handle Lists (don't check exact runtime type, just that both are Lists)
    if (a is List && b is List) {
      if (a.length != b.length) return false;
      for (int i = 0; i < a.length; i++) {
        if (!_deepEquals(a[i], b[i])) return false;
      }
      return true;
    }
    
    // For primitive types and other objects, use regular equality
    return a == b;
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is DocumentEntry &&
          runtimeType == other.runtimeType &&
          _deepEquals(value, other.value) &&
          timestamp == other.timestamp &&
          isDeleted == other.isDeleted &&
          actorId == other.actorId;

  @override
  int get hashCode =>
      _deepHashCode(value) ^
      timestamp.hashCode ^
      isDeleted.hashCode ^
      actorId.hashCode;

  /// Deep hash code computation for values that may contain nested structures.
  static int _deepHashCode(dynamic value) {
    if (value == null) return 0;
    if (value is String || value is num || value is bool) return value.hashCode;
    
    if (value is Map) {
      var hash = 0;
      for (final entry in value.entries) {
        hash ^= entry.key.hashCode ^ _deepHashCode(entry.value);
      }
      return hash;
    }
    
    if (value is List) {
      var hash = 0;
      for (int i = 0; i < value.length; i++) {
        hash ^= i.hashCode ^ _deepHashCode(value[i]);
      }
      return hash;
    }
    
    return value.hashCode;
  }

  @override
  String toString() {
    return 'DocumentEntry(value: $value, timestamp: $timestamp, isDeleted: $isDeleted, actorId: $actorId)';
  }
}

/// Conflict resolution strategies for DocumentCRDT.
enum ConflictResolutionStrategy {
  /// Last Writer Wins - choose the entry with the latest timestamp.
  lastWriterWins,
  
  /// First Writer Wins - choose the entry with the earliest timestamp.
  firstWriterWins,
  
  /// Formerly merged concurrent map and list values. That cannot converge
  /// without per-element metadata (merge order changed the result), so it
  /// now resolves conflicts exactly like [lastWriterWins].
  @Deprecated('Behaves as lastWriterWins; model nested fields as separate keys')
  merge,
  
  /// Use a custom conflict resolver function.
  ///
  /// The resolver is not serialized: decode replicated documents with a
  /// payload factory that re-attaches it with [DocumentCRDT.withConflictResolver].
  /// For replicas to converge it must be commutative, associative and
  /// idempotent, and must not depend on which argument is local.
  custom,
}

/// Function type for custom conflict resolution.
typedef ConflictResolver = DocumentEntry Function(
  String key,
  DocumentEntry local,
  DocumentEntry remote,
);

/// Document-oriented CRDT payload type that implements conflict-free replicated data types
/// for JSON-like document structures. Supports put, delete, patch, and deepMerge operations
/// with configurable conflict resolution strategies.
class DocumentCRDT implements CRDTPayload<Map<String, dynamic>> {
  final Map<String, DocumentEntry> _entries;
  final VectorClock _clock;
  final ConflictResolutionStrategy _strategy;
  final ConflictResolver? _customResolver;

  DocumentCRDT._({
    required Map<String, DocumentEntry> entries,
    required VectorClock clock,
    required ConflictResolutionStrategy strategy,
    ConflictResolver? customResolver,
  })  : _entries = entries,
        _clock = clock,
        _strategy = strategy,
        _customResolver = customResolver;

  /// Creates an empty DocumentCRDT.
  factory DocumentCRDT.empty({
    ConflictResolutionStrategy strategy = ConflictResolutionStrategy.lastWriterWins,
    ConflictResolver? customResolver,
  }) {
    return DocumentCRDT._(
      entries: {},
      clock: VectorClock.empty(),
      strategy: strategy,
      customResolver: customResolver,
    );
  }

  /// Creates a DocumentCRDT from a map of data.
  factory DocumentCRDT.fromMap(
    Map<String, dynamic> data, {
    required String actorId,
    ConflictResolutionStrategy strategy = ConflictResolutionStrategy.lastWriterWins,
    ConflictResolver? customResolver,
  }) {
    var clock = VectorClock.empty();
    final entries = <String, DocumentEntry>{};

    for (final entry in data.entries) {
      clock = clock.increment(actorId);
      entries[entry.key] = DocumentEntry.create(
        entry.value,
        clock,
        actorId,
      );
    }

    return DocumentCRDT._(
      entries: entries,
      clock: clock,
      strategy: strategy,
      customResolver: customResolver,
    );
  }

  /// Returns a new DocumentCRDT with a different conflict resolution strategy.
  DocumentCRDT withConflictResolver(ConflictResolver resolver) {
    return DocumentCRDT._(
      entries: _entries,
      clock: _clock,
      strategy: ConflictResolutionStrategy.custom,
      customResolver: resolver,
    );
  }

  /// Returns a new DocumentCRDT with a different conflict resolution strategy.
  DocumentCRDT withStrategy(ConflictResolutionStrategy strategy) {
    return DocumentCRDT._(
      entries: _entries,
      clock: _clock,
      strategy: strategy,
      customResolver: _customResolver,
    );
  }

  /// Puts a key-value pair into the document.
  /// Returns a new DocumentCRDT with the updated entry.
  DocumentCRDT put(String key, dynamic value, {required String actorId}) {
    final newClock = _clock.increment(actorId);
    final newEntries = Map<String, DocumentEntry>.from(_entries);
    
    newEntries[key] = DocumentEntry.create(value, newClock, actorId);
    
    return DocumentCRDT._(
      entries: newEntries,
      clock: newClock,
      strategy: _strategy,
      customResolver: _customResolver,
    );
  }

  /// Deletes a key from the document by creating a tombstone.
  /// Returns a new DocumentCRDT with the deleted entry.
  DocumentCRDT delete(String key, {required String actorId}) {
    final newClock = _clock.increment(actorId);
    final newEntries = Map<String, DocumentEntry>.from(_entries);
    
    newEntries[key] = DocumentEntry.tombstone(newClock, actorId);
    
    return DocumentCRDT._(
      entries: newEntries,
      clock: newClock,
      strategy: _strategy,
      customResolver: _customResolver,
    );
  }

  /// Applies multiple updates to the document.
  /// Returns a new DocumentCRDT with all updates applied.
  DocumentCRDT patch(Map<String, dynamic> updates, {required String actorId}) {
    var result = this;
    
    for (final entry in updates.entries) {
      result = result.put(entry.key, entry.value, actorId: actorId);
    }
    
    return result;
  }

  /// Performs a deep merge of updates into the document.
  /// For nested maps, merges recursively. For other types, replaces the value.
  DocumentCRDT deepMerge(Map<String, dynamic> updates, {required String actorId}) {
    var result = this;
    
    for (final entry in updates.entries) {
      final key = entry.key;
      final newValue = entry.value;
      final existingEntry = _entries[key];
      
      if (existingEntry != null && 
          !existingEntry.isDeleted && 
          existingEntry.value is Map<String, dynamic> && 
          newValue is Map<String, dynamic>) {
        // Deep merge maps
        final mergedValue = _deepMergeMap(
          existingEntry.value as Map<String, dynamic>,
          newValue,
        );
        result = result.put(key, mergedValue, actorId: actorId);
      } else {
        // Replace value
        result = result.put(key, newValue, actorId: actorId);
      }
    }
    
    return result;
  }

  /// Helper method to recursively merge two maps.
  Map<String, dynamic> _deepMergeMap(
    Map<String, dynamic> existing,
    Map<String, dynamic> updates,
  ) {
    final result = Map<String, dynamic>.from(existing);
    
    for (final entry in updates.entries) {
      final key = entry.key;
      final newValue = entry.value;
      final existingValue = result[key];
      
      if (existingValue is Map<String, dynamic> && 
          newValue is Map<String, dynamic>) {
        result[key] = _deepMergeMap(existingValue, newValue);
      } else {
        result[key] = newValue;
      }
    }
    
    return result;
  }

  @override
  Map<String, dynamic> get value {
    final result = <String, dynamic>{};
    
    for (final entry in _entries.entries) {
      final key = entry.key;
      final docEntry = entry.value;
      
      if (!docEntry.isDeleted) {
        result[key] = docEntry.value;
      }
    }
    
    return result;
  }

  /// Merges [other] into this document. The result does not depend on the
  /// order or grouping of merges: for each key the entry with the highest
  /// rank wins (lowest for [ConflictResolutionStrategy.firstWriterWins]),
  /// where the rank orders entries by the sum of their vector clock, which
  /// respects causality, then by actor ID, tombstone over value, and finally
  /// the value's canonical JSON. If the two documents use different
  /// strategies, the one listed first in [ConflictResolutionStrategy] is used.
  @override
  DocumentCRDT merge(covariant DocumentCRDT other) {
    final useOther = other._strategy.index < _strategy.index;
    final strategy = useOther ? other._strategy : _strategy;
    final resolver = useOther ? other._customResolver : _customResolver;

    final mergedEntries = Map<String, DocumentEntry>.from(_entries);
    for (final entry in other._entries.entries) {
      final localEntry = mergedEntries[entry.key];
      mergedEntries[entry.key] = localEntry == null
          ? entry.value
          : _resolve(strategy, resolver, entry.key, localEntry, entry.value);
    }

    return DocumentCRDT._(
      entries: mergedEntries,
      clock: _clock.merge(other._clock),
      strategy: strategy,
      customResolver: resolver,
    );
  }

  static DocumentEntry _resolve(
    ConflictResolutionStrategy strategy,
    ConflictResolver? resolver,
    String key,
    DocumentEntry local,
    DocumentEntry remote,
  ) {
    if (strategy == ConflictResolutionStrategy.custom && resolver != null) {
      return resolver(key, local, remote);
    }
    final order = compareEntries(local, remote);
    if (strategy == ConflictResolutionStrategy.firstWriterWins) {
      return order <= 0 ? local : remote;
    }
    return order >= 0 ? local : remote;
  }

  /// The total order used to resolve conflicts. Extends the causal order: an
  /// entry written after seeing another always ranks higher.
  static int compareEntries(DocumentEntry a, DocumentEntry b) {
    var c = a.timestamp.sum.compareTo(b.timestamp.sum);
    if (c != 0) return c;
    c = a.actorId.compareTo(b.actorId);
    if (c != 0) return c;
    if (a.isDeleted != b.isDeleted) return a.isDeleted ? 1 : -1;
    return canonicalJsonEncode(a.toJson()).compareTo(canonicalJsonEncode(b.toJson()));
  }

  @override
  Uint8List toCanonicalBytes() {
    // Sort entries by key for canonical representation
    final sortedEntries = _entries.entries.toList()
      ..sort((a, b) => a.key.compareTo(b.key));
    
    final jsonMap = {
      'entries': Map<String, dynamic>.fromEntries(
        sortedEntries.map((e) => MapEntry(e.key, e.value.toJson())),
      ),
      'clock': _clock.toJson(),
      'strategy': _strategy.name,
    };
    
    return canonicalJsonBytes(jsonMap);
  }

  /// Deserializes a DocumentCRDT from bytes.
  factory DocumentCRDT.fromBytes(Uint8List bytes) {
    final json = jsonDecode(utf8.decode(bytes)) as Map<String, dynamic>;
    return DocumentCRDT.fromJson(json);
  }

  /// Serializes the DocumentCRDT to JSON.
  Map<String, dynamic> toJson() {
    return {
      'entries': _entries.map((k, v) => MapEntry(k, v.toJson())),
      'clock': _clock.toJson(),
      'strategy': _strategy.name,
    };
  }

  /// Deserializes a DocumentCRDT from JSON.
  factory DocumentCRDT.fromJson(Map<String, dynamic> json) {
    final entriesJson = json['entries'] as Map<String, dynamic>;
    final entries = entriesJson.map(
      (k, v) => MapEntry(k, DocumentEntry.fromJson(v as Map<String, dynamic>)),
    );
    
    final clock = VectorClock.fromJson(json['clock'] as Map<String, dynamic>);
    final strategyName = json['strategy'] as String;
    final strategy = ConflictResolutionStrategy.values
        .firstWhere((s) => s.name == strategyName);
    
    return DocumentCRDT._(
      entries: entries,
      clock: clock,
      strategy: strategy,
    );
  }

  /// Returns the current vector clock.
  VectorClock get clock => _clock;

  /// Returns the conflict resolution strategy.
  ConflictResolutionStrategy get strategy => _strategy;

  /// Returns all keys in the document (including deleted ones).
  Set<String> get allKeys => _entries.keys.toSet();

  /// Returns only the keys of non-deleted entries.
  Set<String> get keys => _entries.entries
      .where((e) => !e.value.isDeleted)
      .map((e) => e.key)
      .toSet();

  /// Returns true if the document contains the given key (and it's not deleted).
  bool containsKey(String key) {
    final entry = _entries[key];
    return entry != null && !entry.isDeleted;
  }

  /// Returns true if the document is empty (no non-deleted entries).
  bool get isEmpty => keys.isEmpty;

  /// Returns the number of non-deleted entries.
  int get length => keys.length;

  /// Gets the raw DocumentEntry for a key (including deleted entries).
  DocumentEntry? getEntry(String key) => _entries[key];

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is DocumentCRDT &&
          runtimeType == other.runtimeType &&
          _mapsEqual(_entries, other._entries) &&
          _clock == other._clock &&
          _strategy == other._strategy;

  @override
  int get hashCode {
    var hash = _clock.hashCode ^ _strategy.hashCode;
    for (final entry in _entries.entries) {
      hash ^= entry.key.hashCode ^ entry.value.hashCode;
    }
    return hash;
  }

  @override
  String toString() {
    return 'DocumentCRDT(value: $value, clock: $_clock, strategy: $_strategy)';
  }

  /// Helper method to compare two maps for equality.
  bool _mapsEqual(Map<String, DocumentEntry> map1, Map<String, DocumentEntry> map2) {
    if (map1.length != map2.length) return false;
    
    for (final entry in map1.entries) {
      if (map2[entry.key] != entry.value) return false;
    }
    
    return true;
  }
}
