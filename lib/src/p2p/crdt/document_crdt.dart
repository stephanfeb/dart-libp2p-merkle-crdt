import 'dart:convert' show jsonEncode, jsonDecode;
import 'dart:typed_data';

import 'package:merkledag/merkledag.dart' show CRDTPayload;
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
  
  /// Merge values if possible (for maps and lists).
  merge,
  
  /// Use a custom conflict resolver function.
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
  DocumentCRDT put(String key, dynamic value, {String? actorId}) {
    actorId ??= 'default';
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
  DocumentCRDT delete(String key, {String? actorId}) {
    actorId ??= 'default';
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
  DocumentCRDT patch(Map<String, dynamic> updates, {String? actorId}) {
    actorId ??= 'default';
    var result = this;
    
    for (final entry in updates.entries) {
      result = result.put(entry.key, entry.value, actorId: actorId);
    }
    
    return result;
  }

  /// Performs a deep merge of updates into the document.
  /// For nested maps, merges recursively. For other types, replaces the value.
  DocumentCRDT deepMerge(Map<String, dynamic> updates, {String? actorId}) {
    actorId ??= 'default';
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

  @override
  DocumentCRDT merge(covariant DocumentCRDT other) {
    final mergedEntries = Map<String, DocumentEntry>.from(_entries);
    final mergedClock = _clock.merge(other._clock);
    
    for (final entry in other._entries.entries) {
      final key = entry.key;
      final otherEntry = entry.value;
      final localEntry = mergedEntries[key];
      
      if (localEntry == null) {
        // No local entry, take the remote one
        mergedEntries[key] = otherEntry;
      } else {
        // Resolve conflict between local and remote entries
        mergedEntries[key] = _resolveConflict(key, localEntry, otherEntry);
      }
    }
    
    return DocumentCRDT._(
      entries: mergedEntries,
      clock: mergedClock,
      strategy: _strategy,
      customResolver: _customResolver,
    );
  }

  /// Resolves conflicts between two document entries based on the configured strategy.
  DocumentEntry _resolveConflict(
    String key,
    DocumentEntry local,
    DocumentEntry remote,
  ) {
    switch (_strategy) {
      case ConflictResolutionStrategy.lastWriterWins:
        return _lastWriterWins(local, remote);
      
      case ConflictResolutionStrategy.firstWriterWins:
        return _firstWriterWins(local, remote);
      
      case ConflictResolutionStrategy.merge:
        return _mergeValues(key, local, remote);
      
      case ConflictResolutionStrategy.custom:
        if (_customResolver != null) {
          return _customResolver!(key, local, remote);
        }
        // Fallback to LWW if no custom resolver
        return _lastWriterWins(local, remote);
    }
  }

  /// Last Writer Wins conflict resolution.
  DocumentEntry _lastWriterWins(DocumentEntry local, DocumentEntry remote) {
    final relation = local.timestamp.compareTo(remote.timestamp);
    
    switch (relation) {
      case CausalRelation.before:
        return remote; // Remote is newer
      case CausalRelation.after:
        return local; // Local is newer
      case CausalRelation.concurrent:
      case CausalRelation.equal:
        // Tie-breaking: prefer the entry with lexicographically larger actor ID
        return local.actorId.compareTo(remote.actorId) >= 0 ? local : remote;
    }
  }

  /// First Writer Wins conflict resolution.
  DocumentEntry _firstWriterWins(DocumentEntry local, DocumentEntry remote) {
    final relation = local.timestamp.compareTo(remote.timestamp);
    
    switch (relation) {
      case CausalRelation.before:
        return local; // Local is older
      case CausalRelation.after:
        return remote; // Remote is older
      case CausalRelation.concurrent:
      case CausalRelation.equal:
        // Tie-breaking: prefer the entry with lexicographically smaller actor ID
        return local.actorId.compareTo(remote.actorId) <= 0 ? local : remote;
    }
  }

  /// Merge conflict resolution - attempts to merge values if possible.
  DocumentEntry _mergeValues(String key, DocumentEntry local, DocumentEntry remote) {
    // If either is deleted, use LWW
    if (local.isDeleted || remote.isDeleted) {
      return _lastWriterWins(local, remote);
    }
    
    // If both are maps, try to merge them
    if (local.value is Map<String, dynamic> && 
        remote.value is Map<String, dynamic>) {
      final mergedValue = _deepMergeMap(
        local.value as Map<String, dynamic>,
        remote.value as Map<String, dynamic>,
      );
      
      // Use the later timestamp for the merged entry
      final laterEntry = _lastWriterWins(local, remote);
      return laterEntry.copyWith(value: mergedValue);
    }
    
    // If both are lists, concatenate them (removing duplicates)
    if (local.value is List && remote.value is List) {
      final localList = local.value as List;
      final remoteList = remote.value as List;
      final mergedList = [...localList, ...remoteList].toSet().toList();
      
      final laterEntry = _lastWriterWins(local, remote);
      return laterEntry.copyWith(value: mergedList);
    }
    
    // For other types, fall back to LWW
    return _lastWriterWins(local, remote);
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
    
    return Uint8List.fromList(jsonEncode(jsonMap).codeUnits);
  }

  /// Deserializes a DocumentCRDT from bytes.
  factory DocumentCRDT.fromBytes(Uint8List bytes) {
    final json = jsonDecode(String.fromCharCodes(bytes)) as Map<String, dynamic>;
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
