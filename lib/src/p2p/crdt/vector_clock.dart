import 'dart:convert' show jsonDecode, utf8;
import 'dart:typed_data';

import 'canonical_json.dart';

/// Vector clock implementation for tracking causality in distributed systems.
/// Each actor maintains a logical timestamp that increments with each operation.
class VectorClock {
  final Map<String, int> _clocks;

  VectorClock._(this._clocks);

  /// Creates an empty vector clock.
  factory VectorClock.empty() {
    return VectorClock._({});
  }

  /// Creates a vector clock from a map of actor IDs to timestamps.
  factory VectorClock.fromMap(Map<String, int> clocks) {
    return VectorClock._(Map<String, int>.from(clocks));
  }

  /// Creates a vector clock with a single actor's timestamp.
  factory VectorClock.single(String actorId, int timestamp) {
    return VectorClock._({actorId: timestamp});
  }

  /// Gets the timestamp for a specific actor.
  /// Returns 0 if the actor is not present in the clock.
  int getTime(String actorId) {
    return _clocks[actorId] ?? 0;
  }

  /// Increments the timestamp for the given actor.
  /// Returns a new VectorClock with the updated timestamp.
  VectorClock increment(String actorId) {
    final newClocks = Map<String, int>.from(_clocks);
    newClocks[actorId] = (newClocks[actorId] ?? 0) + 1;
    return VectorClock._(newClocks);
  }

  /// Updates the timestamp for the given actor to the specified value.
  /// Returns a new VectorClock with the updated timestamp.
  VectorClock update(String actorId, int timestamp) {
    final newClocks = Map<String, int>.from(_clocks);
    newClocks[actorId] = timestamp;
    return VectorClock._(newClocks);
  }

  /// Merges this vector clock with another, taking the maximum timestamp
  /// for each actor. This is used during CRDT merge operations.
  VectorClock merge(VectorClock other) {
    final mergedClocks = Map<String, int>.from(_clocks);
    
    for (final entry in other._clocks.entries) {
      final actorId = entry.key;
      final otherTime = entry.value;
      final currentTime = mergedClocks[actorId] ?? 0;
      mergedClocks[actorId] = otherTime > currentTime ? otherTime : currentTime;
    }
    
    return VectorClock._(mergedClocks);
  }

  /// Compares this vector clock with another to determine their causal relationship.
  /// Returns:
  /// - CausalRelation.before: this happened before other
  /// - CausalRelation.after: this happened after other
  /// - CausalRelation.concurrent: events are concurrent (no causal relationship)
  /// - CausalRelation.equal: clocks are identical
  CausalRelation compareTo(VectorClock other) {
    bool thisLessOrEqual = true;
    bool thisGreaterOrEqual = true;
    bool hasAnyDifference = false;

    // Get all actor IDs from both clocks
    final allActors = <String>{..._clocks.keys, ...other._clocks.keys};

    for (final actorId in allActors) {
      final thisTime = getTime(actorId);
      final otherTime = other.getTime(actorId);

      if (thisTime > otherTime) {
        thisLessOrEqual = false;
      }
      if (thisTime < otherTime) {
        thisGreaterOrEqual = false;
      }
      if (thisTime != otherTime) {
        hasAnyDifference = true;
      }
    }

    if (!hasAnyDifference) {
      return CausalRelation.equal;
    } else if (thisLessOrEqual && !thisGreaterOrEqual) {
      // All of this clock's timestamps are <= other's, with at least one <
      return CausalRelation.before;
    } else if (thisGreaterOrEqual && !thisLessOrEqual) {
      // All of this clock's timestamps are >= other's, with at least one >
      return CausalRelation.after;
    } else {
      // Neither dominates the other - concurrent
      return CausalRelation.concurrent;
    }
  }

  /// Returns true if this vector clock happened before the other.
  bool happenedBefore(VectorClock other) {
    return compareTo(other) == CausalRelation.before;
  }

  /// Returns true if this vector clock happened after the other.
  bool happenedAfter(VectorClock other) {
    return compareTo(other) == CausalRelation.after;
  }

  /// Returns true if this vector clock is concurrent with the other.
  bool isConcurrentWith(VectorClock other) {
    return compareTo(other) == CausalRelation.concurrent;
  }

  /// Returns true if this vector clock is equal to the other.
  bool isEqualTo(VectorClock other) {
    return compareTo(other) == CausalRelation.equal;
  }

  /// Returns a copy of the internal clocks map.
  Map<String, int> toMap() {
    return Map<String, int>.from(_clocks);
  }

  /// Serializes the vector clock to JSON.
  Map<String, dynamic> toJson() {
    return {'clocks': _clocks};
  }

  /// Deserializes a vector clock from JSON.
  factory VectorClock.fromJson(Map<String, dynamic> json) {
    final clocks = (json['clocks'] as Map<String, dynamic>).cast<String, int>();
    return VectorClock._(clocks);
  }

  /// Serializes the vector clock to bytes for canonical representation.
  Uint8List toBytes() => canonicalJsonBytes(toJson());

  /// Deserializes a vector clock from bytes.
  factory VectorClock.fromBytes(Uint8List bytes) {
    final json = jsonDecode(utf8.decode(bytes)) as Map<String, dynamic>;
    return VectorClock.fromJson(json);
  }

  /// The sum of all actors' counters. It grows strictly along every causal
  /// chain, so ordering by it (then by a tiebreak) extends the causal order
  /// to a total order.
  int get sum => _clocks.values.fold(0, (a, b) => a + b);

  /// Returns all actor IDs present in this vector clock.
  Set<String> get actors => _clocks.keys.toSet();

  /// Returns true if this vector clock is empty (no actors).
  bool get isEmpty => _clocks.isEmpty;

  /// Returns the number of actors in this vector clock.
  int get length => _clocks.length;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is VectorClock &&
          runtimeType == other.runtimeType &&
          _mapsEqual(_clocks, other._clocks);

  @override
  int get hashCode {
    var hash = 0;
    for (final entry in _clocks.entries) {
      hash ^= entry.key.hashCode ^ entry.value.hashCode;
    }
    return hash;
  }

  @override
  String toString() {
    final sortedEntries = _clocks.entries.toList()
      ..sort((a, b) => a.key.compareTo(b.key));
    
    final clockStrings = sortedEntries
        .map((entry) => '${entry.key}:${entry.value}')
        .join(', ');
    
    return 'VectorClock{$clockStrings}';
  }

  /// Helper method to compare two maps for equality.
  bool _mapsEqual(Map<String, int> map1, Map<String, int> map2) {
    if (map1.length != map2.length) return false;
    
    for (final entry in map1.entries) {
      if (map2[entry.key] != entry.value) return false;
    }
    
    return true;
  }
}

/// Represents the causal relationship between two vector clocks.
enum CausalRelation {
  /// The first clock happened before the second.
  before,
  
  /// The first clock happened after the second.
  after,
  
  /// The clocks are concurrent (no causal relationship).
  concurrent,
  
  /// The clocks are identical.
  equal,
}
