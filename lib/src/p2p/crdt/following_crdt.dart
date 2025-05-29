import 'dart:convert' show jsonEncode, jsonDecode;
import 'dart:typed_data';

import 'package:collection/collection.dart'; // For SetEquality
import 'package:merkledag/merkledag.dart' show CRDTPayload;
import 'package:uuid/uuid.dart';

const _uuid = Uuid();

class _TaggedEntry {
  final String value; // The DID string
  final String tag;   // Unique identifier (UUID) for this specific addition

  _TaggedEntry(this.value, this.tag);

  _TaggedEntry.generate(this.value) : tag = _uuid.v4();

  Map<String, dynamic> toJson() => {
        'value': value,
        'tag': tag,
      };

  factory _TaggedEntry.fromJson(Map<String, dynamic> json) {
    return _TaggedEntry(
      json['value'] as String,
      json['tag'] as String,
    );
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is _TaggedEntry &&
          runtimeType == other.runtimeType &&
          value == other.value &&
          tag == other.tag;

  @override
  int get hashCode => value.hashCode ^ tag.hashCode;

  @override
  String toString() => 'TaggedEntry(value: $value, tag: $tag)';
}

/// CRDT payload for managing a set of followed DIDs (OR-Set).
class FollowingCrdtPayload implements CRDTPayload<Set<String>> {
  final Set<_TaggedEntry> _adds;
  final Set<_TaggedEntry> _removes;

  FollowingCrdtPayload._(this._adds, this._removes);

  factory FollowingCrdtPayload.create() {
    return FollowingCrdtPayload._(const {}, const {});
  }

  @override
  Set<String> get value {
    final resultSet = <String>{};
    for (final addEntry in _adds) {
      if (!_removes.contains(addEntry)) {
        resultSet.add(addEntry.value);
      }
    }
    return resultSet;
  }

  FollowingCrdtPayload add(String did) {
    final newEntry = _TaggedEntry.generate(did);
    final newAdds = Set<_TaggedEntry>.from(_adds)..add(newEntry);
    return FollowingCrdtPayload._(newAdds, _removes);
  }

  FollowingCrdtPayload remove(String did) {
    final entriesToRemove =
        _adds.where((entry) => entry.value == did).toSet();
    if (entriesToRemove.isEmpty) {
      return this; // No change if the DID wasn't in the add set
    }
    final newRemoves = Set<_TaggedEntry>.from(_removes)
      ..addAll(entriesToRemove);
    return FollowingCrdtPayload._(_adds, newRemoves);
  }

  @override
  FollowingCrdtPayload merge(covariant FollowingCrdtPayload other) {
    final mergedAdds = _adds.union(other._adds);
    final mergedRemoves = _removes.union(other._removes);
    return FollowingCrdtPayload._(mergedAdds, mergedRemoves);
  }

  @override
  Uint8List toBytes() {
    final jsonMap = {
      'adds': _adds.map((e) => e.toJson()).toList(),
      'removes': _removes.map((e) => e.toJson()).toList(),
    };
    return Uint8List.fromList(jsonEncode(jsonMap).codeUnits);
  }

  factory FollowingCrdtPayload.fromBytes(Uint8List bytes) {
    final jsonMap = jsonDecode(String.fromCharCodes(bytes)) as Map<String, dynamic>;
    final addsList = jsonMap['adds'] as List<dynamic>;
    final removesList = jsonMap['removes'] as List<dynamic>;

    final adds = addsList
        .map((item) => _TaggedEntry.fromJson(item as Map<String, dynamic>))
        .toSet();
    final removes = removesList
        .map((item) => _TaggedEntry.fromJson(item as Map<String, dynamic>))
        .toSet();
    return FollowingCrdtPayload._(adds, removes);
  }

  @override
  Uint8List toCanonicalBytes() {
    // Sort entries by tag, then by value for canonical representation
    List<_TaggedEntry> sortEntries(Set<_TaggedEntry> entries) {
      final list = entries.toList();
      list.sort((a, b) {
        final tagCompare = a.tag.compareTo(b.tag);
        if (tagCompare != 0) return tagCompare;
        return a.value.compareTo(b.value);
      });
      return list;
    }

    final jsonMap = {
      'adds': sortEntries(_adds).map((e) => e.toJson()).toList(),
      'removes': sortEntries(_removes).map((e) => e.toJson()).toList(),
    };
    return Uint8List.fromList(jsonEncode(jsonMap).codeUnits);
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is FollowingCrdtPayload &&
          runtimeType == other.runtimeType &&
          const SetEquality().equals(_adds, other._adds) &&
          const SetEquality().equals(_removes, other._removes);

  @override
  int get hashCode =>
      const SetEquality().hash(_adds) ^ const SetEquality().hash(_removes);

  @override
  String toString() {
    return 'FollowingCrdtPayload(value: ${value.toList()})';
  }
}
