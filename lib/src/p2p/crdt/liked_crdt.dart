import 'dart:convert' show jsonEncode, jsonDecode;
import 'dart:typed_data';

import 'package:collection/collection.dart'; // For SetEquality
import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:merkledag/merkledag.dart' show CRDTPayload;
import 'package:uuid/uuid.dart';

const _uuid = Uuid();

// _TaggedEntry stores a CID string and a unique tag.
class _TaggedCidEntry {
  final String value; // The CID string
  final String tag;   // Unique identifier (UUID) for this specific addition

  _TaggedCidEntry(this.value, this.tag);

  _TaggedCidEntry.generate(CID cid) 
    : value = cid.toString(),
      tag = _uuid.v4();

  Map<String, dynamic> toJson() => {
        'value': value, // Store CID as string
        'tag': tag,
      };

  factory _TaggedCidEntry.fromJson(Map<String, dynamic> json) {
    return _TaggedCidEntry(
      json['value'] as String,
      json['tag'] as String,
    );
  }

  CID get cid => CID.fromString(value);

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is _TaggedCidEntry &&
          runtimeType == other.runtimeType &&
          value == other.value &&
          tag == other.tag;

  @override
  int get hashCode => value.hashCode ^ tag.hashCode;

  @override
  String toString() => 'TaggedCidEntry(value: $value, tag: $tag)';
}

/// CRDT payload for managing a set of liked content CIDs (OR-Set).
class LikedCrdtPayload implements CRDTPayload<Set<CID>> {
  final Set<_TaggedCidEntry> _adds;
  final Set<_TaggedCidEntry> _removes;

  LikedCrdtPayload._(this._adds, this._removes);

  factory LikedCrdtPayload.create() {
    return LikedCrdtPayload._(const {}, const {});
  }

  @override
  Set<CID> get value {
    final resultSet = <CID>{};
    for (final addEntry in _adds) {
      if (!_removes.contains(addEntry)) {
        resultSet.add(addEntry.cid);
      }
    }
    return resultSet;
  }

  LikedCrdtPayload add(CID cid) {
    final newEntry = _TaggedCidEntry.generate(cid);
    final newAdds = Set<_TaggedCidEntry>.from(_adds)..add(newEntry);
    return LikedCrdtPayload._(newAdds, _removes);
  }

  LikedCrdtPayload remove(CID cid) {
    final cidString = cid.toString();
    final entriesToRemove =
        _adds.where((entry) => entry.value == cidString).toSet();
    if (entriesToRemove.isEmpty) {
      return this;
    }
    final newRemoves = Set<_TaggedCidEntry>.from(_removes)
      ..addAll(entriesToRemove);
    return LikedCrdtPayload._(_adds, newRemoves);
  }

  @override
  LikedCrdtPayload merge(covariant LikedCrdtPayload other) {
    final mergedAdds = _adds.union(other._adds);
    final mergedRemoves = _removes.union(other._removes);
    return LikedCrdtPayload._(mergedAdds, mergedRemoves);
  }

  @override
  Uint8List toBytes() {
    final jsonMap = {
      'adds': _adds.map((e) => e.toJson()).toList(),
      'removes': _removes.map((e) => e.toJson()).toList(),
    };
    return Uint8List.fromList(jsonEncode(jsonMap).codeUnits);
  }

  factory LikedCrdtPayload.fromBytes(Uint8List bytes) {
    final jsonMap = jsonDecode(String.fromCharCodes(bytes)) as Map<String, dynamic>;
    final addsList = jsonMap['adds'] as List<dynamic>;
    final removesList = jsonMap['removes'] as List<dynamic>;

    final adds = addsList
        .map((item) => _TaggedCidEntry.fromJson(item as Map<String, dynamic>))
        .toSet();
    final removes = removesList
        .map((item) => _TaggedCidEntry.fromJson(item as Map<String, dynamic>))
        .toSet();
    return LikedCrdtPayload._(adds, removes);
  }

  @override
  Uint8List toCanonicalBytes() {
    List<_TaggedCidEntry> sortEntries(Set<_TaggedCidEntry> entries) {
      final list = entries.toList();
      list.sort((a, b) {
        final tagCompare = a.tag.compareTo(b.tag);
        if (tagCompare != 0) return tagCompare;
        return a.value.compareTo(b.value); // Compare CID strings
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
      other is LikedCrdtPayload &&
          runtimeType == other.runtimeType &&
          const SetEquality().equals(_adds, other._adds) &&
          const SetEquality().equals(_removes, other._removes);

  @override
  int get hashCode =>
      const SetEquality().hash(_adds) ^ const SetEquality().hash(_removes);

  @override
  String toString() {
    return 'LikedCrdtPayload(value: ${value.map((c) => c.toString()).toList()})';
  }
}
