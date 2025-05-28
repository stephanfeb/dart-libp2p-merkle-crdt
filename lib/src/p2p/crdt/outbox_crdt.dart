import 'dart:typed_data';
import 'dart:convert' show jsonEncode, jsonDecode;

import 'package:merkledag/merkledag.dart' show CRDTPayload;
import 'package:dart_cid/dart_cid.dart' show CID;

// Represents the resolved, viewable outbox data
class OutboxData {
  final List<CID> activityCids;

  OutboxData({required this.activityCids});

  @override
  String toString() {
    return 'OutboxData(activityCids: [${activityCids.map((c) => c.toString()).join(', ')}])';
  }
}

// OutboxCrdtPayload implements CRDTPayload<OutboxData>
// This is a simplified append-only log. Merging combines lists and sorts by CID string for canonical order.
// A more sophisticated implementation might use a proper sequence CRDT like RGA.
class OutboxCrdtPayload implements CRDTPayload<OutboxData> {
  // Internally, we store CIDs as strings to make them easily JSON serializable
  // and to simplify sorting for canonical representation.
  final Set<String> _activityCidStrings;

  OutboxCrdtPayload._(this._activityCidStrings);

  factory OutboxCrdtPayload.create() {
    return OutboxCrdtPayload._({});
  }

  @override
  OutboxData get value {
    final cids = _activityCidStrings.map((s) => CID.fromString(s)).toList();
    // Sort by CID string to ensure a consistent order for the view model.
    // This order might not reflect the actual append order if CIDs don't have inherent time.
    // For true append order, a more complex CRDT or use of Merkle-clock links is needed.
    cids.sort((a, b) => a.toString().compareTo(b.toString()));
    return OutboxData(activityCids: cids);
  }

  OutboxCrdtPayload append(CID activityCid) {
    final newCidStrings = Set<String>.from(_activityCidStrings);
    newCidStrings.add(activityCid.toString());
    return OutboxCrdtPayload._(newCidStrings);
  }

  // This 'remove' is for a set-like behavior. If true append-only log semantics
  // are needed where items cannot be removed, this method would not exist.
  // For now, allowing removal from the set of CIDs.
  OutboxCrdtPayload remove(CID activityCid) {
    final newCidStrings = Set<String>.from(_activityCidStrings);
    newCidStrings.remove(activityCid.toString());
    return OutboxCrdtPayload._(newCidStrings);
  }

  @override
  OutboxCrdtPayload merge(covariant OutboxCrdtPayload other) {
    final mergedCidStrings = Set<String>.from(_activityCidStrings);
    mergedCidStrings.addAll(other._activityCidStrings);
    return OutboxCrdtPayload._(mergedCidStrings);
  }

  @override
  Uint8List toBytes() {
    // Convert set to list and sort for a somewhat canonical representation in JSON
    final sortedList = _activityCidStrings.toList()..sort();
    return Uint8List.fromList(jsonEncode(sortedList).codeUnits);
  }

  @override
  Uint8List toCanonicalBytes() {
    // For canonical bytes, ensure the list of CID strings is sorted.
    final sortedList = _activityCidStrings.toList()..sort();
    return Uint8List.fromList(jsonEncode(sortedList).codeUnits);
  }

  factory OutboxCrdtPayload.fromBytes(Uint8List bytes) {
    final list = jsonDecode(String.fromCharCodes(bytes)) as List<dynamic>;
    final cidStrings = list.map((s) => s as String).toSet();
    return OutboxCrdtPayload._(cidStrings);
  }
}
