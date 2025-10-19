import 'dart:typed_data';
import 'dart:convert' show jsonEncode, jsonDecode;

import 'package:crypto/crypto.dart' as crypto;
import 'package:dart_multihash/dart_multihash.dart';
import 'package:merkledag/merkledag.dart' show CRDTPayload;
import 'package:dcid/dcid.dart' show CID;

// OutboxCrdtPayload implements CRDTPayload<List<CID>>
// This is a simplified append-only log (semantically a set of CIDs for now).
// Merging combines sets of CID strings.
// The `value` getter sorts them for a consistent view.
class OutboxCrdtPayload implements CRDTPayload<List<CID>> {
  // Internally, we store CIDs as strings to make them easily JSON serializable
  // and to simplify sorting for canonical representation.
  final Set<String> _activityCidStrings;

  OutboxCrdtPayload._(this._activityCidStrings);

  factory OutboxCrdtPayload.create() {
    return OutboxCrdtPayload._({});
  }



  @override
  List<CID> get value {
    final cids = _activityCidStrings.map((s) => CID.fromString(s)).toList();
    // Sort by CID string to ensure a consistent order for the view model.
    // This order might not reflect the actual append order if CIDs don't have inherent time.
    cids.sort((a, b) => a.toString().compareTo(b.toString()));
    return cids;
  }

  OutboxCrdtPayload add(CID activityCid) { // Renamed from append to add for consistency with set-like behavior
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
