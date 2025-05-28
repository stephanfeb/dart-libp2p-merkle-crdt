import 'dart:typed_data';
import 'dart:convert' show jsonEncode, jsonDecode;
import 'package:test/test.dart';
import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/outbox_crdt.dart';

void main() {
  group('OutboxCrdtPayload Tests', () {
    late OutboxCrdtPayload payload;

    final cid1 = CID.fromString('bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq'); // "a"
    final cid2 = CID.fromString('bafkreidgjalwh3js3n7ot34g3w2pw34b2ihhmswgybylrkv6esqmyV5k3i'); // "b"
    final cid3 = CID.fromString('bafkreic4alt3gdeiubm2pqf5o7t6w7znsj4lb5kafszsw3fujy7smf5lry'); // "c"

    setUp(() {
      payload = OutboxCrdtPayload.create();
    });

    test('Creation results in an empty outbox', () {
      expect(payload.value.activityCids, isEmpty);
    });

    test('Append CIDs', () {
      payload = payload.append(cid1);
      expect(payload.value.activityCids, contains(cid1));
      expect(payload.value.activityCids.length, 1);

      payload = payload.append(cid2);
      expect(payload.value.activityCids, containsAll([cid1, cid2]));
      expect(payload.value.activityCids.length, 2);
    });

    test('Append duplicate CID does not change the set', () {
      payload = payload.append(cid1);
      payload = payload.append(cid1);
      expect(payload.value.activityCids, contains(cid1));
      expect(payload.value.activityCids.length, 1);
    });

    test('Remove CID', () {
      payload = payload.append(cid1);
      payload = payload.append(cid2);
      payload = payload.remove(cid1);
      
      expect(payload.value.activityCids, isNot(contains(cid1)));
      expect(payload.value.activityCids, contains(cid2));
      expect(payload.value.activityCids.length, 1);
    });

    test('Remove non-existent CID does not change the set', () {
      payload = payload.append(cid1);
      payload = payload.remove(cid2); // cid2 not in payload
      expect(payload.value.activityCids, contains(cid1));
      expect(payload.value.activityCids.length, 1);
    });

    test('Value activityCids are sorted by CID string', () {
      payload = payload.append(cid3); // "c"
      payload = payload.append(cid1); // "a"
      payload = payload.append(cid2); // "b"

      final sortedCids = [cid1, cid2, cid3]..sort((a,b) => a.toString().compareTo(b.toString()));
      expect(payload.value.activityCids, orderedEquals(sortedCids));
    });

    test('Merge: disjoint CIDs', () {
      var payload1 = OutboxCrdtPayload.create().append(cid1);
      var payload2 = OutboxCrdtPayload.create().append(cid2);

      final merged = payload1.merge(payload2);
      final expected = [cid1, cid2]..sort((a,b) => a.toString().compareTo(b.toString()));
      expect(merged.value.activityCids, orderedEquals(expected));
    });

    test('Merge: overlapping CIDs', () {
      var payload1 = OutboxCrdtPayload.create().append(cid1).append(cid2);
      var payload2 = OutboxCrdtPayload.create().append(cid2).append(cid3);

      final merged = payload1.merge(payload2);
      final expected = [cid1, cid2, cid3]..sort((a,b) => a.toString().compareTo(b.toString()));
      expect(merged.value.activityCids, orderedEquals(expected));
    });
    
    test('Merge with empty payload', () {
      payload = payload.append(cid1).append(cid2);
      final emptyPayload = OutboxCrdtPayload.create();
      
      final merged1 = payload.merge(emptyPayload);
      expect(merged1.value.activityCids, orderedEquals(payload.value.activityCids));

      final merged2 = emptyPayload.merge(payload);
      expect(merged2.value.activityCids, orderedEquals(payload.value.activityCids));
    });

    test('Serialization and Deserialization (toBytes/fromBytes)', () {
      payload = payload.append(cid3).append(cid1); // c, a
      
      final bytes = payload.toBytes();
      final deserialized = OutboxCrdtPayload.fromBytes(bytes);

      // Expected order after deserialization and value getter is sorted [a, c]
      final expectedCids = [cid1, cid3]..sort((a,b) => a.toString().compareTo(b.toString()));
      expect(deserialized.value.activityCids, orderedEquals(expectedCids));
    });

    test('Serialization and Deserialization (toCanonicalBytes/fromBytes)', () {
      payload = payload.append(cid2).append(cid3).append(cid1); // b, c, a
      
      final bytes = payload.toCanonicalBytes();
      final deserialized = OutboxCrdtPayload.fromBytes(bytes);

      // Expected order after deserialization and value getter is sorted [a, b, c]
      final expectedCids = [cid1, cid2, cid3]..sort((a,b) => a.toString().compareTo(b.toString()));
      expect(deserialized.value.activityCids, orderedEquals(expectedCids));
    });
    
    test('toBytes and toCanonicalBytes produce same output for this CRDT', () {
      payload = payload.append(cid3).append(cid1);
      expect(payload.toBytes(), equals(payload.toCanonicalBytes()));
    });

  });
}
