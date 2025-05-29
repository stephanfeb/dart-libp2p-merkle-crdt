import 'dart:typed_data';
import 'dart:convert' show jsonEncode, jsonDecode;
import 'package:test/test.dart';
import 'package:dart_cid/dart_cid.dart' show CID; // Added import for CID

import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/outbox_crdt.dart';

void main() {
  group('OutboxCrdtPayload', () {
    final cid1 = CID.fromString('bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi');
    final cid2 = CID.fromString('bafybeih3ryd464z3wfwvdns2qflqxlpmsoqgijdj4acpyebrzshnsgxmf4');
    final cid3 = CID.fromString('bafybeibzzoneqmyqflqo7d7k7yvj4vhyqgvayfdtk3fcrqjpvgnorgqbs4');

    List<String> toSortedStringList(List<CID> cids) {
      return cids.map((c) => c.toString()).toList()..sort();
    }

    test('create initializes an empty outbox', () {
      final payload = OutboxCrdtPayload.create();
      expect(payload.value, isEmpty);
    });

    test('add adds CIDs to the outbox', () {
      var payload = OutboxCrdtPayload.create();
      payload = payload.add(cid1);
      expect(toSortedStringList(payload.value), equals(toSortedStringList([cid1])));
      payload = payload.add(cid2);
      expect(toSortedStringList(payload.value), equals(toSortedStringList([cid1, cid2])));
    });

    test('add is idempotent for the same CID in terms of set semantics', () {
      var payload = OutboxCrdtPayload.create();
      payload = payload.add(cid1);
      payload = payload.add(cid1); // Add same CID
      expect(payload.value.length, 1);
      expect(toSortedStringList(payload.value), equals(toSortedStringList([cid1])));
    });

    test('remove CIDs from the outbox', () {
      var payload = OutboxCrdtPayload.create();
      payload = payload.add(cid1);
      payload = payload.add(cid2);
      payload = payload.remove(cid1);
      expect(payload.value, isNot(contains(cid1)));
      expect(payload.value, contains(cid2));
      expect(payload.value.length, 1);
    });

    test('remove on a non-existent CID has no effect', () {
      var payload = OutboxCrdtPayload.create();
      payload = payload.add(cid1);
      final initialValue = List<CID>.from(payload.value);
      payload = payload.remove(cid2); // cid2 was never added
      expect(payload.value, equals(initialValue));
    });

    test('merge combines CIDs from two outboxes, duplicates are handled by Set', () {
      var p1 = OutboxCrdtPayload.create();
      p1 = p1.add(cid1);
      p1 = p1.add(cid2);

      var p2 = OutboxCrdtPayload.create();
      p2 = p2.add(cid2); // cid2 is common
      p2 = p2.add(cid3);

      final merged = p1.merge(p2);
      expect(toSortedStringList(merged.value), equals(toSortedStringList([cid1, cid2, cid3])));
      expect(merged.value.length, 3);
    });

    test('merge with one empty outbox results in the other outbox', () {
      var p1 = OutboxCrdtPayload.create();
      p1 = p1.add(cid1);
      p1 = p1.add(cid2);

      var p2 = OutboxCrdtPayload.create(); // Empty

      final merged1 = p1.merge(p2);
      final merged2 = p2.merge(p1);
      
      expect(toSortedStringList(merged1.value), equals(toSortedStringList([cid1, cid2])));
      expect(toSortedStringList(merged2.value), equals(toSortedStringList([cid1, cid2])));
    });

    test('toBytes and fromBytes round trip', () {
      var payload = OutboxCrdtPayload.create();
      payload = payload.add(cid1);
      payload = payload.add(cid3); 
      payload = payload.add(cid2);

      final bytes = payload.toBytes();
      final reconstructed = OutboxCrdtPayload.fromBytes(bytes);
      
      expect(toSortedStringList(reconstructed.value), toSortedStringList(payload.value));
    });

    test('toCanonicalBytes produces consistent output', () {
      var p1 = OutboxCrdtPayload.create();
      p1 = p1.add(cid1);
      p1 = p1.add(cid2);

      var p2 = OutboxCrdtPayload.create();
      p2 = p2.add(cid2); 
      p2 = p2.add(cid1);

      expect(p1.toCanonicalBytes(), equals(p2.toCanonicalBytes()));
      
      var p3 = OutboxCrdtPayload.create().add(cid3);
      expect(p1.toCanonicalBytes(), isNot(equals(p3.toCanonicalBytes())));
    });

    test('toString returns a non-empty string', () {
      var payload = OutboxCrdtPayload.create().add(cid1);
      expect(payload.toString(), isA<String>());
      expect(payload.toString(), isNotEmpty);
    });
  });
}
