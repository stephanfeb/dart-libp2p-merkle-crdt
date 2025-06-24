import 'dart:convert';
import 'dart:typed_data';

import 'package:dart_cid/dart_cid.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/liked_crdt.dart';
import 'package:test/test.dart';

void main() {
  group('LikedCrdtPayload Tests', () {
    final cid1 = CID.fromString('bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi');
    final cid2 = CID.fromString('bafybeih3ryd464z3wfwvdns2qflqxlpmsoqgijdj4acpyebrzshnsgxmf4');
    final cid3 = CID.fromString('bafybeibzzoneqmyqflqo7d7k7yvj4vhyqgvayfdtk3fcrqjpvgnorgqbs4');

    test('create() initializes an empty payload with empty value set', () {
      final payload = LikedCrdtPayload.create();
      expect(payload.value, isEmpty);
    });

    test('add() adds a CID to the value set', () {
      var payload = LikedCrdtPayload.create();
      payload = payload.add(cid1);
      expect(payload.value.map((c) => c.toString()), equals({cid1.toString()}));
      payload = payload.add(cid2);
      expect(payload.value.map((c) => c.toString()), equals({cid1.toString(), cid2.toString()}));
    });

    test('add() is idempotent for the value set', () {
      var payload = LikedCrdtPayload.create();
      payload = payload.add(cid1);
      payload = payload.add(cid1); // Add same CID again
      expect(payload.value.map((c) => c.toString()), equals({cid1.toString()}));
      expect(payload.value.length, 1);
    });

    test('remove() removes a CID from the value set', () {
      var payload = LikedCrdtPayload.create();
      payload = payload.add(cid1);
      payload = payload.add(cid2);
      payload = payload.remove(cid1);
      expect(payload.value.map((c) => c.toString()), equals({cid2.toString()}));
    });

    test('remove() on a non-existent CID has no effect on value set', () {
      var payload = LikedCrdtPayload.create();
      payload = payload.add(cid1);
      payload = payload.remove(cid2); // cid2 was never added
      expect(payload.value.map((c) => c.toString()), equals({cid1.toString()}));
    });

    test('remove() after multiple adds of the same CID removes it completely', () {
      var payload = LikedCrdtPayload.create();
      payload = payload.add(cid1);
      payload = payload.add(cid1); 
      payload = payload.remove(cid1);
      expect(payload.value, isEmpty);
    });

    test('merge() combines adds and removes from two payloads', () {
      var p1 = LikedCrdtPayload.create().add(cid1).add(cid2);
      var p2 = LikedCrdtPayload.create().add(cid2).add(cid3);
      var merged = p1.merge(p2);
      expect(merged.value.map((c) => c.toString()), equals({cid1.toString(), cid2.toString(), cid3.toString()}));

      var p3 = LikedCrdtPayload.create().add(cid1);
      var p4 = LikedCrdtPayload.create().add(cid1).remove(cid1);
      var merged2 = p3.merge(p4);
      expect(merged2.value.map((c) => c.toString()), equals({cid1.toString()}));
      
      var p5 = LikedCrdtPayload.create().add(cid1).add(cid2);
      var p6_setup = LikedCrdtPayload.create().add(cid1);
      var p6_add_cid2 = p6_setup.add(cid2);
      var p6 = p6_add_cid2.remove(cid2); // p6 value is {cid1}
      
      var merged3 = p5.merge(p6);
      // p5 value: {cid1, cid2}
      // p6 value: {cid1}
      // merged3 value should be {cid1, cid2}
      expect(merged3.value.map((c) => c.toString()), equals({cid1.toString(), cid2.toString()}));
    });

    test('toCanonicalBytes() and fromBytes() round trip', () {
      var payload = LikedCrdtPayload.create();
      payload = payload.add(cid1);
      payload = payload.add(cid2);
      payload = payload.remove(cid1); // Value is {cid2}

      final bytes = payload.toCanonicalBytes();
      final reconstructed = LikedCrdtPayload.fromBytes(bytes);

      expect(reconstructed.value.map((c) => c.toString()), equals({cid2.toString()}));
      expect(reconstructed, equals(payload));
    });
    
    test('toCanonicalBytes() produces consistent output for identical internal state', () {
      var p1 = LikedCrdtPayload.create().add(cid1);
      final p1Bytes = p1.toCanonicalBytes();
      var p1Reconstructed = LikedCrdtPayload.fromBytes(p1.toCanonicalBytes());
      expect(p1Reconstructed.toCanonicalBytes(), equals(p1Bytes));
    });

    test('toCanonicalBytes() differs for different internal states with same value', () {
      var p_v1 = LikedCrdtPayload.create().add(cid1); // (cid1, tagA)
      var p_v2 = LikedCrdtPayload.create().add(cid1); // (cid1, tagB)
      expect(p_v1.value.map((c)=>c.toString()), equals(p_v2.value.map((c)=>c.toString()))); // Both {cid1}
      expect(p_v1.toCanonicalBytes(), isNot(equals(p_v2.toCanonicalBytes())));
    });
    
    test('equality operator works correctly', () {
      var p1 = LikedCrdtPayload.create().add(cid1);
      var p2 = LikedCrdtPayload.create().add(cid1); // Different tags
      var p3 = LikedCrdtPayload.create().add(cid1);
      final p3Bytes = p3.toCanonicalBytes(); // Serialize p3
      var p4 = LikedCrdtPayload.fromBytes(p3Bytes); // Reconstruct to get identical p4


      expect(p1 == p2, isFalse); 
      expect(p3 == p4, isTrue);  
      expect(p1.hashCode == p2.hashCode, isFalse);
      expect(p3.hashCode == p4.hashCode, isTrue);

      var p5 = p1.add(cid2);
      expect(p1 == p5, isFalse);
    });

    test('toString() returns a non-empty string containing the value', () {
      var payload = LikedCrdtPayload.create().add(cid1);
      final str = payload.toString();
      expect(str, isA<String>());
      expect(str, isNotEmpty);
      expect(str, contains('LikedCrdtPayload'));
      expect(str, contains(cid1.toString()));
    });
  });
}
