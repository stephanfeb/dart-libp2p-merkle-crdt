import 'dart:convert';
import 'dart:typed_data';

import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/following_crdt.dart';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart'; // For checking tag format if needed

void main() {
  group('FollowingCrdtPayload Tests', () {
    const did1 = 'did:key:zFollower1';
    const did2 = 'did:key:zFollower2';
    const did3 = 'did:key:zFollower3';

    test('create() initializes an empty payload with empty value set', () {
      final payload = FollowingCrdtPayload.create();
      expect(payload.value, isEmpty);
    });

    test('add() adds a DID to the value set', () {
      var payload = FollowingCrdtPayload.create();
      payload = payload.add(did1);
      expect(payload.value, equals({did1}));
      payload = payload.add(did2);
      expect(payload.value, equals({did1, did2}));
    });

    test('add() is idempotent for the value set', () {
      var payload = FollowingCrdtPayload.create();
      payload = payload.add(did1);
      payload = payload.add(did1); // Add same DID again
      expect(payload.value, equals({did1}));
      expect(payload.value.length, 1);
      // Internally, _adds set will have multiple tagged entries for did1
      // but the resolved value set should only contain did1 once.
    });

    test('remove() removes a DID from the value set', () {
      var payload = FollowingCrdtPayload.create();
      payload = payload.add(did1);
      payload = payload.add(did2);
      payload = payload.remove(did1);
      expect(payload.value, equals({did2}));
    });

    test('remove() on a non-existent DID has no effect on value set', () {
      var payload = FollowingCrdtPayload.create();
      payload = payload.add(did1);
      payload = payload.remove(did2); // did2 was never added
      expect(payload.value, equals({did1}));
    });

    test('remove() after multiple adds of the same DID removes it completely', () {
      var payload = FollowingCrdtPayload.create();
      payload = payload.add(did1);
      payload = payload.add(did1); // Add same DID again (different tags)
      payload = payload.remove(did1);
      expect(payload.value, isEmpty);
    });

    test('merge() combines adds and removes from two payloads', () {
      var p1 = FollowingCrdtPayload.create();
      p1 = p1.add(did1); // p1: adds(d1_t1), removes() -> value {d1}
      p1 = p1.add(did2); // p1: adds(d1_t1, d2_t2), removes() -> value {d1, d2}

      var p2 = FollowingCrdtPayload.create();
      p2 = p2.add(did2); // p2: adds(d2_t3), removes() -> value {d2}
      p2 = p2.add(did3); // p2: adds(d2_t3, d3_t4), removes() -> value {d2, d3}
      p2 = p2.remove(did1); // p2: adds(d2_t3, d3_t4), removes(d1_t?) - no d1 in p2._adds, so no effect on p2.value

      // Merge p1 into p2
      // Expected: adds from p1 and p2, removes from p1 and p2
      // p1._adds has d1_tA, d2_tB. p1._removes is empty.
      // p2._adds has d2_tC, d3_tD. p2._removes is empty (remove(did1) had no effect as d1 wasn't in p2._adds)
      // merged._adds = {d1_tA, d2_tB, d2_tC, d3_tD}
      // merged._removes = {}
      // value = {d1, d2, d3}
      var merged = p1.merge(p2);
      expect(merged.value, equals({did1, did2, did3}));

      // Now, let's test a merge where one has a removal for an item in the other's add set
      var p3 = FollowingCrdtPayload.create().add(did1); // p3 adds d1_tE
      var p4 = FollowingCrdtPayload.create().add(did1).remove(did1); // p4 adds d1_tF, then removes d1_tF

      // p3._adds = {d1_tE}, p3._removes = {}
      // p4._adds = {d1_tF}, p4._removes = {d1_tF}
      // merged2._adds = {d1_tE, d1_tF}
      // merged2._removes = {d1_tF}
      // value should be {d1} (because d1_tE is in adds and not in removes)
      var merged2 = p3.merge(p4);
      expect(merged2.value, equals({did1}));


      var p5 = FollowingCrdtPayload.create().add(did1).add(did2); // p5: adds(d1_tG, d2_tH)
      var p6 = FollowingCrdtPayload.create().add(did1).remove(did2); // p6: adds(d1_tI), removes(d2_tH_if_d2_was_added_by_p6_with_tag_tH)
                                                                    // Actually, p6.remove(did2) will try to remove entries for did2 from p6._adds.
                                                                    // Since did2 was not added to p6, p6._removes will be empty regarding did2.
                                                                    // Let's re-evaluate p6:
      p6 = FollowingCrdtPayload.create().add(did1); // p6: adds(d1_tI)
      var p6_intermediate_add_d2 = p6.add(did2); // p6_intermediate_add_d2: adds(d1_tI, d2_tJ)
      p6 = p6_intermediate_add_d2.remove(did2);  // p6: adds(d1_tI, d2_tJ), removes(d2_tJ) -> value {d1}

      // p5._adds = {d1_tG, d2_tH}, p5._removes = {}
      // p6._adds = {d1_tI, d2_tJ}, p6._removes = {d2_tJ}
      // merged3._adds = {d1_tG, d2_tH, d1_tI, d2_tJ}
      // merged3._removes = {d2_tJ}
      // value should be {d1, d2} (d1_tG, d1_tI, d2_tH are in adds and not in removes)
      var merged3 = p5.merge(p6);
      expect(merged3.value, equals({did1, did2}));
    });

    test('toCanonicalBytes() and fromBytes() round trip', () {
      var payload = FollowingCrdtPayload.create();
      payload = payload.add(did1);
      payload = payload.add(did2);
      payload = payload.remove(did1); // Value is {did2}

      final bytes = payload.toCanonicalBytes();
      final reconstructed = FollowingCrdtPayload.fromBytes(bytes);

      expect(reconstructed.value, equals({did2}));
      // Also check internal sets if possible/necessary, though value is primary
      expect(reconstructed, equals(payload));
    });

    test('toCanonicalBytes() produces consistent output for same logical state', () {
      var p1 = FollowingCrdtPayload.create().add(did1).add(did2);
      var p2 = FollowingCrdtPayload.create().add(did2).add(did1); // Different order of operations

      // Value should be the same
      expect(p1.value, equals(p2.value));
      // Canonical bytes should be the same because _TaggedEntry tags will be different,
      // but the sorting in toCanonicalBytes should make the output consistent
      // if the set of (value,tag) pairs is the same regardless of add order.
      // This is tricky because tags are unique per add operation.
      // The test for canonical bytes should ensure that if two payloads *are identical internally*,
      // their canonical bytes are identical.
      // If they have the same *value* but different internal histories (tags),
      // their canonical bytes will likely differ, which is correct for CRDTs.

      // Test for identical internal state:
      var p3 = FollowingCrdtPayload.create().add(did1);
      final p3Bytes = p3.toCanonicalBytes();
      // Reconstruct and check
      var p3Reconstructed = FollowingCrdtPayload.fromBytes(p3.toCanonicalBytes());
      expect(p3Reconstructed.toCanonicalBytes(), equals(p3Bytes));


      // Test for different internal states but same value
      var p4_v1 = FollowingCrdtPayload.create().add(did1); // (did1, tagA)
      var p4_v2 = FollowingCrdtPayload.create().add(did1); // (did1, tagB)
      expect(p4_v1.value, equals(p4_v2.value)); // Both {did1}
      // Their canonical bytes should be different because tags are different
      expect(p4_v1.toCanonicalBytes(), isNot(equals(p4_v2.toCanonicalBytes())));
    });
    
    test('equality operator works correctly', () {
      var p1 = FollowingCrdtPayload.create().add(did1);
      var p2 = FollowingCrdtPayload.create().add(did1); // Different tags, so not equal
      var p3 = FollowingCrdtPayload.create().add(did1);
      // Manually create p4 to be identical to p3 for testing equality
      final p3Bytes = p3.toCanonicalBytes();
      var p4 = FollowingCrdtPayload.fromBytes(p3Bytes);


      expect(p1 == p2, isFalse); // Different internal tags
      expect(p3 == p4, isTrue);  // Identical internal state after reconstruction
      expect(p1.hashCode == p2.hashCode, isFalse);
      expect(p3.hashCode == p4.hashCode, isTrue);

      var p5 = p1.add(did2);
      expect(p1 == p5, isFalse);
    });

    test('toString() returns a non-empty string containing the value', () {
      var payload = FollowingCrdtPayload.create().add(did1);
      final str = payload.toString();
      expect(str, isA<String>());
      expect(str, isNotEmpty);
      expect(str, contains('FollowingCrdtPayload'));
      expect(str, contains(did1));
    });

  });
}
