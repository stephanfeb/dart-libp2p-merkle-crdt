import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:dart_libp2p_merkle_crdt/src/p2p/crdt/profile_crdt.dart';

void main() {
  group('ProfileCrdtPayload Tests', () {
    const testDid = 'did:key:zTestDID123';
    const testPublicKeyJwk = {'kty': 'OKP', 'crv': 'Ed25519', 'x': 'testPublicKey'};

    late ProfileCrdtPayload payload;

    setUp(() {
      payload = ProfileCrdtPayload.create(testDid, testPublicKeyJwk);
    });

    test('Creation sets DID, PublicKeyJwk, and default type', () {
      final profileData = payload.value;
      expect(profileData.id, equals(testDid));
      expect(profileData.publicKeyJwk, equals(testPublicKeyJwk));
      expect(profileData.type, equals('Person')); // Default type
      expect(profileData.name, isNull);
      expect(profileData.preferredUsername, isNull);
      expect(profileData.summary, isNull);
      expect(profileData.icon, isNull);
      expect(profileData.image, isNull);
    });

    test('Set name', () {
      payload = payload.set('name', 'Alice');
      expect(payload.value.name, equals('Alice'));
    });

    test('Set preferredUsername', () {
      payload = payload.set('preferredUsername', 'alice_user');
      expect(payload.value.preferredUsername, equals('alice_user'));
    });

    test('Set summary', () {
      payload = payload.set('summary', 'A curious person.');
      expect(payload.value.summary, equals('A curious person.'));
    });

    test('Set icon CID', () {
      final iconCid = CID.fromString('bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq');
      payload = payload.set('icon', iconCid);
      expect(payload.value.icon, equals(iconCid));
    });

    test('Set image CID', () {
      final imageCid = CID.fromString('bafkreidgjalwh3js3n7ot34g3w2pw34b2ihhmswgybylrkv6esqmyV5k3i');
      payload = payload.set('image', imageCid);
      expect(payload.value.image, equals(imageCid));
    });

    test('Set type', () {
      payload = payload.set('type', 'Service');
      expect(payload.value.type, equals('Service'));
    });

    test('Set field to null', () {
      payload = payload.set('name', 'Alice');
      expect(payload.value.name, equals('Alice'));
      payload = payload.set('name', null);
      expect(payload.value.name, isNull);
    });

    test('LWW: later set wins', () async {
      payload = payload.set('name', 'Alice');
      await Future.delayed(Duration(milliseconds: 10)); // Ensure timestamp difference
      payload = payload.set('name', 'Bob');
      expect(payload.value.name, equals('Bob'));
    });

    test('Merge: disjoint fields', () {
      var payload1 = ProfileCrdtPayload.create(testDid, testPublicKeyJwk);
      payload1 = payload1.set('name', 'Alice');

      var payload2 = ProfileCrdtPayload.create(testDid, testPublicKeyJwk);
      payload2 = payload2.set('preferredUsername', 'alice_user');

      final merged = payload1.merge(payload2);
      expect(merged.value.name, equals('Alice'));
      expect(merged.value.preferredUsername, equals('alice_user'));

      final mergedReverse = payload2.merge(payload1);
      expect(mergedReverse.value.name, equals('Alice'));
      expect(mergedReverse.value.preferredUsername, equals('alice_user'));
    });

    test('Merge: conflicting fields, LWW wins (simulated by order)', () async {
      var payload1 = ProfileCrdtPayload.create(testDid, testPublicKeyJwk);
      payload1 = payload1.set('name', 'Alice'); // Earlier timestamp

      await Future.delayed(Duration(milliseconds: 10));

      var payload2 = ProfileCrdtPayload.create(testDid, testPublicKeyJwk);
      payload2 = payload2.set('name', 'Bob'); // Later timestamp

      final merged12 = payload1.merge(payload2);
      expect(merged12.value.name, equals('Bob'));

      final merged21 = payload2.merge(payload1);
      expect(merged21.value.name, equals('Bob'));
    });
    
    // Note: True LWW tie-breaking (same timestamp) is hard to test without manipulating clocks
    // or directly creating LwwEntry instances with controlled timestamps.
    // The current implementation's tie-breaking (preferring 'other' if values differ, or lexicographical for strings)
    // can be tested if we can force identical timestamps.
    // For now, we rely on the timestamp difference.

    test('Merge: one payload has null field', () async {
      var payload1 = ProfileCrdtPayload.create(testDid, testPublicKeyJwk);
      payload1 = payload1.set('name', 'Alice');

      await Future.delayed(Duration(milliseconds: 10));
      var payload2 = ProfileCrdtPayload.create(testDid, testPublicKeyJwk);
      payload2 = payload2.set('name', null); // Later timestamp, sets to null

      final merged = payload1.merge(payload2);
      expect(merged.value.name, isNull);
    });

    test('Serialization and Deserialization (toCanonicalBytes/fromBytes)', () {
      final iconCid = CID.fromString('bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq');
      payload = payload.set('name', 'Charlie');
      payload = payload.set('preferredUsername', 'charlie_brown');
      payload = payload.set('summary', 'Good grief!');
      payload = payload.set('icon', iconCid);

      final bytes = payload.toCanonicalBytes();
      final deserialized = ProfileCrdtPayload.fromBytes(bytes);

      expect(deserialized.value.id, equals(testDid));
      expect(deserialized.value.publicKeyJwk, equals(testPublicKeyJwk));
      expect(deserialized.value.name, equals('Charlie'));
      expect(deserialized.value.preferredUsername, equals('charlie_brown'));
      expect(deserialized.value.summary, equals('Good grief!'));
      expect(deserialized.value.icon, equals(iconCid));
      expect(deserialized.value.image, isNull);
    });

    test('Serialization and Deserialization (toCanonicalBytes/fromBytes)', () {
      // For ProfileCrdtPayload, toBytes and toCanonicalBytes might be the same
      // if JSON key order is handled consistently (e.g. by sorting in toCanonicalBytes).
      final imageCid = CID.fromString('bafkreidgjalwh3js3n7ot34g3w2pw34b2ihhmswgybylrkv6esqmyV5k3i');
      payload = payload.set('name', 'David');
      payload = payload.set('type', 'Bot');
      payload = payload.set('image', imageCid);


      final bytes = payload.toCanonicalBytes(); // Using canonical
      final deserialized = ProfileCrdtPayload.fromBytes(bytes);

      expect(deserialized.value.id, equals(testDid));
      expect(deserialized.value.publicKeyJwk, equals(testPublicKeyJwk));
      expect(deserialized.value.name, equals('David'));
      expect(deserialized.value.type, equals('Bot'));
      expect(deserialized.value.image, equals(imageCid));
      expect(deserialized.value.icon, isNull);
    });

     test('Merge with empty payload', () {
      payload = payload.set('name', 'Eve');
      final emptyPayload = ProfileCrdtPayload.create(testDid, testPublicKeyJwk);
      
      final merged1 = payload.merge(emptyPayload);
      expect(merged1.value.name, equals('Eve'));

      final merged2 = emptyPayload.merge(payload);
      expect(merged2.value.name, equals('Eve'));
    });

    test('Throws error if merging profiles with different DIDs', () {
      final payload2 = ProfileCrdtPayload.create('did:key:zDifferentDID', testPublicKeyJwk);
      expect(() => payload.merge(payload2), throwsArgumentError);
    });

    test('Set invalid type for icon (not CID)', () {
       expect(() => payload.set('icon', 'not-a-cid'), throwsArgumentError);
    });
    
    test('Set invalid type for name (not String)', () {
       expect(() => payload.set('name', 123), throwsArgumentError);
    });

  });
}
