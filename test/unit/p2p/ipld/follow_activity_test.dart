import 'dart:convert';
import 'dart:typed_data';

import 'package:cbor/cbor.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/activity.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/follow_activity.dart';
import 'package:test/test.dart';

void main() {
  group('FollowActivity Tests', () {
    const actorDid = 'did:key:zActorFollower';
    const objectDid = 'did:key:zActorFollowed';
    final publishedTime = DateTime.now().toUtc();

    FollowActivity createActivity({Map<String, dynamic>? signature}) {
      return FollowActivity(
        actor: actorDid,
        objectDid: objectDid,
        published: publishedTime,
        context: 'https://custom.context.com', // Test custom context
        signature: signature,
      );
    }

    test('Constructor sets properties correctly', () {
      final activity = createActivity();
      expect(activity.type, equals('Follow'));
      expect(activity.actor, equals(actorDid));
      expect(activity.objectDid, equals(objectDid));
      expect(activity.published, equals(publishedTime));
      expect(activity.context, equals('https://custom.context.com'));
      expect(activity.signature, isNull);
    });

    test('toJson() serializes correctly without signature', () {
      final activity = createActivity();
      final json = activity.toJson();

      expect(json['@context'], equals('https://custom.context.com'));
      expect(json['type'], equals('Follow'));
      expect(json['actor'], equals(actorDid));
      expect(json['object'], equals(objectDid));
      expect(json['published'], equals(publishedTime.toIso8601String()));
      expect(json['signature'], isNull);
    });

    test('toJson() serializes correctly with signature', () {
      final signatureMap = {'type': 'Ed25519Signature2020', 'creator': actorDid, 'signatureValue': 'base64sig'};
      final activity = createActivity(signature: signatureMap);
      final json = activity.toJson();

      expect(json['signature'], equals(signatureMap));
    });

    test('fromJson() deserializes correctly', () {
      final json = {
        '@context': 'https://custom.context.com',
        'type': 'Follow',
        'actor': actorDid,
        'object': objectDid,
        'published': publishedTime.toIso8601String(),
        'signature': {'type': 'Ed25519Signature2020', 'creator': actorDid, 'signatureValue': 'base64sig'},
      };

      final activity = FollowActivity.fromJson(json);

      expect(activity.type, equals('Follow'));
      expect(activity.actor, equals(actorDid));
      expect(activity.objectDid, equals(objectDid));
      expect(activity.published, equals(publishedTime));
      expect(activity.context, equals('https://custom.context.com'));
      expect(activity.signature, equals(json['signature']));
    });

    test('fromJson() throws ArgumentError for incorrect type', () {
      final json = {'type': 'Like', 'actor': actorDid, 'object': objectDid, 'published': publishedTime.toIso8601String()};
      expect(() => FollowActivity.fromJson(json), throwsArgumentError);
    });

    test('toCanonicalBytesForSigning() produces CBOR without signature', () {
      final signatureMap = {'type': 'Ed25519Signature2020', 'creator': actorDid, 'signatureValue': 'base64sig'};
      final activity = createActivity(signature: signatureMap);
      
      final bytes = activity.toCanonicalBytesForSigning();
      final decoded = cbor.decode(bytes) as CborMap;

      expect(decoded[CborString('@context')]?.toString(), equals('https://custom.context.com'));
      expect(decoded[CborString('type')]?.toString(), equals('Follow'));
      expect(decoded[CborString('actor')]?.toString(), equals(actorDid));
      expect(decoded[CborString('object')]?.toString(), equals(objectDid));
      expect(decoded[CborString('published')]?.toString(), equals(publishedTime.toIso8601String()));
      expect(decoded[CborString('signature')], isNull); // Crucial: signature must not be included
    });

    test('toCborMap() and fromCborMap() round trip without signature', () {
      final activity = createActivity();
      final cborMap = activity.toCborMap();
      final reconstructedActivity = FollowActivity.fromCborMap(cborMap);

      expect(reconstructedActivity.type, activity.type);
      expect(reconstructedActivity.actor, activity.actor);
      expect(reconstructedActivity.objectDid, activity.objectDid);
      expect(reconstructedActivity.published, activity.published);
      expect(reconstructedActivity.context, activity.context);
      expect(reconstructedActivity.signature, isNull);
    });

    test('toCborMap() and fromCborMap() round trip with signature', () {
       final signatureMap = {'type': 'Ed25519Signature2020', 'creator': actorDid, 'signatureValue': 'base64sig'};
      final activity = createActivity(signature: signatureMap);
      final cborMap = activity.toCborMap();
      final reconstructedActivity = FollowActivity.fromCborMap(cborMap);

      expect(reconstructedActivity.signature, equals(signatureMap));
    });
     test('fromCborMap() throws ArgumentError for incorrect type', () {
      final cborMap = CborMap({
        CborString('type'): CborString('Like'),
        CborString('actor'): CborString(actorDid),
        CborString('object'): CborString(objectDid),
        CborString('published'): CborString(publishedTime.toIso8601String()),
      });
      expect(() => FollowActivity.fromCborMap(cborMap), throwsArgumentError);
    });

    test('toString() returns a non-empty string', () {
      final activity = createActivity();
      expect(activity.toString(), isA<String>());
      expect(activity.toString(), isNotEmpty);
      expect(activity.toString(), contains('FollowActivity'));
      expect(activity.toString(), contains(actorDid));
      expect(activity.toString(), contains(objectDid));
    });
  });
}
