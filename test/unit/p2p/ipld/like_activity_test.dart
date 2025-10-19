import 'dart:convert';
import 'dart:typed_data';

import 'package:cbor/cbor.dart';
import 'package:dcid/dcid.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/activity.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/like_activity.dart';
import 'package:test/test.dart';

void main() {
  group('LikeActivity Tests', () {
    const actorDid = 'did:key:zActorLiker';
    // Example CID - in a real scenario, this would be a valid CID of some content
    final objectCid = CID.fromString('bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi'); 
    final publishedTime = DateTime.now().toUtc();

    LikeActivity createActivity({Map<String, dynamic>? signature}) {
      return LikeActivity(
        actor: actorDid,
        objectCid: objectCid,
        published: publishedTime,
        context: 'https://custom.like.context.com',
        signature: signature,
      );
    }

    test('Constructor sets properties correctly', () {
      final activity = createActivity();
      expect(activity.type, equals('Like'));
      expect(activity.actor, equals(actorDid));
      expect(activity.objectCid.toString(), equals(objectCid.toString()));
      expect(activity.published, equals(publishedTime));
      expect(activity.context, equals('https://custom.like.context.com'));
      expect(activity.signature, isNull);
    });

    test('toJson() serializes correctly without signature', () {
      final activity = createActivity();
      final json = activity.toJson();

      expect(json['@context'], equals('https://custom.like.context.com'));
      expect(json['type'], equals('Like'));
      expect(json['actor'], equals(actorDid));
      expect(json['object'], equals(objectCid.toString()));
      expect(json['published'], equals(publishedTime.toIso8601String()));
      expect(json['signature'], isNull);
    });

    test('toJson() serializes correctly with signature', () {
      final signatureMap = {'type': 'Ed25519Signature2020', 'creator': actorDid, 'signatureValue': 'base64sigLike'};
      final activity = createActivity(signature: signatureMap);
      final json = activity.toJson();

      expect(json['signature'], equals(signatureMap));
    });

    test('fromJson() deserializes correctly', () {
      final json = {
        '@context': 'https://custom.like.context.com',
        'type': 'Like',
        'actor': actorDid,
        'object': objectCid.toString(),
        'published': publishedTime.toIso8601String(),
        'signature': {'type': 'Ed25519Signature2020', 'creator': actorDid, 'signatureValue': 'base64sigLike'},
      };

      final activity = LikeActivity.fromJson(json);

      expect(activity.type, equals('Like'));
      expect(activity.actor, equals(actorDid));
      expect(activity.objectCid.toString(), equals(objectCid.toString()));
      expect(activity.published, equals(publishedTime));
      expect(activity.context, equals('https://custom.like.context.com'));
      expect(activity.signature, equals(json['signature']));
    });

    test('fromJson() throws ArgumentError for incorrect type', () {
      final json = {'type': 'Follow', 'actor': actorDid, 'object': objectCid.toString(), 'published': publishedTime.toIso8601String()};
      expect(() => LikeActivity.fromJson(json), throwsArgumentError);
    });

    test('toCanonicalBytesForSigning() produces CBOR without signature', () {
      final signatureMap = {'type': 'Ed25519Signature2020', 'creator': actorDid, 'signatureValue': 'base64sigLike'};
      final activity = createActivity(signature: signatureMap);
      
      final bytes = activity.toCanonicalBytesForSigning();
      final decoded = cbor.decode(bytes) as CborMap;

      expect(decoded[CborString('@context')]?.toString(), equals('https://custom.like.context.com'));
      expect(decoded[CborString('type')]?.toString(), equals('Like'));
      expect(decoded[CborString('actor')]?.toString(), equals(actorDid));
      // Object is CID, check its CBOR representation (bytes with tag 42)
      final cborObject = decoded[CborString('object')] as CborBytes?;
      expect(cborObject, isNotNull);
      expect(cborObject?.tags, contains(42));
      expect(CID.fromBytes(Uint8List.fromList(cborObject!.bytes)).toString(), objectCid.toString());
      expect(decoded[CborString('published')]?.toString(), equals(publishedTime.toIso8601String()));
      expect(decoded[CborString('signature')], isNull);
    });

    test('toCborMap() and fromCborMap() round trip without signature', () {
      final activity = createActivity();
      final cborMap = activity.toCborMap();
      final reconstructedActivity = LikeActivity.fromCborMap(cborMap);

      expect(reconstructedActivity.type, activity.type);
      expect(reconstructedActivity.actor, activity.actor);
      expect(reconstructedActivity.objectCid.toString(), activity.objectCid.toString());
      expect(reconstructedActivity.published, activity.published);
      expect(reconstructedActivity.context, activity.context);
      expect(reconstructedActivity.signature, isNull);
    });

    test('toCborMap() and fromCborMap() round trip with signature', () {
       final signatureMap = {'type': 'Ed25519Signature2020', 'creator': actorDid, 'signatureValue': 'base64sigLike'};
      final activity = createActivity(signature: signatureMap);
      final cborMap = activity.toCborMap();
      final reconstructedActivity = LikeActivity.fromCborMap(cborMap);

      expect(reconstructedActivity.signature, equals(signatureMap));
    });

     test('fromCborMap() throws ArgumentError for incorrect type', () {
      final cborMap = CborMap({
        CborString('type'): CborString('Follow'), // Incorrect type
        CborString('actor'): CborString(actorDid),
        CborString('object'): CborBytes(objectCid.toBytes(), tags: [42]),
        CborString('published'): CborString(publishedTime.toIso8601String()),
      });
      expect(() => LikeActivity.fromCborMap(cborMap), throwsArgumentError);
    });

    test('toString() returns a non-empty string', () {
      final activity = createActivity();
      expect(activity.toString(), isA<String>());
      expect(activity.toString(), isNotEmpty);
      expect(activity.toString(), contains('LikeActivity'));
      expect(activity.toString(), contains(actorDid));
      expect(activity.toString(), contains(objectCid.toString()));
    });
  });
}
