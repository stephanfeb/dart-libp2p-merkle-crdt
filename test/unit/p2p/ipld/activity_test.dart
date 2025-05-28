import 'dart:convert' show jsonEncode, jsonDecode; // Added jsonDecode
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/activity.dart';

void main() {
  group('CreateActivity IPLD Tests', () {
    const testActorDid = 'did:key:zTestActorDID123';
    final testObjectCid = CID.fromString('bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq');
    final testSignature = {
      "type": "Ed25519Signature2020",
      "creator": testActorDid,
      "signatureValue": "testSignatureValueBase64Url"
    };

    test('Creation with default published, context, and no signature', () {
      final now = DateTime.now().toUtc();
      final activity = CreateActivity(actor: testActorDid, object: testObjectCid);

      expect(activity.actor, equals(testActorDid));
      expect(activity.object, equals(testObjectCid));
      expect(activity.type, equals('Create'));
      expect(activity.context, equals(activityStreamsContext));
      expect(activity.signature, isNull);
      // Check if published time is recent
      expect(activity.published.isAfter(now.subtract(Duration(seconds: 1))), isTrue);
      expect(activity.published.isBefore(now.add(Duration(seconds: 1))), isTrue);
    });

    test('Creation with specified values including signature', () {
      final publishedTime = DateTime.utc(2023, 1, 1, 12, 0, 0);
      const customContext = 'https://example.com/custom-activity-context';
      
      final activity = CreateActivity(
        actor: testActorDid,
        object: testObjectCid,
        published: publishedTime,
        context: customContext,
        signature: testSignature,
      );

      expect(activity.actor, equals(testActorDid));
      expect(activity.object, equals(testObjectCid));
      expect(activity.published, equals(publishedTime));
      expect(activity.context, equals(customContext));
      expect(activity.signature, equals(testSignature));
    });

    test('toJson produces correct map structure without signature', () {
      final publishedTime = DateTime.utc(2023, 5, 15, 10, 0, 0);
      final activity = CreateActivity(
        actor: testActorDid,
        object: testObjectCid,
        published: publishedTime,
      );
      final json = activity.toJson();

      expect(json['@context'], equals(activityStreamsContext));
      expect(json['type'], equals('Create'));
      expect(json['actor'], equals(testActorDid));
      expect(json['object'], equals(testObjectCid.toString()));
      expect(json['published'], equals(publishedTime.toIso8601String()));
      expect(json.containsKey('signature'), isFalse);
    });

    test('toJson produces correct map structure with signature', () {
      final activity = CreateActivity(
        actor: testActorDid,
        object: testObjectCid,
        signature: testSignature,
      );
      final json = activity.toJson();

      expect(json['signature'], equals(testSignature));
    });

    test('toCanonicalBytesForSigning produces non-empty Uint8List and excludes signature', () {
      final activity = CreateActivity(
        actor: testActorDid,
        object: testObjectCid,
        signature: testSignature, // Signature should be ignored
      );
      final bytes = activity.toCanonicalBytesForSigning();
      final decodedMap = jsonDecode(String.fromCharCodes(bytes)) as Map<String, dynamic>;

      expect(bytes, isA<Uint8List>());
      expect(bytes.isNotEmpty, isTrue);
      expect(decodedMap.containsKey('signature'), isFalse);
      expect(decodedMap['actor'], testActorDid); // Check a field to ensure content is there
    });
    
    test('toCanonicalBytesForSigning is consistent for same input (ignoring signature)', () {
      final activity1 = CreateActivity(actor: testActorDid, object: testObjectCid, published: DateTime.utc(2023,1,1), signature: testSignature);
      final activity2 = CreateActivity(actor: testActorDid, object: testObjectCid, published: DateTime.utc(2023,1,1), signature: {'other': 'sig'}); // Different signature
      
      expect(activity1.toCanonicalBytesForSigning(), equals(activity2.toCanonicalBytesForSigning()));
    });


    test('fromJson creates CreateActivity object correctly without signature', () {
      final publishedTime = DateTime.utc(2023, 10, 20, 9, 0, 0);
      final jsonMap = {
        '@context': activityStreamsContext,
        'type': 'Create',
        'actor': testActorDid,
        'object': testObjectCid.toString(),
        'published': publishedTime.toIso8601String(),
      };

      final activity = CreateActivity.fromJson(jsonMap);
      expect(activity.actor, equals(testActorDid));
      expect(activity.object, equals(testObjectCid));
      expect(activity.published, equals(publishedTime));
      expect(activity.context, equals(activityStreamsContext));
      expect(activity.type, equals('Create'));
      expect(activity.signature, isNull);
    });

    test('fromJson creates CreateActivity object correctly with signature', () {
      final publishedTime = DateTime.utc(2023, 10, 21, 9, 0, 0);
      final jsonMap = {
        '@context': activityStreamsContext,
        'type': 'Create',
        'actor': testActorDid,
        'object': testObjectCid.toString(),
        'published': publishedTime.toIso8601String(),
        'signature': testSignature,
      };
      final activity = CreateActivity.fromJson(jsonMap);
      expect(activity.signature, equals(testSignature));
    });


    test('fromJson throws ArgumentError for incorrect type', () {
      final jsonMap = {
        '@context': activityStreamsContext,
        'type': 'Update', // Incorrect type
        'actor': testActorDid,
        'object': testObjectCid.toString(),
        'published': DateTime.now().toIso8601String(),
      };
      expect(() => CreateActivity.fromJson(jsonMap), throwsArgumentError);
    });
  });
}
