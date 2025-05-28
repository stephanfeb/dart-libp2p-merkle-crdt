import 'dart:convert' show jsonEncode, jsonDecode; // Added jsonDecode
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/activity.dart';
import 'package:cbor/cbor.dart'; // Import for cbor.decode and CborValue types

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
      // Decode CBOR bytes to a Dart Map
      final decodedMap = cbor.decode(bytes) as Map<dynamic, dynamic>;

      expect(bytes, isA<Uint8List>());
      expect(bytes.isNotEmpty, isTrue);
      expect(decodedMap.containsKey('signature'), isFalse, reason: "Decoded CBOR map should not contain 'signature'");
      // Try looking up with CborString key to diagnose if keys are not converted to Dart Strings
      expect(decodedMap[CborString('actor')], equals(CborString(testActorDid)), reason: "Decoded CBOR map should contain correct 'actor' (CborString key)");
      
      // Verify the 'object' field (CID link)
      // cbor.decode of CborBytes(tags:[42]) usually yields the underlying bytes (Uint8List)
      // and the tag information might be lost unless a custom decoder/reviver is used.
      // For this test, we'll check it's Uint8List and matches the original CID bytes.
      // Also try CborString key for 'object' if the above 'actor' change works.
      final dynamic objectKey = decodedMap.containsKey('object') ? 'object' : CborString('object');
      final cborEncodedObject = decodedMap[objectKey];
      expect(cborEncodedObject, isA<CborBytes>(), reason: "Decoded 'object' should be a CborBytes instance representing the CID link");
      expect(cborEncodedObject.bytes, isA<List<int>>(), reason: "The .bytes property of CborBytes should be a List<int>");
      expect(Uint8List.fromList(cborEncodedObject.bytes as List<int>), equals(testObjectCid.toBytes()), reason: "The actual bytes of the decoded object should match original CID bytes");
      expect(cborEncodedObject.tags, contains(42), reason: "The CborBytes for a CID link should be tagged with 42 (RFC 8610 section 3.4.5.2)");

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

    test('toCborMap produces correct CborMap structure without signature', () {
      final publishedTime = DateTime.utc(2023, 5, 15, 10, 0, 0);
      final activity = CreateActivity(
        actor: testActorDid,
        object: testObjectCid,
        published: publishedTime,
      );
      final cborMap = activity.toCborMap();

      expect(cborMap, isA<CborMap>());
      expect(cborMap[CborString('@context')], equals(CborString(activityStreamsContext)));
      expect(cborMap[CborString('type')], equals(CborString('Create')));
      expect(cborMap[CborString('actor')], equals(CborString(testActorDid)));
      expect(cborMap[CborString('published')], equals(CborString(publishedTime.toIso8601String())));
      
      final objectField = cborMap[CborString('object')];
      expect(objectField, isA<CborBytes>());
      expect((objectField as CborBytes).bytes, equals(testObjectCid.toBytes()));
      expect(objectField.tags, equals([42]));
      
      expect(cborMap.containsKey(CborString('signature')), isFalse);
    });

    test('toCborMap produces correct CborMap structure with signature', () {
      final activity = CreateActivity(
        actor: testActorDid,
        object: testObjectCid,
        signature: testSignature,
      );
      final cborMap = activity.toCborMap();

      expect(cborMap.containsKey(CborString('signature')), isTrue);
      final signatureField = cborMap[CborString('signature')];
      expect(signatureField, isA<CborMap>());
      
      final cborSigMap = signatureField as CborMap;
      expect(cborSigMap[CborString('type')], equals(CborString(testSignature['type']!)));
      expect(cborSigMap[CborString('creator')], equals(CborString(testSignature['creator']!)));
      expect(cborSigMap[CborString('signatureValue')], equals(CborString(testSignature['signatureValue']!)));
    });
  });
}
