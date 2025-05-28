import 'dart:convert';
import 'dart:typed_data';

import 'package:dart_cid/dart_cid.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/identity/actor.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/activity.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/note.dart' hide activityStreamsContext; // Hide to resolve ambiguity
// Import only SignatureBase from the correct path
import 'package:pinenacl/api/signatures.dart' show SignatureBase;
import 'package:pinenacl/ed25519.dart';
import 'package:test/test.dart';

// Helper to decode base64url
Uint8List _decodeBase64Url(String input) {
  var output = input.replaceAll('-', '+').replaceAll('_', '/');
  switch (output.length % 4) {
    case 0:
      break;
    case 2:
      output += '==';
      break;
    case 3:
      output += '=';
      break;
    default:
      throw ArgumentError('Illegal base64url string!"');
  }
  return base64Url.decode(output);
}


void main() {
  group('Actor Content Creation and Signing Tests', () {
    late Actor actor;

    setUp(() async {
      actor = await Actor.create();
    });

    test('createNote should return a Note object and its CID', () async {
      const noteContent = 'This is a test note!';
      final result = await actor.createNote(noteContent);

      expect(result, isA<({Note note, CID noteCid})>());
      expect(result.note, isA<Note>());
      expect(result.noteCid, isA<CID>());

      expect(result.note.content, equals(noteContent));
      expect(result.note.published, isA<DateTime>());
      expect(result.note.context, equals(activityStreamsContext)); // Assuming default context
      expect(result.noteCid.codec, equals(0x71), reason: 'Note CID should use dag-cbor codec.');
    });

    test('createAndSignActivity should return a signed CreateActivity and its CID', () async {
      // 1. Create a dummy note to get its CID
      final noteResult = await actor.createNote('Test note for activity.');
      final noteCid = noteResult.noteCid;

      // 2. Create and sign the activity
      final activityResult = await actor.createAndSignActivity(objectCid: noteCid);

      expect(activityResult, isA<({Activity activity, CID activityCid})>());
      expect(activityResult.activity, isA<CreateActivity>());
      expect(activityResult.activityCid, isA<CID>());

      final createActivity = activityResult.activity as CreateActivity;
      expect(createActivity.actor, equals(actor.did));
      expect(createActivity.object.toString(), equals(noteCid.toString())); // Compare CID strings
      expect(activityResult.activityCid.codec, equals(0x71), reason: 'Activity CID should use dag-cbor codec.');
      expect(createActivity.published, isA<DateTime>());
      expect(createActivity.type, equals('Create'));

      // Check signature structure
      expect(createActivity.signature, isNotNull);
      expect(createActivity.signature, isA<Map<String, dynamic>>());
      expect(createActivity.signature!['type'], equals('Ed25519Signature2020'));
      expect(createActivity.signature!['creator'], equals(actor.did));
      expect(createActivity.signature!['signatureValue'], isA<String>());
      
      // Basic check for base64url format (non-empty, no invalid chars)
      final signatureValue = createActivity.signature!['signatureValue'] as String;
      expect(signatureValue, isNotEmpty);
      expect(RegExp(r'^[A-Za-z0-9\-_]+$').hasMatch(signatureValue), isTrue, reason: 'Signature value should be base64url encoded.');

      // 3. Verify the signature (Important!)
      final canonicalBytes = createActivity.toCanonicalBytesForSigning();
      final signature = Signature(_decodeBase64Url(signatureValue));
      
      // pinenacl's verify method takes SignatureBase, which is Uint8List.
      // The Ed25519.verifyKey().verify() method expects a Signature object,
      // but Actor.verify takes SignatureBase.
      // For Ed25519, SignatureBase is effectively the raw signature bytes.
      // signatureBytes is Uint8List. SignatureBase is typedef Uint8List.
      // This direct pass should work if the typedef is correctly resolved.
      final isValidSignature = await actor.verify(canonicalBytes, signature);
      expect(isValidSignature, isTrue, reason: 'The activity signature should be valid.');

    });

    test('createAndSignActivity should throw for unsupported activity types', () async {
      final noteResult = await actor.createNote('Test note for activity.');
      final noteCid = noteResult.noteCid;

      expect(
        () => actor.createAndSignActivity(objectCid: noteCid, activityType: 'Update'),
        throwsA(isA<UnimplementedError>()),
      );
    });
  });
}
