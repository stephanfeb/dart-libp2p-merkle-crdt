import 'dart:typed_data';
import 'package:pinenacl/api.dart';
import 'package:pinenacl/api/signatures.dart';
import 'package:pinenacl/ed25519.dart';
import 'package:test/test.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/identity/actor.dart';

void main() {
  group('Actor Tests', () {
    late Actor actor;

    setUp(() async {
      actor = await Actor.create();
    });

    test('Actor creation generates DID and keys', () {
      expect(actor, isNotNull);
      expect(actor.did, isA<String>());
      expect(actor.publicKey, isNotNull);
      expect(actor.privateKey, isNotNull);
    });

    test('DID has correct did:key:z prefix', () {
      expect(actor.did.startsWith('did:key:z'), isTrue);
    });

    test('publicKeyJwk has correct structure', () {
      final jwk = actor.publicKeyJwk;
      expect(jwk, isA<Map<String, String>>());
      expect(jwk['kty'], equals('OKP'));
      expect(jwk['crv'], equals('Ed25519'));
      expect(jwk['x'], isA<String>());
      expect(jwk['x']!.isNotEmpty, isTrue);
    });

    test('Sign and verify data successfully', () async {
      final data = Uint8List.fromList('Hello, world!'.codeUnits);
      final signature = await actor.sign(data); // Returns SignatureBase
      
      expect(signature, isA<SignatureBase>());
      // The concrete type is likely pna_api.Signature, which extends pna_api.SignatureBase
      expect(signature.toUint8List().isNotEmpty, isTrue);

      final isValid = await actor.verify(data, signature);
      expect(isValid, isTrue);
    });

    test('Verify fails with incorrect data', () async {
      final data = Uint8List.fromList('Hello, world!'.codeUnits);
      final incorrectData = Uint8List.fromList('Hello, P2P!'.codeUnits);
      final signature = await actor.sign(data); // pna_api.SignatureBase

      final isValid = await actor.verify(incorrectData, signature);
      expect(isValid, isFalse);
    });

    test('Verify fails with incorrect signature', () async {
      final data = Uint8List.fromList('Hello, world!'.codeUnits);
      final signature = await actor.sign(data); // pna_api.SignatureBase
      
      // Tamper with the signature bytes
      final originalBytes = signature.toUint8List();
      final tamperedSignatureBytes = Uint8List.fromList(originalBytes);

      if (tamperedSignatureBytes.isNotEmpty) {
        tamperedSignatureBytes[0] = tamperedSignatureBytes[0] ^ 0xFF; // Flip some bits
      } else {
        fail('Signature bytes are empty, cannot tamper for test.');
      }
      // Create a new pna_api.Signature (concrete class) with tampered bytes
      final tamperedSignature = Signature(tamperedSignatureBytes);
      
      final isValid = await actor.verify(data, tamperedSignature);
      expect(isValid, isFalse);
    });

     test('Verify fails with signature from a different key', () async {
      final data = Uint8List.fromList('Hello, world!'.codeUnits);
      final actor2 = await Actor.create();
      final signatureFromActor2 = await actor2.sign(data); // pna_api.SignatureBase

      final isValid = await actor.verify(data, signatureFromActor2);
      expect(isValid, isFalse);
    });
  });
}
