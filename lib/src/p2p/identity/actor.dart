import 'dart:typed_data';
import 'dart:convert' show base64; // For _toBase64Url

import 'package:pinenacl/api/signatures.dart';
import 'package:pinenacl/ed25519.dart' as ed25519;
import 'package:pinenacl/api.dart'; // Import for SignatureBase
import 'package:multibase/multibase.dart'; // Corrected import path

// TODO: Add 'dart_multicodec' package dependency for robust multicodec handling.
// const ed25519PubMulticodecPrefix = 0xed; // From multicodec table for 'ed25519-pub'

class Actor {
  final ed25519.SigningKey privateKey;
  final ed25519.VerifyKey publicKey;
  final String did; // did:key format

  Actor._(this.privateKey, this.publicKey, this.did);

  static Future<Actor> create() async {
    final privateKey = ed25519.SigningKey.generate();
    final publicKey = privateKey.verifyKey;

    final publicKeyBytes = publicKey.asTypedList;

    // Derive did:key from public key
    // did:key format: "did:key:" + multibase_encoded(multicodec_pubkey_prefix + pubkey_bytes)
    // Multicodec prefix for Ed25519 public key is 0xed.
    final prefixedPublicKeyBytes = Uint8List(1 + publicKeyBytes.length);
    prefixedPublicKeyBytes[0] = 0xed; // Hardcoded: ed25519-pub multicodec prefix
    prefixedPublicKeyBytes.setRange(1, publicKeyBytes.length + 1, publicKeyBytes);

    // Multibase encode (Base58BTC is common, prefix 'z')
    final codec = MultibaseCodec(encodeBase: Multibase.base58btc);
    final didKeyString = 'did:key:${codec.encode(prefixedPublicKeyBytes).toString()}';

    return Actor._(privateKey, publicKey, didKeyString);
  }

  // Method to sign data
  Future<SignatureBase> sign(Uint8List data) async {
    final signedMessage = privateKey.sign(data);
    // signedMessage.signature is SignatureBase (or a compatible concrete type like pinenacl.api.Signature)
    return signedMessage.signature; 
  }

  // Method to verify signature
  Future<bool> verify(Uint8List data, SignatureBase signature) async {
    try {
      // publicKey.verify expects a SignatureBase object.
      publicKey.verify(signature: signature, message: data);
      return true;
    } catch (e) {
      // pinenacl's verify throws an exception on failure
      return false;
    }
  }

  // Provides the public key in JWK (JSON Web Key) format.
  // Required for the Actor Profile CRDT.
  Map<String, String> get publicKeyJwk {
    String toBase64Url(Uint8List bytes) {
      var s = base64.encode(bytes);
      s = s.replaceAll('+', '-');
      s = s.replaceAll('/', '_');
      s = s.replaceAll('=', '');
      return s;
    }

    return {
      'kty': 'OKP', // Octet Key Pair
      'crv': 'Ed25519',
      'x': toBase64Url(publicKey.asTypedList),
      // 'kid': did, // Optional: key ID, can be the DID itself.
    };
  }
}
