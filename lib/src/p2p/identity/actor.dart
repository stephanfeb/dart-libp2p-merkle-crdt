import 'dart:typed_data';
import 'dart:convert' show base64, utf8; // For _toBase64Url, utf8
import 'package:crypto/crypto.dart' show sha256;
import 'package:cbor/cbor.dart'; // Removed 'as cbor'
import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:pinenacl/api/signatures.dart'; // For SignatureBase typedef
import 'package:pinenacl/api.dart'; // For other API elements, potentially Signature class
import 'package:pinenacl/ed25519.dart' as ed25519;
import 'package:multibase/multibase.dart'; // Corrected import path

// Import IPLD types
import '../ipld/note.dart';
import '../ipld/activity.dart';


// TODO: Add 'dart_multicodec' package dependency for robust multicodec handling.
// const ed25519PubMulticodecPrefix = 0xed; // From multicodec table for 'ed25519-pub'

// Helper to convert Uint8List to base64url
String _toBase64Url(Uint8List bytes) {
  var s = base64.encode(bytes);
  s = s.replaceAll('+', '-');
  s = s.replaceAll('/', '_');
  s = s.replaceAll('=', '');
  return s;
}

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
      // We cast signatureBytes (Uint8List) to SignatureBase here.
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
    return {
      'kty': 'OKP', // Octet Key Pair
      'crv': 'Ed25519',
      'x': _toBase64Url(publicKey.asTypedList),
      // 'kid': did, // Optional: key ID, can be the DID itself.
    };
  }

  // Creates a Note object and calculates its CID.
  // For simplicity, CID is derived from SHA256 hash of its JSON representation.
  // In a real IPLD system, this would involve creating a DAG node.
  Future<({Note note, CID noteCid})> createNote(String content) async {
    final note = Note(content: content, published: DateTime.now().toUtc());
    
    // Serialize to canonical bytes (CBOR)
    final noteCborValue = note.toCborMap(); // Use toCborMap()
    // Use the global cbor instance's encode method
    final noteBytes = cbor.encode(noteCborValue); 
    
    // Calculate SHA256 hash for CID (v1, dag-cbor codec for content type)
    final digest = sha256.convert(noteBytes);
    final digestBytes = Uint8List.fromList(digest.bytes);
    
    // Manually construct the multihash: <hash_code><digest_length><digest_bytes>
    // 0x12 is sha2-256, 32 is its length in bytes (0x20 hex)
    final multihashBytes = Uint8List.fromList([0x12, 32, ...digestBytes]);
    
    // Create CID: CID(version, ipld_content_type_codec, full_multihash_bytes)
    // 0x71 is the IPLD codec for CBOR (dag-cbor)
    final noteCid = CID(1, 0x71, multihashBytes);

    return (note: note, noteCid: noteCid);
  }

  // Creates and signs an activity (e.g., CreateActivity).
  // Returns the signed activity and its CID.
  Future<({Activity activity, CID activityCid})> createAndSignActivity({
    required CID objectCid, // CID of the object of the activity (e.g., a Note)
    String activityType = 'Create', // Default to CreateActivity
  }) async {
    Activity activity;

    if (activityType == 'Create') {
      activity = CreateActivity(
        actor: did,
        object: objectCid,
        published: DateTime.now().toUtc(),
      );
    } else {
      throw UnimplementedError('Activity type $activityType not supported yet.');
    }

    final bytesToSign = activity.toCanonicalBytesForSigning();
    final signature = await sign(bytesToSign); // This is SignatureBase

    activity.signature = {
      "type": "Ed25519Signature2020", // Matching pinenacl/Ed25519
      "creator": did,
      // Explicitly cast SignatureBase to Uint8List for the analyzer.
      "signatureValue": _toBase64Url(signature.toUint8List()),
    };
    
    // Calculate CID of the signed activity
    // Cast to CreateActivity to access toCborMap, assuming activity is CreateActivity
    final activityCborValue = (activity as CreateActivity).toCborMap(); // Use toCborMap()
    // Use the global cbor instance's encode method
    final activityBytes = cbor.encode(activityCborValue);
    final digest = sha256.convert(activityBytes);
    final digestBytes = Uint8List.fromList(digest.bytes);

    // Manually construct the multihash: <hash_code><digest_length><digest_bytes>
    // 0x12 is sha2-256, 32 is its length in bytes (0x20 hex)
    final multihashBytes = Uint8List.fromList([0x12, 32, ...digestBytes]);

    // Create CID: CID(version, ipld_content_type_codec, full_multihash_bytes)
    // 0x71 is the IPLD codec for CBOR (dag-cbor)
    final activityCid = CID(1, 0x71, multihashBytes);

    return (activity: activity, activityCid: activityCid);
  }
}
