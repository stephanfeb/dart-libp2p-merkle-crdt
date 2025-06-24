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
import '../ipld/follow_activity.dart';
import '../ipld/announce_activity.dart';
import '../ipld/delete_activity.dart'; // Added DeleteActivity

// Import CRDT types
import '../crdt/following_crdt.dart';
import '../crdt/liked_crdt.dart';
import '../crdt/outbox_crdt.dart'; // Added OutboxCrdtPayload

// Import IPLD types for new activities
import '../ipld/like_activity.dart';


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
  late FollowingCrdtPayload _followingCrdt;
  late LikedCrdtPayload _likedCrdt;
  late OutboxCrdtPayload _outboxCrdt;

  Actor._(this.privateKey, this.publicKey, this.did) {
    _followingCrdt = FollowingCrdtPayload.create();
    _likedCrdt = LikedCrdtPayload.create();
    _outboxCrdt = OutboxCrdtPayload.create();
  }

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
  Future<({Note note, CID noteCid})> createNote(String content, {List<Mention>? tags}) async {
    final note = Note(
      content: content, 
      published: DateTime.now().toUtc(),
      tag: tags,
    );
    
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
    CID? inReplyTo, // Optional CID of the content being replied to
    // String activityType = 'Create', // Default to CreateActivity - kept for compatibility if needed, but Create is specific
  }) async {
    final activity = CreateActivity(
      actor: did,
      object: objectCid,
      inReplyTo: inReplyTo, // Pass it here
      published: DateTime.now().toUtc(),
    );
    
    return _signAndGenerateActivityCid(activity, (a) => (a as CreateActivity).toCborMap());
  }

  // Generic helper to sign an activity and generate its CID
  Future<({T activity, CID activityCid})> _signAndGenerateActivityCid<T extends Activity>(
    T activity,
    CborMap Function(T activity) getCborMap,
  ) async {
    final bytesToSign = activity.toCanonicalBytesForSigning();
    final signatureData = await sign(bytesToSign); // This is SignatureBase

    activity.signature = {
      "type": "Ed25519Signature2020",
      "creator": did,
      "signatureValue": _toBase64Url(signatureData.toUint8List()),
    };

    final activityCborValue = getCborMap(activity);
    final activityBytes = cbor.encode(activityCborValue);
    final digest = sha256.convert(activityBytes);
    final digestBytes = Uint8List.fromList(digest.bytes);
    final multihashBytes = Uint8List.fromList([0x12, 32, ...digestBytes]);
    final activityCid = CID(1, 0x71, multihashBytes);

    return (activity: activity, activityCid: activityCid);
  }

  // --- Follow Functionality ---

  /// Gets the set of DIDs this actor is currently following.
  Set<String> get following => _followingCrdt.value;

  /// Creates a FollowActivity, signs it, updates the local following CRDT,
  /// and returns the activity and its CID.
  /// The caller is responsible for adding the activity CID to the outbox.
  Future<({FollowActivity activity, CID activityCid})> follow(String targetDid) async {
    final activity = FollowActivity(
      actor: did,
      objectDid: targetDid,
      published: DateTime.now().toUtc(),
    );

    final signedResult = await _signAndGenerateActivityCid(activity, (a) => (a as FollowActivity).toCborMap());
    
    // Update local CRDT state
    _followingCrdt = _followingCrdt.add(targetDid);

    return (activity: signedResult.activity, activityCid: signedResult.activityCid);
  }

  /// Updates the local following CRDT to remove a followed DID.
  /// Note: This does not automatically create an Undo(Follow) activity for the outbox.
  /// That would be a separate step if required by the protocol.
  void unfollow(String targetDid) {
    _followingCrdt = _followingCrdt.remove(targetDid);
  }

  // Getter for the internal CRDT payload, primarily for testing/integration with managers
  FollowingCrdtPayload get followingCrdtPayload => _followingCrdt;

  // --- Like Functionality ---

  /// Gets the set of CIDs of content this actor has liked.
  Set<CID> get likedItems => _likedCrdt.value;

  /// Creates a LikeActivity, signs it, updates the local liked CRDT,
  /// and returns the activity and its CID.
  /// The caller is responsible for adding the activity CID to the outbox.
  Future<({LikeActivity activity, CID activityCid})> like(CID contentCid) async {
    final activity = LikeActivity(
      actor: did,
      objectCid: contentCid,
      published: DateTime.now().toUtc(),
    );

    final signedResult = await _signAndGenerateActivityCid(activity, (a) => (a as LikeActivity).toCborMap());
    
    // Update local CRDT state
    _likedCrdt = _likedCrdt.add(contentCid);

    return (activity: signedResult.activity, activityCid: signedResult.activityCid);
  }

  /// Updates the local liked CRDT to remove a liked content CID.
  /// Note: This does not automatically create an Undo(Like) activity for the outbox.
  void unlike(CID contentCid) {
    _likedCrdt = _likedCrdt.remove(contentCid);
  }

  // Getter for the internal Liked CRDT payload, for testing/integration
  LikedCrdtPayload get likedCrdtPayload => _likedCrdt;

  // --- Announce Functionality ---

  /// Creates an AnnounceActivity, signs it, and returns the activity and its CID.
  /// The caller is responsible for adding the activity CID to the outbox.
  Future<({AnnounceActivity activity, CID activityCid})> announce(CID contentCid) async {
    final activity = AnnounceActivity(
      actor: did,
      object: contentCid, // CID of the content being announced
      published: DateTime.now().toUtc(),
    );

    // Use the generic signing helper, ensuring the correct CborMap conversion
    final signedResult = await _signAndGenerateActivityCid(
      activity, 
      (a) => (a as AnnounceActivity).toCborMap()
    );
    
    // Unlike follow/like, announce typically doesn't have its own dedicated CRDT state in the actor.
    // It's primarily an outbox event that other clients interpret.

    return (activity: signedResult.activity as AnnounceActivity, activityCid: signedResult.activityCid);
  }

  // --- Delete Functionality ---

  /// Creates a DeleteActivity, signs it, and returns the activity and its CID.
  /// The caller is responsible for adding the activity CID to the outbox.
  /// The `contentCid` can be the CID of a Note or any other Activity.
  Future<({DeleteActivity activity, CID activityCid})> delete(CID contentCid) async {
    final activity = DeleteActivity(
      actor: did,
      object: contentCid, // CID of the content/activity to be deleted
      published: DateTime.now().toUtc(),
    );

    final signedResult = await _signAndGenerateActivityCid(
      activity,
      (a) => (a as DeleteActivity).toCborMap()
    );

    return (activity: signedResult.activity as DeleteActivity, activityCid: signedResult.activityCid);
  }

  // --- Outbox Functionality (basic access and modification) ---

  /// Gets the list of activity CIDs in the actor's outbox.
  List<CID> get outboxActivities => _outboxCrdt.value;

  /// Adds an activity CID to the actor's outbox CRDT.
  /// Note: This is a direct CRDT modification. The caller is responsible for
  /// ensuring the corresponding Activity object is created, signed, and stored/announced.
  void addActivityToOutbox(CID activityCid) {
    _outboxCrdt = _outboxCrdt.add(activityCid);
    // In a real scenario, this modification would then be applied to a MerkleCrdtGossipManager
    // e.g., manager.applyLocalUpdate(_outboxCrdt);
  }

  void removeActivityFromOutbox(CID activityCid) {
    _outboxCrdt = _outboxCrdt.remove(activityCid);
  }

  // Getter for the internal Outbox CRDT payload, for testing/integration
  OutboxCrdtPayload get outboxCrdtPayload => _outboxCrdt;
}
