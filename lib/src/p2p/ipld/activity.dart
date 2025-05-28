import 'dart:convert' show jsonEncode;
import 'dart:typed_data';
import 'dart:convert' show jsonEncode; // Keep for toCanonicalBytesForSigning

import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:pinenacl/api.dart' show SignatureBase; // For signature type
import 'package:cbor/cbor.dart';

// Re-export or define context if not already globally available
const activityStreamsContext = 'https://www.w3.org/ns/activitystreams';

// Base class for activities (optional, but can be useful)
abstract class Activity {
  String get type;
  String get actor; // Actor's DID
  DateTime get published;
  String get context;
  Map<String, dynamic>? signature; // JWS-like structure: {"type": "Ed25519Signature2020", "creator": "did:key:...", "signatureValue": "base64url..."}

  Map<String, dynamic> toJson();
  Uint8List toCanonicalBytesForSigning(); // Excludes signature
}

class CreateActivity implements Activity {
  @override
  final String type = 'Create';
  @override
  final String actor; // DID of the actor performing the create
  final CID object; // CID of the object being created (e.g., a Note)
  @override
  final DateTime published;
  @override
  final String context;
  @override
  Map<String, dynamic>? signature;

  CreateActivity({
    required this.actor,
    required this.object,
    DateTime? published,
    this.context = activityStreamsContext,
    this.signature,
  }) : published = published ?? DateTime.now().toUtc();

  @override
  Map<String, dynamic> toJson() {
    final Map<String, dynamic> json = { // Explicitly type json
      '@context': context,
      'type': type,
      'actor': actor,
      'object': object.toString(),
      'published': published.toIso8601String(),
    };
    if (signature != null) {
      json['signature'] = signature;
    }
    return json;
  }

  // For signing, we serialize without the signature field.
  // Key order should be canonical.
  @override
  Uint8List toCanonicalBytesForSigning() {
    // This should produce the CBOR representation of the activity *without* the signature.
    // It should match the data that the CID is generated from (excluding signature).
    final Map<CborValue, CborValue> dataForSigning = {
      CborString('@context'): CborString(context),
      CborString('type'): CborString(type),
      CborString('actor'): CborString(actor),
      CborString('object'): CborBytes(object.toBytes(), tags: [42]), // dag-cbor link
      CborString('published'): CborString(published.toIso8601String()),
    };
    // Note: Canonical CBOR requires map keys to be sorted.
    // The CborMap constructor does not guarantee key sorting during encoding by default.
    // However, the `cbor.encode` function with `canonical: true` (if available and used in Actor)
    // would handle this. For now, we construct the map.
    // If actor.dart's cbor.encode doesn't use canonical mode, this might not be strictly canonical.
    // For signing, consistency is key. As long as what's signed is what's verified.
    final encodedBytes = cbor.encode(CborMap(dataForSigning));
    // Ensure it's Uint8List if cbor.encode returns List<int> in this environment
    if (encodedBytes is Uint8List) {
      return encodedBytes;
    } else {
      return Uint8List.fromList(encodedBytes);
    }
  }

  CborMap toCborMap() {
    // final CborBytes cidAsCborBytes = CborBytes(object.toBytes()); // Unused
    // Explicitly cast to CborValue to try and appease the analyzer // Comment related to old CborTag approach
    final Map<CborValue, CborValue> cborData = {
      CborString('@context'): CborString(context),
      CborString('type'): CborString(type),
      CborString('actor'): CborString(actor),
      CborString('object'): CborBytes(object.toBytes(), tags: [42]),
      CborString('published'): CborString(published.toIso8601String()),
    };
    if (signature != null) {
      // Assuming signature is Map<String, String> or Map<String, dynamic> with string values
      final sigMap = signature!;
      final cborSigMapEntries = sigMap.entries.map((entry) =>
          MapEntry(CborString(entry.key), CborString(entry.value.toString())));
      cborData[CborString('signature')] = CborMap(Map.fromEntries(cborSigMapEntries));
    }
    return CborMap(cborData);
  }

  factory CreateActivity.fromJson(Map<String, dynamic> json) {
    if (json['type'] != 'Create') {
      throw ArgumentError('Invalid JSON: type is not Create.');
    }
    return CreateActivity(
      actor: json['actor'] as String,
      object: CID.fromString(json['object'] as String),
      published: DateTime.parse(json['published'] as String),
      context: json['@context'] as String? ?? activityStreamsContext,
      signature: json['signature'] as Map<String, dynamic>?,
    );
  }
}

// Example of how signing might be integrated (conceptual)
/*
Future<CreateActivity> signActivity(CreateActivity activity, Actor actorIdentity) async {
  if (activity.actor != actorIdentity.did) {
    throw ArgumentError('Activity actor does not match signer identity.');
  }
  final bytesToSign = activity.toCanonicalBytesForSigning();
  final signatureData = await actorIdentity.sign(bytesToSign); // This returns SignatureBase

  activity.signature = {
    "type": "Ed25519Signature2020", // Or a similar appropriate type
    "creator": actorIdentity.did,
    // signatureValue should be base64url encoded representation of signatureData.bytes
    "signatureValue": base64Url.encode(signatureData.bytes), 
  };
  return activity;
}
*/
