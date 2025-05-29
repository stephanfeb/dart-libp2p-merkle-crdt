import 'dart:convert' show jsonEncode;
import 'dart:typed_data';

import 'package:cbor/cbor.dart';
import 'package:dart_cid/dart_cid.dart' show CID;
import 'activity.dart'; // Provides Activity, activityStreamsContext

/// Represents a "Like" activity in the ActivityPub-inspired model.
///
/// Schema: `{"@context": "...", "type": "Like", "actor": "liker_did", "object": "liked_content_cid", "published": "..."}`
class LikeActivity implements Activity {
  @override
  final String type = 'Like';

  @override
  final String actor; // DID of the actor performing the like

  /// The CID of the content being liked.
  final CID objectCid;

  @override
  final DateTime published;

  @override
  final String context;

  @override
  Map<String, dynamic>? signature;

  LikeActivity({
    required this.actor,
    required this.objectCid,
    DateTime? published,
    this.context = activityStreamsContext,
    this.signature,
  }) : published = published ?? DateTime.now().toUtc();

  @override
  Map<String, dynamic> toJson() {
    final Map<String, dynamic> json = {
      '@context': context,
      'type': type,
      'actor': actor,
      'object': objectCid.toString(), // Object is the CID of liked content
      'published': published.toIso8601String(),
    };
    if (signature != null) {
      json['signature'] = signature;
    }
    return json;
  }

  @override
  Uint8List toCanonicalBytesForSigning() {
    final Map<CborValue, CborValue> dataForSigning = {
      CborString('@context'): CborString(context),
      CborString('type'): CborString(type),
      CborString('actor'): CborString(actor),
      // Object is a CID, store as dag-cbor link (bytes with tag 42)
      CborString('object'): CborBytes(objectCid.toBytes(), tags: [42]),
      CborString('published'): CborString(published.toIso8601String()),
    };
    final encodedBytes = cbor.encode(CborMap(dataForSigning));
    if (encodedBytes is Uint8List) {
      return encodedBytes;
    } else {
      return Uint8List.fromList(encodedBytes);
    }
  }

  CborMap toCborMap() {
    final Map<CborValue, CborValue> cborData = {
      CborString('@context'): CborString(context),
      CborString('type'): CborString(type),
      CborString('actor'): CborString(actor),
      CborString('object'): CborBytes(objectCid.toBytes(), tags: [42]),
      CborString('published'): CborString(published.toIso8601String()),
    };
    if (signature != null) {
      final sigMap = signature!;
      final cborSigMapEntries = sigMap.entries.map((entry) =>
          MapEntry(CborString(entry.key), CborString(entry.value.toString())));
      cborData[CborString('signature')] = CborMap(Map.fromEntries(cborSigMapEntries));
    }
    return CborMap(cborData);
  }

  factory LikeActivity.fromJson(Map<String, dynamic> json) {
    if (json['type'] != 'Like') {
      throw ArgumentError('Invalid JSON: type is not Like.');
    }
    return LikeActivity(
      actor: json['actor'] as String,
      objectCid: CID.fromString(json['object'] as String), // Expecting a CID string
      published: DateTime.parse(json['published'] as String),
      context: json['@context'] as String? ?? activityStreamsContext,
      signature: json['signature'] as Map<String, dynamic>?,
    );
  }

  factory LikeActivity.fromCborMap(Map<dynamic, dynamic> cborMap) {
    final typeValue = cborMap[CborString('type')] as CborString?;
    if (typeValue?.toString() != 'Like') {
      throw ArgumentError('Invalid CBOR: type is not Like or missing.');
    }

    final actorValue = cborMap[CborString('actor')] as CborString?;
    final objectValue = cborMap[CborString('object')] as CborBytes?; // Expecting CID bytes
    final publishedValue = cborMap[CborString('published')] as CborString?;
    final contextValue = cborMap[CborString('@context')] as CborString?;

    if (actorValue == null) throw ArgumentError('Invalid CBOR: actor is missing.');
    if (objectValue == null) throw ArgumentError('Invalid CBOR: object (liked content CID) is missing.');
    if (publishedValue == null) throw ArgumentError('Invalid CBOR: published is missing.');
    
    final objectCid = CID.fromBytes(Uint8List.fromList(objectValue.bytes));

    Map<String, dynamic>? signatureMap;
    final signatureCborMap = cborMap[CborString('signature')] as CborMap?;
    if (signatureCborMap != null) {
      signatureMap = {};
      signatureCborMap.forEach((key, value) {
        if (key is CborString && value is CborString) {
          signatureMap![key.toString()] = value.toString();
        } else if (key is CborString) {
           signatureMap![key.toString()] = value.toObject();
        }
      });
    }

    return LikeActivity(
      actor: actorValue.toString(),
      objectCid: objectCid,
      published: DateTime.parse(publishedValue.toString()),
      context: contextValue?.toString() ?? activityStreamsContext,
      signature: signatureMap,
    );
  }

  @override
  String toString() {
    return 'LikeActivity(type: $type, actor: $actor, objectCid: ${objectCid.toString()}, published: $published, context: $context, signature: $signature)';
  }
}
