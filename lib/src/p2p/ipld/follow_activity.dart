import 'dart:convert' show jsonEncode;
import 'dart:typed_data';

import 'package:cbor/cbor.dart';
// Assuming activityStreamsContext is defined in activity.dart or accessible
// If not, it should be defined here or imported.
// For now, let's assume it's accessible via the activity.dart import.
import 'activity.dart'; // Provides Activity, activityStreamsContext

/// Represents a "Follow" activity in the ActivityPub-inspired model.
///
/// Schema: `{"@context": "...", "type": "Follow", "actor": "follower_did", "object": "followed_actor_did", "published": "..."}`
class FollowActivity implements Activity {
  @override
  final String type = 'Follow';

  @override
  final String actor; // DID of the actor performing the follow

  /// The DID of the actor being followed.
  final String objectDid;

  @override
  final DateTime published;

  @override
  final String context;

  @override
  Map<String, dynamic>? signature;

  FollowActivity({
    required this.actor,
    required this.objectDid, // Renamed from 'object' to be specific
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
      'object': objectDid, // This is the DID of the followed actor
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
      CborString('object'): CborString(objectDid), // Object is a DID (string)
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
      CborString('object'): CborString(objectDid),
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

  factory FollowActivity.fromJson(Map<String, dynamic> json) {
    if (json['type'] != 'Follow') {
      throw ArgumentError('Invalid JSON: type is not Follow.');
    }
    return FollowActivity(
      actor: json['actor'] as String,
      objectDid: json['object'] as String, // Expecting a DID string
      published: DateTime.parse(json['published'] as String),
      context: json['@context'] as String? ?? activityStreamsContext,
      signature: json['signature'] as Map<String, dynamic>?,
    );
  }

  factory FollowActivity.fromCborMap(Map<dynamic, dynamic> cborMap) {
    final typeValue = cborMap[CborString('type')] as CborString?;
    if (typeValue?.toString() != 'Follow') {
      throw ArgumentError('Invalid CBOR: type is not Follow or missing.');
    }

    final actorValue = cborMap[CborString('actor')] as CborString?;
    final objectValue = cborMap[CborString('object')] as CborString?; // Expecting a string (DID)
    final publishedValue = cborMap[CborString('published')] as CborString?;
    final contextValue = cborMap[CborString('@context')] as CborString?;

    if (actorValue == null) throw ArgumentError('Invalid CBOR: actor is missing.');
    if (objectValue == null) throw ArgumentError('Invalid CBOR: object (followed DID) is missing.');
    if (publishedValue == null) throw ArgumentError('Invalid CBOR: published is missing.');

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

    return FollowActivity(
      actor: actorValue.toString(),
      objectDid: objectValue.toString(),
      published: DateTime.parse(publishedValue.toString()),
      context: contextValue?.toString() ?? activityStreamsContext,
      signature: signatureMap,
    );
  }

  @override
  String toString() {
    return 'FollowActivity(type: $type, actor: $actor, objectDid: $objectDid, published: $published, context: $context, signature: $signature)';
  }
}
