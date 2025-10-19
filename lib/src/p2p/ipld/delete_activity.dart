import 'dart:typed_data';

import 'package:dcid/dcid.dart' show CID;
import 'package:cbor/cbor.dart';

import 'activity.dart'; // For activityStreamsContext and Activity interface

class DeleteActivity implements Activity {
  @override
  final String type = 'Delete';
  @override
  final String actor; // DID of the actor performing the delete
  final CID object; // CID of the content being deleted (could be a Note's CID or an Activity's CID)
  @override
  final DateTime published;
  @override
  final String context;
  @override
  Map<String, dynamic>? signature;

  DeleteActivity({
    required this.actor,
    required this.object,
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
      'object': object.toString(), // Store CID as string
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
      CborString('object'): CborBytes(object.toBytes(), tags: [42]), // dag-cbor link
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
      CborString('object'): CborBytes(object.toBytes(), tags: [42]), // dag-cbor link
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

  factory DeleteActivity.fromJson(Map<String, dynamic> json) {
    if (json['type'] != 'Delete') {
      throw ArgumentError('Invalid JSON: type is not Delete.');
    }
    return DeleteActivity(
      actor: json['actor'] as String,
      object: CID.fromString(json['object'] as String),
      published: DateTime.parse(json['published'] as String),
      context: json['@context'] as String? ?? activityStreamsContext,
      signature: json['signature'] as Map<String, dynamic>?,
    );
  }

  factory DeleteActivity.fromCborMap(Map<dynamic, dynamic> cborMap) {
    final type = cborMap[CborString('type')] as CborString?;
    if (type?.toString() != 'Delete') {
      throw ArgumentError('Invalid CBOR: type is not Delete or missing.');
    }

    final actorValue = cborMap[CborString('actor')] as CborString?;
    final objectValue = cborMap[CborString('object')] as CborBytes?;
    final publishedValue = cborMap[CborString('published')] as CborString?;
    final contextValue = cborMap[CborString('@context')] as CborString?;
    
    if (actorValue == null) throw ArgumentError('Invalid CBOR: actor is missing.');
    if (objectValue == null) throw ArgumentError('Invalid CBOR: object is missing.');
    if (publishedValue == null) throw ArgumentError('Invalid CBOR: published is missing.');

    final objectCid = CID.fromBytes(Uint8List.fromList(objectValue.bytes));
    
    Map<String, dynamic>? signatureMap;
    final signatureCborMap = cborMap[CborString('signature')] as CborMap?;
    if (signatureCborMap != null) {
      signatureMap = {};
      signatureCborMap.forEach((key, value) {
        if (key is CborString) {
          signatureMap![key.toString()] = value.toObject().toString();
        }
      });
    }

    return DeleteActivity(
      actor: actorValue.toString(),
      object: objectCid,
      published: DateTime.parse(publishedValue.toString()),
      context: contextValue?.toString() ?? activityStreamsContext,
      signature: signatureMap,
    );
  }
}
