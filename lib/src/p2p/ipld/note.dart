import 'dart:convert' show jsonEncode;
import 'dart:typed_data';

import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:cbor/cbor.dart';

// const activityStreamsContext = 'https://www.w3.org/ns/activitystreams'; // Removed: Will use from activity.dart or direct default

class Mention {
  static const String type = 'Mention';
  final String href; // DID of the mentioned actor
  final String? name; // Optional: name of the mentioned actor

  Mention({required this.href, this.name});

  Map<String, dynamic> toJson() {
    final json = <String, dynamic>{
      'type': Mention.type,
      'href': href,
    };
    if (name != null) {
      json['name'] = name;
    }
    return json;
  }

  factory Mention.fromJson(Map<String, dynamic> json) {
    if (json['type'] != Mention.type) {
      throw ArgumentError('Invalid JSON: type is not Mention.');
    }
    return Mention(
      href: json['href'] as String,
      name: json['name'] as String?,
    );
  }

  CborMap toCborMap() {
    final Map<CborValue, CborValue> map = {
      CborString('type'): CborString(Mention.type),
      CborString('href'): CborString(href),
    };
    if (name != null) {
      map[CborString('name')] = CborString(name!);
    }
    return CborMap(map);
  }

  factory Mention.fromCborMap(Map<dynamic, dynamic> cborMap) {
    final type = cborMap[CborString('type')] as CborString?;
    if (type?.toString() != Mention.type) {
      throw ArgumentError('Invalid CBOR: type is not Mention or missing.');
    }
    final href = cborMap[CborString('href')] as CborString?;
    final name = cborMap[CborString('name')] as CborString?;

    if (href == null) {
      throw ArgumentError('Invalid CBOR: Mention href is missing.');
    }
    return Mention(
      href: href.toString(),
      name: name?.toString(),
    );
  }
}

class Note {
  static const String type = 'Note';

  final String content;
  final DateTime published;
  final String context;
  final List<Mention>? tag; // Optional list of mentions or other tags

  Note({
    required this.content,
    DateTime? published,
    this.context = 'https://www.w3.org/ns/activitystreams', // Defaulting directly
    this.tag,
  }) : published = published ?? DateTime.now().toUtc();

  Map<String, dynamic> toJson() {
    final json = <String, dynamic>{
      '@context': context,
      'type': Note.type,
      'content': content,
      'published': published.toIso8601String(),
    };
    if (tag != null && tag!.isNotEmpty) {
      json['tag'] = tag!.map((t) => t.toJson()).toList();
    }
    return json;
  }

  Uint8List toCanonicalBytes() {
    final map = <String, dynamic>{ // Use Map<String, dynamic> for jsonEncode
      '@context': context,
      'type': Note.type,
      'published': published.toIso8601String(),
      'content': content,
    };
    if (tag != null && tag!.isNotEmpty) {
      map['tag'] = tag!.map((t) => t.toJson()).toList();
    }
    // Ensure keys are sorted for canonical JSON before encoding
    // For simplicity, we're still relying on default ordering of top-level keys
    // but if 'tag' is present, its internal structure is defined by Mention.toJson()
    return Uint8List.fromList(jsonEncode(map).codeUnits);
  }

  factory Note.fromJson(Map<String, dynamic> json) {
    if (json['type'] != Note.type) {
      throw ArgumentError('Invalid JSON: type is not Note.');
    }
    List<Mention>? tags;
    if (json['tag'] != null && json['tag'] is List) {
      tags = (json['tag'] as List)
          .map((t) => Mention.fromJson(t as Map<String, dynamic>))
          .toList();
    }
    return Note(
      content: json['content'] as String,
      published: DateTime.parse(json['published'] as String),
      context: json['@context'] as String? ?? 'https://www.w3.org/ns/activitystreams',
      tag: tags,
    );
  }

  CborMap toCborMap() {
    final Map<CborValue, CborValue> map = {
      CborString('@context'): CborString(context),
      CborString('type'): CborString(Note.type),
      CborString('content'): CborString(content),
      CborString('published'): CborString(published.toIso8601String()),
    };
    if (tag != null && tag!.isNotEmpty) {
      map[CborString('tag')] = CborList(tag!.map((t) => t.toCborMap()).toList());
    }
    return CborMap(map);
  }

  factory Note.fromCborMap(Map<dynamic, dynamic> cborMap) {
    final type = cborMap[CborString('type')] as CborString?;
    if (type?.toString() != Note.type) {
      throw ArgumentError('Invalid CBOR: type is not Note or missing.');
    }

    final contextValue = cborMap[CborString('@context')] as CborString?;
    final contentValue = cborMap[CborString('content')] as CborString?;
    final publishedValue = cborMap[CborString('published')] as CborString?;
    final tagListValue = cborMap[CborString('tag')]; // Can be CborList or null

    if (contentValue == null) {
      throw ArgumentError('Invalid CBOR: content is missing.');
    }
    if (publishedValue == null) {
      throw ArgumentError('Invalid CBOR: published is missing.');
    }
    
    List<Mention>? tags;
    if (tagListValue != null && tagListValue is CborList) {
      tags = tagListValue.map((t) {
        if (t is CborMap) {
          // Convert CborMap to Map<dynamic,dynamic> for Mention.fromCborMap
          Map<dynamic,dynamic> dynamicMap = {};
          // Iterate over the CborMap entries directly
          t.forEach((key, value) {
            if (key is CborString) { // Ensure key is CborString
              dynamicMap[key.toString()] = value.toObject();
            } else {
              // Handle non-CborString keys if necessary, or throw error
              // For simplicity, assuming keys in Mention's CBOR map are CborString
            }
          });
          return Mention.fromCborMap(dynamicMap);
        }
        throw ArgumentError('Invalid CBOR: tag item is not a CborMap. Found ${t.runtimeType}');
      }).toList();
    }

    return Note(
      content: contentValue.toString(),
      published: DateTime.parse(publishedValue.toString()),
      context: contextValue?.toString() ?? 'https://www.w3.org/ns/activitystreams',
      tag: tags,
    );
  }
}
