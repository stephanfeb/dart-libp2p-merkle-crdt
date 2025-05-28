import 'dart:convert' show jsonEncode;
import 'dart:typed_data';

import 'package:dart_cid/dart_cid.dart' show CID;
// We'll need a way to get a CID from this data.
// This typically involves using something like MerkleNode from the 'merkledag' package,
// which would take the canonical bytes of this note's representation.
// For now, this class will focus on the data structure and its JSON representation.

const activityStreamsContext = 'https://www.w3.org/ns/activitystreams';

class Note {
  static const String type = 'Note';

  final String content;
  final DateTime published;
  final String context;

  Note({
    required this.content,
    DateTime? published,
    this.context = activityStreamsContext,
  }) : published = published ?? DateTime.now().toUtc();

  Map<String, dynamic> toJson() {
    return {
      '@context': context,
      'type': Note.type,
      'content': content,
      'published': published.toIso8601String(),
    };
  }

  // Helper to get bytes for CID generation (e.g., to pass to MerkleNode)
  // This should be the canonical representation. JSON key order matters for canonical bytes.
  // For simplicity, we rely on jsonEncode's default ordering here,
  // but a true canonical form might require sorting keys.
  Uint8List toCanonicalBytes() {
    // A more robust canonical JSON would sort keys.
    // For now, using a fixed structure and relying on default jsonEncode.
    final map = {
      '@context': context,
      'type': Note.type, // Ensure consistent key order
      'published': published.toIso8601String(),
      'content': content,
    };
    return Uint8List.fromList(jsonEncode(map).codeUnits);
  }

  factory Note.fromJson(Map<String, dynamic> json) {
    if (json['type'] != Note.type) {
      throw ArgumentError('Invalid JSON: type is not Note.');
    }
    return Note(
      content: json['content'] as String,
      published: DateTime.parse(json['published'] as String),
      context: json['@context'] as String? ?? activityStreamsContext,
    );
  }

  // Placeholder for getting a CID. In a real scenario, this would involve
  // creating a MerkleNode with this note's canonical bytes and a CID function.
  // Future<CID> getCid(Future<CID> Function(Uint8List) cidFunc) async {
  //   return await cidFunc(toCanonicalBytes());
  // }
}
