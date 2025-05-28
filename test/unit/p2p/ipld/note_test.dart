import 'dart:convert' show jsonEncode, jsonDecode;
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:dart_libp2p_merkle_crdt/src/p2p/ipld/note.dart';
import 'package:cbor/cbor.dart'; // Import for CborValue types

void main() {
  group('Note IPLD Tests', () {
    test('Creation with default published and context', () {
      final now = DateTime.now().toUtc();
      final note = Note(content: 'Hello');
      
      expect(note.content, equals('Hello'));
      expect(note.context, equals(activityStreamsContext));
      expect(Note.type, equals('Note')); // Access static member via class
      // Check if published time is recent (within a small delta)
      expect(note.published.isAfter(now.subtract(Duration(seconds: 1))), isTrue);
      expect(note.published.isBefore(now.add(Duration(seconds: 1))), isTrue);
    });

    test('Creation with specified published and context', () {
      final publishedTime = DateTime.utc(2023, 1, 1, 10, 0, 0);
      const customContext = 'https://example.com/custom-context';
      final note = Note(
        content: 'Custom content',
        published: publishedTime,
        context: customContext,
      );

      expect(note.content, equals('Custom content'));
      expect(note.context, equals(customContext));
      expect(note.published, equals(publishedTime));
    });

    test('toJson produces correct map structure', () {
      final publishedTime = DateTime.utc(2023, 5, 15, 12, 30, 0);
      final note = Note(content: 'Test content', published: publishedTime);
      final json = note.toJson();

      expect(json['@context'], equals(activityStreamsContext));
      expect(json['type'], equals('Note'));
      expect(json['content'], equals('Test content'));
      expect(json['published'], equals(publishedTime.toIso8601String()));
    });

    test('toCanonicalBytes produces non-empty Uint8List', () {
      final note = Note(content: 'Some data');
      final bytes = note.toCanonicalBytes();
      expect(bytes, isA<Uint8List>());
      expect(bytes.isNotEmpty, isTrue);
    });

    test('toCanonicalBytes is consistent for same input', () {
      final note1 = Note(content: 'Consistent data', published: DateTime.utc(2023,1,1));
      final note2 = Note(content: 'Consistent data', published: DateTime.utc(2023,1,1));
      expect(note1.toCanonicalBytes(), equals(note2.toCanonicalBytes()));
    });
    
    test('toCanonicalBytes differs for different input', () {
      final note1 = Note(content: 'Data A', published: DateTime.utc(2023,1,1));
      final note2 = Note(content: 'Data B', published: DateTime.utc(2023,1,1));
      expect(note1.toCanonicalBytes(), isNot(equals(note2.toCanonicalBytes())));
    });


    test('fromJson creates Note object correctly', () {
      final publishedTime = DateTime.utc(2023, 10, 20, 8, 45, 0);
      final jsonMap = {
        '@context': activityStreamsContext,
        'type': 'Note',
        'content': 'Deserialized content',
        'published': publishedTime.toIso8601String(),
      };

      final note = Note.fromJson(jsonMap);
      expect(note.content, equals('Deserialized content'));
      expect(note.published, equals(publishedTime));
      expect(note.context, equals(activityStreamsContext));
      expect(Note.type, equals('Note')); // Access static member via class
    });

    test('fromJson throws ArgumentError for incorrect type', () {
      final jsonMap = {
        '@context': activityStreamsContext,
        'type': 'Article', // Incorrect type
        'content': 'Some content',
        'published': DateTime.now().toIso8601String(),
      };
      expect(() => Note.fromJson(jsonMap), throwsArgumentError);
    });

    test('fromJson uses default context if not provided', () {
       final publishedTime = DateTime.utc(2023, 10, 20, 8, 45, 0);
      final jsonMap = {
        // '@context': activityStreamsContext, // Context missing
        'type': 'Note',
        'content': 'Content without explicit context',
        'published': publishedTime.toIso8601String(),
      };
       final note = Note.fromJson(jsonMap);
       expect(note.context, equals(activityStreamsContext));
    });

    test('toCborMap produces correct CborMap structure', () {
      final publishedTime = DateTime.utc(2023, 5, 15, 12, 30, 0);
      const noteContent = 'Test CBOR content';
      const customContext = 'https://example.com/custom-cbor-context';

      final note = Note(
        content: noteContent,
        published: publishedTime,
        context: customContext,
      );
      final cborMap = note.toCborMap();

      expect(cborMap, isA<CborMap>());
      // Access CborMap elements using operator[]
      expect(cborMap[CborString('@context')], equals(CborString(customContext)));
      expect(cborMap[CborString('type')], equals(CborString(Note.type)));
      expect(cborMap[CborString('content')], equals(CborString(noteContent)));
      expect(cborMap[CborString('published')], equals(CborString(publishedTime.toIso8601String())));
    });
  });
}
