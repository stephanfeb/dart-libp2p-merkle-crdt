// test/unit/gossip_manager/block_store_query_test.dart
import 'dart:convert';
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:dart_cid/dart_cid.dart';
import 'package:merkledag/merkledag.dart';
import 'package:dart_libp2p_merkle_crdt/src/gossip_manager/block_store.dart';
import 'package:dart_libp2p_merkle_crdt/src/gossip_manager/field_extractor.dart';
import 'package:dart_libp2p_merkle_crdt/src/gossip_manager/block_query.dart';
import 'package:dart_libp2p_merkle_crdt/src/gossip_manager/pagination.dart';

// Mock payload for testing
class TestPayload extends CRDTPayload<Map<String, dynamic>> {
  @override
  final Map<String, dynamic> value;

  TestPayload(this.value);

  @override
  TestPayload merge(covariant TestPayload other) {
    // Simple merge strategy - combine maps with other taking precedence
    final merged = Map<String, dynamic>.from(value);
    merged.addAll(other.value);
    return TestPayload(merged);
  }

  @override
  Uint8List toCanonicalBytes() {
    // Convert to JSON and then to bytes for canonical representation
    final jsonString = jsonEncode(value);
    return Uint8List.fromList(utf8.encode(jsonString));
  }

  static TestPayload fromBytes(Uint8List bytes) {
    final jsonString = utf8.decode(bytes);
    final map = jsonDecode(jsonString) as Map<String, dynamic>;
    return TestPayload(map);
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is TestPayload &&
          runtimeType == other.runtimeType &&
          jsonEncode(value) == jsonEncode(other.value);

  @override
  int get hashCode => jsonEncode(value).hashCode;
}

// Test field extractor for JSON-like payloads
class TestFieldExtractor extends JsonFieldExtractor<TestPayload, Map<String, dynamic>> {
  TestFieldExtractor() : super(payloadToJson: (payload) => payload.value);

  @override
  List<String> computeSupportedFields() {
    return [
      'id',
      'type',
      'author',
      'content',
      'timestamp',
      'metadata',
      'metadata.created',
      'metadata.updated',
      'tags',
      'tags.0',
      'tags.1',
    ];
  }
}

// Helper function to create MerkleNode from payload
MerkleNode<TestPayload> createTestNode(Map<String, dynamic> data) {
  final payload = TestPayload(data);
  return MerkleNode.create(payload, <CID>{});
}

void main() {
  group('BlockStore Query System Tests', () {
    late InMemoryBlockStore<TestPayload, Map<String, dynamic>> blockStore;
    late TestFieldExtractor fieldExtractor;

    setUp(() async {
      blockStore = InMemoryBlockStore<TestPayload, Map<String, dynamic>>();
      fieldExtractor = TestFieldExtractor();
      await blockStore.open();
    });

    tearDown(() async {
      await blockStore.close();
    });

    group('Field Extraction Tests', () {
      test('should extract simple fields', () async {
        final node = createTestNode({
          'id': 'test-1',
          'type': 'note',
          'author': 'alice',
          'content': 'Hello world',
        });

        expect(fieldExtractor.extractField(node, 'id'), equals('test-1'));
        expect(fieldExtractor.extractField(node, 'type'), equals('note'));
        expect(fieldExtractor.extractField(node, 'author'), equals('alice'));
        expect(fieldExtractor.extractField(node, 'content'), equals('Hello world'));
      });

      test('should extract nested fields', () async {
        final node = createTestNode({
          'id': 'test-2',
          'metadata': {
            'created': '2023-01-01T00:00:00Z',
            'updated': '2023-01-02T00:00:00Z',
          },
        });

        expect(fieldExtractor.extractField(node, 'metadata.created'), 
               equals('2023-01-01T00:00:00Z'));
        expect(fieldExtractor.extractField(node, 'metadata.updated'), 
               equals('2023-01-02T00:00:00Z'));
      });

      test('should return null for non-existent fields', () async {
        final node = createTestNode({'id': 'test-3'});

        expect(fieldExtractor.extractField(node, 'nonexistent'), isNull);
        expect(fieldExtractor.extractField(node, 'metadata.missing'), isNull);
      });
    });

    group('Basic Query Tests', () {
      setUp(() async {
        // Add test data
        final testData = [
          {'id': 'note-1', 'type': 'note', 'author': 'alice', 'content': 'Hello world', 'timestamp': 1000},
          {'id': 'note-2', 'type': 'note', 'author': 'bob', 'content': 'Goodbye world', 'timestamp': 2000},
          {'id': 'post-1', 'type': 'post', 'author': 'alice', 'content': 'My first post', 'timestamp': 1500},
          {'id': 'comment-1', 'type': 'comment', 'author': 'charlie', 'content': 'Nice post!', 'timestamp': 1600},
        ];

        for (int i = 0; i < testData.length; i++) {
          final node = createTestNode(testData[i]);
          await blockStore.put(node);
        }
      });

      test('should find entries by equality', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query.where('type', 'note'),
        );

        expect(results.length, equals(2));
        expect(results.every((entry) => 
          fieldExtractor.extractField(entry.node!, 'type') == 'note'), isTrue);
      });

      test('should find entries by author', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query.where('author', 'alice'),
        );

        expect(results.length, equals(2));
        expect(results.every((entry) => 
          fieldExtractor.extractField(entry.node!, 'author') == 'alice'), isTrue);
      });

      test('should find entries with greater than condition', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query.whereGreaterThan('timestamp', 1500),
        );

        expect(results.length, equals(2));
        expect(results.every((entry) => 
          (fieldExtractor.extractField(entry.node!, 'timestamp') as int) > 1500), isTrue);
      });

      test('should find entries with contains condition', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query.whereContains('content', 'world'),
        );

        expect(results.length, equals(2));
        expect(results.every((entry) => 
          fieldExtractor.extractField(entry.node!, 'content').toString().contains('world')), isTrue);
      });

      test('should find entries with IN condition', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query.whereIn('type', ['note', 'post']),
        );

        expect(results.length, equals(3));
        expect(results.every((entry) {
          final type = fieldExtractor.extractField(entry.node!, 'type');
          return type == 'note' || type == 'post';
        }), isTrue);
      });
    });

    group('Complex Query Tests', () {
      setUp(() async {
        // Add more complex test data
        final testData = [
          {'id': 'item-1', 'type': 'note', 'author': 'alice', 'priority': 1, 'tags': ['urgent', 'work']},
          {'id': 'item-2', 'type': 'note', 'author': 'bob', 'priority': 2, 'tags': ['personal', 'reminder']},
          {'id': 'item-3', 'type': 'post', 'author': 'alice', 'priority': 1, 'tags': ['work', 'announcement']},
          {'id': 'item-4', 'type': 'comment', 'author': 'charlie', 'priority': 3, 'tags': ['feedback']},
        ];

        for (int i = 0; i < testData.length; i++) {
          final node = createTestNode(testData[i]);
          await blockStore.put(node);
        }
      });

      test('should handle complex AND conditions', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query
            .where('author', 'alice')
            .where('priority', 1),
        );

        expect(results.length, equals(2));
        expect(results.every((entry) => 
          fieldExtractor.extractField(entry.node!, 'author') == 'alice' &&
          fieldExtractor.extractField(entry.node!, 'priority') == 1), isTrue);
      });

      test('should handle custom predicates', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query.whereCustom((entry) {
            final priority = fieldExtractor.extractField(entry.node!, 'priority') as int?;
            return priority != null && priority <= 2;
          }),
        );

        expect(results.length, equals(3));
      });

      test('should handle payload-based predicates', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query.wherePayload((payload) {
            return payload.value['author'] == 'alice';
          }),
        );

        expect(results.length, equals(2));
      });
    });

    group('Sorting and Pagination Tests', () {
      setUp(() async {
        // Add data for sorting tests
        final testData = [
          {'id': 'sort-1', 'name': 'Charlie', 'score': 85, 'date': '2023-01-03'},
          {'id': 'sort-2', 'name': 'Alice', 'score': 92, 'date': '2023-01-01'},
          {'id': 'sort-3', 'name': 'Bob', 'score': 78, 'date': '2023-01-02'},
          {'id': 'sort-4', 'name': 'David', 'score': 95, 'date': '2023-01-04'},
          {'id': 'sort-5', 'name': 'Eve', 'score': 88, 'date': '2023-01-05'},
        ];

        for (int i = 0; i < testData.length; i++) {
          final node = createTestNode(testData[i]);
          await blockStore.put(node);
        }
      });

      test('should sort results by name ascending', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query
            .whereNotNull('name')
            .orderBy('name', SortDirection.ascending),
        );

        expect(results.length, equals(5));
        final names = results.map((entry) => 
          fieldExtractor.extractField(entry.node!, 'name')).toList();
        expect(names, equals(['Alice', 'Bob', 'Charlie', 'David', 'Eve']));
      });

      test('should sort results by score descending', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query
            .whereNotNull('score')
            .orderBy('score', SortDirection.descending),
        );

        expect(results.length, equals(5));
        final scores = results.map((entry) => 
          fieldExtractor.extractField(entry.node!, 'score')).toList();
        expect(scores, equals([95, 92, 88, 85, 78]));
      });

      test('should handle pagination', () async {
        final pageRequest = PageRequest(page: 0, size: 2);
        final pageResult = await blockStore.findPage(
          fieldExtractor,
          (query) => query
            .whereNotNull('name')
            .orderBy('name'),
          pageRequest,
        );

        expect(pageResult.content.length, equals(2));
        expect(pageResult.totalElements, equals(5));
        expect(pageResult.totalPages, equals(3));
        expect(pageResult.isFirst, isTrue);
        expect(pageResult.isLast, isFalse);
        expect(pageResult.hasNext, isTrue);
        expect(pageResult.hasPrevious, isFalse);

        final names = pageResult.content.map((entry) => 
          fieldExtractor.extractField(entry.node!, 'name')).toList();
        expect(names, equals(['Alice', 'Bob']));
      });

      test('should handle second page', () async {
        final pageRequest = PageRequest(page: 1, size: 2);
        final pageResult = await blockStore.findPage(
          fieldExtractor,
          (query) => query
            .whereNotNull('name')
            .orderBy('name'),
          pageRequest,
        );

        expect(pageResult.content.length, equals(2));
        expect(pageResult.pageNumber, equals(1));
        expect(pageResult.isFirst, isFalse);
        expect(pageResult.isLast, isFalse);
        expect(pageResult.hasNext, isTrue);
        expect(pageResult.hasPrevious, isTrue);

        final names = pageResult.content.map((entry) => 
          fieldExtractor.extractField(entry.node!, 'name')).toList();
        expect(names, equals(['Charlie', 'David']));
      });
    });

    group('Query Utility Methods Tests', () {
      setUp(() async {
        // Add test data
        final testData = [
          {'id': 'util-1', 'type': 'active', 'status': 'published'},
          {'id': 'util-2', 'type': 'active', 'status': 'draft'},
          {'id': 'util-3', 'type': 'inactive', 'status': 'archived'},
        ];

        for (int i = 0; i < testData.length; i++) {
          final node = createTestNode(testData[i]);
          await blockStore.put(node);
        }
      });

      test('should count matching entries', () async {
        final count = await blockStore.count(
          fieldExtractor,
          (query) => query.where('type', 'active'),
        );

        expect(count, equals(2));
      });

      test('should check if entries exist', () async {
        final exists = await blockStore.exists(
          fieldExtractor,
          (query) => query.where('status', 'published'),
        );

        expect(exists, isTrue);

        final notExists = await blockStore.exists(
          fieldExtractor,
          (query) => query.where('status', 'deleted'),
        );

        expect(notExists, isFalse);
      });

      test('should find first matching entry', () async {
        final result = await blockStore.findOneWhere(
          fieldExtractor,
          (query) => query
            .where('type', 'active')
            .orderBy('id'),
        );

        expect(result, isNotNull);
        expect(fieldExtractor.extractField(result!.node!, 'id'), equals('util-1'));
      });

      test('should return null when no entries match', () async {
        final result = await blockStore.findOneWhere(
          fieldExtractor,
          (query) => query.where('type', 'nonexistent'),
        );

        expect(result, isNull);
      });
    });

    group('Query Builder API Tests', () {
      test('should create query builder', () {
        final query = blockStore.query(fieldExtractor);
        expect(query, isA<BlockQuery<TestPayload, Map<String, dynamic>>>());
      });

      test('should chain query conditions', () async {
        // Add test data
        final node = createTestNode({'id': 'chain-1', 'type': 'test', 'priority': 1});
        await blockStore.put(node);

        final query = blockStore.query(fieldExtractor)
          .where('type', 'test')
          .whereGreaterThan('priority', 0)
          .orderBy('id')
          .limit(10);

        final results = await query.execute();
        expect(results.length, equals(1));
        expect(fieldExtractor.extractField(results.first.node!, 'id'), equals('chain-1'));
      });
    });
  });
}
