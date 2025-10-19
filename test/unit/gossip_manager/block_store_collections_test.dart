// test/unit/gossip_manager/block_store_collections_test.dart
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:dcid/dcid.dart';
import 'package:merkledag/merkledag.dart';

import '../../../lib/src/gossip_manager/block_store.dart';
import '../../../lib/src/gossip_manager/field_extractor.dart';

// Test payload implementation
class TestPayload implements CRDTPayload<Map<String, dynamic>> {
  final Map<String, dynamic> _data;

  TestPayload(this._data);

  @override
  Map<String, dynamic> get value => _data;

  @override
  Uint8List toCanonicalBytes() {
    // Simple JSON-like serialization for testing
    final jsonString = _data.toString();
    return Uint8List.fromList(jsonString.codeUnits);
  }

  @override
  TestPayload merge(covariant TestPayload other) {
    final merged = Map<String, dynamic>.from(_data);
    merged.addAll(other._data);
    return TestPayload(merged);
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is TestPayload &&
          runtimeType == other.runtimeType &&
          _mapEquals(_data, other._data);

  @override
  int get hashCode => _data.hashCode;

  bool _mapEquals(Map<String, dynamic> a, Map<String, dynamic> b) {
    if (a.length != b.length) return false;
    for (final key in a.keys) {
      if (!b.containsKey(key) || a[key] != b[key]) return false;
    }
    return true;
  }
}

// Test field extractor for JSON-like payloads
class TestFieldExtractor implements FieldExtractor<TestPayload, Map<String, dynamic>> {
  @override
  dynamic extractField(MerkleNode<TestPayload> node, String fieldPath) {
    final parts = fieldPath.split('.');
    dynamic current = node.payload.value;
    
    for (final part in parts) {
      if (current is Map<String, dynamic>) {
        current = current[part];
      } else {
        return null;
      }
    }
    
    return current;
  }

  @override
  List<String> getSupportedFields() {
    return ['name', 'type', 'category', 'priority', 'tags', 'metadata.author', 'metadata.version'];
  }

  @override
  bool supportsField(String fieldPath) {
    return getSupportedFields().contains(fieldPath) || fieldPath.startsWith('metadata.');
  }

  @override
  Map<String, dynamic> extractFields(MerkleNode<TestPayload> node, List<String> fieldPaths) {
    final result = <String, dynamic>{};
    for (final fieldPath in fieldPaths) {
      result[fieldPath] = extractField(node, fieldPath);
    }
    return result;
  }
}

void main() {
  group('BlockStore Collections and Metadata Tests', () {
    late InMemoryBlockStore<TestPayload, Map<String, dynamic>> blockStore;
    late TestFieldExtractor fieldExtractor;

    setUp(() {
      blockStore = InMemoryBlockStore<TestPayload, Map<String, dynamic>>();
      fieldExtractor = TestFieldExtractor();
    });

    tearDown(() async {
      await blockStore.close();
    });

    group('Collection Management', () {
      test('should create collections implicitly', () async {
        await blockStore.createCollection('documents');
        final collections = await blockStore.listCollections();
        expect(collections, isEmpty); // Collections are created implicitly when blocks are added
      });

      test('should add blocks to collections', () async {
        final payload1 = TestPayload({'name': 'doc1', 'type': 'document'});
        final payload2 = TestPayload({'name': 'doc2', 'type': 'document'});
        
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});
        final node2 = MerkleNode<TestPayload>.create(payload2, <CID>{});

        await blockStore.putInCollection('documents', node1);
        await blockStore.putInCollection('images', node2);

        final collections = await blockStore.listCollections();
        expect(collections, containsAll(['documents', 'images']));
        expect(collections.length, equals(2));
      });

      test('should find blocks in specific collections', () async {
        final payload1 = TestPayload({'name': 'doc1', 'type': 'document'});
        final payload2 = TestPayload({'name': 'doc2', 'type': 'document'});
        final payload3 = TestPayload({'name': 'img1', 'type': 'image'});
        
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});
        final node2 = MerkleNode<TestPayload>.create(payload2, <CID>{});
        final node3 = MerkleNode<TestPayload>.create(payload3, <CID>{});

        await blockStore.putInCollection('documents', node1);
        await blockStore.putInCollection('documents', node2);
        await blockStore.putInCollection('images', node3);

        final documentsEntries = await blockStore.findInCollection('documents');
        final imagesEntries = await blockStore.findInCollection('images');

        expect(documentsEntries.length, equals(2));
        expect(imagesEntries.length, equals(1));
        
        expect(documentsEntries.every((e) => e.collection == 'documents'), isTrue);
        expect(imagesEntries.every((e) => e.collection == 'images'), isTrue);
      });

      test('should count blocks in collections', () async {
        final payload1 = TestPayload({'name': 'doc1'});
        final payload2 = TestPayload({'name': 'doc2'});
        final payload3 = TestPayload({'name': 'doc3'});
        
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});
        final node2 = MerkleNode<TestPayload>.create(payload2, <CID>{});
        final node3 = MerkleNode<TestPayload>.create(payload3, <CID>{});

        await blockStore.putInCollection('documents', node1);
        await blockStore.putInCollection('documents', node2);
        await blockStore.putInCollection('images', node3);

        final docCount = await blockStore.countInCollection('documents');
        final imgCount = await blockStore.countInCollection('images');
        final emptyCount = await blockStore.countInCollection('empty');

        expect(docCount, equals(2));
        expect(imgCount, equals(1));
        expect(emptyCount, equals(0));
      });

      test('should drop collections and soft-delete entries', () async {
        final payload1 = TestPayload({'name': 'doc1'});
        final payload2 = TestPayload({'name': 'doc2'});
        
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});
        final node2 = MerkleNode<TestPayload>.create(payload2, <CID>{});

        await blockStore.putInCollection('documents', node1);
        await blockStore.putInCollection('documents', node2);

        expect(await blockStore.countInCollection('documents'), equals(2));

        await blockStore.dropCollection('documents');

        expect(await blockStore.countInCollection('documents'), equals(0));
        expect(await blockStore.countInCollection('documents', includeTombstoned: true), equals(2));
        
        final collections = await blockStore.listCollections();
        expect(collections, isEmpty);
      });

      test('should move blocks between collections', () async {
        final payload = TestPayload({'name': 'doc1', 'type': 'document'});
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});

        await blockStore.putInCollection('documents', node);
        
        var entry = await blockStore.getEntry(node.cid);
        expect(entry?.collection, equals('documents'));

        await blockStore.moveToCollection(node.cid, 'archive');
        
        entry = await blockStore.getEntry(node.cid);
        expect(entry?.collection, equals('archive'));

        expect(await blockStore.countInCollection('documents'), equals(0));
        expect(await blockStore.countInCollection('archive'), equals(1));
      });

      test('should query within collections', () async {
        final payload1 = TestPayload({'name': 'doc1', 'type': 'important'});
        final payload2 = TestPayload({'name': 'doc2', 'type': 'normal'});
        final payload3 = TestPayload({'name': 'img1', 'type': 'important'});
        
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});
        final node2 = MerkleNode<TestPayload>.create(payload2, <CID>{});
        final node3 = MerkleNode<TestPayload>.create(payload3, <CID>{});

        await blockStore.putInCollection('documents', node1);
        await blockStore.putInCollection('documents', node2);
        await blockStore.putInCollection('images', node3);

        final importantDocs = await blockStore.findInCollectionWhere(
          'documents',
          fieldExtractor,
          (query) => query.where('type', 'important'),
        );

        expect(importantDocs.length, equals(1));
        expect(importantDocs.first.node?.payload.value['name'], equals('doc1'));
        expect(importantDocs.first.collection, equals('documents'));
      });
    });

    group('Metadata Management', () {
      test('should set and get metadata for blocks', () async {
        final payload = TestPayload({'name': 'doc1'});
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});

        await blockStore.put(node);
        
        final metadata = {'author': 'John Doe', 'version': '1.0', 'tags': ['important']};
        await blockStore.putMetadata(node.cid, metadata);

        final retrievedMetadata = await blockStore.getMetadata(node.cid);
        expect(retrievedMetadata, equals(metadata));
      });

      test('should merge metadata when adding new keys', () async {
        final payload = TestPayload({'name': 'doc1'});
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});

        await blockStore.put(node);
        
        await blockStore.putMetadata(node.cid, {'author': 'John Doe'});
        await blockStore.putMetadata(node.cid, {'version': '1.0'});

        final metadata = await blockStore.getMetadata(node.cid);
        expect(metadata, equals({'author': 'John Doe', 'version': '1.0'}));
      });

      test('should delete specific metadata keys', () async {
        final payload = TestPayload({'name': 'doc1'});
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});

        await blockStore.put(node);
        
        await blockStore.putMetadata(node.cid, {
          'author': 'John Doe',
          'version': '1.0',
          'tags': ['important', 'draft']
        });

        await blockStore.deleteMetadata(node.cid, ['version', 'tags']);

        final metadata = await blockStore.getMetadata(node.cid);
        expect(metadata, equals({'author': 'John Doe'}));
      });

      test('should clear all metadata', () async {
        final payload = TestPayload({'name': 'doc1'});
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});

        await blockStore.put(node);
        
        await blockStore.putMetadata(node.cid, {'author': 'John Doe', 'version': '1.0'});
        await blockStore.clearMetadata(node.cid);

        final metadata = await blockStore.getMetadata(node.cid);
        expect(metadata, equals({}));
      });

      test('should preserve metadata when updating blocks', () async {
        final payload1 = TestPayload({'name': 'doc1', 'version': 1});
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});

        await blockStore.putInCollection('documents', node1, metadata: {'author': 'John Doe'});
        
        var entry = await blockStore.getEntry(node1.cid);
        expect(entry?.metadata, equals({'author': 'John Doe'}));
        expect(entry?.collection, equals('documents'));

        // Update the same node with additional metadata - this tests metadata preservation
        await blockStore.putMetadata(node1.cid, {'version': '2.0', 'status': 'updated'});
        
        entry = await blockStore.getEntry(node1.cid);
        expect(entry?.metadata, equals({
          'author': 'John Doe',
          'version': '2.0', 
          'status': 'updated'
        })); // Metadata merged and preserved
        expect(entry?.collection, equals('documents')); // Collection preserved
        expect(entry?.node?.payload.value['version'], equals(1)); // Original payload unchanged
      });
    });

    group('Collection-based Queries', () {
      setUp(() async {
        // Set up test data
        final docs = [
          TestPayload({'name': 'doc1', 'type': 'important', 'priority': 1}),
          TestPayload({'name': 'doc2', 'type': 'normal', 'priority': 2}),
          TestPayload({'name': 'doc3', 'type': 'important', 'priority': 3}),
        ];
        
        final images = [
          TestPayload({'name': 'img1', 'type': 'important', 'priority': 1}),
          TestPayload({'name': 'img2', 'type': 'normal', 'priority': 2}),
        ];

        for (int i = 0; i < docs.length; i++) {
          final node = MerkleNode<TestPayload>.create(docs[i], <CID>{});
          await blockStore.putInCollection('documents', node);
        }

        for (int i = 0; i < images.length; i++) {
          final node = MerkleNode<TestPayload>.create(images[i], <CID>{});
          await blockStore.putInCollection('images', node);
        }
      });

      test('should filter by specific collection', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query.whereCollection('documents'),
        );

        expect(results.length, equals(3));
        expect(results.every((e) => e.collection == 'documents'), isTrue);
      });

      test('should filter by multiple collections', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query.whereCollectionIn(['documents', 'images']),
        );

        expect(results.length, equals(5));
        expect(results.every((e) => ['documents', 'images'].contains(e.collection)), isTrue);
      });

      test('should exclude specific collections', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query.whereCollectionNotIn(['images']),
        );

        expect(results.length, equals(3));
        expect(results.every((e) => e.collection != 'images'), isTrue);
      });

      test('should filter entries with any collection', () async {
        // Add a block without collection
        final payload = TestPayload({'name': 'orphan'});
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});
        await blockStore.put(node);

        final withCollection = await blockStore.findWhere(
          fieldExtractor,
          (query) => query.whereHasCollection(),
        );

        final withoutCollection = await blockStore.findWhere(
          fieldExtractor,
          (query) => query.whereNoCollection(),
        );

        expect(withCollection.length, equals(5)); // All collection entries
        expect(withoutCollection.length, equals(1)); // The orphan entry
        expect(withoutCollection.first.collection, isNull);
      });

      test('should combine collection and field filters', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query
              .whereCollection('documents')
              .where('type', 'important'),
        );

        expect(results.length, equals(2));
        expect(results.every((e) => e.collection == 'documents'), isTrue);
        expect(results.every((e) => e.node?.payload.value['type'] == 'important'), isTrue);
      });

      test('should sort collection results', () async {
        final results = await blockStore.findWhere(
          fieldExtractor,
          (query) => query
              .whereCollection('documents')
              .orderBy('priority'),
        );

        expect(results.length, equals(3));
        expect(results[0].node?.payload.value['priority'], equals(1));
        expect(results[1].node?.payload.value['priority'], equals(2));
        expect(results[2].node?.payload.value['priority'], equals(3));
      });
    });

    group('Integration with Existing Features', () {
      test('should maintain soft-delete semantics with collections', () async {
        final payload = TestPayload({'name': 'doc1'});
        final node = MerkleNode<TestPayload>.create( payload, <CID>{});

        await blockStore.putInCollection('documents', node, metadata: {'author': 'John'});
        
        expect(await blockStore.countInCollection('documents'), equals(1));

        await blockStore.softDelete(node.cid, reason: 'Test deletion');

        expect(await blockStore.countInCollection('documents'), equals(0));
        expect(await blockStore.countInCollection('documents', includeTombstoned: true), equals(1));

        final entry = await blockStore.getEntry(node.cid, includeTombstoned: true);
        expect(entry?.isTombstoned, isTrue);
        expect(entry?.collection, equals('documents')); // Collection preserved
        expect(entry?.metadata, equals({'author': 'John'})); // Metadata preserved
      });

      test('should work with batch operations', () async {
        final payloads = [
          TestPayload({'name': 'doc1'}),
          TestPayload({'name': 'doc2'}),
          TestPayload({'name': 'doc3'}),
        ];
        
        final nodes = payloads.map((p) => MerkleNode<TestPayload>.create(p, <CID>{})).toList();
        final nodeMap = {for (var node in nodes) node.cid: node};

        // First put them in collections individually
        for (var node in nodes) {
          await blockStore.putInCollection('documents', node);
        }

        // Then batch update them
        await blockStore.putAll(nodeMap);

        // Verify collections are preserved
        final entries = await blockStore.findInCollection('documents');
        expect(entries.length, equals(3));
        expect(entries.every((e) => e.collection == 'documents'), isTrue);
      });
    });
  });
}
