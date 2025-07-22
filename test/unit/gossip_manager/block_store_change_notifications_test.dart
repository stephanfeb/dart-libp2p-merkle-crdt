// test/unit/gossip_manager/block_store_change_notifications_test.dart
import 'dart:async';
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:dart_cid/dart_cid.dart';
import 'package:merkledag/merkledag.dart';

import '../../../lib/src/gossip_manager/block_store.dart';
import '../../../lib/src/gossip_manager/block_change_event.dart';
import '../../../lib/src/gossip_manager/change_subscription.dart';
import '../../../lib/src/gossip_manager/field_extractor.dart';

// Test payload implementation
class TestPayload implements CRDTPayload<String> {
  final String _value;
  
  TestPayload(this._value);
  
  @override
  String get value => _value;
  
  @override
  Uint8List toCanonicalBytes() => Uint8List.fromList(_value.codeUnits);
  
  @override
  TestPayload merge(covariant TestPayload other) {
    return TestPayload(_value + other._value);
  }
  
  @override
  bool operator ==(Object other) {
    return other is TestPayload && other._value == _value;
  }
  
  @override
  int get hashCode => _value.hashCode;
  
  @override
  String toString() => 'TestPayload($_value)';
}

// Simple field extractor for testing
class TestFieldExtractor extends FieldExtractor<TestPayload, String> {
  @override
  dynamic extractField(MerkleNode<TestPayload> node, String fieldPath) {
    switch (fieldPath) {
      case 'value':
        return node.payload.value;
      case 'cid':
        return node.cid.toString();
      default:
        throw ArgumentError('Unknown field: $fieldPath');
    }
  }
  
  @override
  List<String> getSupportedFields() => ['value', 'cid'];
  
  @override
  bool supportsField(String fieldPath) => getSupportedFields().contains(fieldPath);
  
  @override
  Map<String, dynamic> extractFields(MerkleNode<TestPayload> node, List<String> fieldPaths) {
    final result = <String, dynamic>{};
    for (final path in fieldPaths) {
      if (supportsField(path)) {
        result[path] = extractField(node, path);
      }
    }
    return result;
  }
}

void main() {
  group('BlockStore Change Notifications', () {
    late InMemoryBlockStore<TestPayload, String> blockStore;
    late TestFieldExtractor fieldExtractor;
    
    setUp(() {
      blockStore = InMemoryBlockStore<TestPayload, String>();
      fieldExtractor = TestFieldExtractor();
    });
    
    tearDown(() async {
      await blockStore.close();
    });
    
    group('Basic Change Events', () {
      test('should emit insert event when adding new block', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        final subscription = blockStore.subscribe();
        subscription.stream.listen(events.add);
        
        final payload = TestPayload('test-data');
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});

        await blockStore.put(node);
        
        expect(events, hasLength(1));
        final event = events.first;
        expect(event.type, equals(BlockChangeType.inserted));
        expect(event.cid, equals(node.cid));
        expect(event.node, equals(node));
        expect(event.previousNode, isNull);
        expect(event.collection, isNull);
        expect(event.metadata, isEmpty);
      });
      
      test('should emit update event when updating existing block', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        final subscription = blockStore.subscribe();
        subscription.stream.listen(events.add);
        
        final payload1 = TestPayload('test-data-1');
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});
        await blockStore.put(node1);
        
        events.clear(); // Clear insert event
        
        // Create a new node with the same CID but different payload to simulate an update
        final payload2 = TestPayload('test-data-2');
        final node2 = MerkleNode<TestPayload>(
          payload: payload2,
          cid: node1.cid, // Use the same CID to trigger update logic
          children: <CID>{},
        );
        await blockStore.put(node2);
        
        expect(events, hasLength(1));
        final event = events.first;
        expect(event.type, equals(BlockChangeType.updated));
        expect(event.cid, equals(node1.cid));
        expect(event.node, equals(node2));
        expect(event.previousNode, equals(node1));
      });
      
      test('should emit delete event when soft-deleting block', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        final subscription = blockStore.subscribe();
        subscription.stream.listen(events.add);
        
        final payload = TestPayload('test-data');
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});
        await blockStore.put(node);
        
        events.clear(); // Clear insert event
        
        await blockStore.softDelete(node.cid, reason: 'test-reason', deletedBy: 'test-user');
        
        expect(events, hasLength(1));
        final event = events.first;
        expect(event.type, equals(BlockChangeType.deleted));
        expect(event.cid, equals(node.cid));
        expect(event.node, isNull);
        expect(event.previousNode, equals(node));
        expect(event.context['reason'], equals('test-reason'));
        expect(event.context['deletedBy'], equals('test-user'));
      });
      
      test('should emit metadata update event when updating metadata', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        final subscription = blockStore.subscribe();
        subscription.stream.listen(events.add);
        
        final payload = TestPayload('test-data');
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});
        await blockStore.put(node);
        
        events.clear(); // Clear insert event
        
        final metadata = {'key1': 'value1', 'key2': 42};
        await blockStore.putMetadata(node.cid, metadata);
        
        expect(events, hasLength(1));
        final event = events.first;
        expect(event.type, equals(BlockChangeType.metadataUpdated));
        expect(event.cid, equals(node.cid));
        expect(event.node, equals(node));
        expect(event.metadata, equals(metadata));
        expect(event.previousMetadata, isEmpty);
      });
      
      test('should emit collection change event when moving to collection', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        final subscription = blockStore.subscribe();
        subscription.stream.listen(events.add);
        
        final payload = TestPayload('test-data');
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});
        await blockStore.put(node);
        
        events.clear(); // Clear insert event
        
        await blockStore.moveToCollection(node.cid, 'test-collection');
        
        expect(events, hasLength(1));
        final event = events.first;
        expect(event.type, equals(BlockChangeType.collectionChanged));
        expect(event.cid, equals(node.cid));
        expect(event.node, equals(node));
        expect(event.collection, equals('test-collection'));
        expect(event.previousCollection, isNull);
      });
    });
    
    group('Change Filters', () {
      test('should filter events by change type', () async {
        final insertEvents = <BlockChangeEvent<TestPayload, String>>[];
        final deleteEvents = <BlockChangeEvent<TestPayload, String>>[];
        final allEvents = <BlockChangeEvent<TestPayload, String>>[];
        
        final insertFilter = ChangeTypeFilter<TestPayload, String>(BlockChangeType.inserted);
        final deleteFilter = ChangeTypeFilter<TestPayload, String>(BlockChangeType.deleted);
        
        // Debug: Test the filters directly
        print('Testing filters directly...');
        final nodeA = MerkleNode<TestPayload>.create(TestPayload('test'), <CID>{});
        final testInsertEvent = BlockChangeEvent<TestPayload, String>.inserted(
          cid: nodeA.cid,
          node: nodeA,
        );
        final testDeleteEvent = BlockChangeEvent<TestPayload, String>.deleted(
          cid: nodeA.cid
        );
        
        print('Insert filter matches insert event: ${insertFilter.matches(testInsertEvent)}');
        print('Insert filter matches delete event: ${insertFilter.matches(testDeleteEvent)}');
        print('Delete filter matches insert event: ${deleteFilter.matches(testInsertEvent)}');
        print('Delete filter matches delete event: ${deleteFilter.matches(testDeleteEvent)}');
        
        final insertSub = blockStore.subscribe(filter: insertFilter);
        final deleteSub = blockStore.subscribe(filter: deleteFilter);
        final allSub = blockStore.subscribe(); // No filter to see all events
        
        insertSub.stream.listen((event) {
          print('Insert filter received event: ${event.type}');
          insertEvents.add(event);
        });
        deleteSub.stream.listen((event) {
          print('Delete filter received event: ${event.type}');
          deleteEvents.add(event);
        });
        allSub.stream.listen((event) {
          print('All events received event: ${event.type}');
          allEvents.add(event);
        });
        
        final payload = TestPayload('test-data');
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});

        await blockStore.put(node);
        
        // Give a small delay to ensure events are processed
        await Future.delayed(Duration(milliseconds: 1));
        
        await blockStore.softDelete(node.cid);
        
        // Give a small delay to ensure events are processed
        await Future.delayed(Duration(milliseconds: 1));
        
        // Debug: Check what events we actually received
        print('All events received: ${allEvents.length}');
        for (final event in allEvents) {
          print('Event type: ${event.type}');
        }
        print('Insert events: ${insertEvents.length}');
        print('Delete events: ${deleteEvents.length}');
        
        expect(allEvents, hasLength(2)); // Should have insert and delete
        expect(insertEvents, hasLength(1));
        expect(insertEvents.first.type, equals(BlockChangeType.inserted));
        
        expect(deleteEvents, hasLength(1));
        expect(deleteEvents.first.type, equals(BlockChangeType.deleted));
      });
      
      test('should filter events by CID', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        
        final payload1 = TestPayload('test-data-1');
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});

        final payload2 = TestPayload('test-data-2');
        final node2 = MerkleNode<TestPayload>.create(payload2, <CID>{});

        final filter = CidFilter<TestPayload, String>(node1.cid);
        final subscription = blockStore.subscribe(filter: filter);
        subscription.stream.listen(events.add);
        
        await blockStore.put(node1);
        await blockStore.put(node2);
        
        expect(events, hasLength(1));
        expect(events.first.cid, equals(node1.cid));
      });
      
      test('should filter events by collection', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        
        final filter = CollectionFilter<TestPayload, String>('test-collection');
        final subscription = blockStore.subscribe(filter: filter);
        subscription.stream.listen(events.add);
        
        final payload1 = TestPayload('test-data-1');
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});

        final payload2 = TestPayload('test-data-2');
        final node2 = MerkleNode<TestPayload>.create(payload2, <CID>{});

        await blockStore.putInCollection('test-collection', node1);
        await blockStore.putInCollection('other-collection', node2);
        
        expect(events, hasLength(1));
        expect(events.first.cid, equals(node1.cid));
        expect(events.first.collection, equals('test-collection'));
      });
      
      test('should filter events using custom predicate', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        
        final filter = PredicateFilter<TestPayload, String>(
          (event) => event.node?.payload.value.contains('special') ?? false
        );
        final subscription = blockStore.subscribe(filter: filter);
        subscription.stream.listen(events.add);
        
        final payload1 = TestPayload('normal-data');
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});

        final payload2 = TestPayload('special-data');
        final node2 = MerkleNode<TestPayload>.create(payload2, <CID>{});

        await blockStore.put(node1);
        await blockStore.put(node2);
        
        expect(events, hasLength(1));
        expect(events.first.node?.payload.value, equals('special-data'));
      });
      
      test('should combine filters with AND logic', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        
        final typeFilter = ChangeTypeFilter<TestPayload, String>(BlockChangeType.inserted);
        final collectionFilter = CollectionFilter<TestPayload, String>('test-collection');
        final combinedFilter = typeFilter.and(collectionFilter);
        
        final subscription = blockStore.subscribe(filter: combinedFilter);
        subscription.stream.listen(events.add);
        
        final payload1 = TestPayload('test-data-1');
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});

        final payload2 = TestPayload('test-data-2');
        final node2 = MerkleNode<TestPayload>.create(payload2, <CID>{});

        // This should match (insert + test-collection)
        await blockStore.putInCollection('test-collection', node1);
        
        // This should not match (insert + other-collection)
        await blockStore.putInCollection('other-collection', node2);
        
        // This should not match (delete + test-collection)
        await blockStore.softDelete(node1.cid);
        
        expect(events, hasLength(1));
        expect(events.first.type, equals(BlockChangeType.inserted));
        expect(events.first.collection, equals('test-collection'));
      });
      
      test('should combine filters with OR logic', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        
        final insertFilter = ChangeTypeFilter<TestPayload, String>(BlockChangeType.inserted);
        final deleteFilter = ChangeTypeFilter<TestPayload, String>(BlockChangeType.deleted);
        final combinedFilter = insertFilter.or(deleteFilter);
        
        final subscription = blockStore.subscribe(filter: combinedFilter);
        subscription.stream.listen(events.add);
        
        final payload = TestPayload('test-data');
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});

        await blockStore.put(node);
        await blockStore.putMetadata(node.cid, {'key': 'value'});
        await blockStore.softDelete(node.cid);
        
        expect(events, hasLength(2)); // insert and delete, but not metadata update
        expect(events[0].type, equals(BlockChangeType.inserted));
        expect(events[1].type, equals(BlockChangeType.deleted));
      });
      
      test('should negate filters with NOT logic', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        
        final insertFilter = ChangeTypeFilter<TestPayload, String>(BlockChangeType.inserted);
        final notInsertFilter = insertFilter.not();
        
        final subscription = blockStore.subscribe(filter: notInsertFilter);
        subscription.stream.listen(events.add);
        
        final payload = TestPayload('test-data');
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});

        await blockStore.put(node);
        await blockStore.putMetadata(node.cid, {'key': 'value'});
        await blockStore.softDelete(node.cid);
        
        expect(events, hasLength(2)); // metadata update and delete, but not insert
        expect(events[0].type, equals(BlockChangeType.metadataUpdated));
        expect(events[1].type, equals(BlockChangeType.deleted));
      });
    });
    
    group('Watch Methods', () {
      test('watchChanges should return filtered stream', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        final filter = ChangeTypeFilter<TestPayload, String>(BlockChangeType.inserted);
        
        blockStore.watchChanges(filter).listen(events.add);
        
        final payload = TestPayload('test-data');
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});

        await blockStore.put(node);
        await blockStore.softDelete(node.cid);
        
        expect(events, hasLength(1));
        expect(events.first.type, equals(BlockChangeType.inserted));
      });
      
      test('watchCollection should filter by collection', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        
        blockStore.watchCollection('test-collection').listen(events.add);
        
        final payload1 = TestPayload('test-data-1');
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});

        final payload2 = TestPayload('test-data-2');
        final node2 = MerkleNode<TestPayload>.create(payload2, <CID>{});

        await blockStore.putInCollection('test-collection', node1);
        await blockStore.putInCollection('other-collection', node2);
        
        expect(events, hasLength(1));
        expect(events.first.collection, equals('test-collection'));
      });
    });
    
    group('Subscription Management', () {
      test('should create and manage subscriptions', () async {
        expect(blockStore.subscriptionCount, equals(0));
        
        final sub1 = blockStore.subscribe(id: 'sub1');
        expect(blockStore.subscriptionCount, equals(1));
        expect(sub1.id, equals('sub1'));
        expect(sub1.isActive, isTrue);
        
        final sub2 = blockStore.subscribe();
        expect(blockStore.subscriptionCount, equals(2));
        expect(sub2.isActive, isTrue);
        
        await blockStore.unsubscribe('sub1');
        expect(blockStore.subscriptionCount, equals(1));
        expect(sub1.isActive, isFalse);
        
        await sub2.close();
        expect(blockStore.subscriptionCount, equals(0));
        expect(sub2.isActive, isFalse);
      });
      
      test('should handle multiple subscribers to same events', () async {
        final events1 = <BlockChangeEvent<TestPayload, String>>[];
        final events2 = <BlockChangeEvent<TestPayload, String>>[];
        
        final sub1 = blockStore.subscribe();
        final sub2 = blockStore.subscribe();
        
        sub1.stream.listen(events1.add);
        sub2.stream.listen(events2.add);
        
        final payload = TestPayload('test-data');
        // final node = MerkleNode<TestPayload>(payload: payload);
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});

        await blockStore.put(node);
        
        expect(events1, hasLength(1));
        expect(events2, hasLength(1));
        expect(events1.first.cid, equals(events2.first.cid));
      });
      
      test('should stop emitting to closed subscriptions', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        final subscription = blockStore.subscribe();
        subscription.stream.listen(events.add);
        
        final payload1 = TestPayload('test-data-1');
        // final node1 = MerkleNode<TestPayload>(payload: payload1);
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});
        await blockStore.put(node1);
        
        expect(events, hasLength(1));
        
        await subscription.close();
        
        final payload2 = TestPayload('test-data-2');
        // final node2 = MerkleNode<TestPayload>(payload: payload2);
        final node2 = MerkleNode<TestPayload>.create(payload2, <CID>{});
        await blockStore.put(node2);
        
        expect(events, hasLength(1)); // Should not receive second event
      });
    });
    
    group('Time Range Filtering', () {
      test('should filter events by time range', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        
        final startTime = DateTime.now();
        await Future.delayed(Duration(milliseconds: 10));
        
        final filter = TimeRangeFilter<TestPayload, String>(start: startTime);
        final subscription = blockStore.subscribe(filter: filter);
        subscription.stream.listen(events.add);
        
        final payload = TestPayload('test-data');
        final node = MerkleNode<TestPayload>.create(payload, <CID>{});

        await blockStore.put(node);
        
        expect(events, hasLength(1));
        expect(events.first.timestamp.isAfter(startTime), isTrue);
      });
    });
    
    group('Field-based Filtering', () {
      test('should filter events by field values', () async {
        final events = <BlockChangeEvent<TestPayload, String>>[];
        
        final filter = FieldFilter<TestPayload, String>(
          fieldExtractor,
          'value',
          'special-value'
        );
        final subscription = blockStore.subscribe(filter: filter);
        subscription.stream.listen(events.add);
        
        final payload1 = TestPayload('normal-value');
        final node1 = MerkleNode<TestPayload>.create(payload1, <CID>{});

        final payload2 = TestPayload('special-value');
        final node2 = MerkleNode<TestPayload>.create(payload2, <CID>{});

        await blockStore.put(node1);
        await blockStore.put(node2);
        
        expect(events, hasLength(1));
        expect(events.first.node?.payload.value, equals('special-value'));
      });
    });
    
    group('Error Handling', () {
      test('should handle subscription errors gracefully', () async {
        final subscription = blockStore.subscribe();
        
        // Close subscription multiple times should not throw
        await subscription.close();
        await subscription.close();
        
        expect(subscription.isActive, isFalse);
      });
      
      test('should handle unsubscribe of non-existent subscription', () async {
        final result = await blockStore.unsubscribe('non-existent');
        expect(result, isFalse);
      });
    });
  });
}
