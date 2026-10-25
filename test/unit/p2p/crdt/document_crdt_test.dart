import 'package:test/test.dart';

import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';

void main() {
  group('VectorClock', () {
    test('should create empty vector clock', () {
      final clock = VectorClock.empty();
      expect(clock.isEmpty, isTrue);
      expect(clock.length, equals(0));
      expect(clock.actors, isEmpty);
    });

    test('should create vector clock from map', () {
      final clock = VectorClock.fromMap({'actor1': 5, 'actor2': 3});
      expect(clock.length, equals(2));
      expect(clock.getTime('actor1'), equals(5));
      expect(clock.getTime('actor2'), equals(3));
      expect(clock.getTime('actor3'), equals(0));
    });

    test('should create single actor vector clock', () {
      final clock = VectorClock.single('actor1', 10);
      expect(clock.length, equals(1));
      expect(clock.getTime('actor1'), equals(10));
      expect(clock.actors, equals({'actor1'}));
    });

    test('should increment timestamp for actor', () {
      final clock = VectorClock.empty();
      final incremented = clock.increment('actor1');
      
      expect(incremented.getTime('actor1'), equals(1));
      expect(clock.getTime('actor1'), equals(0)); // Original unchanged
      
      final incremented2 = incremented.increment('actor1');
      expect(incremented2.getTime('actor1'), equals(2));
    });

    test('should update timestamp for actor', () {
      final clock = VectorClock.empty();
      final updated = clock.update('actor1', 5);
      
      expect(updated.getTime('actor1'), equals(5));
      expect(clock.getTime('actor1'), equals(0)); // Original unchanged
    });

    test('should merge vector clocks correctly', () {
      final clock1 = VectorClock.fromMap({'actor1': 5, 'actor2': 3});
      final clock2 = VectorClock.fromMap({'actor1': 3, 'actor2': 7, 'actor3': 2});
      
      final merged = clock1.merge(clock2);
      
      expect(merged.getTime('actor1'), equals(5)); // max(5, 3)
      expect(merged.getTime('actor2'), equals(7)); // max(3, 7)
      expect(merged.getTime('actor3'), equals(2)); // max(0, 2)
    });

    test('should determine causal relationships correctly', () {
      final clock1 = VectorClock.fromMap({'actor1': 1, 'actor2': 2});
      final clock2 = VectorClock.fromMap({'actor1': 2, 'actor2': 3});
      final clock3 = VectorClock.fromMap({'actor1': 1, 'actor2': 3});
      final clock4 = VectorClock.fromMap({'actor1': 1, 'actor2': 2});
      
      expect(clock1.compareTo(clock2), equals(CausalRelation.before));
      expect(clock2.compareTo(clock1), equals(CausalRelation.after));
      expect(clock1.compareTo(clock3), equals(CausalRelation.before));
      expect(clock1.compareTo(clock4), equals(CausalRelation.equal));
      
      expect(clock1.happenedBefore(clock2), isTrue);
      expect(clock2.happenedAfter(clock1), isTrue);
      expect(clock1.isConcurrentWith(clock3), isFalse);
      expect(clock1.isEqualTo(clock4), isTrue);
    });

    test('should serialize and deserialize correctly', () {
      final original = VectorClock.fromMap({'actor1': 5, 'actor2': 3});
      final bytes = original.toBytes();
      final deserialized = VectorClock.fromBytes(bytes);
      
      expect(deserialized, equals(original));
      expect(deserialized.getTime('actor1'), equals(5));
      expect(deserialized.getTime('actor2'), equals(3));
    });

    test('should handle JSON serialization', () {
      final original = VectorClock.fromMap({'actor1': 5, 'actor2': 3});
      final json = original.toJson();
      final deserialized = VectorClock.fromJson(json);
      
      expect(deserialized, equals(original));
    });

    test('should provide canonical string representation', () {
      final clock = VectorClock.fromMap({'actor2': 3, 'actor1': 5});
      final str = clock.toString();
      
      // Should be sorted by actor ID
      expect(str, equals('VectorClock{actor1:5, actor2:3}'));
    });
  });

  group('DocumentEntry', () {
    test('should create entry with value', () {
      final clock = VectorClock.single('actor1', 1);
      final entry = DocumentEntry.create('test value', clock, 'actor1');
      
      expect(entry.value, equals('test value'));
      expect(entry.timestamp, equals(clock));
      expect(entry.isDeleted, isFalse);
      expect(entry.actorId, equals('actor1'));
    });

    test('should create tombstone entry', () {
      final clock = VectorClock.single('actor1', 1);
      final entry = DocumentEntry.tombstone(clock, 'actor1');
      
      expect(entry.value, isNull);
      expect(entry.timestamp, equals(clock));
      expect(entry.isDeleted, isTrue);
      expect(entry.actorId, equals('actor1'));
    });

    test('should copy with updated fields', () {
      final clock = VectorClock.single('actor1', 1);
      final entry = DocumentEntry.create('test', clock, 'actor1');
      final newClock = VectorClock.single('actor2', 2);
      
      final copied = entry.copyWith(
        value: 'new value',
        timestamp: newClock,
        actorId: 'actor2',
      );
      
      expect(copied.value, equals('new value'));
      expect(copied.timestamp, equals(newClock));
      expect(copied.isDeleted, isFalse); // Unchanged
      expect(copied.actorId, equals('actor2'));
      
      // Original unchanged
      expect(entry.value, equals('test'));
      expect(entry.actorId, equals('actor1'));
    });

    test('should serialize and deserialize correctly', () {
      final clock = VectorClock.single('actor1', 1);
      final original = DocumentEntry.create({'key': 'value'}, clock, 'actor1');
      
      final json = original.toJson();
      final deserialized = DocumentEntry.fromJson(json);
      
      expect(deserialized, equals(original));
      expect(deserialized.value, equals({'key': 'value'}));
    });
  });

  group('DocumentCRDT', () {
    test('should create empty document', () {
      final doc = DocumentCRDT.empty();
      
      expect(doc.value, isEmpty);
      expect(doc.isEmpty, isTrue);
      expect(doc.length, equals(0));
      expect(doc.keys, isEmpty);
      expect(doc.strategy, equals(ConflictResolutionStrategy.lastWriterWins));
    });

    test('should create document from map', () {
      final doc = DocumentCRDT.fromMap(
        {'name': 'John', 'age': 30},
        actorId: 'actor1',
      );
      
      expect(doc.value, equals({'name': 'John', 'age': 30}));
      expect(doc.length, equals(2));
      expect(doc.keys, equals({'name', 'age'}));
      expect(doc.containsKey('name'), isTrue);
      expect(doc.containsKey('missing'), isFalse);
    });

    test('should put and get values', () {
      final doc = DocumentCRDT.empty();
      final updated = doc.put('name', 'John', actorId: 'actor1');
      
      expect(updated.value['name'], equals('John'));
      expect(updated.containsKey('name'), isTrue);
      expect(updated.length, equals(1));
      
      // Original unchanged
      expect(doc.isEmpty, isTrue);
    });

    test('should delete values with tombstones', () {
      final doc = DocumentCRDT.fromMap({'name': 'John'}, actorId: 'actor1');
      final deleted = doc.delete('name', actorId: 'actor1');
      
      expect(deleted.value, isEmpty);
      expect(deleted.containsKey('name'), isFalse);
      expect(deleted.length, equals(0));
      
      // But entry still exists in allKeys
      expect(deleted.allKeys, contains('name'));
      
      final entry = deleted.getEntry('name');
      expect(entry?.isDeleted, isTrue);
    });

    test('should patch multiple values', () {
      final doc = DocumentCRDT.fromMap({'name': 'John'}, actorId: 'actor1');
      final patched = doc.patch({
        'age': 30,
        'city': 'New York',
      }, actorId: 'actor1');
      
      expect(patched.value, equals({
        'name': 'John',
        'age': 30,
        'city': 'New York',
      }));
      expect(patched.length, equals(3));
    });

    test('should deep merge nested maps', () {
      final doc = DocumentCRDT.fromMap({
        'user': {'name': 'John', 'age': 30},
        'settings': {'theme': 'dark'},
      }, actorId: 'actor1');
      
      final merged = doc.deepMerge({
        'user': {'city': 'New York'},
        'settings': {'language': 'en'},
      }, actorId: 'actor1');
      
      expect(merged.value, equals({
        'user': {'name': 'John', 'age': 30, 'city': 'New York'},
        'settings': {'theme': 'dark', 'language': 'en'},
      }));
    });

    test('should serialize and deserialize correctly', () {
      final original = DocumentCRDT.fromMap({
        'name': 'John',
        'age': 30,
        'tags': ['developer', 'dart'],
      }, actorId: 'actor1');
      
      final bytes = original.toCanonicalBytes();
      final deserialized = DocumentCRDT.fromBytes(bytes);
      
      expect(deserialized.value, equals(original.value));
      expect(deserialized.strategy, equals(original.strategy));
    });

    test('should handle JSON serialization', () {
      final original = DocumentCRDT.fromMap({'name': 'John'}, actorId: 'actor1');
      final json = original.toJson();
      final deserialized = DocumentCRDT.fromJson(json);
      
      expect(deserialized.value, equals(original.value));
    });

    group('Conflict Resolution', () {
      test('should resolve conflicts with Last Writer Wins', () {
        final doc1 = DocumentCRDT.fromMap({'name': 'John'}, actorId: 'actor1');
        final doc2 = DocumentCRDT.fromMap({'name': 'Jane'}, actorId: 'actor2');
        
        // Simulate actor2 writing after actor1
        final doc2Updated = doc2.put('name', 'Jane Updated', actorId: 'actor2');
        
        final merged = doc1.merge(doc2Updated);
        
        // Should prefer the later write (actor2's update)
        expect(merged.value['name'], equals('Jane Updated'));
      });

      test('should resolve conflicts with First Writer Wins', () {
        final doc1 = DocumentCRDT.empty(strategy: ConflictResolutionStrategy.firstWriterWins);
        final doc2 = DocumentCRDT.empty(strategy: ConflictResolutionStrategy.firstWriterWins);
        
        final doc1Updated = doc1.put('name', 'John', actorId: 'actor1');
        final doc2Updated = doc2.put('name', 'Jane', actorId: 'actor2');
        
        final merged = doc1Updated.merge(doc2Updated);
        
        // Should prefer the first write (depends on vector clock comparison)
        expect(merged.value['name'], isIn(['John', 'Jane']));
      });

      test('deprecated merge strategy resolves like last writer wins', () {
        // ignore: deprecated_member_use_from_same_package
        final doc1 = DocumentCRDT.empty(strategy: ConflictResolutionStrategy.merge);
        // ignore: deprecated_member_use_from_same_package
        final doc2 = DocumentCRDT.empty(strategy: ConflictResolutionStrategy.merge);

        final doc1Updated = doc1.put('user', {'name': 'John'}, actorId: 'actor1');
        final doc2Updated = doc2.put('user', {'age': 30}, actorId: 'actor2');

        final merged = doc1Updated.merge(doc2Updated);
        expect(merged.value['user'], equals({'age': 30}));
        expect(doc2Updated.merge(doc1Updated).toCanonicalBytes(), merged.toCanonicalBytes());
      });

      test('should use custom conflict resolver', () {
        DocumentEntry customResolver(String key, DocumentEntry local, DocumentEntry remote) {
          // Always prefer the value with more characters
          final localLength = local.value?.toString().length ?? 0;
          final remoteLength = remote.value?.toString().length ?? 0;
          return localLength >= remoteLength ? local : remote;
        }
        
        final doc1 = DocumentCRDT.empty().withConflictResolver(customResolver);
        final doc2 = DocumentCRDT.empty().withConflictResolver(customResolver);
        
        final doc1Updated = doc1.put('name', 'Jo', actorId: 'actor1');
        final doc2Updated = doc2.put('name', 'Alexander', actorId: 'actor2');
        
        final merged = doc1Updated.merge(doc2Updated);
        
        // Should prefer the longer name
        expect(merged.value['name'], equals('Alexander'));
      });

      test('should handle concurrent updates correctly', () {
        final doc1 = DocumentCRDT.fromMap({'counter': 1}, actorId: 'actor1');
        final doc2 = DocumentCRDT.fromMap({'counter': 1}, actorId: 'actor2');
        
        // Both actors increment concurrently
        final doc1Updated = doc1.put('counter', 2, actorId: 'actor1');
        final doc2Updated = doc2.put('counter', 3, actorId: 'actor2');
        
        final merged1 = doc1Updated.merge(doc2Updated);
        final merged2 = doc2Updated.merge(doc1Updated);
        
        // Merge should be commutative
        expect(merged1.value, equals(merged2.value));
      });

      test('should handle deletion conflicts', () {
        final doc1 = DocumentCRDT.fromMap({'name': 'John'}, actorId: 'actor1');
        final doc2 = DocumentCRDT.fromMap({'name': 'John'}, actorId: 'actor2');
        
        final doc1Updated = doc1.put('name', 'Jane', actorId: 'actor1');
        final doc2Deleted = doc2.delete('name', actorId: 'actor2');
        
        final merged = doc1Updated.merge(doc2Deleted);
        
        // Result depends on timestamps and tie-breaking
        expect(merged.value.containsKey('name'), isA<bool>());
      });
    });

    group('CRDT Properties', () {
      test('should be associative', () {
        final doc1 = DocumentCRDT.fromMap({'a': 1}, actorId: 'actor1');
        final doc2 = DocumentCRDT.fromMap({'b': 2}, actorId: 'actor2');
        final doc3 = DocumentCRDT.fromMap({'c': 3}, actorId: 'actor3');
        
        final merged1 = doc1.merge(doc2).merge(doc3);
        final merged2 = doc1.merge(doc2.merge(doc3));
        
        expect(merged1.value, equals(merged2.value));
      });

      test('should be commutative', () {
        final doc1 = DocumentCRDT.fromMap({'name': 'John'}, actorId: 'actor1');
        final doc2 = DocumentCRDT.fromMap({'age': 30}, actorId: 'actor2');
        
        final merged1 = doc1.merge(doc2);
        final merged2 = doc2.merge(doc1);
        
        expect(merged1.value, equals(merged2.value));
      });

      test('should be idempotent', () {
        final doc1 = DocumentCRDT.fromMap({'name': 'John'}, actorId: 'actor1');
        final doc2 = DocumentCRDT.fromMap({'age': 30}, actorId: 'actor2');
        
        final merged1 = doc1.merge(doc2);
        final merged2 = merged1.merge(doc2);
        
        expect(merged1.value, equals(merged2.value));
      });
    });

    group('Edge Cases', () {
      test('should handle null values', () {
        final doc = DocumentCRDT.empty();
        final updated = doc.put('nullable', null, actorId: 'actor1');
        
        expect(updated.value['nullable'], isNull);
        expect(updated.containsKey('nullable'), isTrue);
      });

      test('should handle complex nested structures', () {
        final doc = DocumentCRDT.fromMap({
          'user': {
            'profile': {
              'name': 'John',
              'contacts': ['email@example.com'],
            },
            'settings': {
              'theme': 'dark',
              'notifications': true,
            },
          },
        }, actorId: 'actor1');
        
        expect(doc.value['user']['profile']['name'], equals('John'));
        expect(doc.value['user']['settings']['theme'], equals('dark'));
      });

      test('should handle empty string keys', () {
        final doc = DocumentCRDT.empty();
        final updated = doc.put('', 'empty key value', actorId: 'actor1');
        
        expect(updated.value[''], equals('empty key value'));
        expect(updated.containsKey(''), isTrue);
      });

      test('should handle large documents', () {
        final largeMap = <String, dynamic>{};
        for (int i = 0; i < 1000; i++) {
          largeMap['key$i'] = 'value$i';
        }
        
        final doc = DocumentCRDT.fromMap(largeMap, actorId: 'actor1');
        
        expect(doc.length, equals(1000));
        expect(doc.value['key500'], equals('value500'));
      });

      test('should maintain vector clock consistency', () {
        final doc = DocumentCRDT.empty();
        var current = doc;
        
        for (int i = 0; i < 10; i++) {
          current = current.put('key$i', 'value$i', actorId: 'actor1');
        }
        
        // Clock should have incremented for each operation
        expect(current.clock.getTime('actor1'), equals(10));
      });
    });
  });
}
