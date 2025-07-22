# BlockStore Developer Guide

A practical guide for implementing custom storage backends for the dart-libp2p-merkle-crdt library.

## Overview

The BlockStore is your app's storage layer. Think of it as a database interface that stores CRDT data with built-in querying, change notifications, and soft deletes.

### Why Custom BlockStore?

The library includes `InMemoryBlockStore` for development, but production apps need persistent storage:

- **SQLite** - Great for mobile apps, simple setup
- **Isar** - High-performance Flutter database
- **PostgreSQL** - For server applications
- **Your favorite database** - Implement the interface

## Core Interface

Your BlockStore needs to implement these essential methods:

```dart
abstract class BlockStore<P extends CRDTPayload<V>, V> {
  // Basic storage
  Future<MerkleNode<P>?> get(CID cid);
  Future<void> put(MerkleNode<P> node);
  Future<void> softDelete(CID cid);
  
  // Queries (optional but recommended)
  Future<List<BlockStoreEntry<P>>> query(BlockQuery query);
  
  // Change notifications (optional)
  Stream<BlockChangeEvent<P>> get changeStream;
}
```

That's it! Everything else is optional enhancements.

## Quick SQLite Example

Here's a minimal SQLite implementation to get you started:

```dart
class SQLiteBlockStore implements BlockStore<MyPayload, MyData> {
  late Database _db;
  
  Future<void> open() async {
    _db = await openDatabase('app.db', onCreate: (db, version) {
      return db.execute('''
        CREATE TABLE blocks(
          cid TEXT PRIMARY KEY,
          data BLOB,
          deleted INTEGER DEFAULT 0,
          created_at INTEGER
        )
      ''');
    });
  }
  
  @override
  Future<MerkleNode<MyPayload>?> get(CID cid) async {
    final results = await _db.query('blocks', 
      where: 'cid = ? AND deleted = 0',
      whereArgs: [cid.toString()],
    );
    
    if (results.isEmpty) return null;
    
    final data = results.first['data'] as Uint8List;
    return MerkleNode.fromBytes(data);
  }
  
  @override
  Future<void> put(MerkleNode<MyPayload> node) async {
    await _db.insert('blocks', {
      'cid': node.cid.toString(),
      'data': node.toBytes(),
      'created_at': DateTime.now().millisecondsSinceEpoch,
    }, conflictAlgorithm: ConflictAlgorithm.replace);
  }
  
  @override
  Future<void> softDelete(CID cid) async {
    await _db.update('blocks', 
      {'deleted': 1},
      where: 'cid = ?',
      whereArgs: [cid.toString()],
    );
  }
}
```

## Database Schema

Your database needs to store these fields:

| Field | Type | Purpose |
|-------|------|---------|
| `cid` | String | Primary key (content identifier) |
| `data` | Blob | Serialized MerkleNode |
| `deleted` | Boolean | Soft delete flag |
| `created_at` | Timestamp | When stored |
| `collection` | String | Optional grouping |

### Indexes for Performance

Add these indexes for better query performance:

```sql
CREATE INDEX idx_deleted ON blocks(deleted);
CREATE INDEX idx_created_at ON blocks(created_at);
CREATE INDEX idx_collection ON blocks(collection);
```

## Serialization

You need to convert `MerkleNode` objects to/from bytes:

```dart
// Store
final bytes = node.toCanonicalBytes();
await db.insert('blocks', {'data': bytes});

// Retrieve  
final bytes = row['data'] as Uint8List;
final node = MerkleNode.fromCanonicalBytes(bytes);
```

## Adding Queries (Optional)

To support the query system, implement field extraction:

```dart
class MyFieldExtractor implements FieldExtractor<MyPayload> {
  @override
  Map<String, dynamic> extractFields(MyPayload payload) {
    final data = payload.value;
    return {
      'title': data['title'],
      'author': data['author'],
      'created': data['created'],
      // Add fields you want to query
    };
  }
}
```

Then store extracted fields in a separate table:

```sql
CREATE TABLE block_fields(
  cid TEXT,
  field_name TEXT,
  field_value TEXT,
  FOREIGN KEY(cid) REFERENCES blocks(cid)
);
```

## Change Notifications (Optional)

For real-time updates, emit change events:

```dart
class SQLiteBlockStore implements BlockStore<MyPayload, MyData> {
  final _changeController = StreamController<BlockChangeEvent>.broadcast();
  
  @override
  Stream<BlockChangeEvent> get changeStream => _changeController.stream;
  
  @override
  Future<void> put(MerkleNode<MyPayload> node) async {
    // Store in database
    await _db.insert(/* ... */);
    
    // Notify listeners
    _changeController.add(BlockChangeEvent.created(node.cid, node.payload));
  }
}
```

## Testing Your Implementation

Create tests to verify your BlockStore works correctly:

```dart
test('should store and retrieve data', () async {
  final store = SQLiteBlockStore();
  await store.open();
  
  final payload = MyPayload('test data');
  final node = MerkleNode(payload);
  
  await store.put(node);
  final retrieved = await store.get(node.cid);
  
  expect(retrieved?.payload.value, equals('test data'));
});

test('should handle soft deletes', () async {
  final store = SQLiteBlockStore();
  await store.open();
  
  final node = MerkleNode(MyPayload('test'));
  await store.put(node);
  await store.softDelete(node.cid);
  
  final retrieved = await store.get(node.cid);
  expect(retrieved, isNull);
});
```

## Production Considerations

### Connection Pooling

For high-traffic apps, use connection pooling:

```dart
class ConnectionPool {
  final Queue<Database> _available = Queue();
  final int maxConnections;
  
  Future<Database> acquire() async {
    if (_available.isNotEmpty) {
      return _available.removeFirst();
    }
    // Create new connection or wait
  }
  
  void release(Database db) {
    _available.add(db);
  }
}
```

### Error Handling

Wrap database operations in try-catch:

```dart
@override
Future<MerkleNode<MyPayload>?> get(CID cid) async {
  try {
    final results = await _db.query(/* ... */);
    // Process results
  } catch (e) {
    logger.error('Failed to get $cid: $e');
    throw BlockStoreException('Database error');
  }
}
```

### Batch Operations

For better performance, implement batch operations:

```dart
@override
Future<void> putMany(List<MerkleNode<MyPayload>> nodes) async {
  await _db.transaction((txn) async {
    final batch = txn.batch();
    for (final node in nodes) {
      batch.insert('blocks', {
        'cid': node.cid.toString(),
        'data': node.toBytes(),
      });
    }
    await batch.commit();
  });
}
```

## Database-Specific Tips

### SQLite
- Use WAL mode for better concurrency: `PRAGMA journal_mode=WAL`
- Increase cache size: `PRAGMA cache_size=10000`
- Use transactions for batch operations

### Isar
- Define your schema with `@collection` classes
- Use indexes on frequently queried fields
- Leverage Isar's built-in change notifications

### PostgreSQL
- Use connection pooling (pgpool, connection_pool package)
- Consider JSONB columns for flexible document storage
- Use proper indexes on JSONB fields

## Common Pitfalls

**Forgetting soft deletes**
- Always check the `deleted` flag when retrieving data
- Provide `includeTombstoned` parameter for debugging

**Poor indexing**
- Index frequently queried fields
- Don't over-index (slows down writes)

**Memory leaks**
- Close database connections properly
- Cancel stream subscriptions

**Serialization issues**
- Test with complex nested data
- Handle null values properly
- Use deterministic serialization

## Getting Help

- Check the test suite for examples
- Look at `InMemoryBlockStore` implementation
- File issues on GitHub for bugs
- Join our Discord for questions

## Next Steps

1. **Start simple** - Implement basic get/put first
2. **Add persistence** - Choose your database
3. **Test thoroughly** - Especially concurrent access
4. **Add queries** - If your app needs them
5. **Optimize** - Profile and improve performance

Remember: You don't need to implement everything at once. Start with basic storage and add features as needed.

---

*For complete API reference, see the main DEVELOPER-GUIDE.md*
