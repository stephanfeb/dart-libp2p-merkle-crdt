# Dart LibP2P Merkle-CRDT Developer Guide

A practical guide for building P2P applications with conflict-free replicated data types (CRDTs) over libp2p networks.

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Core Concepts](#core-concepts)
4. [Building Your First CRDT App](#building-your-first-crdt-app)
5. [Working with Documents](#working-with-documents)
6. [Storage and Queries](#storage-and-queries)
7. [Real-time Updates](#real-time-updates)
8. [Testing Your App](#testing-your-app)
9. [Production Deployment](#production-deployment)
10. [Troubleshooting](#troubleshooting)

## Overview

This library helps you build decentralized applications where multiple users can edit data simultaneously without conflicts. Think Google Docs, but peer-to-peer without a central server.

### What You Get

- **Conflict-free data synchronization** - Multiple users can edit simultaneously
- **Offline-first** - Works without internet, syncs when reconnected
- **Real-time updates** - See changes from other users instantly
- **Rich queries** - Search and filter your data efficiently
- **Mobile-friendly** - Designed for mobile P2P networks

### When to Use This

✅ **Perfect for:**
- Collaborative editors (documents, whiteboards)
- Chat applications
- Social networks
- Offline-first mobile apps
- Distributed databases

❌ **Not suitable for:**
- Banking/financial systems (need strong consistency)
- Real-time gaming (need low latency)
- Simple CRUD apps (overkill)

## Quick Start

### 1. Installation

```yaml
dependencies:
  dart_libp2p_merkle_crdt: ^1.0.0
```

### 2. Basic Setup

```dart
import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';

// Create your app
final app = await CRDTApp.create();

// Listen for changes
app.onDataChanged.listen((data) {
  print('Data updated: $data');
});

// Make changes
await app.updateData({'message': 'Hello, world!'});
```

### 3. That's It!

Your app now automatically syncs data between all connected peers. No servers needed.

## Core Concepts

### CRDTs (Conflict-free Replicated Data Types)

Think of CRDTs as "smart data structures" that automatically resolve conflicts when multiple people edit the same data simultaneously.

**Key Properties:**
- **Commutative**: Order doesn't matter - `merge(A, B) = merge(B, A)`
- **Associative**: Grouping doesn't matter - `merge(A, merge(B, C)) = merge(merge(A, B), C)`
- **Idempotent**: Applying twice is safe - `merge(A, A) = A`

### Vector Clocks

Vector clocks track "who did what when" without needing synchronized clocks. Each user has their own logical clock that increments with each change.

```dart
// Alice makes change 1, Bob makes change 1
alice: {alice: 1, bob: 0}
bob:   {alice: 0, bob: 1}

// After sync, both have
both:  {alice: 1, bob: 1}
```

### Content Addressing

Every piece of data gets a unique fingerprint (CID). If the data changes, the fingerprint changes. This prevents tampering and enables efficient syncing.

## Building Your First CRDT App

Let's build a simple collaborative note-taking app.

### Step 1: Define Your Data Structure

```dart
class Note {
  final String id;
  final String title;
  final String content;
  final String author;
  final DateTime lastModified;
  
  Note({
    required this.id,
    required this.title,
    required this.content,
    required this.author,
    required this.lastModified,
  });
}
```

### Step 2: Create a CRDT Wrapper

```dart
class NoteCRDT implements CRDTPayload<Note> {
  final DocumentCrdt _document;
  
  NoteCRDT._(this._document);
  
  factory NoteCRDT.create(String authorId, Note note) {
    return NoteCRDT._(DocumentCrdt.create(authorId, {
      'id': note.id,
      'title': note.title,
      'content': note.content,
      'author': note.author,
      'lastModified': note.lastModified.toIso8601String(),
    }));
  }
  
  @override
  Note get value {
    final data = _document.data;
    return Note(
      id: data['id'],
      title: data['title'],
      content: data['content'],
      author: data['author'],
      lastModified: DateTime.parse(data['lastModified']),
    );
  }
  
  NoteCRDT updateTitle(String authorId, String newTitle) {
    final updated = _document.update(authorId, {
      'title': newTitle,
      'lastModified': DateTime.now().toIso8601String(),
    });
    return NoteCRDT._(updated);
  }
  
  NoteCRDT updateContent(String authorId, String newContent) {
    final updated = _document.update(authorId, {
      'content': newContent,
      'lastModified': DateTime.now().toIso8601String(),
    });
    return NoteCRDT._(updated);
  }
  
  @override
  NoteCRDT merge(covariant NoteCRDT other) {
    return NoteCRDT._(_document.merge(other._document));
  }
  
  @override
  Uint8List toCanonicalBytes() => _document.toCanonicalBytes();
}
```

### Step 3: Set Up Networking

```dart
class NoteApp {
  late final MerkleCrdtGossipManager<Note, NoteCRDT> _manager;
  final String _userId;
  
  NoteApp._(this._userId, this._manager);
  
  static Future<NoteApp> create(String userId) async {
    final host = await createLibp2pHost();
    final blockStore = InMemoryBlockStore<NoteCRDT, Note>();
    
    final dagFetcher = P2PStreamDagFetcher<NoteCRDT, Note>(
      host, blockStore, '/notes/sync/1.0.0', NoteCRDT.fromBytes,
    );
    
    final announcer = GossipSubAnnouncer(host.pubsub, '/notes/heads/1.0.0');
    
    final manager = MerkleCrdtGossipManager<Note, NoteCRDT>(
      dagFetcher: dagFetcher,
      announcer: announcer,
    );
    
    return NoteApp._(userId, manager);
  }
  
  Stream<Note?> get noteStream => _manager.onStateChanged;
  
  Future<void> createNote(String title, String content) async {
    final note = Note(
      id: generateId(),
      title: title,
      content: content,
      author: _userId,
      lastModified: DateTime.now(),
    );
    
    final crdt = NoteCRDT.create(_userId, note);
    await _manager.applyLocalUpdate(crdt);
  }
  
  Future<void> updateTitle(String newTitle) async {
    final current = await _manager.getState();
    if (current != null) {
      final currentCRDT = NoteCRDT.create(_userId, current);
      final updated = currentCRDT.updateTitle(_userId, newTitle);
      await _manager.applyLocalUpdate(updated);
    }
  }
}
```

### Step 4: Use in Your UI

```dart
class NoteEditor extends StatefulWidget {
  @override
  _NoteEditorState createState() => _NoteEditorState();
}

class _NoteEditorState extends State<NoteEditor> {
  late NoteApp _app;
  Note? _currentNote;
  
  @override
  void initState() {
    super.initState();
    _initApp();
  }
  
  void _initApp() async {
    _app = await NoteApp.create('user123');
    
    _app.noteStream.listen((note) {
      setState(() {
        _currentNote = note;
      });
    });
  }
  
  @override
  Widget build(BuildContext context) {
    if (_currentNote == null) {
      return Center(child: CircularProgressIndicator());
    }
    
    return Column(
      children: [
        TextField(
          value: _currentNote!.title,
          onChanged: (title) => _app.updateTitle(title),
          decoration: InputDecoration(labelText: 'Title'),
        ),
        Expanded(
          child: TextField(
            value: _currentNote!.content,
            onChanged: (content) => _app.updateContent(content),
            maxLines: null,
            decoration: InputDecoration(labelText: 'Content'),
          ),
        ),
        Text('Last modified by: ${_currentNote!.author}'),
      ],
    );
  }
}
```

## Working with Documents

The `DocumentCRDT` is perfect for structured data like forms, profiles, or settings.

### Creating Documents

```dart
// Create a user profile
final profile = DocumentCrdt.create('alice', {
  'name': 'Alice Smith',
  'email': 'alice@example.com',
  'bio': 'Software developer',
  'avatar': 'https://example.com/alice.jpg',
});
```

### Updating Fields

```dart
// Alice updates her bio
final updated = profile.update('alice', {
  'bio': 'Senior Software Developer at ACME Corp',
  'location': 'San Francisco, CA',
});
```

### Handling Conflicts

When two users edit the same field simultaneously, DocumentCRDT uses Last-Writer-Wins with deterministic tie-breaking:

```dart
// Alice and Bob both update the bio at the same time
final aliceUpdate = profile.update('alice', {'bio': 'Alice bio'});
final bobUpdate = profile.update('bob', {'bio': 'Bob bio'});

// Merge resolves the conflict
final merged = aliceUpdate.merge(bobUpdate);
// Winner is determined by timestamp, then lexicographic order
```

### Best Practices

- **Keep documents reasonably sized** - Large documents are slower to sync
- **Use meaningful field names** - They're part of the permanent data structure
- **Include metadata** - Timestamps, authors, etc. help with debugging
- **Test conflict scenarios** - Simulate concurrent edits in your tests

## Storage and Queries

The BlockStore system provides persistent storage with rich querying capabilities.

### Basic Storage

```dart
final blockStore = InMemoryBlockStore<NoteCRDT, Note>(
  fieldExtractor: NoteFieldExtractor(),
);

// Store a note
await blockStore.put(noteNode);

// Retrieve a note
final note = await blockStore.get(cid);
```

### Querying Data

```dart
// Find recent notes by Alice
final query = BlockQuery()
  ..where('author', isEqualTo: 'alice')
  ..where('lastModified', isGreaterThan: DateTime.now().subtract(Duration(days: 7)))
  ..orderBy('lastModified', descending: true)
  ..limit(10);

final results = await blockStore.query(query);
```

### Custom Storage Backends

For production apps, implement custom storage backends:

```dart
class SQLiteBlockStore implements BlockStore<NoteCRDT, Note> {
  // Implement using your preferred database
  // See blockstore-developer-guide.md for details
}
```

### Field Extraction

Define how to extract searchable fields from your data:

```dart
class NoteFieldExtractor implements FieldExtractor<NoteCRDT> {
  @override
  Map<String, dynamic> extractFields(NoteCRDT payload) {
    final note = payload.value;
    return {
      'title': note.title,
      'author': note.author,
      'lastModified': note.lastModified.toIso8601String(),
      'wordCount': note.content.split(' ').length,
    };
  }
}
```

## Real-time Updates

Get notified when data changes in real-time.

### Change Notifications

```dart
// Listen to all changes
blockStore.changeStream.listen((event) {
  switch (event.type) {
    case ChangeEventType.created:
      print('New note created');
      break;
    case ChangeEventType.updated:
      print('Note updated');
      break;
    case ChangeEventType.deleted:
      print('Note deleted');
      break;
  }
});
```

### Filtered Subscriptions

```dart
// Only listen to changes from specific users
final subscription = blockStore.subscribe(
  filter: (event) {
    final note = event.payload?.value;
    return note?.author == 'alice';
  },
);

subscription.stream.listen((event) {
  print('Alice made a change: ${event.type}');
});
```

### Streaming Queries

```dart
// Get live updates for a query
final query = BlockQuery()..where('author', isEqualTo: 'alice');
final stream = blockStore.queryStream(query);

stream.listen((results) {
  print('Alice has ${results.length} notes');
  // UI automatically updates
});
```

## Testing Your App

### Unit Tests

Test your CRDT logic:

```dart
test('should merge concurrent updates correctly', () {
  final base = DocumentCrdt.create('alice', {'counter': 0});
  
  final update1 = base.update('alice', {'counter': 1});
  final update2 = base.update('bob', {'counter': 2});
  
  final merged = update1.merge(update2);
  
  // Test that merge is deterministic
  expect(merged.data['counter'], isA<int>());
});
```

### Integration Tests

Test the full sync process:

```dart
test('should sync between peers', () async {
  final peer1 = await createTestPeer();
  final peer2 = await createTestPeer();
  
  await connectPeers(peer1, peer2);
  
  // Peer1 creates data
  await peer1.createNote('Test', 'Content');
  
  // Wait for sync
  await Future.delayed(Duration(seconds: 1));
  
  // Peer2 should have the data
  final peer2Data = await peer2.getCurrentNote();
  expect(peer2Data?.title, equals('Test'));
});
```

### Performance Tests

```dart
test('should handle many concurrent updates', () async {
  final stopwatch = Stopwatch()..start();
  
  var doc = DocumentCrdt.create('alice', {'counter': 0});
  
  for (int i = 0; i < 1000; i++) {
    doc = doc.update('alice', {'counter': i});
  }
  
  stopwatch.stop();
  expect(stopwatch.elapsedMilliseconds, lessThan(1000));
});
```

## Production Deployment

### Database Backend

Use a proper database for production:

```dart
// SQLite example
final db = await openDatabase('app.db');
final blockStore = SQLiteBlockStore(db, fieldExtractor);
```

### Network Configuration

Configure for your network environment:

```dart
final host = await createLibp2pHost(
  listenAddresses: ['/ip4/0.0.0.0/tcp/0'],
  enableDHT: true,
  enableRelay: true, // For mobile networks
);
```

### Error Handling

```dart
try {
  await manager.applyLocalUpdate(payload);
} catch (e) {
  // Log error and show user-friendly message
  logger.error('Failed to save changes: $e');
  showErrorDialog('Could not save changes. Please try again.');
}
```

### Monitoring

```dart
// Monitor sync status
manager.onCidChanged.listen((cid) {
  logger.info('Synced to CID: $cid');
});

// Monitor peer connections
host.network.onPeerConnected.listen((peer) {
  logger.info('Connected to peer: $peer');
});
```

## Troubleshooting

### Common Issues

**Data not syncing between peers**
- Check network connectivity
- Verify protocol IDs match
- Ensure DHT is enabled
- Check firewall settings

**High memory usage**
- Implement LRU cache in BlockStore
- Monitor DAG growth
- Use pagination for large datasets

**Slow performance**
- Add indexes to frequently queried fields
- Use batch operations
- Implement lazy loading

**Merge conflicts**
- Test your merge logic thoroughly
- Use deterministic serialization
- Include timestamps in your data

### Debug Tools

Enable detailed logging:

```dart
Logger.root.level = Level.FINE;
Logger.root.onRecord.listen((record) {
  print('${record.level.name}: ${record.message}');
});
```

Monitor network activity:

```dart
final stats = dagFetcher.getStats();
print('Connected peers: ${stats['peer_count']}');
print('Synced blocks: ${stats['block_count']}');
```

### Getting Help

- Check the [test suite](test/) for examples
- Read the [BlockStore Developer Guide](blockstore-developer-guide.md) for custom storage
- File issues on GitHub for bugs
- Join our Discord for questions

---

## Next Steps

1. **Start small** - Build a simple app first
2. **Test thoroughly** - Especially concurrent scenarios  
3. **Monitor performance** - Watch memory and network usage
4. **Plan for scale** - Consider database backends early
5. **Handle errors gracefully** - Network issues are common in P2P

This library provides the foundation - now build something amazing! 🚀
