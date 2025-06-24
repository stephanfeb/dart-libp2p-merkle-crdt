# Dart LibP2P Merkle-CRDT Developer Guide

A comprehensive guide for building P2P applications with conflict-free replicated data types (CRDTs) over libp2p networks.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Quick Start](#quick-start)
4. [Core Components](#core-components)
5. [CRDT Payload Implementation](#crdt-payload-implementation)
6. [Network Layer](#network-layer)
7. [ActivityPub Integration](#activitypub-integration)
8. [Best Practices](#best-practices)
9. [Performance Tuning](#performance-tuning)
10. [Troubleshooting](#troubleshooting)
11. [Advanced Usage](#advanced-usage)

## Overview

This library provides a robust foundation for building decentralized applications using Merkle-CRDTs over libp2p networks. It combines the conflict-free properties of CRDTs with the content-addressed storage of Merkle DAGs, enabling reliable P2P data synchronization.

### Key Features

- **Merkle-CRDT Integration**: Built on the `merkledag` package for proper CRDT semantics
- **P2P Networking**: Uses libp2p for peer discovery, content routing, and data exchange
- **Content Addressing**: All data is content-addressed using CIDs for tamper detection
- **ActivityPub Support**: Built-in support for ActivityPub-style social networking primitives
- **Mobile-Friendly**: Designed for mobile P2P environments with network resilience features

### When to Use This Library

✅ **Good for:**
- Decentralized social networks
- Collaborative applications
- P2P content sharing
- Offline-first applications
- Applications requiring conflict-free data synchronization

❌ **Not suitable for:**
- Applications requiring strong consistency
- High-frequency trading systems
- Applications with strict ordering requirements
- Simple client-server architectures

## Architecture

The library follows a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
│  (Your App Logic, Actor, Activities, Notes)                 │
├─────────────────────────────────────────────────────────────┤
│                 CRDT Management Layer                       │
│              MerkleCrdtGossipManager                        │
├─────────────────────────────────────────────────────────────┤
│                   Network Layer                             │
│  ┌─────────────────┐  ┌─────────────────┐                   │
│  │  GossipSub      │  │  P2PStreamDag   │                   │
│  │  Announcer      │  │  Fetcher        │                   │
│  └─────────────────┘  └─────────────────┘                   │
├─────────────────────────────────────────────────────────────┤
│                  Storage Layer                              │
│  ┌─────────────────┐  ┌─────────────────┐                   │
│  │   BlockStore    │  │   DHTProvider   │                   │
│  └─────────────────┘  └─────────────────┘                   │
├─────────────────────────────────────────────────────────────┤
│                 LibP2P Foundation                           │
│        (Host, PubSub, DHT, Streams)                         │
└─────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

- **MerkleCrdtGossipManager**: High-level CRDT state management and event handling
- **P2PStreamDagFetcher**: DAG node synchronization over P2P streams
- **GossipSubAnnouncer**: Head CID announcements via GossipSub
- **BlockStore**: Local content-addressed storage
- **DHTProvider**: Content discovery and routing
- **P2PNodeProtocol**: Low-level P2P message handling

## Quick Start

### 1. Add Dependencies

```yaml
dependencies:
  dart_libp2p_merkle_crdt: ^1.0.0
  dart_libp2p: ^1.0.0
  merkledag: ^1.0.0
```

### 2. Basic Setup

```dart
import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:dart_libp2p/dart_libp2p.dart';

// Create libp2p host
final host = await createLibp2pHost();

// Create storage components
final blockStore = MemoryBlockStore<MyPayload, String>();
final dagFetcher = P2PStreamDagFetcher<MyPayload, String>(
  host,
  blockStore,
  '/my-app/dag-sync/1.0.0',
  myPayloadFactory,
  dht: host.dht, // Optional DHT for content discovery
);

// Create announcer for head CID broadcasts
final announcer = GossipSubAnnouncer(
  host.pubsub,
  '/my-app/heads/1.0.0',
);

// Create the main CRDT manager
final crdtManager = MerkleCrdtGossipManager<String, MyPayload>(
  dagFetcher: dagFetcher,
  announcer: announcer,
);

// Listen for state changes
crdtManager.onStateChanged.listen((newState) {
  print('CRDT state updated: $newState');
});

// Apply local updates
await crdtManager.applyLocalUpdate(MyPayload('new data'));
```

### 3. Define Your CRDT Payload

```dart
class MyPayload implements CRDTPayload<String> {
  final String _data;
  
  MyPayload(this._data);
  
  @override
  String get value => _data;
  
  @override
  MyPayload merge(covariant MyPayload other) {
    // Implement your merge logic
    return MyPayload('${_data}_${other._data}');
  }
  
  @override
  Uint8List toCanonicalBytes() {
    return Uint8List.fromList(utf8.encode(_data));
  }
  
  static MyPayload fromCanonicalBytes(Uint8List bytes) {
    return MyPayload(utf8.decode(bytes));
  }
}

// Factory function for deserialization
MyPayload myPayloadFactory(Uint8List bytes) {
  return MyPayload.fromCanonicalBytes(bytes);
}
```

## Core Components

### MerkleCrdtGossipManager

The main interface for CRDT operations. Manages state, handles network events, and provides reactive streams.

#### Key Methods

```dart
// Apply local updates
await manager.applyLocalUpdate(payload);

// Get current state
final currentState = await manager.getState();
final cachedState = manager.currentState; // Synchronous cached access

// Get latest CID
final latestCid = manager.latestKnownCid;

// Force re-announcement (useful for mobile networks)
await manager.forceAnnounceCurrentHead();

// Listen to changes
manager.onStateChanged.listen((state) => handleStateChange(state));
manager.onCidChanged.listen((cid) => handleNewHead(cid));
```

#### Configuration Options

```dart
final manager = MerkleCrdtGossipManager<V, P>(
  dagFetcher: dagFetcher,
  announcer: announcer,
  loggerName: 'MyApp.CRDT', // Custom logger name
);
```

### P2PStreamDagFetcher

Implements the `DAGSyncer<P>` interface for content synchronization.

#### Key Features

- **Content Discovery**: Uses DHT to find providers
- **Stream-based Sync**: Direct P2P streams for efficient data transfer
- **Content Verification**: Cryptographic verification of all received data
- **Mobile Resilience**: Timeout handling and retry logic

#### Usage

```dart
// The fetcher is typically used internally by MerkleCrdtGossipManager
// but can be used directly for advanced use cases

// Get a node by CID
final node = await dagFetcher.get(cid);

// Store a node
await dagFetcher.put(node);

// Check DHT availability
if (dagFetcher.isDHTAvailable) {
  print('DHT is available for content discovery');
}

// Get statistics
final stats = dagFetcher.getStats();
print('DAG Syncer Stats: $stats');
```

### BlockStore

Content-addressed storage interface. Implement this for your storage backend.

```dart
abstract class BlockStore<P extends CRDTPayload<V>, V> {
  Future<MerkleNode<P>?> get(CID cid);
  Future<void> put(MerkleNode<P> node);
}

// Example implementation
class MyBlockStore implements BlockStore<MyPayload, String> {
  final Map<String, MerkleNode<MyPayload>> _storage = {};
  
  @override
  Future<MerkleNode<MyPayload>?> get(CID cid) async {
    return _storage[cid.toString()];
  }
  
  @override
  Future<void> put(MerkleNode<MyPayload> node) async {
    _storage[node.cid.toString()] = node;
  }
}
```

## CRDT Payload Implementation

### Required Interface

All CRDT payloads must implement `CRDTPayload<V>`:

```dart
abstract class CRDTPayload<V> {
  /// The logical value of this CRDT state
  V get value;
  
  /// Merge this payload with another, resolving conflicts
  CRDTPayload<V> merge(covariant CRDTPayload<V> other);
  
  /// Serialize to canonical bytes for content addressing
  Uint8List toCanonicalBytes();
}
```

### Implementation Guidelines

#### 1. Deterministic Serialization

```dart
@override
Uint8List toCanonicalBytes() {
  // MUST be deterministic - same logical state = same bytes
  final map = <String, dynamic>{
    'type': 'MyPayload',
    'data': _data,
    'timestamp': _timestamp,
  };
  
  // Sort keys for deterministic JSON
  final sortedKeys = map.keys.toList()..sort();
  final sortedMap = {for (var key in sortedKeys) key: map[key]};
  
  return Uint8List.fromList(utf8.encode(jsonEncode(sortedMap)));
}
```

#### 2. Conflict-Free Merge Logic

```dart
@override
MyPayload merge(covariant MyPayload other) {
  // Example: Last-Writer-Wins with tie-breaking
  if (_timestamp > other._timestamp) {
    return this;
  } else if (_timestamp < other._timestamp) {
    return other;
  } else {
    // Tie-breaking: use lexicographic order
    return _data.compareTo(other._data) >= 0 ? this : other;
  }
}
```

#### 3. Built-in CRDT Types

The library provides several pre-built CRDT types:

##### GSet (Grow-only Set)
```dart
import 'package:merkledag/merkledag.dart' show GSet;

// Use directly - no wrapper needed
final gset = GSet<String>();
final updated = gset.add('item');

// Factory for deserialization
GSet<String> gsetFactory(Uint8List bytes) {
  return GSet.fromCanonicalBytes(bytes);
}
```

##### LWW-Map (Last-Writer-Wins Map)
```dart
// See ProfileCrdtPayload for a complete example
class ProfileCrdtPayload implements CRDTPayload<ProfileData> {
  final Map<String, LwwEntry<dynamic>> _fields;
  
  ProfileCrdtPayload set(String key, dynamic value) {
    final timestamp = DateTime.now().millisecondsSinceEpoch;
    final newFields = Map.from(_fields);
    newFields[key] = LwwEntry(value, timestamp);
    return ProfileCrdtPayload._(_id, _publicKeyJwk, newFields);
  }
  
  @override
  ProfileCrdtPayload merge(covariant ProfileCrdtPayload other) {
    final mergedFields = Map<String, LwwEntry<dynamic>>.from(_fields);
    
    other._fields.forEach((key, otherEntry) {
      final localEntry = mergedFields[key];
      if (localEntry == null || otherEntry.timestamp > localEntry.timestamp) {
        mergedFields[key] = otherEntry;
      } else if (otherEntry.timestamp == localEntry.timestamp) {
        // Tie-breaking logic
        if (otherEntry.value.toString().compareTo(localEntry.value.toString()) > 0) {
          mergedFields[key] = otherEntry;
        }
      }
    });
    
    return ProfileCrdtPayload._(_id, _publicKeyJwk, mergedFields);
  }
}
```

## Network Layer

### GossipSub Integration

The library uses GossipSub for efficient head CID announcements:

```dart
final announcer = GossipSubAnnouncer(
  pubsub,
  '/my-app/heads/1.0.0', // Topic for head announcements
);

// The announcer is used internally by MerkleCrdtGossipManager
// but can be used directly:
await announcer.broadcast('QmHash...');
```

### DHT Integration

DHT is used for content discovery and provider announcements:

```dart
// DHT is optional but recommended for content discovery
final dagFetcher = P2PStreamDagFetcher<MyPayload, String>(
  host,
  blockStore,
  protocolId,
  payloadFactory,
  dht: host.dht, // Enable DHT integration
);

// Check DHT status
if (dagFetcher.isDHTAvailable) {
  print('DHT enabled for content discovery');
}
```

### Protocol Configuration

```dart
// Use versioned protocol IDs for compatibility
const DAG_SYNC_PROTOCOL = '/my-app/dag-sync/1.0.0';
const HEAD_TOPIC = '/my-app/heads/1.0.0';

// Consider network-specific prefixes
const TESTNET_PREFIX = '/testnet';
const MAINNET_PREFIX = '/mainnet';
```

## ActivityPub Integration

The library includes built-in support for ActivityPub-style social networking.

### Actor System

```dart
// Create an actor (identity)
final actor = await Actor.create();
print('Actor DID: ${actor.did}');
print('Public Key: ${actor.publicKeyJwk}');

// Create and sign content
final noteResult = await actor.createNote("Hello, decentralized world!");
final activityResult = await actor.createAndSignActivity(
  objectCid: noteResult.noteCid,
);

// Social actions
final followResult = await actor.follow('did:key:other-actor');
final likeResult = await actor.like(someContentCid);
final announceResult = await actor.announce(someActivityCid);

// Access CRDT states
final following = actor.following; // Set<String>
final liked = actor.likedItems; // Set<CID>
final outbox = actor.outboxActivities; // List<CID>
```

### Activity Types

The library supports standard ActivityPub activities:

```dart
// Create activities
final createActivity = CreateActivity(
  actor: actor.did,
  object: noteCid,
  published: DateTime.now().toUtc(),
);

// Follow activities
final followActivity = FollowActivity(
  actor: actor.did,
  objectDid: targetDid,
  published: DateTime.now().toUtc(),
);

// Like activities
final likeActivity = LikeActivity(
  actor: actor.did,
  objectCid: contentCid,
  published: DateTime.now().toUtc(),
);

// Announce (boost/retweet) activities
final announceActivity = AnnounceActivity(
  actor: actor.did,
  object: activityCid,
  published: DateTime.now().toUtc(),
);

// Delete activities
final deleteActivity = DeleteActivity(
  actor: actor.did,
  object: contentCid,
  published: DateTime.now().toUtc(),
);
```

### CRDT Integration

Integrate Actor CRDTs with the network layer:

```dart
// Create managers for each CRDT type
final followingManager = MerkleCrdtGossipManager<Set<String>, FollowingCrdtPayload>(
  dagFetcher: followingDagFetcher,
  announcer: followingAnnouncer,
);

final outboxManager = MerkleCrdtGossipManager<List<CID>, OutboxCrdtPayload>(
  dagFetcher: outboxDagFetcher,
  announcer: outboxAnnouncer,
);

// Sync actor state with network
await followingManager.applyLocalUpdate(actor.followingCrdtPayload);
await outboxManager.applyLocalUpdate(actor.outboxCrdtPayload);

// Listen for remote updates
followingManager.onStateChanged.listen((following) {
  // Handle remote follow/unfollow events
  updateUI(following);
});
```

## Best Practices

### 1. CRDT Design

#### ✅ Do:
- Design for commutativity: `merge(a, b) == merge(b, a)`
- Make merge operations idempotent: `merge(a, a) == a`
- Use deterministic serialization
- Include timestamps for LWW semantics
- Design for your specific conflict resolution needs

#### ❌ Don't:
- Rely on operation ordering
- Use non-deterministic data in payloads (random numbers, etc.)
- Create circular references in merge logic
- Ignore the performance implications of large CRDTs

### 2. Network Architecture

#### ✅ Do:
- Use versioned protocol IDs
- Implement proper error handling and timeouts
- Design for mobile/intermittent connectivity
- Use content addressing for tamper detection
- Implement proper backpressure handling

#### ❌ Don't:
- Bypass the DAGSyncer interface
- Ignore DHT availability in mobile environments
- Use blocking operations in event handlers
- Forget to handle network partitions

### 3. Performance Optimization

#### ✅ Do:
- Implement efficient BlockStore backends
- Use appropriate cache sizes
- Monitor DAG growth
- Implement batching for bulk operations
- Use lazy loading for large datasets

#### ❌ Don't:
- Keep unlimited data in memory
- Ignore serialization performance
- Create overly deep DAG structures
- Forget to clean up resources

### 4. Security Considerations

#### ✅ Do:
- Verify all received content against CIDs
- Implement proper signature verification
- Use secure random number generation
- Validate input data thoroughly
- Implement rate limiting

#### ❌ Don't:
- Trust unverified content
- Ignore signature validation
- Store sensitive data in CRDTs
- Forget about DoS attack vectors

## Performance Tuning

### Memory Management

```dart
// Configure cache sizes based on your environment
final blockStore = LRUBlockStore<MyPayload, String>(
  maxSize: 1000, // Adjust based on available memory
);

// Monitor memory usage
final stats = dagFetcher.getStats();
print('Memory usage: ${stats['memory_usage']}');
```

### Network Optimization

```dart
// Batch operations when possible
final updates = <MyPayload>[
  MyPayload('update1'),
  MyPayload('update2'),
  MyPayload('update3'),
];

// Apply in batch (if your CRDT supports it)
for (final update in updates) {
  await manager.applyLocalUpdate(update);
}

// Or implement custom batching logic
```

### DHT Configuration

```dart
// Tune DHT parameters for your network
final dhtConfig = DHTConfig(
  findProvidersTimeout: Duration(seconds: 10),
  maxProviders: 20,
  provideTimeout: Duration(seconds: 5),
);
```

### Mobile Optimization

```dart
// Use mobile-friendly settings
await manager.forceAnnounceCurrentHead(
  maxRetries: 5,
  retryDelay: Duration(seconds: 3),
);

// Monitor network state
if (await isNetworkAvailable()) {
  await manager.refreshState();
}
```

## Troubleshooting

### Common Issues

#### 1. Content Not Syncing

**Symptoms**: Remote updates not appearing locally

**Possible Causes**:
- DHT connectivity issues
- GossipSub subscription problems
- Firewall blocking P2P connections
- Incorrect protocol IDs

**Solutions**:
```dart
// Check DHT availability
if (!dagFetcher.isDHTAvailable) {
  print('DHT not available - check network connectivity');
}

// Verify protocol IDs match across peers
const PROTOCOL_ID = '/my-app/dag-sync/1.0.0';

// Check GossipSub subscriptions
final topics = pubsub.getSubscribedTopics();
print('Subscribed topics: $topics');

// Force refresh
await manager.refreshState();
```

#### 2. High Memory Usage

**Symptoms**: Memory usage growing over time

**Possible Causes**:
- Unbounded cache growth
- Memory leaks in event handlers
- Large CRDT payloads

**Solutions**:
```dart
// Implement bounded storage
final blockStore = LRUBlockStore<MyPayload, String>(maxSize: 1000);

// Monitor and clean up
Timer.periodic(Duration(minutes: 5), (_) {
  final stats = dagFetcher.getStats();
  if (stats['cache_size'] > 1000) {
    // Implement cleanup logic
  }
});

// Use weak references for event handlers
```

#### 3. Merge Conflicts

**Symptoms**: Unexpected CRDT states after merging

**Possible Causes**:
- Non-commutative merge logic
- Non-deterministic serialization
- Clock synchronization issues

**Solutions**:
```dart
// Test merge commutativity
final a = MyPayload('a');
final b = MyPayload('b');
assert(a.merge(b).value == b.merge(a).value);

// Use logical timestamps instead of wall clock
class MyPayload implements CRDTPayload<String> {
  final int logicalClock;
  
  MyPayload merge(covariant MyPayload other) {
    final newClock = math.max(logicalClock, other.logicalClock) + 1;
    // ... merge logic
  }
}
```

### Debugging Tools

#### Enable Debug Logging

```dart
import 'package:logging/logging.dart';

Logger.root.level = Level.FINE;
Logger.root.onRecord.listen((record) {
  print('${record.level.name}: ${record.loggerName}: ${record.message}');
});

// Create manager with custom logger
final manager = MerkleCrdtGossipManager<V, P>(
  dagFetcher: dagFetcher,
  announcer: announcer,
  loggerName: 'MyApp.Debug',
);
```

#### Monitor Network Activity

```dart
// Check peer connections
final peers = host.network.peers;
print('Connected peers: ${peers.length}');

// Monitor DHT activity
final dhtStats = dagFetcher.getStats();
print('DHT stats: $dhtStats');

// Track CRDT operations
manager.onStateChanged.listen((state) {
  print('State changed: $state');
});

manager.onCidChanged.listen((cid) {
  print('New head CID: $cid');
});
```

## Advanced Usage

### Custom CRDT Types

Implement domain-specific CRDTs:

```dart
class CounterCRDT implements CRDTPayload<int> {
  final Map<String, int> _counts;
  
  CounterCRDT(this._counts);
  
  @override
  int get value => _counts.values.fold(0, (a, b) => a + b);
  
  CounterCRDT increment(String actorId) {
    final newCounts = Map<String, int>.from(_counts);
    newCounts[actorId] = (newCounts[actorId] ?? 0) + 1;
    return CounterCRDT(newCounts);
  }
  
  @override
  CounterCRDT merge(covariant CounterCRDT other) {
    final merged = Map<String, int>.from(_counts);
    other._counts.forEach((actor, count) {
      merged[actor] = math.max(merged[actor] ?? 0, count);
    });
    return CounterCRDT(merged);
  }
  
  @override
  Uint8List toCanonicalBytes() {
    final sortedEntries = _counts.entries.toList()
      ..sort((a, b) => a.key.compareTo(b.key));
    final map = Map.fromEntries(sortedEntries);
    return Uint8List.fromList(utf8.encode(jsonEncode(map)));
  }
}
```

### Multi-CRDT Applications

Manage multiple CRDTs in a single application:

```dart
class MultiCRDTApp {
  final Map<String, MerkleCrdtGossipManager> _managers = {};
  
  Future<void> addCRDT<V, P extends CRDTPayload<V>>(
    String name,
    MerkleCrdtGossipManager<V, P> manager,
  ) async {
    _managers[name] = manager;
    
    // Cross-CRDT event handling
    manager.onStateChanged.listen((state) {
      _handleCRDTUpdate(name, state);
    });
  }
  
  void _handleCRDTUpdate(String crdtName, dynamic state) {
    // Implement cross-CRDT logic
    switch (crdtName) {
      case 'following':
        _updateFollowingUI(state);
        break;
      case 'posts':
        _updatePostsUI(state);
        break;
    }
  }
}
```

### Custom Network Protocols

Extend the networking layer:

```dart
class CustomP2PProtocol extends P2PNodeProtocol<MyPayload, String> {
  CustomP2PProtocol(
    Host host,
    String protocolId,
    MyPayload Function(Uint8List) payloadFactory,
    Future<MerkleNode<MyPayload>?> Function(CID) localNodeGetter,
  ) : super(host, protocolId, payloadFactory, localNodeGetter);
  
  @override
  Future<void> handleCustomMessage(String message, P2PStream stream) async {
    if (message.startsWith('CUSTOM_REQUEST ')) {
      // Handle custom protocol messages
      await _handleCustomRequest(message, stream);
    } else {
      // Delegate to parent
      await super.handleIncomingMessage(message, stream);
    }
  }
}
```

### Integration with External Systems

Bridge with traditional databases:

```dart
class DatabaseBridge<V, P extends CRDTPayload<V>> {
  final MerkleCrdtGossipManager<V, P> _crdtManager;
  final Database _database;
  
  DatabaseBridge(this._crdtManager, this._database) {
    _crdtManager.onStateChanged.listen(_syncToDatabase);
  }
  
  Future<void> _syncToDatabase(V? state) async {
    if (state != null) {
      await _database.upsert('crdt_state', {
        'data': state,
        'cid': _crdtManager.latestKnownCid,
        'updated_at': DateTime.now().toIso8601String(),
      });
    }
  }
  
  Future<void> loadFromDatabase() async {
    final row = await _database.query('crdt_state').first;
    if (row != null) {
      // Reconstruct CRDT state from database
      // This requires careful handling of the CRDT semantics
    }
  }
}
```

---

## Conclusion

This library provides a solid foundation for building decentralized applications with strong eventual consistency guarantees. By following the patterns and best practices outlined in this guide, you can build robust P2P applications that work reliably across diverse network conditions.

For more examples and advanced usage patterns, see the `/example` directory and the test suite.

### Additional Resources

- [Merkledag Package Documentation](https://pub.dev/packages/merkledag)
- [LibP2P Dart Documentation](https://pub.dev/packages/dart_libp2p)
- [CRDT Research Papers](https://crdt.tech/)
- [ActivityPub Specification](https://www.w3.org/TR/activitypub/)

### Contributing

This library is actively maintained. Please see the CONTRIBUTING.md file for guidelines on contributing to the project.
