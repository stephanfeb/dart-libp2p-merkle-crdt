# Dart Libp2p Merkle-CRDT Sync

This package provides a synchronization layer for `MerkleCRDT` instances (from the `merkledag` package) over a `dart_libp2p` network. It facilitates the replication of Merkle-CRDTs among multiple peers by:

*   Announcing new CRDT heads (root CIDs) using libp2p GossipSub.
*   Fetching required Merkle-DAG nodes directly from peers via libp2p streams.
*   Utilizing a Kademlia DHT (if available) for discovering peers that can provide specific DAG nodes.

It's designed to be used in conjunction with the `merkledag` package for the core Merkle-CRDT logic and `dart_libp2p` for the underlying peer-to-peer networking.

## Features

*   **Merkle-CRDT State Synchronization:** Keeps Merkle-CRDT instances eventually consistent across multiple libp2p peers.
*   **GossipSub for Head Announcements:** Efficiently disseminates new CRDT state heads (root CIDs) using `dart_libp2p_pubsub`.
*   **Direct P2P Stream-based DAG Syncing:** Fetches only the necessary Merkle-DAG nodes directly from peers, minimizing redundant data transfer.
*   **DHT Provider Discovery:** Can leverage a Kademlia DHT (e.g., `dart_libp2p_kat_dht`) to find peers holding specific DAG nodes.
*   **Local Block Storage:** Includes an in-memory block store (`InMemoryBlockStore`) and an interface (`BlockStore`) for custom persistent storage solutions.
*   **Centralized Management:** Provides a `MerkleCrdtGossipManager` class to orchestrate the CRDT instance, DAG syncer, and broadcaster.
*   **Generic:** Works with any CRDT payload type `P` that implements `CRDTPayload<V>` from the `merkledag` package.

## Core Components

*   **`MerkleCrdtGossipManager<V, P>`:** The main class an application interacts with. It manages a `MerkleCRDT` instance and coordinates synchronization.
*   **`P2PStreamDagFetcher<P, V>`:** Implements the `DAGSyncer<P>` interface from `merkledag`. Responsible for fetching and storing `MerkleNode<P>` objects. Uses libp2p streams for direct transfer and can query a DHT for providers.
*   **`GossipSubAnnouncer`:** Implements the `Broadcaster` interface from `merkledag`. Uses `dart_libp2p_pubsub` to broadcast new head CIDs and subscribe to remote head announcements.
*   **`BlockStore<P, V>`:** Interface for storing `MerkleNode<P>` objects locally.
    *   **`InMemoryBlockStore<P, V>`:** A simple in-memory implementation.

## Getting Started

### Prerequisites

*   A working Dart environment.
*   A configured libp2p `Host` instance from `dart_libp2p`.
*   A `PubSub` instance from `dart_libp2p_pubsub` (typically using `GossipSubRouter`).
*   Optionally, an `IpfsDHT` instance from `dart_libp2p_kat_dht` if DHT provider discovery is desired.

### Add Dependencies

Add this package and its peer dependencies to your `pubspec.yaml`. Since these are likely local/path dependencies during development:

```yaml
dependencies:
  # This package
  dart_libp2p_merkle_crdt:
    path: [path_to_this_package] # Replace with actual path or version if published

  # Core Merkle-CRDT logic
  merkledag:
    path: [path_to_merkledag_package] # e.g., /Users/stephanfeb/IdeaProjects/bsv_apps/merkledag
  
  # Libp2p stack
  dart_libp2p:
    path: [path_to_dart_libp2p_package] # e.g., /Users/stephanfeb/IdeaProjects/dart-libp2p
  dart_libp2p_pubsub:
    path: [path_to_dart_libp2p_pubsub_package] # e.g., /Users/stephanfeb/IdeaProjects/bsv_apps/dart-libp2p-pubsub
  dart_libp2p_kat_dht: # Optional, for DHT support in P2PStreamDagFetcher
    path: [path_to_dart_libp2p_kat_dht_package] # e.g., /Users/stephanfeb/IdeaProjects/bsv_apps/dart-libp2p-kat-dht
  
  # Other common dependencies
  dart_cid:
    path: [path_to_dart_cid_package] # e.g., /Users/stephanfeb/IdeaProjects/bsv_apps/dart-cid
  logging: ^1.2.0 # Or your preferred logging package version
```

Run `dart pub get`.

## Usage

Here's an example of how to set up and use the `MerkleCrdtGossipManager` with a `GSet<String>` CRDT.

```dart
import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
// Assuming you have set up your libp2p Host, PubSub, and optionally DHT
// import your_libp2p_setup_file.dart'; 

import 'package:merkledag/merkledag.dart' show GSet, CRDTPayload; // For GSet and CRDTPayload
import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:logging/logging.dart';

// Example Payload Factory for GSet<String>
// P (Payload Type) is GSet<String>
// V (Value Type) is Set<String>
GSet<String> gsetPayloadFactory(Uint8List payloadBytes) {
  // This is a simplified example. GSet would need a proper fromBytes constructor.
  // For now, assuming it can be reconstructed or this factory is more complex.
  // If GSet.fromCanonicalBytes() existed: return GSet.fromCanonicalBytes(payloadBytes);
  // For this example, let's assume it's empty if bytes are unrecognized.
  // In a real scenario, GSet would need a proper deserialization method.
  _logger.warning('gsetPayloadFactory: Deserialization from Uint8List is a placeholder.');
  return GSet<String>(); // Placeholder: returns an empty GSet
}

// Placeholder for your libp2p Host, PubSub, and DHT instances
late Host myHost;
late PubSub myPubSub;
IpfsDHT? myDht; // Optional

final _logger = Logger('MerkleCrdtUsageExample');

void main() async {
  // --- Setup Phase (Illustrative - replace with your actual libp2p setup) ---
  // 1. Initialize Logger (optional)
  Logger.root.level = Level.INFO;
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
    if (record.error != null) print('ERROR: ${record.error}, ${record.stackTrace}');
  });

  // 2. Initialize your libp2p Host, PubSub, and DHT (myHost, myPubSub, myDht)
  // This part is highly dependent on your application's libp2p setup.
  // For example, using `createLibp2pNode` from `test/real_net_stack.dart` in this package
  // or a similar utility in your main application.
  // myHost = await setupMyLibp2pHost();
  // myPubSub = PubSub(myHost, GossipSubRouter()); // Example
  // await myPubSub.start();
  // myDht = await setupMyDht(myHost); // Example
  // await myDht?.bootstrap();
  // For this example to run, these would need to be actual initialized instances.
  // This example will not run as-is without a concrete libp2p stack.
  print('Placeholder: Initialize myHost, myPubSub, and optionally myDht here.');
  // return; // Uncomment if you want to stop before manager setup without real instances.


  // --- MerkleCrdtGossipManager Setup ---

  // 3. Create a unique ID for this CRDT instance (e.g., a document ID)
  // This ID is used to scope GossipSub topics.
  const crdtInstanceId = 'my-shared-document-123';

  // 4. Create the BlockStore
  final blockStore = InMemoryBlockStore<GSet<String>, Set<String>>();

  // 5. Create the P2PStreamDagFetcher (DAGSyncer)
  // The protocol ID can be any unique string for your DAG sync protocol.
  final dagFetcher = P2PStreamDagFetcher<GSet<String>, Set<String>>(
    myHost,
    blockStore,
    '/my-app/merkle-crdt-dag-sync/1.0.0',
    gsetPayloadFactory, // Provide the factory for your payload type P
    // cidFunction was removed from P2PStreamDagFetcher constructor
    dht: myDht, // Pass the DHT instance if you have one
  );

  // 6. Create the GossipSubAnnouncer (Broadcaster)
  final announcer = GossipSubAnnouncer(
    myHost,
    myPubSub,
    crdtInstanceId, // Ensures announcements are scoped to this CRDT
  );

  // 7. Create the MerkleCrdtGossipManager
  // V = Set<String>, P = GSet<String>
  final manager = MerkleCrdtGossipManager<Set<String>, GSet<String>>(
    dagFetcher: dagFetcher,
    announcer: announcer,
  );

  // --- Using the Manager ---

  // 8. Listen to state changes
  manager.onStateChanged.listen((Set<String>? newState) {
    _logger.info('CRDT state changed (logical value): $newState');
    // Note: onStateChanged emits V?, which is Set<String>? in this case.
  });

  // 9. Apply a local update
  _logger.info('Applying first local update...');
  final update1 = GSet<String>();
  update1.add('apple');
  update1.add('banana');
  await manager.applyLocalUpdate(update1);
  // Current state (logical value V) will be {'apple', 'banana'}
  // This will also trigger a broadcast of the new head CID.

  // Simulate some time for gossip and potential remote updates
  await Future.delayed(Duration(seconds: 2));

  // 10. Apply another local update
  _logger.info('Applying second local update...');
  final update2 = GSet<String>();
  update2.add('cherry');
  // Note: GSet merge logic means 'apple' and 'banana' are preserved.
  await manager.applyLocalUpdate(update2);
  // Current state (logical value V) will be {'apple', 'banana', 'cherry'}

  // 11. Get the current state directly
  Set<String>? currentState = await manager.getState();
  _logger.info('Directly fetched current state (logical value): $currentState');

  // 12. Refresh state (useful if MerkleCRDT doesn't have its own state stream for remote changes)
  // This explicitly fetches the latest state from the underlying MerkleCRDT,
  // which would include any remote changes processed internally by MerkleCRDT.
  await manager.refreshState();
  _logger.info('State after explicit refresh (logical value): ${manager.currentState}');


  // --- Cleanup ---
  // Depending on your application, you might want to close the manager
  // when it's no longer needed. This will stop listening for remote heads.
  // The underlying Host, PubSub, DHT, etc., should be managed separately.
  // await manager.close();
  // await myPubSub.stop();
  // await myDht?.close();
  // await myHost.close();
}
```

## Architecture Notes

*   **`MerkleCRDT` (from `merkledag` package):** This is the core data structure. It manages the Merkle-DAG of CRDT payloads and their Merkle clock. It uses a `DAGSyncer` to fetch/store nodes and a `Broadcaster` to announce/receive heads.
*   **`P2PStreamDagFetcher` (`DAGSyncer`):**
    *   When `MerkleCRDT` needs a node (`get(CID)`), the fetcher first checks its local `BlockStore`.
    *   If not found, and if a DHT is provided, it queries the DHT for peers (`AddrInfo`) providing that CID.
    *   It then attempts to connect to these peers (or the peer from which a head was announced) and requests the node over a direct libp2p stream using a custom protocol.
    *   When `MerkleCRDT` creates a new node (`put(MerkleNode)`), the fetcher stores it in the `BlockStore` and (if a DHT is provided) announces itself as a provider for that node's CID on the DHT.
*   **`GossipSubAnnouncer` (`Broadcaster`):**
    *   When `MerkleCRDT` has a new head (e.g., after `add(payload)`), it calls `broadcast(cidString)` on the announcer. The announcer then publishes a small JSON message `{ "cid": "...", "senderPeerId": "..." }` to a CRDT-instance-specific GossipSub topic.
    *   The `MerkleCRDT` (via the announcer passed to its constructor) also calls `subscribe()` on the announcer. The `MerkleCRDT` itself listens to this stream of incoming CID strings and internally processes them (fetching data via the `DAGSyncer` and merging).
*   **`MerkleCrdtGossipManager`:**
    *   Initializes and holds the `MerkleCRDT`, `P2PStreamDagFetcher`, and `GossipSubAnnouncer`.
    *   Provides a simplified API (`applyLocalUpdate`, `getState`, `onStateChanged`) to the application, dealing with the logical value type `V`.
    *   Converts between the logical value `V` and the CRDT payload `P` where necessary (e.g., `P.value` gives `V`).

## Important Considerations

*   **`MerkleCRDT`'s Internal Head Processing:** This library assumes that the `MerkleCRDT` class from the `merkledag` package, when given a `Broadcaster`, will internally call `broadcaster.subscribe()` and process the incoming CIDs to trigger its merge logic (which in turn uses the `DAGSyncer`). If this is not the case, the `MerkleCrdtGossipManager` would need to be adjusted to explicitly handle CIDs from the announcer's stream and call an appropriate method on `MerkleCRDT` to incorporate them.
*   **Payload Deserialization (`payloadFactory`):** The `P2PStreamDagFetcher` requires a `payloadFactory` function (`P Function(Uint8List payloadBytes)`) to deserialize `MerkleNode` payloads received over the network. You must provide a correct factory for your specific `CRDTPayload` type `P`.
*   **Serialization/Deserialization of `MerkleNode`:** The current implementation in `P2PStreamDagFetcher` uses a simplified JSON-based serialization for `MerkleNode` objects for transmission over P2P streams. For production, consider a more efficient and robust binary format (e.g., Protobuf).
*   **Error Handling & Resilience:** The current error handling is basic. Production systems would require more sophisticated retry mechanisms, connection management, and error reporting.

## Additional Information

*   **`merkledag` package:** [Link to your `merkledag` package or its documentation]
*   **`dart_libp2p` suite:** [Link to `dart_libp2p` and related packages]

To contribute, please file an issue or submit a pull request.
