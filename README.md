# dart_libp2p_merkle_crdt

Replicates Merkle-CRDTs from the [`merkledag`](https://pub.dev/packages/merkledag) package between [`dart_libp2p`](https://pub.dev/packages/dart_libp2p) peers.

- Each replica announces its heads (the root CIDs of its DAG) on a GossipSub topic.
- When a peer hears a head it doesn't have, it fetches the missing blocks over a libp2p stream protocol, verifies them, and merges.
- It asks the announcing peer first, so sync works without a DHT, for example over a BLE mesh. A Kademlia DHT can be added as a fallback for finding blocks.

## Security model

- **Blocks are verified.** A fetched block is accepted only if its content hashes to the CID that was requested. Forged content under a real CID is rejected. A stored block is never replaced, because a CID names exactly one block.
- **Only allowed writers can publish heads.** Each `GossipSubAnnouncer` takes a `WriterAuthorizer`. Announcements from other peers are dropped before anything is fetched, and they are not relayed. The check uses the message's author, which GossipSub authenticates when messages are signed (StrictSign, the default). Authorising a head also authorises the DAG behind it, because every block in that DAG is hash-verified.
- **Work is bounded.** Merging visits at most `maxTraversalNodes` nodes (10,000 by default) and gives up after `maxMissingNodes` unavailable nodes. Its cost grows with how far two replicas have diverged, not with the length of history. Fetches have a timeout and a maximum block size (4 MiB by default), and the server side enforces both too. A slow or hostile peer cannot hold up local writes.

## Usage

```dart
final fetcher = P2PStreamDagFetcher<GSet<String>, Set<String>>(
  host,
  InMemoryBlockStore(),           // Or your own persistent BlockStore.
  defaultFetchProtocolId,
  GSet<String>.fromCanonicalBytes, // Decodes payload bytes.
  dht: dht,                        // Optional.
);
final announcer = GossipSubAnnouncer(
  host, pubsub, 'my-crdt-instance',
  writers: WriterAuthorizer.only([alice, bob]),
);
final replica = MerkleCrdtGossipManager<Set<String>, GSet<String>>(
  dagFetcher: fetcher,
  announcer: announcer,
);

replica.onStateChanged.listen((state) => print('now: $state'));
await replica.applyLocalUpdate(GSet<String>.fromList(['hello']));

// After connectivity returns. GossipSub does not replay missed messages.
await replica.forceAnnounceCurrentHead();

// Stops syncing, leaves the topic and stops serving blocks.
await replica.close();
```

[`example/dart_libp2p_merkle_crdt_example.dart`](example/dart_libp2p_merkle_crdt_example.dart) runs two peers end to end.

### Payloads

Any `CRDTPayload<V>` works. Merkle-CRDT payloads here are state-based: each new node carries the full merged state, and the replicated value is the merge of the current heads. So the payload's `merge` must be commutative, associative and idempotent, and its `toCanonicalBytes` must be deterministic. A payload whose canonical bytes do not decode back to the same bytes is rejected when received.

Build updates from the current payload (`await replica.getPayload()`), so each update carries the causal history it replaces.

This package also ships two payloads:

- `DocumentCRDT`: a JSON-like map with per-key last-writer-wins or first-writer-wins, and tombstones for deletes.
- `VectorClock`: the causality tracking `DocumentCRDT` uses.

### Deleting data

Deleting is a CRDT operation, such as `DocumentCRDT.delete`, which leaves a tombstone. Blocks themselves are immutable and are never removed from the store. Peers may still need them to walk the DAG, and an announcement would fetch a removed block again anyway.

### Writer authorisation

| | |
|---|---|
| `WriterAuthorizer.only(peerIds)` | A fixed allowlist. |
| `WriterAuthorizer.allowAll()` | Any peer may write. Only for open CRDTs. |
| Your own subclass | For example, membership that changes at runtime. |

## Wire protocol

- **Heads topic:** `merkle-crdt/heads/2.0.0/<instance id>`. Each message is the raw bytes of one CID.
- **Block fetch:** `/merkle-crdt/fetch/2.0.0`. One request and one response per stream, each sent as a frame (an unsigned-varint length followed by the bytes).
  - Request: `0x01` followed by the CID bytes.
  - Response: one of
    - `0x00` followed by the node bytes (`MerkleNode.toBytes`), if the block is held;
    - `0x01`, if it is not;
    - `0x02` followed by a UTF-8 error message.

Version 2.0.0 does not interoperate with 1.x peers. See the [changelog](CHANGELOG.md).

## Related packages

- `dart_libp2p_merkle_crdt_social`: profile, following, liked and outbox payloads and ActivityPub-style activities. These moved out of this package in 2.0.0.
- [`dart_libp2p_bitswap`](https://github.com/stephanfeb/dart-libp2p-bitswap): Bitswap. This package never used it to sync; a copy of it was removed in 2.0.0.
