## 2.0.0

Breaking: the wire protocol changed (new heads topic and fetch protocol), CIDs changed (merkledag 2.0.0), and the API is smaller. 1.x and 2.x peers do not interoperate.

Security and correctness:

- **Fetched blocks are verified.** They must hash to the requested CID. 1.x compared the requested CID with a CID copied from the response, so any peer could serve forged content. `BlockStore.put` no longer replaces a stored block.
- **Writer authorisation.** `GossipSubAnnouncer` requires a `WriterAuthorizer`, checked against the authenticated message author. Announcements from other peers are not fetched, merged or relayed.
- **Merging is bounded** and no longer holds the CRDT lock while fetching. See merkledag 2.0.0.
- **Fetch protocol v2** (`/merkle-crdt/fetch/2.0.0`): length-prefixed binary frames, so replies larger than one muxer frame work. Both sides enforce size limits and timeouts.
- **Sync works without a DHT.** Blocks are requested from the announcing peer first, then from DHT providers if a DHT is configured. The fixed 100 ms and 50 ms delays are gone.
- **One fetch per announcement.** Duplicate announcements are coalesced. A head that cannot be fetched is retried with backoff and then merged, and state notifications fire only after a real change.
- **`MerkleCrdtGossipManager.close()` stops everything.** It removes the stream handler, cancels the subscriptions and retries, and leaves the topic. The manager now owns, and closes, its fetcher and announcer.
- **UTF-8.** `DocumentCRDT` and `VectorClock` encode text as UTF-8; 1.x corrupted non-ASCII text. Their canonical bytes now sort nested map keys.
- **`DocumentCRDT` converges.**
  - Conflicts are resolved by a total order that respects causality. 1.x's order could cycle, so three replicas could disagree.
  - `actorId` is now required.
  - `ConflictResolutionStrategy.merge` is deprecated and behaves as `lastWriterWins`, because merging nested values cannot converge without per-element metadata.

API:

- New: `WriterAuthorizer`, `HeadAnnouncement`, `GossipSubAnnouncer.announcements`, `MerkleCrdtGossipManager.heads`, `MerkleCrdtGossipManager.getPayload()`, `defaultFetchProtocolId`, `FrameReader`, `encodeFrame`.
- `BlockStore` is now `get`, `has` and `put`. Removed: the query builder (its `or()`, `not()` and `endGroup()` did nothing), collections, metadata, change notifications and soft deletes.
- `forceAnnounceCurrentHead()` takes no arguments and announces every head.
- Removed: the social payloads, `Actor` and the ActivityPub types (moved to `dart_libp2p_merkle_crdt_social`), and the vendored Bitswap copy (use `dart_libp2p_bitswap`).

Dependencies:

- Requires merkledag ^2.0.0.
- Requires dart_libp2p 4.0.1 to <5.0.0 (earlier releases fail the integration tests inside libp2p) and dart_libp2p_pubsub 1.6.0 to <4.0.0.
- Seven unused dependencies dropped, plus those used only by the removed code.

## 1.0.0

- First release on pub.dev.
- Depends on `merkledag` from pub.dev instead of a local path.
