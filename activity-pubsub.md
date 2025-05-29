# ActivityPub-Inspired P2P Primitives: Phased Development Plan ("activity-pubsub")

This document outlines a phased approach to building a set of P2P social primitives inspired by ActivityPub, leveraging Merkle-CRDTs, IPLD, DIDs, and libp2p (including Bitswap and GossipSub). The goal is to create a foundational toolkit for decentralized social applications.

## Core Technologies & Assumptions

*   **Identities**: Decentralized Identifiers (DIDs), likely `did:key` initially.
*   **Data Model**: IPLD for content objects and activities.
*   **Conflict Resolution**: CRDTs for mutable collections and actor states.
*   **Replication**: `dart-libp2p-merkle-crdt` for CRDT replication via GossipSub (for CRDT heads/updates).
*   **Content Exchange**: A working Bitswap implementation in Dart for fetching IPLD blocks by CID.
*   **Discovery**: Topic-based discovery via GossipSub/DHT for actor CRDTs.

## Phase 1: Core Identity, Content Foundation & Basic Publishing

**Goal**: Establish actor identity, allow creation and publication of basic content, and enable content retrieval.

*   **[x] Actor Identity & Key Management**
    *   [x] Generate/manage cryptographic key pairs for actors.
    *   [x] Derive DIDs (e.g., `did:key`) from public keys.
*   **[x] Actor Profile CRDT**
    *   [x] Define Profile CRDT structure (LWW-Map or JSON-CRDT).
        *   Required fields: `id` (DID), `type` ("Person", "Service", etc.), `publicKeyJwk`.
        *   Recommended fields: `name`, `preferredUsername`, `summary`, `icon` (CID to image), `image` (CID to image).
    *   [x] Implement actor ability to create and update their Profile CRDT.
    *   [x] Mechanism for Profile CRDT head to be gossiped (e.g., on topic `profiles/<actor_did>`).
    *   [x] Mechanism for clients to discover and sync an actor's Profile CRDT.
*   **[x] Content Object (`Note`) - IPLD**
    *   [x] Define IPLD schema for a simple `Note` (e.g., `{"@context": "...", "type": "Note", "content": "...", "published": "timestamp"}`).
    *   [x] Actors can create `Note` IPLD blocks, resulting in a CID.
*   **[x] `Create` Activity - IPLD**
    *   [x] Define IPLD schema for `Create` activity (`{"@context": "...", "type": "Create", "actor": "actor_did", "object": "note_cid", "published": "timestamp"}`).
    *   [x] Activities are signed by the actor's private key.
    *   [x] Actors can create `Create` activity IPLD blocks, resulting in a CID.
*   **[x] `outbox` CRDT (Append-only Log)**
    *   [x] Define `outbox` as an append-only log CRDT storing CIDs of activities.
    *   [x] Implement actor ability to append new activity CIDs to their `outbox`.
    *   [x] Mechanism for `outbox` CRDT head to be gossiped (e.g., on topic `outboxes/<actor_did>` or discoverable via Profile CRDT).
    *   [x] Mechanism for clients to discover and sync an actor's `outbox`.
*   **[x] Bitswap Integration & Content Availability**
    *   [x] Client ability to request IPLD blocks (Activities, Notes) by CID via Bitswap.
    *   [x] Actors automatically "pin" (make available via Bitswap) their own created content and activities (via local BlockStore).
*   **[x] Basic Client Functionality (Proof of Concept)**
    *   [x] Create an actor profile.
    *   [x] Publish a `Note` to the actor's `outbox`.
    *   [x] View another actor's profile.
    *   [x] View another actor's `outbox` and retrieve/display their `Note`s.

## Phase 2: Basic Social Interactions

**Goal**: Enable core social graph actions like following and basic engagement like liking.

*   **[x] `Follow` Activity - IPLD**
    *   [x] Define IPLD schema for `Follow` activity (`{"type": "Follow", "actor": "follower_did", "object": "followed_actor_did"}`).
    *   [x] `Follow` activity is signed and added to the follower's `outbox`.
*   **[x] `following` CRDT (OR-Set)**
    *   [x] Define `following` as an OR-Set CRDT storing DIDs of actors being followed.
    *   [x] Implement actor ability to add/remove DIDs from their `following` set.
    *   [x] Mechanism for `following` CRDT head to be gossiped/discovered.
*   **[x] `Like` Activity - IPLD**
    *   [x] Define IPLD schema for `Like` activity (`{"type": "Like", "actor": "liker_did", "object": "liked_content_cid"}`).
    *   [x] `Like` activity is signed and added to the liker's `outbox`.
*   **[x] `liked` CRDT (OR-Set)**
    *   [x] Define `liked` as an OR-Set CRDT storing CIDs of liked content.
    *   [x] Implement actor ability to add/remove CIDs from their `liked` set.
    *   [x] Mechanism for `liked` CRDT head to be gossiped/discovered.
*   **[x] Client-Side Aggregation & Feed**
    *   [x] Client can sync `following` set of the local user.
    *   [x] Client syncs `outbox` CRDTs of followed actors.
    *   [x] Basic chronological feed generation from aggregated outboxes.
    *   [x] Display liked content (e.g., on a profile or content item).

## Phase 3: Enhanced Interactions & Content Management

**Goal**: Introduce replies, content sharing (boosts), and a mechanism for content removal.

*   **[x] `Reply` Activity & `inReplyTo` - IPLD**
    *   [x] `Reply` content is a `Note` object (IPLD).
    *   [x] `Create` activity for the reply includes `inReplyTo: "original_content_cid"` property.
    *   [x] Reply activity is signed and added to the replier's `outbox`.
*   **[x] Threading & Reply Discovery (Client-Side)**
    *   [x] Client reconstructs conversation threads by linking `inReplyTo` properties.
    *   [x] Initial reply discovery: e.g., client scans outboxes of followed users for replies to current user's content.
*   **[x] `Announce` (Boost/Share) Activity - IPLD**
    *   [x] Define IPLD schema for `Announce` activity (`{"type": "Announce", "actor": "announcer_did", "object": "announced_content_cid"}`).
    *   [x] `Announce` activity is signed and added to the announcer's `outbox`.
    *   [x] Client displays announced content, attributing original author and announcer.
*   **[x] `Delete` Activity (Tombstoning) - IPLD**
    *   [x] Define IPLD schema for `Delete` activity (`{"type": "Delete", "actor": "deleter_did", "object": "content_to_delete_cid"}`).
    *   [x] `Delete` activity is signed and added to the actor's `outbox`.
*   **[x] Client Handling of `Delete`**
    *   [x] Client hides content associated with a `Delete` activity.
    *   [x] Actor's client unpins (stops serving via Bitswap) their "deleted" content.
    *   [x] Other clients may optionally unpin "deleted" content from their local cache.
*   **[x] Actor Mentions (Basic)**
    *   [x] Convention for `Mention` tags within `Note` content (e.g., `{"type": "Mention", "href": "mentioned_actor_did"}`).
    *   [x] Initial mention discovery: e.g., client-side scanning of content from followed users.

## Phase 4: Scalability, UX Enhancements & Advanced Features (Future Roadmap)

**Goal**: Address scalability, improve user experience, and explore more advanced P2P social features. This phase is more research-oriented.

*   **[ ] Advanced Discovery Mechanisms (Replies, Mentions)**
    *   [ ] Research/prototype lightweight decentralized indexing for replies/mentions.
    *   [ ] Research/prototype targeted P2P notifications (if feasible without central points).
*   **[ ] Storage & Replication Strategies**
    *   [ ] Implement intelligent client-side caching policies (e.g., LRU, size limits).
    *   [ ] Allow users to explicitly "pin" important content on their node.
    *   [ ] Explore concepts for community pinning/replication nodes (incentivized or altruistic).
*   **[ ] Moderation Primitives (Client-Side)**
    *   [ ] Actor-specific block list (e.g., a CRDT set of DIDs to ignore).
    *   [ ] Actor-specific mute list (e.g., a CRDT set of DIDs whose content is hidden by default).
*   **[ ] Group Functionality (Exploratory)**
    *   [ ] Concept for `Group` actors.
    *   [ ] CRDTs for group membership, shared content within groups.
*   **[ ] Selective Sync & Large Object Handling**
    *   [ ] Strategies for syncing only parts of large outboxes (e.g., by date range, by count).
    *   [ ] Handling large media: IPLD manifests for different resolutions/chunks, fetch-on-demand for full versions.
*   **[ ] Reputation Systems (Research)**
*   **[ ] Private Interactions / Direct Messaging (Research)**
    *   [ ] End-to-end encrypted DMs using P2P protocols (e.g., libp2p stream encryption, Signal protocol variants).

This phased approach allows for iterative development, focusing on delivering core functionality first and then building upon it. Each checklist item represents a significant piece of work that may involve schema definition, CRDT implementation choices, network interaction logic, and client UI considerations.
