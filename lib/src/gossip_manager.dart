// lib/src/gossip_manager.dart

/// Manages a Merkle-CRDT instance over a libp2p network using GossipSub for head announcements
/// and direct P2P streams for DAG node synchronization.
library merkle_crdt_gossip_manager;

// Core gossip manager components
export 'gossip_manager/block_store.dart';
export 'gossip_manager/p2p_stream_dag_fetcher.dart';
export 'gossip_manager/gossipsub_announcer.dart';
export 'gossip_manager/merkle_crdt_gossip_manager.dart';

// BlockStore interface and supporting classes
export 'gossip_manager/block_store_entry.dart';
export 'gossip_manager/block_store_exceptions.dart';
export 'gossip_manager/tombstone_metadata.dart';

// Query system
export 'gossip_manager/block_query.dart';
export 'gossip_manager/field_extractor.dart';
export 'gossip_manager/document_field_extractor.dart';
export 'gossip_manager/pagination.dart';

// Change notification system
export 'gossip_manager/block_change_event.dart';
export 'gossip_manager/change_subscription.dart';

// P2P and DHT components
export 'gossip_manager/p2p_node_protocol.dart';
export 'gossip_manager/dht_provider.dart';

// CRDT implementations
export 'p2p/crdt/vector_clock.dart';
export 'p2p/crdt/document_crdt.dart';
export 'p2p/crdt/profile_crdt.dart';
export 'p2p/crdt/following_crdt.dart';
export 'p2p/crdt/liked_crdt.dart';
export 'p2p/crdt/outbox_crdt.dart';

// Identity and actor system
export 'p2p/identity/actor.dart';

// IPLD activity types
export 'p2p/ipld/activity.dart';
export 'p2p/ipld/announce_activity.dart';
export 'p2p/ipld/delete_activity.dart';
export 'p2p/ipld/follow_activity.dart';
export 'p2p/ipld/like_activity.dart';
export 'p2p/ipld/note.dart';
