// lib/src/gossip_manager.dart

/// Manages a Merkle-CRDT instance over a libp2p network using GossipSub for head announcements
/// and direct P2P streams for DAG node synchronization.
library merkle_crdt_gossip_manager;

// Core gossip manager components
export 'gossip_manager/block_store.dart';
export 'gossip_manager/p2p_stream_dag_fetcher.dart';
export 'gossip_manager/gossipsub_announcer.dart';
export 'gossip_manager/merkle_crdt_gossip_manager.dart';
export 'gossip_manager/writer_authorizer.dart';
export 'gossip_manager/framing.dart';

// P2P and DHT components
export 'gossip_manager/p2p_node_protocol.dart';
export 'gossip_manager/dht_provider.dart';

// CRDT implementations
export 'p2p/crdt/vector_clock.dart';
export 'p2p/crdt/document_crdt.dart';
