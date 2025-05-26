// lib/src/gossip_manager.dart

/// Manages a Merkle-CRDT instance over a libp2p network using GossipSub for head announcements
/// and direct P2P streams for DAG node synchronization.
library merkle_crdt_gossip_manager;

export 'gossip_manager/block_store.dart';
export 'gossip_manager/p2p_stream_dag_fetcher.dart';
export 'gossip_manager/gossipsub_announcer.dart';
export 'gossip_manager/merkle_crdt_gossip_manager.dart';
