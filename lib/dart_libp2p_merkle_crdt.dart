/// Replicates Merkle-CRDTs (the merkledag package) between dart_libp2p peers.
///
/// Each replica announces its heads over GossipSub and serves DAG blocks over
/// a libp2p stream protocol. Received blocks are verified against their CIDs,
/// announcements are accepted only from authorised writers, and merging is
/// bounded, so a misbehaving peer cannot forge content, write without
/// permission, or stall the replica.
library;

export 'src/gossip_manager.dart';
