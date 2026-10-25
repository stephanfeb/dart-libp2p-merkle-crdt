// lib/src/gossip_manager/block_store.dart
import 'package:dcid/dcid.dart' show CID;
import 'package:merkledag/merkledag.dart' show MerkleNode, CRDTPayload;

/// Content-addressed storage for Merkle-DAG nodes.
///
/// A CID names exactly one node, so blocks are immutable: [put] of a CID that
/// is already stored leaves the stored node untouched. Callers store only
/// nodes whose CID they have verified (nodes made with `MerkleNode.create`,
/// or decoded with `MerkleNode.fromBytes`).
///
/// Deleting data is a CRDT operation (a tombstone in the payload), not a
/// storage operation: removing a block that peers can still reach would break
/// the DAG for them, and the next announcement would fetch it again.
abstract class BlockStore<P extends CRDTPayload<V>, V> {
  /// Returns the node stored under [cid], or null.
  Future<MerkleNode<P>?> get(CID cid);

  /// Whether a node is stored under [cid].
  Future<bool> has(CID cid);

  /// Stores [node] under its CID unless that CID is already stored.
  Future<void> put(MerkleNode<P> node);
}

/// A [BlockStore] held in memory.
class InMemoryBlockStore<P extends CRDTPayload<V>, V> implements BlockStore<P, V> {
  final Map<CID, MerkleNode<P>> _nodes = {};

  /// Number of stored nodes.
  int get length => _nodes.length;

  @override
  Future<MerkleNode<P>?> get(CID cid) async => _nodes[cid];

  @override
  Future<bool> has(CID cid) async => _nodes.containsKey(cid);

  @override
  Future<void> put(MerkleNode<P> node) async {
    _nodes.putIfAbsent(node.cid, () => node);
  }
}
