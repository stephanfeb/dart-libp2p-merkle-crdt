// lib/src/gossip_manager/block_store.dart
import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:merkledag/merkledag.dart' show MerkleNode, CRDTPayload;

abstract class BlockStore<P extends CRDTPayload<V>, V> {
  Future<MerkleNode<P>?> get(CID cid);
  Future<void> put(MerkleNode<P> node);
  Future<void> open(); // Optional: for stores that need initialization
  Future<void> close(); // Optional: for stores that need cleanup
}

class InMemoryBlockStore<P extends CRDTPayload<V>, V> implements BlockStore<P,V> {
  final Map<String, MerkleNode<P>> _store = {};

  @override
  Future<MerkleNode<P>?> get(CID cid) async {
    return _store[cid.toString()];
  }

  @override
  Future<void> put(MerkleNode<P> node) async {
    _store[node.cid.toString()] = node;
  }

  @override
  Future<void> open() async {
    // No-op for in-memory
  }

  @override
  Future<void> close() async {
    _store.clear();
    // No-op for in-memory
  }
}
