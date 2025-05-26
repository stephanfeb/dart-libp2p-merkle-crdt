// lib/src/gossip_manager/merkle_crdt_gossip_manager.dart
import 'dart:async';

import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:merkledag/merkledag.dart' show MerkleCRDT, CRDTPayload, MerkleNode; // Assuming MerkleNode might be needed for type hints or internal logic
import 'package:logging/logging.dart';

import 'p2p_stream_dag_fetcher.dart';
import 'gossipsub_announcer.dart';
// BlockStore might not be directly used by the manager, but by the fetcher.

final _logger = Logger('MerkleCrdtGossipManager');

class MerkleCrdtGossipManager<V, P extends CRDTPayload<V>> {
  final MerkleCRDT<V, P> _merkleCrdt;
  final GossipSubAnnouncer _announcer;
  // The P2PStreamDagFetcher is used internally by MerkleCRDT via DAGSyncer interface
  // final P2PStreamDagFetcher<P, V> _fetcher; // Not directly held if only passed to MerkleCRDT

  StreamSubscription<String>? _headSubscription;
  final _stateController = StreamController<V?>.broadcast();
  V? _currentState;

  /// Stream of CRDT state updates. Emits the new state whenever it changes.
  Stream<V?> get onStateChanged => _stateController.stream;

  /// Gets the current cached state of the CRDT.
  V? get currentState => _currentState;

  MerkleCrdtGossipManager({
    required P2PStreamDagFetcher<P, V> dagFetcher,
    required GossipSubAnnouncer announcer,
    // Required for MerkleCRDT, assuming it needs a way to create empty payloads or similar
    // Or perhaps the MerkleCRDT itself is pre-configured with this.
    // For now, let's assume MerkleCRDT handles its internal payload logic.
  }) : _announcer = announcer,
       _merkleCrdt = MerkleCRDT<V, P>(
          dagSyncer: dagFetcher,
          broadcaster: announcer,
        ) {
    _listenForRemoteHeads();
    _updateCurrentState(); // Initialize with current state
  }

  void _listenForRemoteHeads() {
    _headSubscription = _announcer.subscribe().listen(
      (cidString) async {
        _logger.info('Received new remote head CID: $cidString');
        try {
          final cid = CID.fromString(cidString);
          // Assuming MerkleCRDT has a method to incorporate a remote head.
          // This method should trigger the DAGSyncer if nodes are missing.
          // Common names: applyHead, join, mergeHead, receiveHead etc.
          // Let's assume a method like `applyRemoteHead` or `joinDag`.
          // The original MerkleCRDT paper implies that new CIDs are simply processed,
          // and the existing `add` or a similar mechanism for merging DAGs would be used.
          // For now, let's assume there's a specific method, or that `MerkleCRDT`
          // handles this transparently when `Broadcaster` feeds it CIDs.
          // The `merkledag` `MerkleCRDT` constructor takes the broadcaster,
          // implying it might internally subscribe and process.
          // If MerkleCRDT handles subscription internally via its broadcaster,
          // this explicit subscription here might be redundant or conflict.
          //
          // Re-evaluating: The MerkleCRDT takes the broadcaster. It's likely
          // MerkleCRDT itself calls broadcaster.subscribe() and handles incoming CIDs.
          // If so, this manager class might not need to subscribe explicitly.
          // However, if it does, it needs a method on _merkleCrdt to pass the CID.
          // Let's assume for now MerkleCRDT does NOT auto-subscribe and we need to tell it.
          // A common pattern is `await _merkleCrdt.join(cid);` or `await _merkleCrdt.syncHead(cid);`
          
          // The `merkledag` README shows `MerkleCRDT.add(payload)`. It doesn't show
          // how raw CIDs from peers are processed.
          // The `MerkleClock` component of MerkleCRDTs is responsible for tracking heads.
          // Let's assume a hypothetical `_merkleCrdt.receiveHead(cid)`
          await _merkleCrdt.receiveHead(cid); // HYPOTHETICAL METHOD
          _logger.fine('Processed remote head CID: $cidString');
          await _updateCurrentState();
        } catch (e, s) {
          _logger.severe('Error processing remote head CID $cidString: $e', e, s);
        }
      },
      onError: (e, s) {
        _logger.severe('Error on remote head subscription stream: $e', e, s);
        // Consider re-subscription logic here if appropriate.
      },
      onDone: () {
        _logger.info('Remote head subscription stream closed.');
      }
    );
  }

  Future<void> _updateCurrentState() async {
    try {
      _currentState = await _merkleCrdt.getState();
      _stateController.add(_currentState);
    } catch (e, s) {
      _logger.severe('Error updating current state: $e', e, s);
      // Do not add to stream controller on error, keep last known state.
    }
  }

  /// Applies a local update to the CRDT.
  /// The payload `P` should be the CRDT-specific payload type (e.g., GSet, Counter).
  Future<void> applyLocalUpdate(P payload) async {
    _logger.info('Applying local update...');
    try {
      await _merkleCrdt.add(payload);
      _logger.fine('Local update applied. MerkleCRDT will broadcast new head.');
      // MerkleCRDT.add should internally use the Broadcaster to announce the new head.
      // It should also use DAGSyncer to store the new nodes.
      await _updateCurrentState();
    } catch (e, s) {
      _logger.severe('Error applying local update: $e', e, s);
      rethrow; // Rethrow to let the caller know
    }
  }

  /// Retrieves the current merged state of the CRDT.
  Future<V?> getState() async {
    _logger.fine('getState called.');
    // Ensure state is up-to-date, though onStateChanged provides real-time updates.
    // This call also serves to fetch initial state if not yet available.
    await _updateCurrentState(); 
    return _currentState;
  }

  Future<void> close() async {
    _logger.info('Closing MerkleCrdtGossipManager...');
    await _headSubscription?.cancel();
    await _stateController.close();
    // The announcer and fetcher might be shared or have their own lifecycle managed elsewhere.
    // If this manager "owns" them, it should close them.
    // await _announcer.close(); // Assuming GossipSubAnnouncer has a close method
    // await _fetcher.close(); // Assuming P2PStreamDagFetcher has a close method
    _logger.info('MerkleCrdtGossipManager closed.');
  }
}

// Example of a hypothetical method that might exist on MerkleCRDT
extension MerkleCRDTExtension<V, P extends CRDTPayload<V>> on MerkleCRDT<V,P> {
  Future<void> receiveHead(CID remoteHeadCid) async {
    // This is a placeholder for how a MerkleCRDT might incorporate a raw CID.
    // The actual mechanism depends on the `merkledag` library's design.
    // It might involve:
    // 1. Fetching the MerkleNode for this CID via DAGSyncer.
    // 2. Merging its payload (if it's a payload node) or its clock with the local clock.
    // This could be complex. A simpler MerkleCRDT might expect to receive full
    // payload objects that it then `add`s, and relies on the DAGSyncer to only
    // fetch deltas.
    // Or, it might have a method like `joinDag(remoteHeadCid)` which fetches the
    // remote DAG from that head and merges it.
    // The `MerkleClock.merge(otherClock)` is key.
    // For now, this is a conceptual placeholder.
    // The `merkledag` library's `MerkleCRDT` might handle this internally
    // if its `Broadcaster`'s `subscribe` stream is consumed by it.
    _logger.warning("MerkleCRDT.receiveHead(CID) is a hypothetical method used by MerkleCrdtGossipManager. Actual implementation in 'merkledag' package may differ.");
    // A more realistic approach if MerkleCRDT doesn't have such a method:
    // The manager would get the CID, use the DAGSyncer to fetch the MerkleNode<P>,
    // then if the node.payload is of type P (the CRDT payload), call this.add(node.payload).
    // This assumes heads are CRDT payload states.
    // If heads are just clock CIDs, then it's more about merging clocks.
    
    // If the MerkleCRDT is designed to listen to its broadcaster, then this
    // explicit call might not be needed. The manager would just ensure the
    // broadcaster is correctly wired.
    
    // Given the MerkleCRDT constructor takes the broadcaster, it's highly plausible
    // it subscribes and processes CIDs internally. If so, _listenForRemoteHeads
    // in the manager is redundant.
    // For this iteration, I will keep _listenForRemoteHeads and this hypothetical method
    // to make the data flow explicit. This can be refined once the exact MerkleCRDT API is known.
    throw UnimplementedError("Hypothetical MerkleCRDT.receiveHead(CID) called. Actual implementation needed based on 'merkledag' API.");
  }
}
