// lib/src/gossip_manager/merkle_crdt_gossip_manager.dart
import 'dart:async';

// CID is not directly used here anymore after removing receiveHead, but good for context.
// import 'package:dart_cid/dart_cid.dart' show CID; 
import 'package:merkledag/merkledag.dart' show MerkleCRDT, CRDTPayload;
import 'package:logging/logging.dart';

import 'p2p_stream_dag_fetcher.dart';
import 'gossipsub_announcer.dart';

final _logger = Logger('MerkleCrdtGossipManager');

class MerkleCrdtGossipManager<V, P extends CRDTPayload<V>> {
  final MerkleCRDT<V, P> _merkleCrdt;
  // _announcer is passed to MerkleCRDT, which uses it internally.
  // final GossipSubAnnouncer _announcer; // Not strictly needed to be stored if only used in constructor

  // No longer needed as MerkleCRDT handles its broadcaster subscription internally.
  // StreamSubscription<String>? _headSubscription; 
  
  final StreamController<V?> _stateController = StreamController<V?>.broadcast();
  V? _currentState;

  /// Stream of CRDT state updates (logical value V). Emits the new state whenever it changes.
  Stream<V?> get onStateChanged => _stateController.stream;

  /// Gets the current cached logical value of the CRDT.
  V? get currentState => _currentState;

  MerkleCrdtGossipManager({
    required P2PStreamDagFetcher<P, V> dagFetcher,
    required GossipSubAnnouncer announcer,
  }) : _merkleCrdt = MerkleCRDT<V, P>(
          dagSyncer: dagFetcher,
          broadcaster: announcer, // MerkleCRDT will use this to subscribe and broadcast
        ) {
    // Initial state update
    _updateCurrentStateAndNotify();

    // MerkleCRDT is expected to call broadcaster.subscribe() itself.
    // If we want to be notified of MerkleCRDT state changes beyond our own actions,
    // MerkleCRDT would need to provide its own state update stream, or we poll/check after known events.
    // For now, we update state after our actions (applyLocalUpdate)
    // and assume that remote changes handled by MerkleCRDT will eventually lead to
    // a different state if we call getState().
    // A more reactive system would involve MerkleCRDT itself emitting events when its state changes due to internal processing of broadcasts.
    // For simplicity, this manager will update its state primarily after local actions,
    // and its getState() will always fetch the latest. The onStateChanged stream will reflect these.
  }

  Future<void> _updateCurrentStateAndNotify() async {
    _logger.fine('Updating and notifying current state...');
    try {
      P? payloadState = await _merkleCrdt.getState(); // Returns P?
      V? logicalValue = payloadState?.value;       // Convert P? to V?

      if (_currentState != logicalValue) { // Only update and notify if different
        _currentState = logicalValue;
        _stateController.add(_currentState);
        _logger.fine('CRDT state updated. New logical value: $_currentState');
      } else {
        _logger.finer('CRDT state unchanged. Logical value: $_currentState');
      }
    } catch (e, s) {
      _logger.severe('Error getting or updating CRDT state: $e', e, s);
      // Do not change _currentState or add to _stateController on error
    }
  }

  /// Applies a local update to the CRDT.
  /// The payload `P` should be the CRDT-specific payload type (e.g., GSet, Counter).
  Future<void> applyLocalUpdate(P payload) async {
    _logger.info('Applying local update...');
    try {
      await _merkleCrdt.add(payload);
      _logger.fine('Local update applied. MerkleCRDT will broadcast new head via its broadcaster.');
      // After a local update, the state will have changed.
      await _updateCurrentStateAndNotify();
    } catch (e, s) {
      _logger.severe('Error applying local update: $e', e, s);
      rethrow; // Rethrow to let the caller know
    }
  }

  /// Retrieves the current merged logical value of the CRDT.
  /// This fetches the latest state from the underlying MerkleCRDT.
  Future<V?> getState() async {
    _logger.fine('getState() called, fetching latest from MerkleCRDT.');
    try {
      P? payloadState = await _merkleCrdt.getState(); // Returns P?
      V? logicalValue = payloadState?.value;       // Convert P? to V?
      // Update local cache and notify if it changed, even if called directly.
      if (_currentState != logicalValue) {
        _currentState = logicalValue;
        _stateController.add(_currentState);
      }
      return logicalValue;
    } catch (e, s) {
      _logger.severe('Error during getState(): $e', e, s);
      // Return the last known good state in case of error, or rethrow/return null
      // For now, return the cached state which might be stale but better than erroring.
      _logger.warning('Returning cached state due to error in getState: $_currentState');
      return _currentState; 
    }
  }
  
  /// Call this method periodically or after events that might indicate remote changes
  /// if MerkleCRDT doesn't provide its own state update stream.
  Future<void> refreshState() async {
    _logger.info('Refreshing CRDT state...');
    await _updateCurrentStateAndNotify();
  }

  Future<void> close() async {
    _logger.info('Closing MerkleCrdtGossipManager...');
    // _headSubscription was removed.
    await _stateController.close();
    // The announcer and fetcher lifecycles are assumed to be managed externally
    // or tied to the lifecycle of the Host/PubSub instances they use.
    // If this manager "owned" them, it would call:
    // await _announcer.close(); 
    // (P2PStreamDagFetcher doesn't have an explicit close for its stream handler management yet,
    // but BlockStore might).
    _logger.info('MerkleCrdtGossipManager closed.');
  }
}
