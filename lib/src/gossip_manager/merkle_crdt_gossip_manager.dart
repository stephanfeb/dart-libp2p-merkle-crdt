// lib/src/gossip_manager/merkle_crdt_gossip_manager.dart
import 'dart:async';

import 'package:dart_cid/dart_cid.dart' show CID; // Import CID
import 'package:merkledag/merkledag.dart' show MerkleCRDT, CRDTPayload, MerkleNode; // MerkleNode is exported here
import 'package:logging/logging.dart';

// Correct import for BlockStore
import 'block_store.dart'; 
import 'p2p_stream_dag_fetcher.dart';
import 'gossipsub_announcer.dart';

// Existing logger
// final _logger = Logger('MerkleCrdtGossipManager'); 
// No change needed for the logger instance itself, just its usage.

class MerkleCrdtGossipManager<V, P extends CRDTPayload<V>> {
  // Logger for this class - will be instance-specific
  final Logger _logger;

  late final MerkleCRDT<V, P> _merkleCrdt; // Changed to late final
  final P2PStreamDagFetcher<P, V> _dagFetcher; // Store the dagFetcher
  final GossipSubAnnouncer _announcer; // Store the announcer instance
  String? _latestKnownCid; // Store the latest known CID

  String get _shortPeerId => _dagFetcher.peerId.toBase58().substring(0, 6);

  // No longer needed as MerkleCRDT handles its broadcaster subscription internally.
  // StreamSubscription<String>? _headSubscription;


  final StreamController<V?> _stateController = StreamController<V?>.broadcast();
  final StreamController<String> _cidController = StreamController<String>.broadcast(); // For CID changes
  V? _currentState;

  /// Stream of CRDT state updates (logical value V). Emits the new state whenever it changes.
  Stream<V?> get onStateChanged {
    _logger.finer('onStateChanged: Stream accessed.');
    return _stateController.stream;
  }

  /// Stream of new head CIDs for this CRDT (after local or remote updates).
  Stream<String> get onCidChanged => _cidController.stream;

  /// Gets the latest known root CID of this CRDT.
  /// This is updated after a successful local or remote update.
  String? get latestKnownCid => _latestKnownCid;

  /// Gets the current cached logical value of the CRDT.
  V? get currentState {
    _logger.finer('currentState: Getter accessed. Returning: $_currentState');
    return _currentState;
  }

  MerkleCrdtGossipManager({
    required P2PStreamDagFetcher<P, V> dagFetcher,
    required GossipSubAnnouncer announcer,
    String? loggerName, // Added loggerName parameter
    // Parameters for MerkleCRDT constructor if it's different from default
    // For example, if it needs BlockStore, initialPayload, factories directly:
    // required BlockStore<P,V> blockStore,
    // required P initialPayload,
    // required P Function(Uint8List) payloadFactory,
    // required Uint8List Function(P) payloadSerializer,

  }) : _logger = Logger(loggerName ?? 'MerkleCrdtGossipManager'),
       _dagFetcher = dagFetcher,
       _announcer = announcer { // Initialize _logger, _dagFetcher, and _announcer

    // Initialize _merkleCrdt in the constructor body
    _merkleCrdt = MerkleCRDT<V, P>(
      dagSyncer: _dagFetcher, 
      broadcaster: _announcer, // Use the stored announcer
      onRemoteUpdateProcessed: _onMerkleCRDTRemoteUpdate,
    );

    _logger.info('Instance created. Effective logger name: ${_logger.name}. ShortPeerId: $_shortPeerId'); 
    _logger.info('Node $_shortPeerId: Constructor logic continues. dagFetcher: ${dagFetcher.runtimeType}, announcer: ${announcer.runtimeType}');
    // Initial state update
    _updateCurrentStateAndNotify(); 

    _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: Initialized successfully. MerkleCRDT instance created.');
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

  /// Handles the callback from MerkleCRDT when a remote update is processed.
  void _onMerkleCRDTRemoteUpdate() async { // Made async
    // This method is called on a fully constructed instance.
    _logger.info('Node $_shortPeerId: MerkleCRDT reported remote update. Refreshing manager state.');
    await _updateCurrentStateAndNotify(); // This updates _currentState
    try {
      final Set<CID> currentHeadsSet = await _merkleCrdt.roots; // Use the 'roots' getter, which returns a Set
      if (currentHeadsSet.isNotEmpty) {
        final newCidString = currentHeadsSet.first.toString(); // Take the first head's CID from the set
        if (_latestKnownCid != newCidString) {
          _latestKnownCid = newCidString;
          _logger.info('Node $_shortPeerId: Remote update processed. New head CID: $_latestKnownCid');
          if (!_cidController.isClosed) {
            _cidController.add(_latestKnownCid!);
          }
        }
      }
    } catch (e,s) {
      _logger.severe('Node $_shortPeerId: Error getting head CID after remote update: $e', e, s);
    }
  }

  Future<void> _updateCurrentStateAndNotify() async {
    _logger.fine('[MerkleCrdtGossipManager] Node $_shortPeerId: _updateCurrentStateAndNotify: Updating and notifying current state...');
    try {
      P? payloadState = await _merkleCrdt.getState(); 
      _logger.finer('[MerkleCrdtGossipManager] Node $_shortPeerId: _updateCurrentStateAndNotify: Fetched payload state from MerkleCRDT: ${payloadState?.toString() ?? "null"}');
      V? logicalValue = payloadState?.value;       
      _logger.finer('[MerkleCrdtGossipManager] Node $_shortPeerId: _updateCurrentStateAndNotify: Derived logical value: ${logicalValue?.toString() ?? "null"}');

      // Using `identical` for potentially complex objects, or `==` if overridden appropriately.
      // For simple types or if `==` is well-defined for V, `_currentState != logicalValue` is fine.
      // If V can be null, ensure comparison handles it.
      bool changed;
      if (_currentState == null && logicalValue == null) {
        changed = false;
      } else if (_currentState == null || logicalValue == null) {
        changed = true; // One is null, the other isn't
      } else {
        // This assumes V has a meaningful equality operator.
        // For complex Set<String> or custom objects, ensure `==` is correctly implemented.
        // If V is a collection, deep equality might be needed if `==` isn't overridden.
        changed = (_currentState != logicalValue); 
      }

      if (changed) { 
        _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: _updateCurrentStateAndNotify: State changed. Old: $_currentState, New: $logicalValue. Notifying ${_stateController.hasListener ? "listeners" : "NO listeners"}.');
        _currentState = logicalValue;
        _stateController.add(_currentState);
      } else {
        _logger.finer('[MerkleCrdtGossipManager] Node $_shortPeerId: _updateCurrentStateAndNotify: CRDT state unchanged. Logical value: $_currentState');
      }
    } catch (e, s) {
      _logger.severe('[MerkleCrdtGossipManager] Node $_shortPeerId: _updateCurrentStateAndNotify: Error getting or updating CRDT state: $e', e, s);
    }
  }

  /// Applies a local update to the CRDT.
  /// The payload `P` should be the CRDT-specific payload type (e.g., GSet, Counter).
  Future<void> applyLocalUpdate(P payload) async {
    _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: applyLocalUpdate: Applying local update with payload: ${payload.toString()} (Value: ${payload.value})');
    try {
      final CID newHeadCid = await _merkleCrdt.add(payload);
      final newCidString = newHeadCid.toString();
      bool cidChanged = _latestKnownCid != newCidString;
      _latestKnownCid = newCidString; // Update latest known CID
      _logger.fine('[MerkleCrdtGossipManager] Node $_shortPeerId: applyLocalUpdate: Local update applied to MerkleCRDT. New head CID: $_latestKnownCid. MerkleCRDT will broadcast new head.');

      if (cidChanged && !_cidController.isClosed) {
        _cidController.add(_latestKnownCid!);
      }

      // Advertise the new head CID on the DHT
      if (_dagFetcher.dht != null) {
        _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: applyLocalUpdate: Attempting to provide new head CID $newHeadCid to local DHT.');
        try {
          await _dagFetcher.dht!.provide(newHeadCid, true); // Announce to DHT
          _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: applyLocalUpdate: Successfully provided new head CID $newHeadCid to local DHT.');
        } catch (e, s) {
          _logger.severe('[MerkleCrdtGossipManager] Node $_shortPeerId: applyLocalUpdate: Error providing new head CID $newHeadCid to DHT: $e', e, s);
          // Optionally rethrow or handle; for now, log and continue
        }
      } else {
        _logger.warning('[MerkleCrdtGossipManager] Node $_shortPeerId: applyLocalUpdate: _dagFetcher.dht is null, cannot provide new head CID $newHeadCid to DHT.');
      }

      await _updateCurrentStateAndNotify(); // Refresh and notify after local change
      _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: applyLocalUpdate: Successfully applied local update. Current state: $_currentState');
    } catch (e, s) {
      _logger.severe('[MerkleCrdtGossipManager] Node $_shortPeerId: applyLocalUpdate: Error applying local update with payload ${payload.toString()}: $e', e, s);
      rethrow; 
    }
  }

  /// Retrieves the current merged logical value of the CRDT.
  /// This fetches the latest state from the underlying MerkleCRDT.
  Future<V?> getState() async {
    _logger.fine('[MerkleCrdtGossipManager] Node $_shortPeerId: getState: Called, fetching latest state from MerkleCRDT.');
    try {
      P? payloadState = await _merkleCrdt.getState(); 
      _logger.finer('[MerkleCrdtGossipManager] Node $_shortPeerId: getState: Fetched payload state from MerkleCRDT: ${payloadState?.toString() ?? "null"}');
      V? logicalValue = payloadState?.value;       
      _logger.finer('[MerkleCrdtGossipManager] Node $_shortPeerId: getState: Derived logical value: ${logicalValue?.toString() ?? "null"}');
      
      // This method is primarily a getter, but we can still update our cache
      // and notify if a direct fetch reveals a change not yet reflected.
      // This method should primarily be a getter and not have side effects like updating _currentState
      // or notifying listeners. _updateCurrentStateAndNotify handles that.
      // However, if called, it should return the most up-to-date logical value.
      // For consistency, we can call _updateCurrentStateAndNotify to ensure cache is fresh,
      // though this makes getState() have side effects.
      // A cleaner approach might be for _updateCurrentStateAndNotify to be called by MerkleCRDT's
      // onRemoteUpdateProcessed and after local add, and getState just returns _currentState.
      // For now, let's keep the existing logic which updates the cache if different.
      bool changed;
      if (_currentState == null && logicalValue == null) {
        changed = false;
      } else if (_currentState == null || logicalValue == null) {
        changed = true;
      } else {
        changed = (_currentState != logicalValue);
      }

      if (changed) {
        _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: getState: Fetched state ($logicalValue) differs from cached state ($_currentState). Updating cache and notifying.');
        _currentState = logicalValue;
        _stateController.add(_currentState);
      } else {
        _logger.finer('[MerkleCrdtGossipManager] Node $_shortPeerId: getState: Fetched state is same as cached. Value: $_currentState');
      }
      // Update _latestKnownCid if possible, though getState doesn't directly yield a CID.
      // The CID is associated with the MerkleCRDT's head, which getState reflects.
      // If MerkleCRDT exposed its current head CID, we could update _latestKnownCid here.
      // For now, _latestKnownCid is only updated on local `add`.
      return logicalValue;
    } catch (e, s) {
      _logger.severe('[MerkleCrdtGossipManager] Node $_shortPeerId: getState: Error during MerkleCRDT.getState(): $e', e, s);
      _logger.warning('[MerkleCrdtGossipManager] Node $_shortPeerId: getState: Returning cached state due to error: $_currentState');
      return _currentState; 
    }
  }
  
  /// Call this method periodically or after events that might indicate remote changes
  /// if MerkleCRDT doesn't provide its own state update stream.
  Future<void> refreshState() async {
    _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: refreshState: Explicitly refreshing CRDT state...');
    await _updateCurrentStateAndNotify(); // This already logs its actions
    _logger.fine('[MerkleCrdtGossipManager] Node $_shortPeerId: refreshState: State refresh complete. Current cached state: $_currentState');
  }

  Future<void> close() async {
    _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: close: Closing MerkleCrdtGossipManager...');
    // _headSubscription was removed.
    await _stateController.close();
    await _cidController.close(); // Close CID controller
    _logger.fine('[MerkleCrdtGossipManager] Node $_shortPeerId: close: State and CID controllers closed.');
    // The announcer and fetcher lifecycles are assumed to be managed externally
    // or tied to the lifecycle of the Host/PubSub instances they use.
    // If this manager "owned" them, it would call:
    // await _announcer.close(); 
    // (P2PStreamDagFetcher doesn't have an explicit close for its stream handler management yet,
    // but BlockStore might).
    _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: MerkleCrdtGossipManager closed.');
  }
}
