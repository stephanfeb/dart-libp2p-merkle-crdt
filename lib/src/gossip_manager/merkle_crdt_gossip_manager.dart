// lib/src/gossip_manager/merkle_crdt_gossip_manager.dart
import 'dart:async';

import 'package:dart_cid/dart_cid.dart' show CID; // Import CID
import 'package:merkledag/merkledag.dart' show MerkleCRDT, CRDTPayload;
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

  String get _shortPeerId => _dagFetcher.peerId.toBase58().substring(0, 6);

  final StreamController<V?> _stateController = StreamController<V?>.broadcast();
  final StreamController<String> _cidController = StreamController<String>.broadcast(); // For CID changes
  
  // Event-driven cache - only updated through event system, never manually
  V? _cachedState;

  /// Stream of CRDT state updates (logical value V). Emits the new state whenever it changes.
  Stream<V?> get onStateChanged {
    _logger.finer('onStateChanged: Stream accessed.');
    return _stateController.stream;
  }

  /// Stream of new head CIDs for this CRDT (after local or remote updates).
  Stream<String> get onCidChanged => _cidController.stream;

  /// Gets the latest known root CID of this CRDT.
  /// Uses MerkleCRDT.roots directly instead of manual tracking.
  String? get latestKnownCid {
    final roots = _merkleCrdt.roots;
    return roots.isNotEmpty ? roots.first.toString() : null;
  }

  /// Gets the current cached logical value of the CRDT.
  V? get currentState {
    _logger.finer('currentState: Getter accessed. Returning: $_cachedState');
    return _cachedState;
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
    _notifyStateChange(); 

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
    _logger.info('Node $_shortPeerId: MerkleCRDT reported remote update. Emitting events.');
    
    // Emit events for remote changes
    await _notifyStateChange();
    await _notifyCidChange();
  }

  /// Event-driven state change notification - fetches fresh state from MerkleCRDT
  Future<void> _notifyStateChange() async {
    _logger.fine('[MerkleCrdtGossipManager] Node $_shortPeerId: _notifyStateChange: Fetching fresh state from MerkleCRDT...');
    try {
      final V? newState = await getState();
      if (_cachedState != newState) {
        _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: _notifyStateChange: State changed. Old: $_cachedState, New: $newState. Notifying ${_stateController.hasListener ? "listeners" : "NO listeners"}.');
        _cachedState = newState;
        if (!_stateController.isClosed) {
          _stateController.add(_cachedState);
        }
      } else {
        _logger.finer('[MerkleCrdtGossipManager] Node $_shortPeerId: _notifyStateChange: State unchanged. Value: $_cachedState');
      }
    } catch (e, s) {
      _logger.severe('[MerkleCrdtGossipManager] Node $_shortPeerId: _notifyStateChange: Error getting state: $e', e, s);
    }
  }

  /// Event-driven CID change notification - uses MerkleCRDT.roots directly
  Future<void> _notifyCidChange() async {
    _logger.fine('[MerkleCrdtGossipManager] Node $_shortPeerId: _notifyCidChange: Checking for CID changes...');
    try {
      final String? currentCid = latestKnownCid;
      if (currentCid != null && !_cidController.isClosed) {
        _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: _notifyCidChange: Emitting CID: $currentCid');
        _cidController.add(currentCid);
      }
    } catch (e, s) {
      _logger.severe('[MerkleCrdtGossipManager] Node $_shortPeerId: _notifyCidChange: Error getting CID: $e', e, s);
    }
  }

  /// Applies a local update to the CRDT.
  /// The payload `P` should be the CRDT-specific payload type (e.g., GSet, Counter).
  Future<CID> applyLocalUpdate(P payload) async {
    _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: applyLocalUpdate: Applying local update with payload: ${payload.toString()} (Value: ${payload.value})');
    try {
      final CID newHeadCid = await _merkleCrdt.add(payload);
      _logger.fine('[MerkleCrdtGossipManager] Node $_shortPeerId: applyLocalUpdate: Local update applied to MerkleCRDT. New head CID: $newHeadCid. MerkleCRDT handles broadcasting and DHT operations.');

      // Emit events for local changes - MerkleCRDT handles the actual broadcasting
      await _notifyStateChange();
      await _notifyCidChange();
      
      _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: applyLocalUpdate: Successfully applied local update.');

      return newHeadCid;
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
      final P? payloadState = await _merkleCrdt.getState(); 
      _logger.finer('[MerkleCrdtGossipManager] Node $_shortPeerId: getState: Fetched payload state from MerkleCRDT: ${payloadState?.toString() ?? "null"}');
      final V? logicalValue = payloadState?.value;       
      _logger.finer('[MerkleCrdtGossipManager] Node $_shortPeerId: getState: Derived logical value: ${logicalValue?.toString() ?? "null"}');
      return logicalValue;
    } catch (e, s) {
      _logger.severe('[MerkleCrdtGossipManager] Node $_shortPeerId: getState: Error during MerkleCRDT.getState(): $e', e, s);
      _logger.warning('[MerkleCrdtGossipManager] Node $_shortPeerId: getState: Returning cached state due to error: $_cachedState');
      return _cachedState; 
    }
  }
  
  /// Call this method periodically or after events that might indicate remote changes
  /// if MerkleCRDT doesn't provide its own state update stream.
  Future<void> refreshState() async {
    _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: refreshState: Explicitly refreshing CRDT state...');
    await _notifyStateChange(); // Use the new event-driven approach
    _logger.fine('[MerkleCrdtGossipManager] Node $_shortPeerId: refreshState: State refresh complete. Current cached state: $_cachedState');
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

  /// Explicitly re-announces the current latest known head CID.
  ///
  /// This method provides network resilience for mobile P2P environments by:
  /// 1. Re-broadcasting the current head CID via GossipSub
  /// 2. Re-providing the content to DHT for discovery
  ///
  /// Uses proper architectural patterns by delegating to existing abstractions
  /// rather than bypassing the DAGSyncer and DHTProvider layers.
  ///
  /// This can be useful if initial announcements might have failed due to
  /// network connectivity issues common in mobile environments.
  Future<void> forceAnnounceCurrentHead({
    int maxRetries = 3,
    Duration retryDelay = const Duration(seconds: 2),
  }) async {
    final String? currentCid = latestKnownCid;
    _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: forceAnnounceCurrentHead: Attempting to re-announce latestKnownCid: $currentCid');
    
    if (currentCid == null) {
      _logger.warning('[MerkleCrdtGossipManager] Node $_shortPeerId: forceAnnounceCurrentHead: No latestKnownCid to announce.');
      return;
    }

    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await _performReannouncement(currentCid);
        _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: forceAnnounceCurrentHead: Re-announcement succeeded on attempt $attempt for CID $currentCid');
        return;
      } catch (e, s) {
        if (attempt == maxRetries) {
          _logger.severe('[MerkleCrdtGossipManager] Node $_shortPeerId: forceAnnounceCurrentHead: Re-announcement failed after $maxRetries attempts for CID $currentCid: $e', e, s);
          rethrow;
        }
        _logger.warning('[MerkleCrdtGossipManager] Node $_shortPeerId: forceAnnounceCurrentHead: Attempt $attempt failed for CID $currentCid, retrying in ${retryDelay.inSeconds}s: $e');
        await Future.delayed(retryDelay);
      }
    }
  }

  /// Performs the actual re-announcement using proper architectural patterns.
  /// Delegates to existing abstractions rather than bypassing them.
  Future<void> _performReannouncement(String currentCid) async {
    final cidObj = CID.fromString(currentCid);
    
    _logger.fine('[MerkleCrdtGossipManager] Node $_shortPeerId: _performReannouncement: Starting re-announcement for CID $currentCid');

    // 1. Re-broadcast via the proper broadcaster abstraction
    _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: _performReannouncement: Broadcasting CID $currentCid via announcer.');
    await _announcer.broadcast(currentCid);
    _logger.fine('[MerkleCrdtGossipManager] Node $_shortPeerId: _performReannouncement: Successfully broadcasted CID $currentCid.');

    // 2. Re-provide to DHT by delegating to DAGSyncer (follows merkledag patterns)
    // This approach uses get() then put() which is a standard merkledag pattern
    // and ensures we go through the proper DHTProvider abstraction
    if (_dagFetcher.isDHTAvailable) {
      _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: _performReannouncement: Re-providing CID $cidObj to DHT via DAGSyncer.');
      
      final node = await _dagFetcher.get(cidObj);
      if (node != null) {
        // Re-put the node, which triggers DHT provide via DHTProvider
        await _dagFetcher.put(node);
        _logger.fine('[MerkleCrdtGossipManager] Node $_shortPeerId: _performReannouncement: Successfully re-provided CID $cidObj to DHT via DAGSyncer.');
      } else {
        _logger.warning('[MerkleCrdtGossipManager] Node $_shortPeerId: _performReannouncement: Could not retrieve node for CID $cidObj from local storage, skipping DHT re-provide.');
      }
    } else {
      _logger.warning('[MerkleCrdtGossipManager] Node $_shortPeerId: _performReannouncement: DHT not available, only broadcasting via GossipSub for CID $currentCid.');
    }

    _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: _performReannouncement: Re-announcement process complete for CID $currentCid.');
  }
}
