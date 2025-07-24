// lib/src/gossip_manager/merkle_crdt_gossip_manager.dart
import 'dart:async';
import 'dart:convert'; // For JSON encoding/decoding
import 'dart:math' as math; // For random backoff

import 'package:dart_cid/dart_cid.dart' show CID; // Import CID
import 'package:dart_libp2p/core/peer/addr_info.dart' show AddrInfo; // For provider info
import 'package:merkledag/merkledag.dart' show MerkleCRDT, CRDTPayload, MerkleNode, Broadcaster;
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

  // PHASE 2: Removed complex state management systems
  // Removed: _pendingSyncCids, _lastSyncAttempts, _syncRetryCount, _peerSyncRatings
  // Removed: _syncRetryTimer, _proactiveSyncTimer, _consecutiveFailedSyncs, etc.
  // These were causing traffic overhead and complexity without significant benefit

  StreamSubscription<String>? _announcementSubscription;

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
    return _cachedState;
  }

  MerkleCrdtGossipManager({
    required P2PStreamDagFetcher<P, V> dagFetcher,
    required GossipSubAnnouncer announcer,
    String? loggerName,
  }) : _dagFetcher = dagFetcher,
       _announcer = announcer,
       _logger = Logger(loggerName ?? 'MerkleCrdtGossipManager') {

    // Set up enhanced sync detection first
    _setupSyncDetection();

    // Create a sync-aware announcer that wraps the original announcer
    final syncAwareAnnouncer = _SyncAwareBroadcaster(_announcer, _handleIncomingCidAnnouncement, this);

    // Initialize _merkleCrdt with the sync-aware announcer
    _merkleCrdt = MerkleCRDT<V, P>(
      dagSyncer: _dagFetcher, 
      broadcaster: syncAwareAnnouncer, // Use the sync-aware announcer
      onRemoteUpdateProcessed: _onMerkleCRDTRemoteUpdate,
    );

    _logger.info('Instance created. Effective logger name: ${_logger.name}. ShortPeerId: $_shortPeerId'); 
    _logger.info('Node $_shortPeerId: Constructor logic continues. dagFetcher: ${dagFetcher.runtimeType}, announcer: ${announcer.runtimeType}');
    // Initial state update
    _notifyStateChange(); 

    _logger.info('[MerkleCrdtGossipManager] Node $_shortPeerId: Initialized successfully with enhanced sync detection.');
  }

  /// Sets up enhanced sync detection for out-of-sync scenarios
  void _setupSyncDetection() {
    // PHASE 2: Simplified sync detection - removed complex monitoring and retry systems
    _logger.info('Node $_shortPeerId: Setting up simplified sync detection (Phase 2 clean architecture)');
    // Removed: periodic retry timers, proactive health monitoring, peer rating systems
    // These were causing traffic overhead and complexity without significant benefit
    _logger.info('Node $_shortPeerId: Simplified sync detection setup complete');
  }

  /// Handles incoming CID announcements with simple retry for consistency
  Future<void> _handleIncomingCidAnnouncement(String announcedCid) async {
    _logger.info('Node $_shortPeerId: Received CID announcement: $announcedCid');
    
    try {
      final cid = CID.fromString(announcedCid);
      
      // Check if we already have this CID
      final existingNode = await _dagFetcher.blockStore.get(cid);
      if (existingNode != null) {
        _logger.fine('Node $_shortPeerId: Already have CID $announcedCid, skipping');
        return;
      }

      // Try to fetch the announced CID directly
      _logger.info('Node $_shortPeerId: Attempting to fetch announced CID $announcedCid');
      final fetchedNode = await _dagFetcher.get(cid);
      
      if (fetchedNode != null) {
        // Successfully fetched - the MerkleCRDT should handle it automatically
        _logger.info('Node $_shortPeerId: Successfully fetched and processed CID $announcedCid');
        await _notifyStateChange();
        await _notifyCidChange();
      } else {
        // CRITICAL FIX: Simple retry for failed fetches to restore consistency
        _logger.warning('Node $_shortPeerId: Failed to fetch CID $announcedCid - trying simple retry');
        await _simpleRetryFetch(announcedCid);
      }
    } catch (e, s) {
      _logger.severe('Node $_shortPeerId: Error handling CID announcement $announcedCid: $e', e, s);
      // On error, also try simple retry
      await _simpleRetryFetch(announcedCid);
    }
  }

  /// PHASE 1 EMERGENCY FIX: Complex sync recovery disabled to reduce traffic
  /// This method was causing traffic amplification - will be redesigned in Phase 2
  Future<void> _initiateSyncRecovery(String detachedHeadCid) async {
    _logger.info('Node $_shortPeerId: Complex sync recovery disabled during performance refactoring for $detachedHeadCid');
    // Early return - disable complex recovery protocols that generate excessive traffic
    // The underlying issue was the 10-second delay causing false sync failures
    // Simple retry logic will be implemented in Phase 2 if needed
    return;
  }

  /// Attempts sync via DHT provider discovery with intelligent peer selection
  Future<bool> _syncViaDHT(String targetCid) async {
    _logger.info('Node $_shortPeerId: Attempting enhanced DHT-based sync for $targetCid');
    
    try {
      final cid = CID.fromString(targetCid);
      final providers = _dagFetcher.dhtProvider.findProviders(cid, maxProviders: 15); // Increased from 5
      
      // Collect and rank providers
      final availableProviders = <AddrInfo>[];
      await for (final provider in providers.take(10)) { // Increased from 3
        if (provider.id.toBase58() != _dagFetcher.peerId.toBase58()) {
          availableProviders.add(provider);
        }
      }
      
      // Sort providers by rating (best first)
      // Removed: final rankedProviders = _rankProvidersByRating(availableProviders);
      
      _logger.info('Node $_shortPeerId: Found ${availableProviders.length} providers for $targetCid, trying in priority order');
      
      // Try providers in order of rating
      for (final provider in availableProviders) {
        final peerId = provider.id.toBase58();
        final peerIdShort = peerId.substring(0, 6);
        final startTime = DateTime.now();
        
        try {
          _logger.info('Node $_shortPeerId: Trying to sync from rated provider $peerIdShort');
          
          // Attempt to fetch the entire DAG path with enhanced tracking
          final success = await _fetchDAGPathWithRating(provider, targetCid);
          final responseTime = DateTime.now().difference(startTime);
          
          if (success) {
            // Removed: _updatePeerRating(peerId, isSuccess: true, responseTime: responseTime);
            _logger.info('Node $_shortPeerId: Successfully synced via DHT from $peerIdShort in ${responseTime.inMilliseconds}ms');
            return true;
          } else {
            // Removed: _updatePeerRating(peerId, isSuccess: false, responseTime: responseTime);
          }
        } catch (e) {
          final responseTime = DateTime.now().difference(startTime);
          // Removed: _updatePeerRating(peerId, isSuccess: false, responseTime: responseTime);
          _logger.warning('Node $_shortPeerId: Failed to sync from provider $peerIdShort: $e');
        }
      }
      
      _logger.warning('Node $_shortPeerId: Enhanced DHT sync failed for $targetCid after trying ${availableProviders.length} providers');
      return false;
    } catch (e, s) {
      _logger.severe('Node $_shortPeerId: Error in enhanced DHT sync for $targetCid: $e', e, s);
      return false;
    }
  }

  /// Ranks providers by their sync rating (best first)
  // Removed: List<AddrInfo> _rankProvidersByRating(List<AddrInfo> providers) {
  // Removed:   return providers..sort((a, b) {
  // Removed:     final ratingA = _peerSyncRatings[a.id.toBase58()]?.overallRating ?? 0.0;
  // Removed:     final ratingB = _peerSyncRatings[b.id.toBase58()]?.overallRating ?? 0.0;
  // Removed:     
  // Removed:     // If both have no rating, randomize to avoid always picking the same peer
  // Removed:     if (ratingA == 0.0 && ratingB == 0.0) {
  // Removed:       return DateTime.now().millisecond % 2 == 0 ? -1 : 1;
  // Removed:     }
  // Removed:     
  // Removed:     return ratingB.compareTo(ratingA); // Higher rating first
  // Removed:   });
  // Removed: }

  /// Enhanced version of _fetchDAGPath with better error handling and tracking
  Future<bool> _fetchDAGPathWithRating(AddrInfo provider, String targetCid) async {
    // PHASE 2: Removed enhanced DAG path fetching with peer rating
    _logger.info('Node $_shortPeerId: Enhanced DAG path fetching with rating removed in Phase 2 for $targetCid');
    return false;
  }

  /// Attempts sync via GossipSub sync request
  Future<void> _syncViaGossipSubRequest(String targetCid) async {
    // PHASE 2: Removed complex GossipSub sync request protocol
    // This was part of the complex recovery system that generated excessive traffic
    _logger.info('Node $_shortPeerId: Complex GossipSub sync requests removed in Phase 2 for $targetCid');
    return;
  }

  /// Attempts to fetch the complete DAG path from a provider  
  Future<bool> _fetchDAGPath(AddrInfo provider, String targetCid) async {
    // PHASE 2: Removed complex DAG path fetching - now handled by simplified DAGSyncer
    _logger.info('Node $_shortPeerId: Complex DAG path fetching removed in Phase 2 for $targetCid');
    return false;
  }

  /// Recursively fetches missing parent nodes to build complete DAG path
  Future<void> _fetchMissingParents(MerkleNode<P> node) async {
    // PHASE 2: Removed recursive parent fetching - now handled by simplified DAGSyncer
    _logger.fine('Node $_shortPeerId: Recursive parent fetching removed in Phase 2 for ${node.cid}');
    return;
  }

  /// Retries pending sync operations
  // Removed: Future<void> _retryPendingSyncs() async {
  // Removed:   if (_lastSyncAttempts.isEmpty) return;
  // Removed:   
  // Removed:   final now = DateTime.now();
  // Removed:   final toRetry = <String>[];
  // Removed:   
  // Removed:   for (final entry in _lastSyncAttempts.entries) {
  // Removed:     final cid = entry.key;
  // Removed:     final lastAttempt = entry.value;
  // Removed:     final retryCount = _syncRetryCount[cid] ?? 0;
  // Removed:     
  // Removed:     if (retryCount < _maxSyncRetries && now.difference(lastAttempt) > _syncRetryDelay) {
  // Removed:       toRetry.add(cid);
  // Removed:     }
  // Removed:   }
  // Removed:   
  // Removed:   for (final cid in toRetry) {
  // Removed:     _logger.info('Node $_shortPeerId: Retrying sync for $cid (attempt ${(_syncRetryCount[cid] ?? 0) + 1})');
  // Removed:     await _initiateSyncRecovery(cid);
  // Removed:   }
  // Removed: }

  /// PHASE 1 EMERGENCY FIX: Proactive monitoring disabled to reduce traffic
  // Removed: Future<void> _performProactiveSyncHealthCheck() async {
  // Removed:   _logger.fine('Node $_shortPeerId: Proactive sync monitoring disabled during performance refactoring');
  // Removed:   // Early return - disable proactive monitoring that generates network traffic
  // Removed:   // Simple health monitoring will be redesigned in Phase 3 if needed
  // Removed:   return;
  // Removed: }

  /// Initiates proactive peer discovery when we suspect we're isolated or out of sync
  // Removed: Future<void> _initiateProactivePeerDiscovery() async {
  // Removed:   _logger.info('Node $_shortPeerId: Initiating proactive peer discovery...');
  // Removed:   
  // Removed:   try {
  // Removed:     // Strategy 1: Query DHT for our own current heads to find peers who also have them
  // Removed:     final currentHeads = _merkleCrdt.roots;
  // Removed:     if (currentHeads.isNotEmpty) {
  // Removed:       for (final headCid in currentHeads.take(2)) { // Limit to 2 heads to avoid spam
  // Removed:         await _discoverPeersViaHeadQuery(headCid);
  // Removed:       }
  // Removed:     }
  // Removed:     
  // Removed:     // Strategy 2: Broadcast a "sync health check" message
  // Removed:     await _broadcastSyncHealthCheck();
  // Removed:     
  // Removed:     // Reset consecutive failure counter after proactive discovery
  // Removed:     _consecutiveFailedSyncs = 0;
  // Removed:     
  // Removed:   } catch (e, s) {
  // Removed:     _logger.severe('Node $_shortPeerId: Error in proactive peer discovery: $e', e, s);
  // Removed:   }
  // Removed: }

  /// Discovers peers by querying DHT for our current heads
  // Removed: Future<void> _discoverPeersViaHeadQuery(CID headCid) async {
  // Removed:   _logger.info('Node $_shortPeerId: Discovering peers via head query for $headCid');
  // Removed:   
  // Removed:   try {
  // Removed:     if (!_dagFetcher.isDHTAvailable) return;
  // Removed:     
  // Removed:     final providers = _dagFetcher.dhtProvider.findProviders(headCid, maxProviders: 10);
  // Removed:     await for (final provider in providers.take(5)) {
  // Removed:       final peerId = provider.id.toBase58();
  // Removed:       if (peerId != _dagFetcher.peerId.toBase58()) {
  // Removed:         // Update peer rating (discovery bonus)
  // Removed:         _updatePeerRating(peerId, isSuccess: true, responseTime: Duration(seconds: 1));
  // Removed:         _logger.fine('Node $_shortPeerId: Discovered peer ${peerId.substring(0, 6)} via head query');
  // Removed:       }
  // Removed:     }
  // Removed:   } catch (e, s) {
  // Removed:     _logger.warning('Node $_shortPeerId: Error in head query peer discovery: $e', e, s);
  // Removed:   }
  // Removed: }

  /// PHASE 1 EMERGENCY FIX: Health check broadcasts disabled to reduce traffic
  // Removed: Future<void> _broadcastSyncHealthCheck() async {
  // Removed:   _logger.fine('Node $_shortPeerId: Health check broadcasts disabled during performance refactoring');
  // Removed:   // Early return - disable periodic health broadcasts that generate unnecessary traffic
  // Removed:   // Health monitoring will be redesigned in Phase 3 with proper metrics
  // Removed:   return;
  // Removed: }

  /// Updates peer rating based on sync performance
  // Removed: void _updatePeerRating(String peerId, {required bool isSuccess, Duration? responseTime}) {
  // Removed:   final rating = _peerSyncRatings.putIfAbsent(peerId, () => _PeerSyncRating(peerId));
  // Removed:   
  // Removed:   if (isSuccess) {
  // Removed:     rating.recordSuccess(responseTime ?? Duration(seconds: 5));
  // Removed:     _lastSuccessfulSyncTimes[peerId] = DateTime.now();
  // Removed:     _consecutiveFailedSyncs = 0; // Reset failure counter on success
  // Removed:   } else {
  // Removed:     rating.recordFailure();
  // Removed:     _consecutiveFailedSyncs++;
  // Removed:   }
  // Removed:   
  // Removed:   _logger.fine('Node $_shortPeerId: Updated peer rating for ${peerId.substring(0, 6)}: ${rating.overallRating.toStringAsFixed(2)}');
  // Removed: }

  /// Gets top-rated peers for sync operations
  // Removed: List<_PeerSyncRating> _getTopRatedPeers(int count) {
  // Removed:   final sortedPeers = _peerSyncRatings.values.toList()
  // Removed:     ..sort((a, b) => b.overallRating.compareTo(a.overallRating));
  // Removed:   return sortedPeers.take(count).toList();
  // Removed: }

  /// Cleans up old peer ratings to prevent memory growth
  // Removed: void _cleanupOldPeerRatings() {
  // Removed:   final now = DateTime.now();
  // Removed:   final cutoff = now.subtract(Duration(hours: 24)); // Keep ratings for 24 hours
  // Removed:   
  // Removed:   _peerSyncRatings.removeWhere((peerId, rating) => 
  // Removed:     rating.lastSyncAttempt.isBefore(cutoff));
  // Removed:   
  // Removed:   _lastSuccessfulSyncTimes.removeWhere((peerId, time) => 
  // Removed:     time.isBefore(cutoff));
  // Removed: }

  /// Cleans up sync state after successful sync
  // Removed: void _cleanupSyncState(String cid) {
  // Removed:   _pendingSyncCids.remove(cid);
  // Removed:   _lastSyncAttempts.remove(cid);
  // Removed:   _syncRetryCount.remove(cid);
  // Removed:   _logger.fine('Node $_shortPeerId: Cleaned up sync state for $cid');
  // Removed: }

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

  /// Closes the manager and its components
  Future<void> close() async {
    _logger.info('Node $_shortPeerId: Closing MerkleCrdtGossipManager...');
    
    // Cancel timers
    // Removed: _syncRetryTimer?.cancel();
    // Removed: _proactiveSyncTimer?.cancel();
    
    // Cancel subscriptions
    await _announcementSubscription?.cancel();
    
    // Close controllers
    if (!_stateController.isClosed) {
      await _stateController.close();
    }
    if (!_cidController.isClosed) {
      await _cidController.close();
    }
    
    _logger.info('Node $_shortPeerId: MerkleCrdtGossipManager closed');
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

  /// CRITICAL CONSISTENCY FIX: Simple retry mechanism for failed fetches
  /// This replaces the complex recovery protocols with basic retry logic
  Future<void> _simpleRetryFetch(String cidString) async {
    _logger.info('Node $_shortPeerId: Starting simple retry for $cidString');
    
    try {
      final cid = CID.fromString(cidString);
      
      // Wait a bit for DHT to stabilize, then retry
      await Future.delayed(Duration(seconds: 2));
      
      final retryNode = await _dagFetcher.get(cid);
      if (retryNode != null) {
        _logger.info('Node $_shortPeerId: Simple retry successful for $cidString');
        await _notifyStateChange();
        await _notifyCidChange();
      } else {
        // One more try with longer delay
        _logger.warning('Node $_shortPeerId: First retry failed for $cidString, trying once more');
        await Future.delayed(Duration(seconds: 5));
        
        final finalRetry = await _dagFetcher.get(cid);
        if (finalRetry != null) {
          _logger.info('Node $_shortPeerId: Final retry successful for $cidString');
          await _notifyStateChange();
          await _notifyCidChange();
        } else {
          _logger.severe('Node $_shortPeerId: All retries failed for $cidString - permanent sync failure');
        }
      }
    } catch (e, s) {
      _logger.severe('Node $_shortPeerId: Error in simple retry for $cidString: $e', e, s);
    }
  }
}

/// Tracks sync performance metrics for individual peers
// Removed: class _PeerSyncRating {
// Removed:   final String peerId;
// Removed:   int successfulSyncs = 0;
// Removed:   int failedSyncs = 0;
// Removed:   int totalRequests = 0;
// Removed:   double averageResponseTime = 0.0;
// Removed:   DateTime lastSyncAttempt = DateTime.now();
// Removed:   DateTime? lastSuccessfulSync;
// Removed:   
// Removed:   _PeerSyncRating(this.peerId);
// Removed:   
// Removed:   double get successRate => totalRequests == 0 ? 0.0 : successfulSyncs / totalRequests;
// Removed:   
// Removed:   double get overallRating {
// Removed:     if (totalRequests == 0) return 0.0;
// Removed:     
// Removed:     final rateScore = successRate * 0.7; // 70% weight on success rate
// Removed:     final speedScore = averageResponseTime > 0 ? (1.0 / (1.0 + averageResponseTime)) * 0.2 : 0.0; // 20% weight on speed
// Removed:     final recentnessScore = lastSuccessfulSync != null ? 
// Removed:       (1.0 / (1.0 + DateTime.now().difference(lastSuccessfulSync!).inHours)) * 0.1 : 0.0; // 10% weight on recency
// Removed:     
// Removed:     return rateScore + speedScore + recentnessScore;
// Removed:   }
// Removed:   
// Removed:   void recordSuccess(Duration responseTime) {
// Removed:     successfulSyncs++;
// Removed:     totalRequests++;
// Removed:     lastSuccessfulSync = DateTime.now();
// Removed:     lastSyncAttempt = DateTime.now();
// Removed:     
// Removed:     // Update rolling average response time
// Removed:     if (totalRequests == 1) {
// Removed:       averageResponseTime = responseTime.inMilliseconds.toDouble();
// Removed:     } else {
// Removed:       averageResponseTime = (averageResponseTime * 0.8) + (responseTime.inMilliseconds * 0.2);
// Removed:     }
// Removed:   }
// Removed:   
// Removed:   void recordFailure() {
// Removed:     failedSyncs++;
// Removed:     totalRequests++;
// Removed:     lastSyncAttempt = DateTime.now();
// Removed:   }
// Removed: }

/// A sync-aware broadcaster that implements comprehensive sync request/response protocol
/// via GossipSub while intercepting announcements for sync detection
class _SyncAwareBroadcaster implements Broadcaster {
  final GossipSubAnnouncer _originalAnnouncer;
  final Future<void> Function(String) _syncDetectionHandler;
  final MerkleCrdtGossipManager _manager;
  final Logger _logger = Logger('_SyncAwareBroadcaster');

  _SyncAwareBroadcaster(this._originalAnnouncer, this._syncDetectionHandler, this._manager);

  @override
  Future<void> broadcast(String cidString) async {
    _logger.info('broadcast: Broadcasting CID $cidString with minimal DHT provide delay');
    // CRITICAL CONSISTENCY FIX: Add minimal delay to allow DHT provide to complete
    // This prevents the race condition where peers receive announcements before 
    // the content is available in the DHT, causing permanent sync failures
    // 100ms is sufficient for local DHT operations while keeping performance gains
    await Future.delayed(Duration(milliseconds: 100));
    await _originalAnnouncer.broadcast(cidString);
  }

  @override
  Stream<String> subscribe() {
    // PHASE 2: Simplified sync protocol - only handle basic CID announcements
    // Removed complex JSON message types: SYNC_REQUEST, SYNC_RESPONSE, SYNC_HEALTH, SYNC_OFFER
    return _originalAnnouncer.subscribe().asyncMap((message) async {
      try {
        // Only handle regular CID announcements - no special message types
        _syncDetectionHandler(message).catchError((error) {
          _logger.warning('Sync detection error for $message: $error');
        });
        return message;
      } catch (e) {
        _logger.severe('Error processing message "$message": $e');
        return message; // Pass through on error
      }
    });
  }
}
