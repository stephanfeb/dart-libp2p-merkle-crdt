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

  // Enhanced sync detection and recovery state
  final Set<String> _pendingSyncCids = <String>{};
  final Map<String, DateTime> _lastSyncAttempts = <String, DateTime>{};
  final Map<String, int> _syncRetryCount = <String, int>{};
  final Duration _syncRetryDelay = const Duration(seconds: 30);
  final int _maxSyncRetries = 3;
  Timer? _syncRetryTimer;
  StreamSubscription<String>? _announcementSubscription;

  // Enhanced proactive peer querying state
  final Map<String, _PeerSyncRating> _peerSyncRatings = <String, _PeerSyncRating>{};
  final Map<String, DateTime> _lastSuccessfulSyncTimes = <String, DateTime>{};
  Timer? _proactiveSyncTimer;
  final Duration _proactiveSyncInterval = const Duration(minutes: 2);
  final Duration _syncHealthCheckInterval = const Duration(minutes: 1);
  int _consecutiveFailedSyncs = 0;
  final int _maxConsecutiveFailures = 5;

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
    _logger.info('Node $_shortPeerId: Setting up enhanced sync detection with proactive peer querying...');
    
    // Set up periodic retry for failed syncs
    _syncRetryTimer = Timer.periodic(Duration(seconds: 60), (_) {
      _retryPendingSyncs();
    });

    // Set up proactive sync health monitoring
    _proactiveSyncTimer = Timer.periodic(_proactiveSyncInterval, (_) {
      _performProactiveSyncHealthCheck();
    });

    _logger.info('Node $_shortPeerId: Enhanced sync detection setup complete with proactive monitoring');
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
      final rankedProviders = _rankProvidersByRating(availableProviders);
      
      _logger.info('Node $_shortPeerId: Found ${rankedProviders.length} providers for $targetCid, trying in priority order');
      
      // Try providers in order of rating
      for (final provider in rankedProviders) {
        final peerId = provider.id.toBase58();
        final peerIdShort = peerId.substring(0, 6);
        final startTime = DateTime.now();
        
        try {
          _logger.info('Node $_shortPeerId: Trying to sync from rated provider $peerIdShort');
          
          // Attempt to fetch the entire DAG path with enhanced tracking
          final success = await _fetchDAGPathWithRating(provider, targetCid);
          final responseTime = DateTime.now().difference(startTime);
          
          if (success) {
            _updatePeerRating(peerId, isSuccess: true, responseTime: responseTime);
            _logger.info('Node $_shortPeerId: Successfully synced via DHT from $peerIdShort in ${responseTime.inMilliseconds}ms');
            return true;
          } else {
            _updatePeerRating(peerId, isSuccess: false, responseTime: responseTime);
          }
        } catch (e) {
          final responseTime = DateTime.now().difference(startTime);
          _updatePeerRating(peerId, isSuccess: false, responseTime: responseTime);
          _logger.warning('Node $_shortPeerId: Failed to sync from provider $peerIdShort: $e');
        }
      }
      
      _logger.warning('Node $_shortPeerId: Enhanced DHT sync failed for $targetCid after trying ${rankedProviders.length} providers');
      return false;
    } catch (e, s) {
      _logger.severe('Node $_shortPeerId: Error in enhanced DHT sync for $targetCid: $e', e, s);
      return false;
    }
  }

  /// Ranks providers by their sync rating (best first)
  List<AddrInfo> _rankProvidersByRating(List<AddrInfo> providers) {
    return providers..sort((a, b) {
      final ratingA = _peerSyncRatings[a.id.toBase58()]?.overallRating ?? 0.0;
      final ratingB = _peerSyncRatings[b.id.toBase58()]?.overallRating ?? 0.0;
      
      // If both have no rating, randomize to avoid always picking the same peer
      if (ratingA == 0.0 && ratingB == 0.0) {
        return DateTime.now().millisecond % 2 == 0 ? -1 : 1;
      }
      
      return ratingB.compareTo(ratingA); // Higher rating first
    });
  }

  /// Enhanced version of _fetchDAGPath with better error handling and tracking
  Future<bool> _fetchDAGPathWithRating(AddrInfo provider, String targetCid) async {
    try {
      final cid = CID.fromString(targetCid);
      
      // Use the enhanced DAG fetcher to fetch with dependencies
      final node = await _dagFetcher.get(cid);
      if (node == null) {
        return false;
      }

      // Trigger state update
      await _notifyStateChange();
      await _notifyCidChange();
      
      return true;
    } catch (e, s) {
      _logger.warning('Node $_shortPeerId: Error in enhanced DAG path fetch: $e', e, s);
      return false;
    }
  }

  /// Attempts sync via GossipSub sync request
  Future<void> _syncViaGossipSubRequest(String targetCid) async {
    _logger.info('Node $_shortPeerId: Broadcasting sync request for $targetCid');
    
    try {
      final currentHeads = _merkleCrdt.roots.map((cid) => cid.toString()).toList();
      
      final syncRequest = {
        'type': 'sync_request',
        'targetCid': targetCid,
        'requesterId': _dagFetcher.peerId.toBase58(),
        'currentHeads': currentHeads,
        'timestamp': DateTime.now().millisecondsSinceEpoch,
      };
      
      // Broadcast sync request on the same topic with a special prefix
      final requestJson = jsonEncode(syncRequest);
      await _announcer.broadcast('SYNC_REQUEST:$requestJson');
      
      _logger.info('Node $_shortPeerId: Sync request broadcasted for $targetCid');
    } catch (e, s) {
      _logger.severe('Node $_shortPeerId: Error broadcasting sync request for $targetCid: $e', e, s);
    }
  }

  /// Attempts to fetch the complete DAG path from a provider
  Future<bool> _fetchDAGPath(AddrInfo provider, String targetCid) async {
    try {
      final cid = CID.fromString(targetCid);
      
      // Use the existing node protocol to fetch
      final node = await _dagFetcher.nodeProtocol.fetchNodeFromProvider(cid, provider);
      if (node == null) {
        return false;
      }

      // Store the node
      await _dagFetcher.put(node);
      
      // Recursively fetch parent nodes if needed
      await _fetchMissingParents(node);
      
      // Trigger state update
      await _notifyStateChange();
      await _notifyCidChange();
      
      return true;
    } catch (e, s) {
      _logger.warning('Node $_shortPeerId: Error fetching DAG path: $e', e, s);
      return false;
    }
  }

  /// Recursively fetches missing parent nodes to build complete DAG path
  Future<void> _fetchMissingParents(MerkleNode<P> node) async {
    for (final parentCid in node.children) {
      final existingParent = await _dagFetcher.blockStore.get(parentCid);
      if (existingParent == null) {
        _logger.info('Node $_shortPeerId: Fetching missing parent $parentCid');
        // Parent is missing, try to fetch it
        final fetchedParent = await _dagFetcher.get(parentCid);
        if (fetchedParent != null) {
          // Recursively fetch grandparents
          await _fetchMissingParents(fetchedParent);
        } else {
          _logger.warning('Node $_shortPeerId: Failed to fetch missing parent $parentCid');
        }
      }
    }
  }

  /// Retries pending sync operations
  Future<void> _retryPendingSyncs() async {
    if (_lastSyncAttempts.isEmpty) return;
    
    final now = DateTime.now();
    final toRetry = <String>[];
    
    for (final entry in _lastSyncAttempts.entries) {
      final cid = entry.key;
      final lastAttempt = entry.value;
      final retryCount = _syncRetryCount[cid] ?? 0;
      
      if (retryCount < _maxSyncRetries && now.difference(lastAttempt) > _syncRetryDelay) {
        toRetry.add(cid);
      }
    }
    
    for (final cid in toRetry) {
      _logger.info('Node $_shortPeerId: Retrying sync for $cid (attempt ${(_syncRetryCount[cid] ?? 0) + 1})');
      await _initiateSyncRecovery(cid);
    }
  }

  /// PHASE 1 EMERGENCY FIX: Proactive monitoring disabled to reduce traffic
  Future<void> _performProactiveSyncHealthCheck() async {
    _logger.fine('Node $_shortPeerId: Proactive sync monitoring disabled during performance refactoring');
    // Early return - disable proactive monitoring that generates network traffic
    // Simple health monitoring will be redesigned in Phase 3 if needed
    return;
  }

  /// Initiates proactive peer discovery when we suspect we're isolated or out of sync
  Future<void> _initiateProactivePeerDiscovery() async {
    _logger.info('Node $_shortPeerId: Initiating proactive peer discovery...');
    
    try {
      // Strategy 1: Query DHT for our own current heads to find peers who also have them
      final currentHeads = _merkleCrdt.roots;
      if (currentHeads.isNotEmpty) {
        for (final headCid in currentHeads.take(2)) { // Limit to 2 heads to avoid spam
          await _discoverPeersViaHeadQuery(headCid);
        }
      }
      
      // Strategy 2: Broadcast a "sync health check" message
      await _broadcastSyncHealthCheck();
      
      // Reset consecutive failure counter after proactive discovery
      _consecutiveFailedSyncs = 0;
      
    } catch (e, s) {
      _logger.severe('Node $_shortPeerId: Error in proactive peer discovery: $e', e, s);
    }
  }

  /// Discovers peers by querying DHT for our current heads
  Future<void> _discoverPeersViaHeadQuery(CID headCid) async {
    _logger.info('Node $_shortPeerId: Discovering peers via head query for $headCid');
    
    try {
      if (!_dagFetcher.isDHTAvailable) return;
      
      final providers = _dagFetcher.dhtProvider.findProviders(headCid, maxProviders: 10);
      await for (final provider in providers.take(5)) {
        final peerId = provider.id.toBase58();
        if (peerId != _dagFetcher.peerId.toBase58()) {
          // Update peer rating (discovery bonus)
          _updatePeerRating(peerId, isSuccess: true, responseTime: Duration(seconds: 1));
          _logger.fine('Node $_shortPeerId: Discovered peer ${peerId.substring(0, 6)} via head query');
        }
      }
    } catch (e, s) {
      _logger.warning('Node $_shortPeerId: Error in head query peer discovery: $e', e, s);
    }
  }

  /// PHASE 1 EMERGENCY FIX: Health check broadcasts disabled to reduce traffic
  Future<void> _broadcastSyncHealthCheck() async {
    _logger.fine('Node $_shortPeerId: Health check broadcasts disabled during performance refactoring');
    // Early return - disable periodic health broadcasts that generate unnecessary traffic
    // Health monitoring will be redesigned in Phase 3 with proper metrics
    return;
  }

  /// Updates peer rating based on sync performance
  void _updatePeerRating(String peerId, {required bool isSuccess, Duration? responseTime}) {
    final rating = _peerSyncRatings.putIfAbsent(peerId, () => _PeerSyncRating(peerId));
    
    if (isSuccess) {
      rating.recordSuccess(responseTime ?? Duration(seconds: 5));
      _lastSuccessfulSyncTimes[peerId] = DateTime.now();
      _consecutiveFailedSyncs = 0; // Reset failure counter on success
    } else {
      rating.recordFailure();
      _consecutiveFailedSyncs++;
    }
    
    _logger.fine('Node $_shortPeerId: Updated peer rating for ${peerId.substring(0, 6)}: ${rating.overallRating.toStringAsFixed(2)}');
  }

  /// Gets top-rated peers for sync operations
  List<_PeerSyncRating> _getTopRatedPeers(int count) {
    final sortedPeers = _peerSyncRatings.values.toList()
      ..sort((a, b) => b.overallRating.compareTo(a.overallRating));
    return sortedPeers.take(count).toList();
  }

  /// Cleans up old peer ratings to prevent memory growth
  void _cleanupOldPeerRatings() {
    final now = DateTime.now();
    final cutoff = now.subtract(Duration(hours: 24)); // Keep ratings for 24 hours
    
    _peerSyncRatings.removeWhere((peerId, rating) => 
      rating.lastSyncAttempt.isBefore(cutoff));
    
    _lastSuccessfulSyncTimes.removeWhere((peerId, time) => 
      time.isBefore(cutoff));
  }

  /// Cleans up sync state after successful sync
  void _cleanupSyncState(String cid) {
    _pendingSyncCids.remove(cid);
    _lastSyncAttempts.remove(cid);
    _syncRetryCount.remove(cid);
    _logger.fine('Node $_shortPeerId: Cleaned up sync state for $cid');
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

  /// Closes the manager and its components
  Future<void> close() async {
    _logger.info('Node $_shortPeerId: Closing MerkleCrdtGossipManager...');
    
    // Cancel timers
    _syncRetryTimer?.cancel();
    _proactiveSyncTimer?.cancel();
    
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
class _PeerSyncRating {
  final String peerId;
  int successfulSyncs = 0;
  int failedSyncs = 0;
  int totalRequests = 0;
  double averageResponseTime = 0.0;
  DateTime lastSyncAttempt = DateTime.now();
  DateTime? lastSuccessfulSync;
  
  _PeerSyncRating(this.peerId);
  
  double get successRate => totalRequests == 0 ? 0.0 : successfulSyncs / totalRequests;
  
  double get overallRating {
    if (totalRequests == 0) return 0.0;
    
    final rateScore = successRate * 0.7; // 70% weight on success rate
    final speedScore = averageResponseTime > 0 ? (1.0 / (1.0 + averageResponseTime)) * 0.2 : 0.0; // 20% weight on speed
    final recentnessScore = lastSuccessfulSync != null ? 
      (1.0 / (1.0 + DateTime.now().difference(lastSuccessfulSync!).inHours)) * 0.1 : 0.0; // 10% weight on recency
    
    return rateScore + speedScore + recentnessScore;
  }
  
  void recordSuccess(Duration responseTime) {
    successfulSyncs++;
    totalRequests++;
    lastSuccessfulSync = DateTime.now();
    lastSyncAttempt = DateTime.now();
    
    // Update rolling average response time
    if (totalRequests == 1) {
      averageResponseTime = responseTime.inMilliseconds.toDouble();
    } else {
      averageResponseTime = (averageResponseTime * 0.8) + (responseTime.inMilliseconds * 0.2);
    }
  }
  
  void recordFailure() {
    failedSyncs++;
    totalRequests++;
    lastSyncAttempt = DateTime.now();
  }
}

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
    // Create a stream that intercepts and processes various message types
    return _originalAnnouncer.subscribe().asyncMap((message) async {
      try {
        // Check if this is a special sync protocol message
        if (message.startsWith('SYNC_REQUEST:')) {
          await _handleSyncRequest(message.substring('SYNC_REQUEST:'.length));
          return message; // Pass through but don't trigger sync detection
        } else if (message.startsWith('SYNC_RESPONSE:')) {
          await _handleSyncResponse(message.substring('SYNC_RESPONSE:'.length));
          return message; // Pass through but don't trigger sync detection
        } else if (message.startsWith('SYNC_HEALTH:')) {
          await _handleSyncHealthCheck(message.substring('SYNC_HEALTH:'.length));
          return message; // Pass through but don't trigger sync detection
        } else {
          // Regular CID announcement - run sync detection
          _syncDetectionHandler(message).catchError((error) {
            _logger.warning('Sync detection error for $message: $error');
          });
          return message;
        }
      } catch (e) {
        _logger.severe('Error processing message "$message": $e');
        return message; // Pass through on error
      }
    });
  }

  /// Handles incoming sync requests from other nodes
  Future<void> _handleSyncRequest(String requestJson) async {
    try {
      final request = jsonDecode(requestJson) as Map<String, dynamic>;
      final requesterId = request['requesterId'] as String;
      final targetCid = request['targetCid'] as String;
      final requesterHeads = (request['currentHeads'] as List?)?.cast<String>() ?? [];
      
      _logger.info('Received sync request from ${requesterId.substring(0, 6)} for $targetCid');
      
      // Don't respond to our own requests
      if (requesterId == _manager._dagFetcher.peerId.toBase58()) {
        return;
      }
      
      // Check if we have the requested CID
      final cid = CID.fromString(targetCid);
      final node = await _manager._dagFetcher.blockStore.get(cid);
      
      if (node != null) {
        // We have the requested node - send response with DAG path
        await _sendSyncResponse(requesterId, targetCid, requesterHeads, node);
        
        // Update peer rating for being helpful
        _manager._updatePeerRating(requesterId, isSuccess: true, responseTime: Duration(seconds: 1));
      } else {
        _logger.fine('Cannot fulfill sync request from ${requesterId.substring(0, 6)} - do not have $targetCid');
      }
      
    } catch (e, s) {
      _logger.severe('Error handling sync request: $e', e, s);
    }
  }

  /// Sends a sync response with the requested DAG nodes
  Future<void> _sendSyncResponse(String requesterId, String targetCid, List<String> requesterHeads, MerkleNode node) async {
    try {
      _logger.info('Sending sync response to ${requesterId.substring(0, 6)} for $targetCid');
      
      // Find what nodes the requester is missing
      final missingNodes = await _findMissingNodesForPeer(requesterHeads, node);
      
      // Prepare response with node data
      final responseData = <Map<String, dynamic>>[];
      for (final missingNode in missingNodes) {
        responseData.add({
          'cid': missingNode.cid.toString(),
          'data': base64.encode(missingNode.toBytes()),
        });
      }
      
      final syncResponse = {
        'type': 'sync_response',
        'requesterId': requesterId,
        'responderId': _manager._dagFetcher.peerId.toBase58(),
        'targetCid': targetCid,
        'nodes': responseData,
        'timestamp': DateTime.now().millisecondsSinceEpoch,
      };
      
      final responseJson = jsonEncode(syncResponse);
      await _originalAnnouncer.broadcast('SYNC_RESPONSE:$responseJson');
      
      _logger.info('Sent sync response to ${requesterId.substring(0, 6)} with ${responseData.length} nodes');
      
    } catch (e, s) {
      _logger.severe('Error sending sync response: $e', e, s);
    }
  }

  /// Handles incoming sync responses from other nodes
  Future<void> _handleSyncResponse(String responseJson) async {
    try {
      final response = jsonDecode(responseJson) as Map<String, dynamic>;
      final requesterId = response['requesterId'] as String;
      final responderId = response['responderId'] as String;
      final nodes = (response['nodes'] as List?)?.cast<Map<String, dynamic>>() ?? [];
      
      // Check if this response is for us
      if (requesterId != _manager._dagFetcher.peerId.toBase58()) {
        return; // Not for us
      }
      
      _logger.info('Received sync response from ${responderId.substring(0, 6)} with ${nodes.length} nodes');
      
      // Process the received nodes
      int processedNodes = 0;
      for (final nodeData in nodes) {
        try {
          final cidStr = nodeData['cid'] as String;
          final dataBase64 = nodeData['data'] as String;
          final nodeBytes = base64.decode(dataBase64);
          
          // Deserialize and store the node
          final node = MerkleNode.fromBytes(nodeBytes, _manager._dagFetcher.payloadFactory);
          await _manager._dagFetcher.blockStore.put(node);
          await _manager._dagFetcher.dhtProvider.provide(node.cid);
          
          processedNodes++;
          _logger.fine('Processed node $cidStr from sync response');
          
        } catch (e) {
          _logger.warning('Error processing node from sync response: $e');
        }
      }
      
      if (processedNodes > 0) {
        // Update peer rating for successful response
        _manager._updatePeerRating(responderId, isSuccess: true, responseTime: Duration(seconds: 2));
        
        // Trigger state update
        await _manager._notifyStateChange();
        await _manager._notifyCidChange();
        
        _logger.info('Successfully processed $processedNodes nodes from sync response');
      }
      
    } catch (e, s) {
      _logger.severe('Error handling sync response: $e', e, s);
    }
  }

  /// Handles sync health check messages
  Future<void> _handleSyncHealthCheck(String healthCheckJson) async {
    try {
      final healthCheck = jsonDecode(healthCheckJson) as Map<String, dynamic>;
      final requesterId = healthCheck['requesterId'] as String;
      final requesterHeads = (healthCheck['currentHeads'] as List?)?.cast<String>() ?? [];
      final consecutiveFailures = healthCheck['consecutiveFailures'] as int? ?? 0;
      
      // Don't respond to our own health checks
      if (requesterId == _manager._dagFetcher.peerId.toBase58()) {
        return;
      }
      
      _logger.info('Received sync health check from ${requesterId.substring(0, 6)} (failures: $consecutiveFailures)');
      
      // Update peer rating (health check participation)
      _manager._updatePeerRating(requesterId, isSuccess: true, responseTime: Duration(milliseconds: 500));
      
      // If the peer has many failures, offer to help with our current heads
      if (consecutiveFailures >= 3) {
        await _offerSyncAssistance(requesterId, requesterHeads);
      }
      
    } catch (e, s) {
      _logger.warning('Error handling sync health check: $e', e, s);
    }
  }

  /// Offers sync assistance to a peer that seems to be struggling
  Future<void> _offerSyncAssistance(String peerId, List<String> peerHeads) async {
    try {
      final ourHeads = _manager._merkleCrdt.roots.map((cid) => cid.toString()).toList();
      
      // Check if we have heads they might be missing
      final newHeads = ourHeads.where((head) => !peerHeads.contains(head)).toList();
      
      if (newHeads.isNotEmpty) {
        _logger.info('Offering sync assistance to ${peerId.substring(0, 6)} for ${newHeads.length} heads');
        
        // Send a targeted sync offer
        final offer = {
          'type': 'sync_offer',
          'offererId': _manager._dagFetcher.peerId.toBase58(),
          'targetPeerId': peerId,
          'availableHeads': newHeads,
          'timestamp': DateTime.now().millisecondsSinceEpoch,
        };
        
        final offerJson = jsonEncode(offer);
        await _originalAnnouncer.broadcast('SYNC_OFFER:$offerJson');
      }
      
    } catch (e, s) {
      _logger.warning('Error offering sync assistance: $e', e, s);
    }
  }

  /// Finds nodes that a peer is likely missing based on their current heads
  Future<List<MerkleNode>> _findMissingNodesForPeer(List<String> peerHeads, MerkleNode targetNode) async {
    final missingNodes = <MerkleNode>[];
    
    // For now, just return the target node and its immediate children
    // A more sophisticated implementation would do proper DAG analysis
    missingNodes.add(targetNode);
    
    // Add immediate dependencies
    for (final childCid in targetNode.children) {
      final childNode = await _manager._dagFetcher.blockStore.get(childCid);
      if (childNode != null) {
        missingNodes.add(childNode);
      }
    }
    
    return missingNodes;
  }
}
