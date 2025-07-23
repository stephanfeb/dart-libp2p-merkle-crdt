// lib/src/gossip_manager/p2p_stream_dag_fetcher.dart
import 'dart:async';
import 'dart:typed_data';

import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p_kat_dht/dart_libp2p_kat_dht.dart' show IpfsDHTv2;
import 'package:merkledag/merkledag.dart' show DAGSyncer, MerkleNode, CRDTPayload;
import 'package:logging/logging.dart';

import 'block_store.dart';
import 'p2p_node_protocol.dart';
import 'dht_provider.dart';

final _logger = Logger('P2PStreamDagFetcher');

/// Enhanced DAGSyncer implementation with comprehensive DAG path reconstruction.
/// When nodes are out of sync, this fetcher can trace back through the DAG 
/// to find and fetch all missing intermediate nodes in dependency order.
class P2PStreamDagFetcher<P extends CRDTPayload<V>, V> implements DAGSyncer<P> {
  final BlockStore<P, V> _blockStore;
  final P2PNodeProtocol<P, V> _nodeProtocol;
  final DHTProvider _dhtProvider;

  /// Public getter for the payload factory.
  P Function(Uint8List payloadBytes) get payloadFactory => _nodeProtocol.payloadFactory;

  /// Public getter for the host's PeerId.
  PeerId get peerId => _nodeProtocol.host.id;

  /// Public getter for DHT availability.
  bool get isDHTAvailable => _dhtProvider.isAvailable;

  /// Legacy getter for DHT (for backward compatibility)
  /// Returns the actual DHT instance or null
  IpfsDHTv2? get dht => _dhtProvider.dht;

  /// Public getter for the block store (for sync operations)
  BlockStore<P, V> get blockStore => _blockStore;

  /// Public getter for the DHT provider (for sync operations)
  DHTProvider get dhtProvider => _dhtProvider;

  /// Public getter for the node protocol (for sync operations)
  P2PNodeProtocol<P, V> get nodeProtocol => _nodeProtocol;

  P2PStreamDagFetcher(
    Host host,
    this._blockStore,
    String protocolId,
    P Function(Uint8List payloadBytes) payloadFactory,
    {IpfsDHTv2? dht}
  ) : _nodeProtocol = P2PNodeProtocol<P, V>(
        host,
        protocolId,
        payloadFactory,
        (cid) => _blockStore.get(cid),
      ),
      _dhtProvider = DHTProvider(dht) {
    _logger.info('P2PStreamDagFetcher: Initialized with protocol $protocolId, DHT available: ${_dhtProvider.isAvailable}');
  }

  @override
  Future<MerkleNode<P>?> get(CID cid) async {
    _logger.info('get: Attempting to get CID: $cid');
    
    // Check local storage first
    var node = await _blockStore.get(cid);
    if (node != null) {
      _logger.info('get: Found CID $cid in local block store');
      return node;
    }
    
    _logger.info('get: CID $cid not in local store, attempting network fetch with DAG reconstruction...');
    
    // Try enhanced fetch with DAG path reconstruction
    node = await _fetchWithDagReconstruction(cid);
    
    if (node != null) {
      _logger.info('get: Successfully fetched and stored $cid via DAG reconstruction');
      return node;
    }
    
    _logger.warning('get: Could not find CID $cid from any provider');
    return null;
  }

  /// Enhanced fetch method that handles DAG path reconstruction for out-of-sync scenarios
  Future<MerkleNode<P>?> _fetchWithDagReconstruction(CID targetCid) async {
    _logger.info('_fetchWithDagReconstruction: Starting DAG reconstruction for $targetCid');
    
    // Find providers via DHT
    final providers = _dhtProvider.findProviders(targetCid);
    
    // Try to fetch from each provider with full DAG reconstruction
    await for (final providerInfo in providers) {
      try {
        final node = await _fetchNodeWithDependencies(targetCid, providerInfo);
        if (node != null) {
          _logger.info('_fetchWithDagReconstruction: Successfully reconstructed DAG for $targetCid');
          return node;
        }
      } catch (e) {
        _logger.warning('_fetchWithDagReconstruction: Failed to fetch from provider ${providerInfo.id.toBase58().substring(0, 6)}: $e');
      }
    }
    
    return null;
  }

  /// Fetches a node and all its missing dependencies from a specific provider
  Future<MerkleNode<P>?> _fetchNodeWithDependencies(CID targetCid, providerInfo) async {
    final providerPeerStr = providerInfo.id.toBase58().substring(0, 6);
    _logger.info('_fetchNodeWithDependencies: Fetching $targetCid and dependencies from $providerPeerStr');
    
    // First, try to fetch the target node
    final targetNode = await _nodeProtocol.fetchNodeFromProvider(targetCid, providerInfo);
    if (targetNode == null) {
      _logger.warning('_fetchNodeWithDependencies: Failed to fetch target node $targetCid from $providerPeerStr');
      return null;
    }
    
    // Analyze and fetch missing dependencies
    final missingCids = await _findMissingDependencies([targetNode]);
    if (missingCids.isNotEmpty) {
      _logger.info('_fetchNodeWithDependencies: Found ${missingCids.length} missing dependencies for $targetCid');
      
      // Fetch all missing dependencies in topological order
      final success = await _fetchMissingNodes(missingCids, providerInfo);
      if (!success) {
        _logger.warning('_fetchNodeWithDependencies: Failed to fetch all dependencies for $targetCid');
        return null;
      }
    }
    
    // Store the target node after all dependencies are available
    await _blockStore.put(targetNode);
    _logger.fine('_fetchNodeWithDependencies: Stored target node $targetCid');
    
    // Provide to DHT for future queries
    await _dhtProvider.provide(targetNode.cid);
    
    return targetNode;
  }

  /// Analyzes a set of nodes to find all missing dependencies in our local store
  Future<Set<CID>> _findMissingDependencies(List<MerkleNode<P>> nodes) async {
    final missingCids = <CID>{};
    final visited = <CID>{};
    final toProcess = <MerkleNode<P>>[...nodes];
    
    while (toProcess.isNotEmpty) {
      final currentNode = toProcess.removeAt(0);
      final currentCid = currentNode.cid;
      
      if (visited.contains(currentCid)) continue;
      visited.add(currentCid);
      
      // Check each child/parent dependency
      for (final childCid in currentNode.children) {
        if (visited.contains(childCid)) continue;
        
        final existingChild = await _blockStore.get(childCid);
        if (existingChild == null) {
          // This dependency is missing
          missingCids.add(childCid);
          _logger.fine('_findMissingDependencies: Found missing dependency $childCid');
        } else {
          // We have this node, but we need to check its dependencies too
          toProcess.add(existingChild);
        }
      }
    }
    
    return missingCids;
  }

  /// Fetches a set of missing nodes from a provider in dependency order
  Future<bool> _fetchMissingNodes(Set<CID> missingCids, providerInfo) async {
    final providerPeerStr = providerInfo.id.toBase58().substring(0, 6);
    _logger.info('_fetchMissingNodes: Fetching ${missingCids.length} missing nodes from $providerPeerStr');
    
    // Track nodes we've successfully fetched in this session
    final fetchedNodes = <CID, MerkleNode<P>>{};
    final remaining = <CID>{...missingCids};
    
    // Keep trying until we've fetched all nodes or can't make progress
    int lastRemainingCount = remaining.length;
    while (remaining.isNotEmpty) {
      final currentBatch = <CID>[];
      
      // Find nodes we can fetch now (their dependencies are satisfied)
      for (final cid in remaining) {
        if (await _canFetchNow(cid, fetchedNodes)) {
          currentBatch.add(cid);
        }
      }
      
      // If we can't fetch any nodes in this iteration, try fetching any remaining nodes
      // This handles cases where we don't have full dependency information
      if (currentBatch.isEmpty && remaining.isNotEmpty) {
        _logger.info('_fetchMissingNodes: No nodes ready to fetch, trying remaining ${remaining.length} nodes directly');
        currentBatch.addAll(remaining.take(5)); // Try up to 5 nodes at once
      }
      
      // Fetch the current batch
      bool progress = false;
      for (final cid in currentBatch) {
        try {
          final node = await _nodeProtocol.fetchNodeFromProvider(cid, providerInfo);
          if (node != null) {
            await _blockStore.put(node);
            fetchedNodes[cid] = node;
            remaining.remove(cid);
            progress = true;
            _logger.fine('_fetchMissingNodes: Successfully fetched $cid');
          } else {
            _logger.warning('_fetchMissingNodes: Failed to fetch $cid from $providerPeerStr');
          }
        } catch (e) {
          _logger.warning('_fetchMissingNodes: Error fetching $cid: $e');
        }
      }
      
      // Check if we're making progress
      if (!progress || remaining.length == lastRemainingCount) {
        _logger.warning('_fetchMissingNodes: No progress made, ${remaining.length} nodes still missing');
        return false;
      }
      
      lastRemainingCount = remaining.length;
    }
    
    _logger.info('_fetchMissingNodes: Successfully fetched all ${missingCids.length} missing nodes');
    return true;
  }

  /// Checks if a CID can be fetched now (all its dependencies are available)
  Future<bool> _canFetchNow(CID cid, Map<CID, MerkleNode<P>> fetchedNodes) async {
    // For now, we don't have dependency information until we fetch the node
    // So we assume we can try to fetch any node
    // In a more sophisticated implementation, we could maintain a dependency graph
    return true;
  }

  /// Attempts to reconstruct the DAG path from current state to a target head
  /// This is used when we detect we're out of sync with a remote head
  Future<List<CID>?> findMissingPath(CID targetHead, List<CID> currentHeads) async {
    _logger.info('findMissingPath: Finding path from current heads to target $targetHead');
    
    // Try to fetch the target head first to understand its structure
    final providers = _dhtProvider.findProviders(targetHead, maxProviders: 3);
    
    await for (final provider in providers) {
      try {
        final targetNode = await _nodeProtocol.fetchNodeFromProvider(targetHead, provider);
        if (targetNode != null) {
          // Analyze the path from our current state
          final missingPath = await _traceMissingPath(targetNode, currentHeads);
          if (missingPath != null) {
            _logger.info('findMissingPath: Found path with ${missingPath.length} missing nodes');
            return missingPath;
          }
        }
      } catch (e) {
        _logger.warning('findMissingPath: Error analyzing path: $e');
      }
    }
    
    _logger.warning('findMissingPath: Could not find path to $targetHead');
    return null;
  }

  /// Traces back from a target node to find the missing path from current heads
  Future<List<CID>?> _traceMissingPath(MerkleNode<P> targetNode, List<CID> currentHeads) async {
    // This is a simplified implementation
    // A more sophisticated version would do proper graph traversal
    final missingPath = <CID>[];
    final toExplore = [targetNode];
    final visited = <CID>{};
    
    while (toExplore.isNotEmpty) {
      final currentNode = toExplore.removeAt(0);
      final currentCid = currentNode.cid;
      
      if (visited.contains(currentCid)) continue;
      visited.add(currentCid);
      
      // Check if we already have this node
      final existingNode = await _blockStore.get(currentCid);
      if (existingNode == null) {
        missingPath.add(currentCid);
      }
      
      // Explore parents
      for (final childCid in currentNode.children) {
        final childNode = await _blockStore.get(childCid);
        if (childNode != null) {
          toExplore.add(childNode);
        }
      }
    }
    
    return missingPath.isEmpty ? null : missingPath;
  }

  @override
  Future<void> put(MerkleNode<P> node) async {
    _logger.info('put: Storing node ${node.cid}');
    
    // Store in local block store
    await _blockStore.put(node);
    _logger.fine('put: Node ${node.cid} stored in local block store');
    
    // Provide to DHT for content discovery
    await _dhtProvider.provide(node.cid);
    _logger.fine('put: Node ${node.cid} provided to DHT');
  }

  /// Gets statistics for monitoring
  Map<String, dynamic> getStats() {
    return {
      'dag_syncer_type': 'P2PStreamDagFetcher',
      'peer_id': peerId.toBase58(),
      ...?_dhtProvider.getStats(),
    };
  }

  /// Closes the DAG syncer and its components
  Future<void> close() async {
    _logger.info('close: Closing P2PStreamDagFetcher');
    
    await Future.wait([
      _nodeProtocol.close(),
      _dhtProvider.close(),
    ]);
    
    _logger.info('P2PStreamDagFetcher closed');
  }
}
