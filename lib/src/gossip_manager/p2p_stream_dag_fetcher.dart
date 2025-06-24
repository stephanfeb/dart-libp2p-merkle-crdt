// lib/src/gossip_manager/p2p_stream_dag_fetcher.dart
import 'dart:async';
import 'dart:typed_data';

import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p_kat_dht/dart_libp2p_kat_dht.dart' show IpfsDHT;
import 'package:merkledag/merkledag.dart' show DAGSyncer, MerkleNode, CRDTPayload;
import 'package:logging/logging.dart';

import 'block_store.dart';
import 'p2p_node_protocol.dart';
import 'dht_provider.dart';

final _logger = Logger('P2PStreamDagFetcher');

/// Simplified DAGSyncer implementation focused on the interface contract.
/// Uses P2PNodeProtocol for networking and DHTProvider for content discovery.
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
  IpfsDHT? get dht => _dhtProvider.dht;

  P2PStreamDagFetcher(
    Host host,
    this._blockStore,
    String protocolId,
    P Function(Uint8List payloadBytes) payloadFactory,
    {IpfsDHT? dht}
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
    
    _logger.info('get: CID $cid not in local store, querying network...');
    
    // Find providers via DHT
    final providers = _dhtProvider.findProviders(cid);
    
    // Try to fetch from each provider
    await for (final providerInfo in providers) {
      node = await _nodeProtocol.fetchNodeFromProvider(cid, providerInfo);
      if (node != null) {
        _logger.info('get: Successfully fetched $cid from network');
        await _blockStore.put(node);
        _logger.fine('get: Stored fetched node $cid in local block store');
        return node;
      }
    }
    
    _logger.warning('get: Could not find CID $cid from any provider');
    return null;
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
