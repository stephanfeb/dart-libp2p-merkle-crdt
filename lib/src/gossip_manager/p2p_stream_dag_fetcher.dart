// lib/src/gossip_manager/p2p_stream_dag_fetcher.dart
import 'dart:async';
import 'dart:typed_data';

import 'package:dcid/dcid.dart' show CID;
import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p_kad_dht/dart_libp2p_kad_dht.dart' show IpfsDHTv2;
import 'package:merkledag/merkledag.dart' show DAGSyncer, MerkleNode, CRDTPayload;
import 'package:logging/logging.dart';

import 'block_store.dart';
import 'p2p_node_protocol.dart';
import 'dht_provider.dart';

final _logger = Logger('P2PStreamDagFetcher');

/// A [DAGSyncer] that stores blocks in a [BlockStore], serves them to peers,
/// and fetches missing ones over the block-fetch protocol.
///
/// A missing block is asked for first from the peers that most recently
/// announced a head ([notePeer]), since they hold the DAG behind it, and then
/// from DHT providers if a DHT is configured. Without a DHT, sync relies on
/// announcers alone, which is what a mesh without a DHT (such as BLE) needs.
/// Every fetched block is verified against its CID before it is stored.
class P2PStreamDagFetcher<P extends CRDTPayload<V>, V> implements DAGSyncer<P> {
  final BlockStore<P, V> _blockStore;
  final P2PNodeProtocol<P, V> _nodeProtocol;
  final DHTProvider _dhtProvider;

  /// How many recent announcers to remember and ask.
  final int maxHintPeers;

  /// How many DHT providers to ask for one block.
  final int maxProviders;

  final List<PeerId> _hintPeers = [];
  final Map<CID, Future<MerkleNode<P>?>> _inFlight = {};

  /// Public getter for the payload factory.
  P Function(Uint8List payloadBytes) get payloadFactory => _nodeProtocol.payloadFactory;

  /// Public getter for the host's PeerId.
  PeerId get peerId => _nodeProtocol.host.id;

  /// Public getter for DHT availability.
  bool get isDHTAvailable => _dhtProvider.isAvailable;

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
    P Function(Uint8List payloadBytes) payloadFactory, {
    IpfsDHTv2? dht,
    int maxBlockSize = 4 * 1024 * 1024,
    Duration fetchTimeout = const Duration(seconds: 10),
    this.maxHintPeers = 8,
    this.maxProviders = 3,
  })  : _nodeProtocol = P2PNodeProtocol<P, V>(
          host,
          protocolId,
          payloadFactory,
          (cid) => _blockStore.get(cid),
          maxBlockSize: maxBlockSize,
          timeout: fetchTimeout,
        ),
        _dhtProvider = DHTProvider(dht);

  /// Records that [peer] announced a head, so it is asked first for blocks.
  void notePeer(PeerId peer) {
    if (peer == peerId) return;
    _hintPeers.remove(peer);
    _hintPeers.insert(0, peer);
    if (_hintPeers.length > maxHintPeers) _hintPeers.removeLast();
  }

  @override
  Future<MerkleNode<P>?> get(CID cid) async {
    final local = await _blockStore.get(cid);
    if (local != null) return local;
    // The callback must not return the removed future: whenComplete would
    // wait on it, that is, on itself.
    return _inFlight[cid] ??= _fetch(cid).whenComplete(() {
      _inFlight.remove(cid);
    });
  }

  Future<MerkleNode<P>?> _fetch(CID cid) async {
    for (final peer in List.of(_hintPeers)) {
      final node = await _nodeProtocol.fetch(cid, peer);
      if (node != null) return _store(node);
    }
    await for (final provider in _dhtProvider.findProviders(cid, maxProviders: maxProviders)) {
      final node = await _nodeProtocol.fetchNodeFromProvider(cid, provider);
      if (node != null) return _store(node);
    }
    _logger.fine('No peer supplied $cid');
    return null;
  }

  Future<MerkleNode<P>> _store(MerkleNode<P> node) async {
    await put(node);
    return node;
  }

  /// Stores [node] and, if a DHT is configured, starts providing it. Does not
  /// wait for the DHT: peers fetch from the announcer first.
  @override
  Future<void> put(MerkleNode<P> node) async {
    await _blockStore.put(node);
    if (_dhtProvider.isAvailable) unawaited(_dhtProvider.provide(node.cid));
  }

  /// Gets statistics for monitoring
  Map<String, dynamic> getStats() {
    return {
      'dag_syncer_type': 'P2PStreamDagFetcher',
      'peer_id': peerId.toBase58(),
      'hint_peers': _hintPeers.length,
      'in_flight': _inFlight.length,
      ..._dhtProvider.getStats(),
    };
  }

  /// Stops serving blocks. Fetches already running finish on their own.
  Future<void> close() => _nodeProtocol.close();
}
