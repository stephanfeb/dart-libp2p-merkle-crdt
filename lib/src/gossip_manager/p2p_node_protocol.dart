// lib/src/gossip_manager/p2p_node_protocol.dart
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:dcid/dcid.dart' show CID;
import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/network/stream.dart' show P2PStream;
import 'package:dart_libp2p/core/network/context.dart' show Context;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p/core/peer/addr_info.dart' show AddrInfo;
import 'package:merkledag/merkledag.dart' show MerkleNode, CRDTPayload, InvalidNodeException;
import 'package:logging/logging.dart';

import 'framing.dart';

final _logger = Logger('P2PNodeProtocol');

/// The default protocol ID for fetching blocks.
const String defaultFetchProtocolId = '/merkle-crdt/fetch/2.0.0';

/// The block-fetch protocol: one request and one response per stream.
///
/// Every message is a frame: an unsigned-varint length followed by that many
/// bytes. The request is `0x01` followed by the CID's bytes. The response is
/// `0x00` followed by the node's bytes (`MerkleNode.toBytes`), `0x01` if the
/// block is not held, or `0x02` followed by a UTF-8 error message.
///
/// Both sides enforce [timeout] and size limits. A fetched node is returned
/// only if its content hashes to the CID that was asked for.
class P2PNodeProtocol<P extends CRDTPayload<V>, V> {
  static const int _request = 0x01;
  static const int _found = 0x00;
  static const int _notFound = 0x01;
  static const int _error = 0x02;

  /// Requests carry one CID; anything longer is not a valid request.
  static const int _maxRequestSize = 256;

  final Host _host;
  final String _protocolId;
  final P Function(Uint8List payloadBytes) _payloadFactory;
  final Future<MerkleNode<P>?> Function(CID cid) _localNodeGetter;

  /// Largest serialized node this side sends or accepts.
  final int maxBlockSize;

  /// Time limit for a whole fetch, and for each read or write when serving.
  final Duration timeout;

  bool _closed = false;

  /// Public getter for the host
  Host get host => _host;

  /// Public getter for the payload factory
  P Function(Uint8List payloadBytes) get payloadFactory => _payloadFactory;

  P2PNodeProtocol(
    this._host,
    this._protocolId,
    this._payloadFactory,
    this._localNodeGetter, {
    this.maxBlockSize = 4 * 1024 * 1024,
    this.timeout = const Duration(seconds: 10),
  }) {
    _host.setStreamHandler(_protocolId, _handleIncomingStream);
  }

  Future<void> _handleIncomingStream(P2PStream stream, PeerId remotePeer) async {
    try {
      final request = await FrameReader(stream)
          .readFrame(maxLength: _maxRequestSize)
          .timeout(timeout);
      final response = await _respondTo(request);
      await stream.write(encodeFrame(response)).timeout(timeout);
      await stream.close();
    } catch (e) {
      _logger.fine('Serving ${_short(remotePeer)} failed: $e');
      try {
        await stream.reset();
      } catch (_) {}
    }
  }

  Future<Uint8List> _respondTo(Uint8List request) async {
    if (request.isEmpty || request[0] != _request) {
      return _errorResponse('Unknown request');
    }
    final CID cid;
    try {
      cid = CID.fromBytes(Uint8List.sublistView(request, 1));
    } catch (_) {
      return _errorResponse('Invalid CID');
    }
    final node = _closed ? null : await _localNodeGetter(cid);
    if (node == null) return Uint8List.fromList([_notFound]);
    final nodeBytes = node.toBytes();
    if (nodeBytes.length > maxBlockSize) return _errorResponse('Block too large');
    return Uint8List(nodeBytes.length + 1)
      ..[0] = _found
      ..setRange(1, nodeBytes.length + 1, nodeBytes);
  }

  static Uint8List _errorResponse(String message) =>
      Uint8List.fromList([_error, ...utf8.encode(message)]);

  /// Fetches [cid] from [peer] over an existing or newly dialled connection.
  ///
  /// Returns the verified node, or null if the peer does not have it, cannot
  /// be reached, times out, or serves content that does not match [cid].
  Future<MerkleNode<P>?> fetch(CID cid, PeerId peer) async {
    if (_closed || peer == _host.id) return null;
    P2PStream? stream;
    try {
      return await () async {
        stream = await _host.newStream(peer, [_protocolId], Context());
        await stream!.write(encodeFrame(Uint8List.fromList([_request, ...cid.toBytes()])));
        final response = await FrameReader(stream!).readFrame(maxLength: maxBlockSize + 1);
        if (response.isEmpty) throw const FrameException('Empty response');
        switch (response[0]) {
          case _found:
            return MerkleNode.fromBytes<P, V>(
                Uint8List.sublistView(response, 1), _payloadFactory,
                expectedCid: cid);
          case _notFound:
            return null;
          case _error:
            _logger.fine('${_short(peer)} could not serve $cid: '
                '${utf8.decode(response.sublist(1), allowMalformed: true)}');
            return null;
          default:
            throw FrameException('Unknown response type ${response[0]}');
        }
      }()
          .timeout(timeout);
    } on InvalidNodeException catch (e) {
      _logger.warning('${_short(peer)} served an invalid block for $cid: ${e.message}');
      return null;
    } catch (e) {
      _logger.fine('Fetching $cid from ${_short(peer)} failed: $e');
      return null;
    } finally {
      final s = stream;
      if (s != null && !s.isClosed) {
        unawaited(s.close().catchError((_) {}));
      }
    }
  }

  /// Fetches [cid] from a provider found through the DHT, dialling its
  /// addresses first if it has any.
  Future<MerkleNode<P>?> fetchNodeFromProvider(CID cid, AddrInfo providerInfo) async {
    if (providerInfo.id == _host.id) return null;
    if (providerInfo.addrs.isNotEmpty) {
      try {
        await _host.connect(providerInfo).timeout(timeout);
      } catch (e) {
        _logger.fine('Could not connect to provider ${_short(providerInfo.id)}: $e');
        return null;
      }
    }
    return fetch(cid, providerInfo.id);
  }

  /// Stops serving blocks and removes the stream handler.
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    _host.removeStreamHandler(_protocolId);
  }

  static String _short(PeerId peer) {
    final s = peer.toBase58();
    return s.length > 10 ? s.substring(s.length - 6) : s;
  }
}
