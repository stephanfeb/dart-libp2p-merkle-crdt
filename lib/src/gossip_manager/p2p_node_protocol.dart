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
import 'package:merkledag/merkledag.dart' show MerkleNode, CRDTPayload;
import 'package:logging/logging.dart';

final _logger = Logger('P2PNodeProtocol');

/// Handles low-level P2P protocol operations for node exchange.
/// Manages stream lifecycle, message parsing, and connection handling.
class P2PNodeProtocol<P extends CRDTPayload<V>, V> {
  final Host _host;
  final String _protocolId;
  final P Function(Uint8List payloadBytes) _payloadFactory;
  final Future<MerkleNode<P>?> Function(CID cid) _localNodeGetter;

  /// Public getter for the host
  Host get host => _host;

  /// Public getter for the payload factory
  P Function(Uint8List payloadBytes) get payloadFactory => _payloadFactory;

  P2PNodeProtocol(
    this._host,
    this._protocolId,
    this._payloadFactory,
    this._localNodeGetter,
  ) {
    _logger.info('P2PNodeProtocol: Initializing for protocol $_protocolId');
    _host.setStreamHandler(_protocolId, _handleIncomingStream);
    _logger.fine('P2PNodeProtocol: Stream handler set for protocol $_protocolId');
  }

  /// Handles incoming streams for node requests
  Future<void> _handleIncomingStream(P2PStream stream, PeerId remotePeer) async {
    final remotePeerStr = remotePeer.toBase58().substring(0, 10);
    _logger.info('_handleIncomingStream: Received stream from $remotePeerStr (Stream ID: ${stream.id()})');
    
    try {
      final requestBytes = await stream.read();
      if (requestBytes.isEmpty) {
        _logger.warning('_handleIncomingStream: Empty request from $remotePeerStr');
        return;
      }

      final message = utf8.decode(requestBytes);
      _logger.fine('_handleIncomingStream: Message from $remotePeerStr: "$message"');

      if (message.startsWith('REQUEST_NODE ')) {
        await _handleNodeRequest(stream, message, remotePeerStr);
      } else {
        _logger.warning('_handleIncomingStream: Unknown message type from $remotePeerStr: "$message"');
        await _sendErrorResponse(stream, 'Unknown message type: $message');
      }
    } catch (e, s) {
      _logger.severe('_handleIncomingStream: Error handling stream from $remotePeerStr: $e', e, s);
      if (!stream.isClosed) {
        await stream.reset();
      }
    } finally {
      _logger.fine('_handleIncomingStream: Closing stream from $remotePeerStr');
      if (!stream.isClosed) {
        await stream.close();
      }
    }
  }

  /// Handles a node request message
  Future<void> _handleNodeRequest(P2PStream stream, String message, String remotePeerStr) async {
    final cidString = message.substring('REQUEST_NODE '.length);
    _logger.info('_handleNodeRequest: Request for CID $cidString from $remotePeerStr');

    CID cid;
    try {
      cid = CID.fromString(cidString);
    } catch (e) {
      _logger.warning('_handleNodeRequest: Invalid CID "$cidString" from $remotePeerStr: $e');
      await _sendErrorResponse(stream, 'Invalid CID format: $cidString');
      return;
    }

    final node = await _localNodeGetter(cid);
    if (node != null) {
      await _sendNodeResponse(stream, node, remotePeerStr);
    } else {
      await _sendNotFoundResponse(stream, cidString, remotePeerStr);
    }
  }

  /// Sends a node data response
  Future<void> _sendNodeResponse(P2PStream stream, MerkleNode<P> node, String remotePeerStr) async {
    _logger.info('_sendNodeResponse: Serving node ${node.cid} to $remotePeerStr');
    
    // Use standard merkledag serialization to preserve CID consistency
    final nodeBytes = node.toBytes();
    final response = 'NODE_DATA ${node.cid} ${base64.encode(nodeBytes)}';
    
    _logger.finer('_sendNodeResponse: Response length: ${response.length}, serialized to ${nodeBytes.length} bytes');
    await stream.write(utf8.encode(response));
  }

  /// Sends a node not found response
  Future<void> _sendNotFoundResponse(P2PStream stream, String cidString, String remotePeerStr) async {
    _logger.info('_sendNotFoundResponse: Node $cidString not found for $remotePeerStr');
    final response = 'NODE_NOT_FOUND $cidString';
    await stream.write(utf8.encode(response));
  }

  /// Sends an error response
  Future<void> _sendErrorResponse(P2PStream stream, String error) async {
    final response = 'ERROR $error';
    await stream.write(utf8.encode(response));
  }

  /// Fetches a node from a specific provider
  Future<MerkleNode<P>?> fetchNodeFromProvider(CID cid, AddrInfo providerInfo) async {
    final providerPeerId = providerInfo.id;
    final providerPeerStr = providerPeerId.toBase58().substring(0, 10);

    if (providerInfo.addrs.isEmpty) {
      _logger.fine('fetchNodeFromProvider: Provider $providerPeerStr has no addresses');
      return null;
    }

    if (providerPeerId == _host.id) {
      _logger.finer('fetchNodeFromProvider: Skipping self as provider');
      return null;
    }

    _logger.info('fetchNodeFromProvider: Fetching $cid from $providerPeerStr');

    P2PStream? clientStream;
    try {
      // Connect to provider
      await _host.connect(providerInfo);
      _logger.fine('fetchNodeFromProvider: Connected to $providerPeerStr');

      // Open stream
      clientStream = await _host.newStream(providerPeerId, [_protocolId], Context());
      _logger.info('fetchNodeFromProvider: Opened stream ${clientStream.id()} to $providerPeerStr');

      // Send request
      await clientStream.write(utf8.encode('REQUEST_NODE ${cid.toString()}'));
      _logger.fine('fetchNodeFromProvider: Sent REQUEST_NODE to $providerPeerStr');

      // Read response with timeout
      final responseBytes = await clientStream.read().timeout(
        Duration(seconds: 10),
        onTimeout: () {
          _logger.warning('fetchNodeFromProvider: Timeout reading from $providerPeerStr');
          throw TimeoutException('Timeout reading response from $providerPeerStr');
        },
      );

      if (responseBytes.isEmpty) {
        _logger.warning('fetchNodeFromProvider: Empty response from $providerPeerStr');
        return null;
      }

      return _parseNodeResponse(responseBytes, cid, providerPeerStr);
    } catch (e, s) {
      _logger.warning('fetchNodeFromProvider: Failed to fetch from $providerPeerStr: $e', e, s);
      return null;
    } finally {
      if (clientStream != null && !clientStream.isClosed) {
        _logger.finer('fetchNodeFromProvider: Closing stream to $providerPeerStr');
        await clientStream.close().catchError((err) => 
          _logger.warning('fetchNodeFromProvider: Error closing stream: $err'));
      }
    }
  }

  /// Parses a node response and validates content
  MerkleNode<P>? _parseNodeResponse(Uint8List responseBytes, CID requestedCid, String providerPeerStr) {
    final response = utf8.decode(responseBytes);
    _logger.fine('_parseNodeResponse: Response from $providerPeerStr: "$response"');

    if (response.startsWith('NODE_DATA ')) {
      return _parseNodeData(response, requestedCid, providerPeerStr);
    } else if (response.startsWith('NODE_NOT_FOUND ')) {
      _logger.info('_parseNodeResponse: Node not found at $providerPeerStr');
      return null;
    } else if (response.startsWith('ERROR ')) {
      _logger.warning('_parseNodeResponse: Error from $providerPeerStr: $response');
      return null;
    } else {
      _logger.warning('_parseNodeResponse: Unknown response from $providerPeerStr: "$response"');
      return null;
    }
  }

  /// Parses NODE_DATA response and validates content integrity
  MerkleNode<P>? _parseNodeData(String response, CID requestedCid, String providerPeerStr) {
    final parts = response.split(' ');
    if (parts.length < 3) {
      _logger.warning('_parseNodeData: Malformed NODE_DATA from $providerPeerStr: "$response"');
      return null;
    }

    final receivedCidString = parts[1];
    final nodeDataEnc = parts[2];

    try {
      final nodeBytes = base64.decode(nodeDataEnc);
      // Use standard merkledag deserialization to preserve CID consistency
      final fetchedNode = MerkleNode.fromBytes(nodeBytes, _payloadFactory);

      // Content verification - critical for tamper detection
      if (fetchedNode.cid.toString() == requestedCid.toString()) {
        _logger.info('_parseNodeData: Successfully verified node $requestedCid from $providerPeerStr');
        return fetchedNode;
      } else {
        // Proper exception for tamper detection as recommended in Phase 1
        final errorMsg = 'Content verification failed: Fetched node CID ${fetchedNode.cid} '
            'does not match requested CID $requestedCid from $providerPeerStr. '
            'This indicates potential data corruption or tampering.';
        _logger.severe(errorMsg);
        throw StateError(errorMsg);
      }
    } catch (e, s) {
      _logger.warning('_parseNodeData: Failed to parse node data from $providerPeerStr: $e', e, s);
      return null;
    }
  }


  /// Closes the protocol handler
  Future<void> close() async {
    _logger.info('close: Closing P2PNodeProtocol for $_protocolId');
    // Stream handler cleanup is typically handled by host.close()
    _logger.info('P2PNodeProtocol closed for $_protocolId');
  }
}
