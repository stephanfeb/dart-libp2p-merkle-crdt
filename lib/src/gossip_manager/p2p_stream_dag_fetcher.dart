// lib/src/gossip_manager/p2p_stream_dag_fetcher.dart
import 'dart:async';
import 'dart:convert'; // For utf8, base64
import 'dart:typed_data';

import 'package:dart_cid/dart_cid.dart' show CID;
import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/network/conn.dart' show Conn;
import 'package:dart_libp2p/core/network/mux.dart' show MuxedStream, MuxedConn; // Added MuxedConn
import 'package:dart_libp2p/core/network/stream.dart' show P2PStream;
import 'package:dart_libp2p/core/network/context.dart' show Context; // Added Context
import 'package:dart_libp2p/core/multiaddr.dart' show Multiaddr;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p/core/peer/addr_info.dart' show AddrInfo;
import 'package:dart_libp2p_kat_dht/dart_libp2p_kat_dht.dart' show IpfsDHT;
import 'package:merkledag/merkledag.dart' show DAGSyncer, MerkleNode, CRDTPayload;
import 'package:logging/logging.dart';

import 'block_store.dart';

// Ensure logger is specific to this class or correctly scoped
// If this file-level logger is intended for the class, it's fine.
// Otherwise, define it inside the class as static final.
final _logger = Logger('P2PStreamDagFetcher'); 

// Hypothetical serialization for MerkleNode<P>
Uint8List _serializeMerkleNode<P extends CRDTPayload<V>, V>(MerkleNode<P> node) {
  _logger.finer('_serializeMerkleNode: Serializing node ${node.cid}');
  _logger.finest('_serializeMerkleNode: Node details - CID: ${node.cid}, Children: ${node.children.length}, Payload type: ${node.payload.runtimeType}');
  final nodeMap = {
    'cid': node.cid.toString(),
    'children': node.children.map((child) => child.toString()).toList(), // Changed links to children
    'payload': base64.encode(node.payload.toCanonicalBytes()),
  };
  final encodedBytes = utf8.encode(json.encode(nodeMap));
  _logger.finer('_serializeMerkleNode: Serialized node ${node.cid} to ${encodedBytes.length} bytes.');
  return encodedBytes;
}

// Hypothetical deserialization for MerkleNode<P>
MerkleNode<P> _deserializeMerkleNode<P extends CRDTPayload<V>, V>(
  Uint8List bytes,
  P Function(Uint8List payloadBytes) payloadFactory,
  // CID Function(Uint8List data, P payload) cidFunction, // cidFunction not used in this simplified version
) {
  _logger.finer('_deserializeMerkleNode: Attempting to deserialize ${bytes.length} bytes.');
  final nodeMap = json.decode(utf8.decode(bytes)) as Map<String, dynamic>;
  _logger.finest('_deserializeMerkleNode: Decoded map: $nodeMap');

  final payloadBytes = base64.decode(nodeMap['payload'] as String);
  final payload = payloadFactory(payloadBytes);
  _logger.finest('_deserializeMerkleNode: Deserialized payload of type ${payload.runtimeType}.');

  final linkStrings = nodeMap['children'] as List?; // Changed links to children
  final links = linkStrings?.map((s) {
    try {
      return CID.fromString(s as String);
    } catch (e) {
      _logger.warning('_deserializeMerkleNode: Invalid CID string in children: "$s", error: $e');
      return null;
    }
  }).whereType<CID>().toSet() ?? <CID>{};
  _logger.finest('_deserializeMerkleNode: Parsed ${links.length} children CIDs.');
  
  final cidString = nodeMap['cid'] as String?;
  if (cidString == null) {
    _logger.severe("_deserializeMerkleNode: Serialized node missing CID string. Map: $nodeMap");
    throw FormatException("Serialized node missing CID string");
  }
  CID cid;
  try {
    cid = CID.fromString(cidString);
    _logger.finest('_deserializeMerkleNode: Expected CID from data: $cid');
  } catch (e) {
    _logger.severe("_deserializeMerkleNode: Serialized node has invalid CID string: $cidString, error: $e");
    throw FormatException("Serialized node has invalid CID string: $cidString, error: $e");
  }

  final deserializedNode = MerkleNode<P>.create(payload, links); 
  _logger.finer('_deserializeMerkleNode: Created MerkleNode with CID ${deserializedNode.cid}.');
  if (deserializedNode.cid != cid) {
    _logger.warning('_deserializeMerkleNode: Deserialized node CID ${deserializedNode.cid} does not match expected CID $cid from serialized data. This may indicate a hash mismatch or data corruption.');
    // Depending on policy, this could throw an error or the node could be rejected.
  }
  return deserializedNode;
}


class P2PStreamDagFetcher<P extends CRDTPayload<V>, V> implements DAGSyncer<P> {
  final Host _host;
  final BlockStore<P, V> _blockStore;
  final IpfsDHT? _dht;
  final String _protocolId;
  final P Function(Uint8List payloadBytes) _payloadFactory;
  // cidFunction removed from constructor as it's not used in the simplified _deserializeMerkleNode
  // final CID Function(Uint8List data, P payload) _cidFunction;

  /// Public getter for the DHT instance, if available.
  IpfsDHT? get dht => _dht;

  /// Public getter for the payload factory.
  P Function(Uint8List payloadBytes) get payloadFactory => _payloadFactory;

  /// Public getter for the host's PeerId.
  PeerId get peerId => _host.id;

  P2PStreamDagFetcher(
    this._host,
    this._blockStore,
    this._protocolId,
    this._payloadFactory,
    // this._cidFunction, 
    { IpfsDHT? dht }
  ) : _dht = dht {
    _logger.info('P2PStreamDagFetcher: Constructor started. Protocol ID: $_protocolId, DHT available: ${_dht != null}');
    _host.setStreamHandler(_protocolId, _handleIncomingStream);
    _logger.fine('P2PStreamDagFetcher: Stream handler set for protocol $_protocolId.');
  }

  Future<void> _handleIncomingStream(P2PStream stream, PeerId remotePeer) async {
    final remotePeerStr = remotePeer.toBase58().substring(0,10);
    _logger.info('_handleIncomingStream: Received incoming stream for protocol $_protocolId from $remotePeerStr (Stream ID: ${stream.id()})');
    try {
      final requestBytes = await stream.read(); 
      if (requestBytes.isEmpty) {
        _logger.warning('_handleIncomingStream: Received empty request from $remotePeerStr on protocol $_protocolId (Stream ID: ${stream.id()})');
        return;
      }
      _logger.finer('_handleIncomingStream: Received ${requestBytes.length} bytes from $remotePeerStr.');

      final message = utf8.decode(requestBytes);
      _logger.fine('_handleIncomingStream: Received message on $_protocolId from $remotePeerStr: "$message"');

      if (message.startsWith('REQUEST_NODE ')) {
        final cidString = message.substring('REQUEST_NODE '.length);
        _logger.info('_handleIncomingStream: Received REQUEST_NODE for CID $cidString from $remotePeerStr.');
        CID cid;
        try {
          cid = CID.fromString(cidString);
        } catch (e) {
          _logger.warning('_handleIncomingStream: Received request for invalid CID string "$cidString" from $remotePeerStr: $e');
          await stream.write(utf8.encode('ERROR Invalid CID format: $cidString')); 
          return; 
        }
        final node = await _blockStore.get(cid);

        if (node != null) {
          _logger.info('_handleIncomingStream: Serving node $cidString to $remotePeerStr.');
          final nodeBytes = _serializeMerkleNode<P,V>(node);
          final response = 'NODE_DATA $cidString ${base64.encode(nodeBytes)}';
          _logger.finer('_handleIncomingStream: Sending NODE_DATA for $cidString to $remotePeerStr. Response length: ${response.length}');
          await stream.write(utf8.encode(response)); 
        } else {
          _logger.info('_handleIncomingStream: Node $cidString not found locally for $remotePeerStr.');
          final response = 'NODE_NOT_FOUND $cidString';
          await stream.write(utf8.encode(response));
        }
      } else {
        _logger.warning('_handleIncomingStream: Received unknown message type on $_protocolId from $remotePeerStr: "$message"');
        await stream.write(utf8.encode('ERROR Unknown message type: $message'));
      }
    } catch (e, s) {
      _logger.severe('_handleIncomingStream: Error handling incoming stream for $_protocolId from $remotePeerStr: $e', e, s);
      if (!stream.isClosed) { 
        await stream.reset(); 
      }
    } finally {
      _logger.info('_handleIncomingStream: Finished handling incoming stream on $_protocolId from $remotePeerStr (Stream ID: ${stream.id()}). Closing stream.');
      if (!stream.isClosed) {
        await stream.close(); 
      }
    }
  }

  @override
  Future<MerkleNode<P>?> get(CID cid) async {
    _logger.info('get: Attempting to get CID: $cid');
    var node = await _blockStore.get(cid);
    if (node != null) {
      _logger.info('get: Found CID $cid in local block store.');
      return node;
    }
    _logger.info('get: CID $cid not in local block store. Querying network...');

    if (_dht == null) {
        _logger.warning('get: DHT is not available. Cannot find providers for $cid.');
        return null;
    }

    _logger.fine('get: Finding providers for CID $cid via DHT.');
    final providersStream = _dht!.findProvidersAsync(cid.toV1(), 20).timeout(
      Duration(seconds: 10), 
      onTimeout: (EventSink<AddrInfo> sink) { 
        _logger.warning('get: Timeout finding providers for $cid after 10 seconds.');
        sink.close(); 
      }
    );

    await for (final providerAddrInfo in providersStream) {
      PeerId providerPeerId = providerAddrInfo.id;
      final maddrSet = providerAddrInfo.addrs;
      final providerPeerStr = providerPeerId.toBase58().substring(0,10);

      if (maddrSet.isEmpty) {
        _logger.fine('get: Provider $providerPeerStr for CID $cid has no addresses, skipping.');
        continue;
      }
      if (providerPeerId == _host.id) {
        _logger.finer('get: Skipping self as provider for $cid.');
        continue;
      }

      _logger.info('get: Attempting to fetch $cid from provider: $providerPeerStr using addrs: $maddrSet');
      P2PStream? clientStream; 
      try {
        _logger.finer('get: Connecting to provider $providerPeerStr for $cid.');
        await _host.connect(providerAddrInfo);
        _logger.fine('get: Connected to provider $providerPeerStr. Opening new stream for $_protocolId.');

        clientStream = await _host.newStream(providerPeerId, [_protocolId], Context());
        _logger.info('get: Opened stream ${clientStream.id()} to $providerPeerStr for $_protocolId to fetch $cid.');

        _logger.fine('get: Sending REQUEST_NODE $cid to $providerPeerStr via stream ${clientStream.id()}'); 
        await clientStream.write(utf8.encode('REQUEST_NODE ${cid.toString()}')); 

        final responseBytes = await clientStream.read().timeout(Duration(seconds: 10), onTimeout: () { 
          _logger.warning('get: Timeout reading response for $cid from $providerPeerStr on stream ${clientStream?.id()}.');
          throw TimeoutException('Timeout reading response for $cid from $providerPeerStr');
        });
        _logger.finer('get: Received ${responseBytes.length} bytes in response for $cid from $providerPeerStr.');

        if (responseBytes.isNotEmpty) {
          final response = utf8.decode(responseBytes);
          _logger.fine('get: Response from $providerPeerStr for $cid: "$response"');
          if (response.startsWith('NODE_DATA ')) {
            final parts = response.split(' ');
            if (parts.length < 3) {
               _logger.warning('get: Malformed NODE_DATA response from $providerPeerStr for $cid: "$response"');
               continue;
            }
            final receivedCidString = parts[1];
            final nodeDataEnc = parts[2];
            _logger.finer('get: Received NODE_DATA for CID $receivedCidString from $providerPeerStr. Encoded data length: ${nodeDataEnc.length}');
            final nodeBytes = base64.decode(nodeDataEnc);
            final fetchedNode = _deserializeMerkleNode<P,V>(nodeBytes, _payloadFactory); 
            if (fetchedNode.cid.toString() == cid.toString()) { // Compare string representations for safety
              _logger.info('get: Successfully fetched and deserialized node $cid from $providerPeerStr.');
              await _blockStore.put(fetchedNode);
              _logger.fine('get: Stored fetched node $cid into local block store.');
              // Optionally provide to DHT:
              // await _dht?.provide(fetchedNode.cid.toV1(), true);
              return fetchedNode;
            } else {
              _logger.warning('get: Fetched node CID ${fetchedNode.cid} does not match requested CID $cid from $providerPeerStr.');
            }
          } else if (response.startsWith('NODE_NOT_FOUND ')) {
            _logger.info('get: Provider $providerPeerStr does not have node $cid.');
          } else {
            _logger.warning('get: Unknown response for $cid from $providerPeerStr: "$response"');
          }
        } else {
          _logger.warning('get: Empty response for $cid from $providerPeerStr.');
        }
      } catch (e, s) {
        _logger.warning('get: Failed to connect or fetch $cid from $providerPeerStr: $e', e, s);
      } finally {
        if (clientStream != null && !clientStream.isClosed) { 
          _logger.finer('get: Closing client stream ${clientStream.id()} to $providerPeerStr for $cid.');
          await clientStream.close().catchError((err) => _logger.warning("get: Error closing clientStream ${clientStream?.id()} in finally: $err")); 
        }
      }
    }

    _logger.warning('get: Could not find CID $cid from any provider after checking all known providers.');
    return null;
  }

  @override
  Future<void> put(MerkleNode<P> node) async {
    _logger.info('put: Putting node ${node.cid} into BlockStore.');
    await _blockStore.put(node);
    _logger.fine('put: Node ${node.cid} successfully put into BlockStore.');
    if (_dht != null) {
      _logger.info('put: Providing CID ${node.cid} to DHT.');
      await _dht!.provide(node.cid.toV1(), true).then((_) {
        _logger.fine('put: Successfully initiated provide for CID ${node.cid} to DHT.');
      }).catchError((e, s) { 
        _logger.warning('put: Failed to provide CID ${node.cid} to DHT: $e', e, s);
      });
    } else {
        _logger.warning('put: DHT is not available. Cannot provide CID ${node.cid}.');
    }
  }

  Future<void> close() async {
    _logger.info('close: Closing P2PStreamDagFetcher for protocol $_protocolId.');
    // If _host.removeStreamHandler exists, call it. Otherwise, it might be handled by host.close().
    // Example: if (_host.isClosed == false) { _host.removeStreamHandler(_protocolId); }
    // For now, assume host close handles this or it's not strictly necessary for this component's lifecycle.
    _logger.info('P2PStreamDagFetcher closed for protocol $_protocolId.');
  }
}
