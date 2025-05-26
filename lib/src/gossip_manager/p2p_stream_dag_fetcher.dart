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

final _logger = Logger('P2PStreamDagFetcher');

// Hypothetical serialization for MerkleNode<P>
Uint8List _serializeMerkleNode<P extends CRDTPayload<V>, V>(MerkleNode<P> node) {
  final nodeMap = {
    'cid': node.cid.toString(),
    'children': node.children.map((child) => child.toString()).toList(), // Changed links to children
    'payload': base64.encode(node.payload.toCanonicalBytes()),
  };
  return utf8.encode(json.encode(nodeMap));
}

// Hypothetical deserialization for MerkleNode<P>
MerkleNode<P> _deserializeMerkleNode<P extends CRDTPayload<V>, V>(
  Uint8List bytes,
  P Function(Uint8List payloadBytes) payloadFactory,
  // CID Function(Uint8List data, P payload) cidFunction, // cidFunction not used in this simplified version
) {
  final nodeMap = json.decode(utf8.decode(bytes)) as Map<String, dynamic>;
  final payloadBytes = base64.decode(nodeMap['payload'] as String);
  final payload = payloadFactory(payloadBytes);
  final linkStrings = nodeMap['children'] as List?; // Changed links to children
  final links = linkStrings?.map((s) {
    try {
      return CID.fromString(s as String);
    } catch (e) {
      _logger.warning('Invalid CID string in links: $s, error: $e');
      return null;
    }
  }).whereType<CID>().toSet() ?? <CID>{};
  
  final cidString = nodeMap['cid'] as String?;
  if (cidString == null) {
    throw FormatException("Serialized node missing CID string");
  }
  CID cid;
  try {
    cid = CID.fromString(cidString);
  } catch (e) {
    throw FormatException("Serialized node has invalid CID string: $cidString, error: $e");
  }

  final deserializedNode = MerkleNode<P>.create(payload, links); 
  if (deserializedNode.cid != cid) {
    _logger.warning('Deserialized node CID ${deserializedNode.cid} does not match expected CID $cid from serialized data.');
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

  P2PStreamDagFetcher(
    this._host,
    this._blockStore,
    this._protocolId,
    this._payloadFactory,
    // this._cidFunction, 
    { IpfsDHT? dht }
  ) : _dht = dht {
    _host.setStreamHandler(_protocolId, _handleIncomingStream);
  }

  Future<void> _handleIncomingStream(P2PStream stream, PeerId remotePeer) async {
    _logger.info('Received incoming stream for protocol $_protocolId from ${remotePeer.toBase58()}');
    try {
      final requestBytes = await stream.read(); // Assuming P2PStream has read()
      if (requestBytes.isEmpty) {
        _logger.warning('Received empty request from ${remotePeer.toBase58()} on protocol $_protocolId');
        return;
      }

      final message = utf8.decode(requestBytes);
      _logger.fine('Received message on $_protocolId from ${remotePeer.toBase58()}: $message');

      if (message.startsWith('REQUEST_NODE ')) {
        final cidString = message.substring('REQUEST_NODE '.length);
        CID cid;
        try {
          cid = CID.fromString(cidString);
        } catch (e) {
          _logger.warning('Received request for invalid CID string from ${remotePeer.toBase58()}: $cidString, error: $e');
          await stream.write(utf8.encode('ERROR Invalid CID format')); // Send error back
          return; 
        }
        final node = await _blockStore.get(cid);

        if (node != null) {
          _logger.info('Serving node $cidString to ${remotePeer.toBase58()}');
          final nodeBytes = _serializeMerkleNode<P,V>(node);
          final response = 'NODE_DATA $cidString ${base64.encode(nodeBytes)}';
          await stream.write(utf8.encode(response)); // Assuming P2PStream has write()
        } else {
          _logger.info('Node $cidString not found locally for ${remotePeer.toBase58()}');
          final response = 'NODE_NOT_FOUND $cidString';
          await stream.write(utf8.encode(response));
        }
      } else {
        _logger.warning('Received unknown message type on $_protocolId from ${remotePeer.toBase58()}: $message');
        await stream.write(utf8.encode('ERROR Unknown message type'));
      }
    } catch (e, s) {
      _logger.severe('Error handling incoming stream for $_protocolId from ${remotePeer.toBase58()}: $e', e, s);
      if (!stream.isClosed) { // Assuming P2PStream has isClosed
        await stream.reset(); // Assuming P2PStream has reset()
      }
    } finally {
      _logger.info('Finished handling incoming stream on $_protocolId from ${remotePeer.toBase58()}');
      if (!stream.isClosed) {
        await stream.close(); // Assuming P2PStream has close()
      }
    }
  }

  @override
  Future<MerkleNode<P>?> get(CID cid) async {
    _logger.fine('Attempting to get CID: $cid');
    var node = await _blockStore.get(cid);
    if (node != null) {
      _logger.info('Found CID $cid in local block store.');
      return node;
    }
    _logger.info('CID $cid not in local block store. Querying network...');

    if (_dht == null) {
        _logger.warning('DHT is not available to P2PStreamDagFetcher. Cannot find providers for $cid.');
        return null;
    }

    final providersStream = _dht!.findProvidersAsync(cid.toV1(), 20).timeout(
      Duration(seconds: 10), 
      onTimeout: (EventSink<AddrInfo> sink) { // Correct signature for onTimeout
        _logger.warning('Timeout finding providers for $cid');
        sink.close(); // Close the sink, the stream will then complete empty.
      }
    );

    await for (final providerAddrInfo in providersStream) {
      PeerId providerPeerId = providerAddrInfo.id;
      final maddrSet = providerAddrInfo.addrs;

      if (maddrSet.isEmpty) {
        _logger.fine('Provider ${providerPeerId.toBase58()} has no addresses, skipping.');
        continue;
      }
      if (providerPeerId == _host.id) {
        _logger.fine('Skipping self as provider for $cid');
        continue;
      }

      _logger.info('Attempting to fetch $cid from provider: ${providerPeerId.toBase58()} using addrs: $maddrSet');
      P2PStream? clientStream; // Declare here to be accessible in finally, typed as P2PStream
      try {
        await _host.connect(providerAddrInfo);

        clientStream = await _host.newStream(providerPeerId, [_protocolId], Context());

        _logger.fine('Sending REQUEST_NODE $cid to ${providerPeerId.toBase58()} via stream ${clientStream.id()}'); 
        await clientStream.write(utf8.encode('REQUEST_NODE ${cid.toString()}')); 

        final responseBytes = await clientStream.read().timeout(Duration(seconds: 10), onTimeout: () { 
          _logger.warning('Timeout reading response for $cid from ${providerPeerId.toBase58()}');
          throw TimeoutException('Timeout reading response');
        });

        if (responseBytes.isNotEmpty) {
          final response = utf8.decode(responseBytes);
          if (response.startsWith('NODE_DATA ')) {
            final parts = response.split(' ');
            final nodeDataEnc = parts[2];
            final nodeBytes = base64.decode(nodeDataEnc);
            // Removed cidFunction from call as it's not used in the simplified version
            final fetchedNode = _deserializeMerkleNode<P,V>(nodeBytes, _payloadFactory); 
            if (fetchedNode.cid == cid) {
              _logger.info('Successfully fetched node $cid from ${providerPeerId.toBase58()}');
              await _blockStore.put(fetchedNode);
              // Optionally provide to DHT:
              // await _dht?.provide(fetchedNode.cid.toV1(), true);
              return fetchedNode;
            } else {
              _logger.warning('Fetched node CID ${fetchedNode.cid} does not match requested CID $cid from ${providerPeerId.toBase58()}');
            }
          } else if (response.startsWith('NODE_NOT_FOUND ')) {
            _logger.info('Provider ${providerPeerId.toBase58()} does not have node $cid');
          } else {
            _logger.warning('Unknown response for $cid from ${providerPeerId.toBase58()}: $response');
          }
        } else {
          _logger.warning('Empty response for $cid from ${providerPeerId.toBase58()}');
        }
      } catch (e, s) {
        _logger.warning('Failed to connect or fetch $cid from ${providerPeerId.toBase58()}: $e', e, s);
      } finally {
        if (clientStream != null && !clientStream.isClosed) { // Assumes MuxedStream has isClosed
          await clientStream.close().catchError((err) => _logger.warning("Error closing clientStream in finally: $err")); // Assumes MuxedStream has close()
        }
      }
    }

    _logger.warning('Could not find CID $cid from any provider.');
    return null;
  }

  @override
  Future<void> put(MerkleNode<P> node) async {
    _logger.fine('Putting node ${node.cid} into BlockStore.');
    await _blockStore.put(node);
    if (_dht != null) {
      _logger.info('Providing CID ${node.cid} to DHT.');
      await _dht!.provide(node.cid.toV1(), true).catchError((e, s) { // Added await
        _logger.warning('Failed to provide CID ${node.cid} to DHT: $e', e, s);
      });
    } else {
        _logger.warning('DHT is not available to P2PStreamDagFetcher. Cannot provide CID ${node.cid}.');
    }
  }

  Future<void> close() async {
    // If _host.removeStreamHandler exists, call it. Otherwise, it might be handled by host.close().
    // _host.removeStreamHandler(_protocolId); 
    _logger.info('P2PStreamDagFetcher closed.');
  }
}
