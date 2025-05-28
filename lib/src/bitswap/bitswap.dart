import 'dart:async'; // Added for StreamSubscription
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p; // Assuming this is the corrected import
import 'package:dart_cid/dart_cid.dart' as dart_cid; // Using alias for clarity with CID class
import 'package:dart_multihash/dart_multihash.dart' as dart_multihash;
import 'package:crypto/crypto.dart' as crypto;

import './proto/bitswap_message.pb.dart' as pb;
import './want_manager.dart';
import './network_service.dart';
import 'package:convert/convert.dart'; // For hex encoding in logs

// Interface for a block store
abstract class IBlockStore {
  Future<bool> has(dart_cid.CID cid);
  Future<Uint8List?> get(dart_cid.CID cid);
  Future<void> put(dart_cid.CID cid, Uint8List data);
}

class Bitswap {
  // Helper to keep track of peers known to support Bitswap.
  // This could be populated when streams are initiated or messages are received.
  // For simplicity, not fully managed here yet.
  final Set<String> _activeBitswapPeers = {}; 
  final libp2p.Host _host;
  final IBlockStore _blockStore;
  late final WantManager _wantManager;
  late final BitswapNetworkService _networkService;
  StreamSubscription<dynamic>? _connectednessChangeSubscription;
  final String nodeNameForLogging; // <-- ADDED THIS

  Bitswap({
    required libp2p.Host host,
    required IBlockStore blockStore,
    required Stream<dynamic> peerEventsStream,
    required this.nodeNameForLogging, // <-- ADDED THIS
  })  : _host = host,
        _blockStore = blockStore {
    _wantManager = WantManager(); // WantManager logs are self-contained
    _networkService = BitswapNetworkService(_host, handleIncomingMessage, nodeNameForLogging); // <-- PASS nodeNameForLogging
    _networkService.start();
    print('// $nodeNameForLogging BSW_LOG: Bitswap constructor: Network service started.');

    try {
      _connectednessChangeSubscription = peerEventsStream.listen((event) {
        if (event is libp2p.EvtPeerConnectednessChanged) {
          if (event.connectedness == libp2p.Connectedness.notConnected) {
            print('// $nodeNameForLogging BSW_LOG: Peer connectedness changed: ${event.peer.toString()} is NOT CONNECTED.');
            onPeerDisconnected(event.peer);
          } else if (event.connectedness == libp2p.Connectedness.connected) {
            print('// $nodeNameForLogging BSW_LOG: Peer connectedness changed: ${event.peer.toString()} is CONNECTED.');
            // print('Bitswap: Peer connected: ${event.peer.toString()}'); // Redundant
          }
        }
      });
      print('// $nodeNameForLogging BSW_LOG: Bitswap: Listening to provided peer connectedness event stream.');
      // print('Bitswap: Listening to provided peer connectedness event stream.'); // Redundant
    } catch (e) {
      print('// $nodeNameForLogging BSW_LOG: Bitswap: Error listening to peer connectedness event stream: $e. Disconnection cleanup might not work.');
      // print('Bitswap: Error listening to peer connectedness event stream: $e. Disconnection cleanup might not work.'); // Redundant
    }
    print('// $nodeNameForLogging BSW_LOG: Bitswap instance created for host: ${_host.id.toBase58()}.');
    // print('Bitswap instance created and network service started.'); // Redundant
  }

  Future<void> stop() async {
    print('// $nodeNameForLogging BSW_LOG: Bitswap stopping...');
    // print('Bitswap stopping...'); // Redundant
    await _connectednessChangeSubscription?.cancel();
    await _networkService.stop();
    // print('Bitswap stopped.'); // Redundant
  }
  
  void onPeerDisconnected(libp2p.PeerId peerId) {
    final peerIdStr = peerId.toString();
    print('// $nodeNameForLogging BSW_LOG: onPeerDisconnected: $peerIdStr');
    // print('Bitswap: Peer disconnected: $peerIdStr. Cleaning up...'); // Redundant
    _activeBitswapPeers.remove(peerIdStr);
    _wantManager.clearPeerWantlist(peerIdStr);
    _networkService.handlePeerDisconnected(peerIdStr);
  }

  void handleIncomingMessage(String peerId, pb.Message message) {
    print('// $nodeNameForLogging BSW_LOG: handleIncomingMessage from $peerId. HasWantlist: ${message.hasWantlist()}, PayloadCount: ${message.payload.length}, PresenceCount: ${message.blockPresences.length}');
    if (message.hasWantlist()) {
      _handleWantlist(peerId, message.wantlist);
    }

    for (final block in message.payload) {
      _handleReceivedBlock(peerId, block);
    }

    for (final presence in message.blockPresences) {
      _handleBlockPresence(peerId, presence);
    }

    if (message.hasPendingBytes()) {
      print('// $nodeNameForLogging BSW_LOG: Peer $peerId has ${message.pendingBytes} pending bytes for us.');
      // print('Peer $peerId has ${message.pendingBytes} pending bytes for us.'); // Redundant
    }
  }

  void _handleWantlist(String peerId, pb.Message_Wantlist wantlist) async {
    print('// $nodeNameForLogging BSW_LOG: _handleWantlist from $peerId. Entries: ${wantlist.entries.length}, Full: ${wantlist.full}');
    // print('Received wantlist from $peerId: ${wantlist.entries.length} entries, full: ${wantlist.full}'); // Redundant
    
    _activeBitswapPeers.add(peerId);
    print('// $nodeNameForLogging BSW_LOG: _handleWantlist: Added $peerId to active Bitswap peers.');

    if (wantlist.full) {
      print('// $nodeNameForLogging BSW_LOG: _handleWantlist: Received full wantlist from $peerId. Clearing existing wants for this peer.');
      // print('Received full wantlist from $peerId. Clearing existing wants for this peer.'); // Redundant
      _wantManager.clearPeerWantlist(peerId); 
    }

    for (final entry in wantlist.entries) {
      dart_cid.CID cid; 
      try {
        cid = dart_cid.CID.fromBytes(Uint8List.fromList(entry.block));
      } catch (e) {
        print('// $nodeNameForLogging BSW_LOG: _handleWantlist: Error parsing CID from wantlist entry from $peerId: $e. Entry: ${entry.toDebugString()}');
        // print('Error parsing CID from wantlist entry from $peerId: $e'); // Redundant
        continue;
      }

      print('// $nodeNameForLogging BSW_LOG: _handleWantlist: Processing entry for CID $cid from $peerId. Cancel: ${entry.cancel}, WantType: ${entry.wantType}, Priority: ${entry.priority}, SendDontHave: ${entry.sendDontHave}');
      if (entry.cancel) {
        print('// $nodeNameForLogging BSW_LOG: _handleWantlist: Peer $peerId cancelled want for CID $cid');
        // print('Peer $peerId cancelled want for CID $cid'); // Redundant
        _wantManager.removePeerWant(peerId, cid);
      } else {
        print('// $nodeNameForLogging BSW_LOG: _handleWantlist: Peer $peerId wants CID $cid, type: ${entry.wantType}, priority: ${entry.priority}, sendDontHave: ${entry.sendDontHave}');
        // print('Peer $peerId wants CID $cid, type: ${entry.wantType}, priority: ${entry.priority}, sendDontHave: ${entry.sendDontHave}'); // Redundant
        _wantManager.addPeerWant(
          peerId,
          cid,
          priority: entry.priority,
          wantType: entry.wantType,
          sendDontHave: entry.sendDontHave,
        );

        bool hasBlock = await _blockStore.has(cid);
        print('// $nodeNameForLogging BSW_LOG: _handleWantlist: Checked blockStore for $cid. HasBlock: $hasBlock');

        if (entry.wantType == pb.Message_Wantlist_WantType.Block) {
          if (hasBlock) {
            final blockData = await _blockStore.get(cid);
            print('// $nodeNameForLogging BSW_LOG: _handleWantlist: Retrieved block $cid from blockStore. Data length: ${blockData?.length}');
            if (blockData != null) {
              BytesBuilder prefixBuilder = BytesBuilder();
              prefixBuilder.add(dart_cid.encodeVarint(cid.version)); 
              prefixBuilder.add(dart_cid.encodeVarint(cid.codec));
              
              final decodedMh = dart_multihash.Multihash.decode(cid.multihash);
              prefixBuilder.add(dart_cid.encodeVarint(decodedMh.code));
              prefixBuilder.add(dart_cid.encodeVarint(decodedMh.digest.length));

              final blockProto = pb.Message_Block()
                ..prefix = prefixBuilder.toBytes()
                ..data = blockData;
              final responseMessage = pb.Message()..payload.add(blockProto);
              print('// $nodeNameForLogging BSW_LOG: _handleWantlist: Sending block $cid to $peerId');
              // print('Sending block $cid to $peerId'); // Redundant
              _networkService.sendMessage(peerId, responseMessage);
            }
          } else if (entry.sendDontHave) {
            final presenceProto = pb.Message_BlockPresence()
              ..cid = cid.toBytes()
              ..type = pb.Message_BlockPresenceType.DontHave;
            final responseMessage = pb.Message()..blockPresences.add(presenceProto);
            print('// $nodeNameForLogging BSW_LOG: _handleWantlist: Sending DONT_HAVE for $cid to $peerId (block not found, sendDontHave true)');
            // print('Sending DONT_HAVE for $cid to $peerId'); // Redundant
            _networkService.sendMessage(peerId, responseMessage);
          }
        } else if (entry.wantType == pb.Message_Wantlist_WantType.Have) {
          if (hasBlock) {
            final presenceProto = pb.Message_BlockPresence()
              ..cid = cid.toBytes()
              ..type = pb.Message_BlockPresenceType.Have;
            final responseMessage = pb.Message()..blockPresences.add(presenceProto);
            print('// $nodeNameForLogging BSW_LOG: _handleWantlist: Sending HAVE for $cid to $peerId');
            // print('Sending HAVE for $cid to $peerId'); // Redundant
            _networkService.sendMessage(peerId, responseMessage);
          } else if (entry.sendDontHave) {
            final presenceProto = pb.Message_BlockPresence()
              ..cid = cid.toBytes()
              ..type = pb.Message_BlockPresenceType.DontHave;
            final responseMessage = pb.Message()..blockPresences.add(presenceProto);
            print('// $nodeNameForLogging BSW_LOG: _handleWantlist: Sending DONT_HAVE for $cid to $peerId (HAVE type, block not found, sendDontHave true)');
            // print('Sending DONT_HAVE for $cid to $peerId'); // Redundant
            _networkService.sendMessage(peerId, responseMessage);
          }
        }
      }
    }
  }

  void _handleReceivedBlock(String peerId, pb.Message_Block blockProto) async {
    print('// $nodeNameForLogging BSW_LOG: _handleReceivedBlock from $peerId. Prefix hex: ${hex.encode(blockProto.prefix)}, Data length: ${blockProto.data.length}.');
    // print('Received block from $peerId. Prefix length: ${blockProto.prefix.length}, Data length: ${blockProto.data.length}.'); // Redundant

    final Uint8List prefixBytes = Uint8List.fromList(blockProto.prefix);
    final Uint8List dataBytes = Uint8List.fromList(blockProto.data);
    dart_cid.CID? receivedCid;

    try {
      int offset = 0;
      final versionEntry = dart_cid.decodeVarint(prefixBytes, offset);
      int cidVersion = versionEntry.key;
      offset += versionEntry.value;

      int cidCodec;
      int multihashCode;
      int digestLength;

      if (cidVersion == dart_cid.CID.V1) {
        final codecEntry = dart_cid.decodeVarint(prefixBytes, offset);
        cidCodec = codecEntry.key;
        offset += codecEntry.value;

        final mhCodeEntry = dart_cid.decodeVarint(prefixBytes, offset);
        multihashCode = mhCodeEntry.key;
        offset += mhCodeEntry.value;

        final mhLenEntry = dart_cid.decodeVarint(prefixBytes, offset);
        digestLength = mhLenEntry.key;
        offset += mhLenEntry.value;

        if (offset != prefixBytes.length) {
          print('// $nodeNameForLogging BSW_LOG: WARN: CIDv1 prefix bytes length mismatch after parsing. Expected $offset, got ${prefixBytes.length}');
        }
      } else if (cidVersion == dart_cid.CID.V0) {
        if (prefixBytes.length >= 2) {
            final mhCodeEntryV0 = dart_cid.decodeVarint(prefixBytes, offset);
            multihashCode = mhCodeEntryV0.key;
            offset += mhCodeEntryV0.value;

            final mhLenEntryV0 = dart_cid.decodeVarint(prefixBytes, offset);
            digestLength = mhLenEntryV0.key;
            offset += mhLenEntryV0.value;
            
            cidCodec = dart_cid.codecNameToCode['dag-pb']!;
            
            if (multihashCode != dart_cid.codecNameToCode['sha2-256'] || digestLength != 32) {
                 print('// $nodeNameForLogging BSW_LOG: WARN: Received CIDv0 prefix components (mhCode: $multihashCode, digestLen: $digestLength) do not conform to sha2-256 32-byte digest.');
                 return;
            }
        } else {
            print('// $nodeNameForLogging BSW_LOG: ERROR: Received CIDv0 prefix is too short. Prefix: ${hex.encode(prefixBytes)}');
            return;
        }
      } else {
        print('// $nodeNameForLogging BSW_LOG: ERROR: Unknown CID version $cidVersion parsed from prefix.');
        return;
      }

      Uint8List digest;
      String? multihashName = dart_cid.codecCodeToName[multihashCode];
      
      if (multihashName == null) {
        print('// $nodeNameForLogging BSW_LOG: ERROR: Unknown multihash code $multihashCode from prefix. Cannot determine hash algorithm.');
        return;
      }

      if (multihashName == 'sha2-256') {
        digest = Uint8List.fromList(crypto.sha256.convert(dataBytes).bytes);
      } else if (multihashName == 'sha2-512') {
        digest = Uint8List.fromList(crypto.sha512.convert(dataBytes).bytes);
      } else {
        print('// $nodeNameForLogging BSW_LOG: ERROR: Unsupported multihash type code $multihashCode ($multihashName) for hashing received block data.');
        return;
      }

      if (digest.length != digestLength) {
        print('// $nodeNameForLogging BSW_LOG: ERROR: Computed digest length ${digest.length} does not match digest length from prefix $digestLength.');
        return;
      }

      final fullMultihashBytes = dart_multihash.Multihash.encode(multihashName, digest).toBytes();
      receivedCid = dart_cid.CID(cidVersion, cidCodec, fullMultihashBytes);

      print('// $nodeNameForLogging BSW_LOG: _handleReceivedBlock: Successfully reconstructed CID: $receivedCid from $peerId.');
      // print('Successfully reconstructed CID: $receivedCid from $peerId.'); // Redundant

      if (_wantManager.isWantingLocally(receivedCid)) { 
        print('// $nodeNameForLogging BSW_LOG: _handleReceivedBlock: CID $receivedCid was wanted. Storing block.');
        await _blockStore.put(receivedCid, dataBytes);
        _wantManager.blockReceived(receivedCid, dataBytes); 
        print('// $nodeNameForLogging BSW_LOG: _handleReceivedBlock: Stored block $receivedCid from $peerId and notified WantManager.');
        // print('Stored block $receivedCid from $peerId'); // Redundant
        
        _sendCancelMessages(receivedCid);
      } else {
        print('// $nodeNameForLogging BSW_LOG: _handleReceivedBlock: Received unsolicited block $receivedCid from $peerId. Prefix: ${hex.encode(prefixBytes)}');
        // print('Received unsolicited block $receivedCid from $peerId. Prefix: ${hex.encode(prefixBytes)}'); // Redundant
      }
    } catch (e) {
      print('// $nodeNameForLogging BSW_LOG: _handleReceivedBlock: Error processing received block from $peerId: $e. Prefix: ${hex.encode(prefixBytes)}, DataLen: ${dataBytes.length}');
      // print('Error processing received block from $peerId: $e. Prefix: ${hex.encode(prefixBytes)}, DataLen: ${dataBytes.length}'); // Redundant
    }
  }

  void _handleBlockPresence(String peerId, pb.Message_BlockPresence presence) {
    try {
      final cid = dart_cid.CID.fromBytes(Uint8List.fromList(presence.cid)); 
      print('// $nodeNameForLogging BSW_LOG: _handleBlockPresence from $peerId for CID $cid: ${presence.type}');
      _activeBitswapPeers.add(peerId);
      // print('Received block presence from $peerId for CID $cid: ${presence.type}'); // Redundant
      if (presence.type == pb.Message_BlockPresenceType.Have) {
        _wantManager.receivedHave(peerId, cid);
      } else if (presence.type == pb.Message_BlockPresenceType.DontHave) {
        _wantManager.receivedDontHave(peerId, cid);
      }
    } catch (e) {
      print('// $nodeNameForLogging BSW_LOG: _handleBlockPresence: Error parsing CID from block presence from $peerId: $e');
      // print('Error parsing CID from block presence from $peerId: $e'); // Redundant
    }
  }

  Future<void> wantBlocks(List<dart_cid.CID> cidsToWant) async { 
    print('// $nodeNameForLogging BSW_LOG: wantBlocks called with ${cidsToWant.length} CIDs: ${cidsToWant.map((c) => c.toString()).join(', ')}');
    if (cidsToWant.isEmpty) return;

    final wantlistEntries = <pb.Message_Wantlist_Entry>[];
    for (final cid in cidsToWant) {
      final entry = _wantManager.addLocalWant(cid);
      if (entry != null) {
        wantlistEntries.add(pb.Message_Wantlist_Entry()
          ..block = cid.toBytes()
          ..wantType = pb.Message_Wantlist_WantType.Block
        );
      }
    }
    
    if (wantlistEntries.isEmpty) {
      print('// $nodeNameForLogging BSW_LOG: wantBlocks: No new wants to send after checking WantManager.');
      // print('No new wants to send.'); // Redundant
      return;
    }

    final wantlistProto = pb.Message_Wantlist()..entries.addAll(wantlistEntries);
    final message = pb.Message()..wantlist = wantlistProto;

    print('// $nodeNameForLogging BSW_LOG: wantBlocks: Wanting ${wantlistEntries.length} blocks. Broadcasting wantlist.');
    // print('Wanting ${wantlistEntries.length} blocks. Broadcasting wantlist.'); // Redundant
    
    final List<String> bitswapPeers = await _getBitswapPeers();
    print('// $nodeNameForLogging BSW_LOG: wantBlocks: Peers to send wantlist to: $bitswapPeers');

    if (bitswapPeers.isEmpty) {
      print('// $nodeNameForLogging BSW_LOG: wantBlocks: No known Bitswap peers to send wantlist to.');
      // print('No known Bitswap peers to send wantlist to.'); // Redundant
    }

    for (final peerId in bitswapPeers) {
      print('// $nodeNameForLogging BSW_LOG: wantBlocks: Sending wantlist to peer $peerId for ${wantlistEntries.length} CIDs.');
      // print('Sending wantlist to peer $peerId for ${wantlistEntries.length} CIDs.'); // Redundant
      _networkService.sendMessage(peerId, message);
      for (final entryProto in wantlistEntries) {
        try {
            final cidForRecording = dart_cid.CID.fromBytes(Uint8List.fromList(entryProto.block));
            _wantManager.recordWantSentToPeer(cidForRecording, peerId);
        } catch (e) {
            print('// $nodeNameForLogging BSW_LOG: wantBlocks: Error creating CID for recording sent want to $peerId: $e');
            // print('Error creating CID for recording sent want to $peerId: $e'); // Redundant
        }
      }
    }
  }

  Future<List<String>> _getBitswapPeers() async {
    print('// $nodeNameForLogging BSW_LOG: _getBitswapPeers: Starting peer discovery.');
    final Set<String> peers = {};
    peers.addAll(_activeBitswapPeers);
    print('// $nodeNameForLogging BSW_LOG: _getBitswapPeers: Initial active peers: $_activeBitswapPeers');

    try {
      final connections = _host.network.conns;
      print('// $nodeNameForLogging BSW_LOG: _getBitswapPeers: Found ${connections.length} connections from host.network.conns.');
      for (final conn in connections) {
        final remotePeerId = conn.remotePeer;
        print('// $nodeNameForLogging BSW_LOG: _getBitswapPeers: Checking connection to ${remotePeerId.toBase58()}');
        final peerInfo = await _host.peerStore.getPeer(remotePeerId);
        
        if (peerInfo != null && peerInfo.protocols.contains(_networkService.protocolId)) {
          peers.add(remotePeerId.toBase58());
          print('// $nodeNameForLogging BSW_LOG: _getBitswapPeers: Peer ${remotePeerId.toBase58()} supports Bitswap. Protocols: ${peerInfo.protocols}');
        } else {
          print('// $nodeNameForLogging BSW_LOG: _getBitswapPeers: Peer ${remotePeerId.toBase58()} does NOT support Bitswap or peerInfo is null. PeerInfo: ${peerInfo}, Protocols: ${peerInfo?.protocols}');
        }
      }
    } catch (e) {
      print('// $nodeNameForLogging BSW_LOG: _getBitswapPeers: Error querying connected peers for Bitswap support: $e. Falling back to known active peers.');
      // print('Error querying connected peers for Bitswap support: $e. Falling back to known active peers.'); // Redundant
    }

    if (peers.isEmpty && _activeBitswapPeers.isEmpty) {
      print('// $nodeNameForLogging BSW_LOG: _getBitswapPeers: WARN: Found no Bitswap-supporting peers after checking active and connected peers.');
      // print('WARN: _getBitswapPeers() found no Bitswap-supporting peers after checking active and connected peers.'); // Redundant
    }
    print('// $nodeNameForLogging BSW_LOG: _getBitswapPeers: Final list of Bitswap peers: ${peers.toList()}');
    return peers.toList();
  }

  void _sendCancelMessages(dart_cid.CID cid) { 
    print('// $nodeNameForLogging BSW_LOG: _sendCancelMessages for CID $cid');
    final cancelEntry = pb.Message_Wantlist_Entry()
      ..block = cid.toBytes()
      ..cancel = true;
    final cancelWantlist = pb.Message_Wantlist()..entries.add(cancelEntry);
    final cancelMessage = pb.Message()..wantlist = cancelWantlist;

    final Set<String> peersToCancel = _wantManager.getPeersSentWantForCid(cid);
    print('// $nodeNameForLogging BSW_LOG: _sendCancelMessages: Peers to send CANCEL to for $cid: $peersToCancel');

    if (peersToCancel.isEmpty) {
      print('// $nodeNameForLogging BSW_LOG: _sendCancelMessages: No peers to send CANCEL for CID $cid.');
      // print('No peers to send CANCEL for CID $cid.'); // Redundant
      return;
    }

    for (final peerId in peersToCancel) {
      print('// $nodeNameForLogging BSW_LOG: _sendCancelMessages: Sending CANCEL for CID $cid to peer $peerId');
      // print('Sending CANCEL for CID $cid to peer $peerId'); // Redundant
      _networkService.sendMessage(peerId, cancelMessage);
    }
  }

  Future<Uint8List?> getBlock(dart_cid.CID cid) async {
    print('// $nodeNameForLogging BSW_LOG: getBlock called for CID $cid');
    if (await _blockStore.has(cid)) {
      print('// $nodeNameForLogging BSW_LOG: getBlock: Block $cid found in local store.');
      // print('Block $cid found in local store.'); // Redundant
      return _blockStore.get(cid);
    }

    print('// $nodeNameForLogging BSW_LOG: getBlock: Block $cid not in local store. Adding to wantlist and attempting to fetch.');
    // print('Block $cid not in local store. Adding to wantlist and attempting to fetch.'); // Redundant
    await wantBlocks([cid]);
    print('// $nodeNameForLogging BSW_LOG: getBlock: Called wantBlocks for $cid. Now waiting on whenBlockReceived.');

    try {
      return await _wantManager.whenBlockReceived(cid).timeout(const Duration(seconds: 30));
    } on TimeoutException {
      print('// $nodeNameForLogging BSW_LOG: getBlock: Timeout waiting for block $cid');
      // print('Timeout waiting for block $cid'); // Redundant
      _wantManager.removeLocalWant(cid);
      return null;
    } catch (e) {
      print('// $nodeNameForLogging BSW_LOG: getBlock: Error while waiting for block $cid: $e');
      // print('Error while waiting for block $cid: $e'); // Redundant
      _wantManager.removeLocalWant(cid);
      return null;
    }
  }
}
