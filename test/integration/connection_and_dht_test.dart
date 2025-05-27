import 'dart:async';
import 'dart:convert'; // For utf8
import 'dart:typed_data'; // For Uint8List

import 'package:dart_cid/dart_cid.dart';
import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p/core/peer/addr_info.dart' show AddrInfo;
import 'package:dart_libp2p/core/multiaddr.dart' show MultiAddr;
import 'package:dart_libp2p/core/peerstore.dart' show AddressTTL;
import 'package:dart_libp2p_kat_dht/src/providers/provider_store.dart' show MemoryProviderStore;
import 'package:dart_libp2p_kat_dht/src/dht/dht.dart' show IpfsDHT, DHTOptions, DHTMode;
import 'package:dart_multihash/dart_multihash.dart'; // Added for Multihash and DefaultHashAlg
import 'package:logging/logging.dart';
import 'package:test/test.dart';

// Imports from real_net_stack.dart for node creation
import '../real_net_stack.dart' show Libp2pNode, createLibp2pNode;
import 'package:dart_udx/udx.dart';
import 'package:dart_libp2p/core/network/rcmgr.dart' show NullResourceManager;
import 'package:dart_libp2p/p2p/transport/connection_manager.dart' as p2p_conn_mgr;
import 'package:dart_libp2p/p2p/host/eventbus/basic.dart' as p2p_event_bus;

final _logger = Logger('ConnectionAndDHTTest');

// Helper to transform listen addresses to dialable local addresses
List<MultiAddr> _transformToDialable(List<MultiAddr> addrs) {
  final List<MultiAddr> dialableAddrs = [];
  for (final addr in addrs) {
    String addrStr = addr.toString();
    if (addrStr.startsWith('/ip4/0.0.0.0/')) {
      try {
        dialableAddrs.add(MultiAddr(addrStr.replaceFirst('/ip4/0.0.0.0/', '/ip4/127.0.0.1/')));
      } catch (e) {
        _logger.warning("Failed to transform 0.0.0.0 address '$addrStr' to 127.0.0.1: $e");
        dialableAddrs.add(addr); // Fallback
      }
    } else {
      dialableAddrs.add(addr);
    }
  }
  return dialableAddrs;
}

class _TestNode {
  late Libp2pNode nodeDetails;
  late IpfsDHT dht;
  late Host host;
  late PeerId peerId;
  late UDX _udxInstance;
  String name;

  _TestNode(this.name);

  static Future<_TestNode> create(String name, {String? userAgentPrefix}) async {
    final node = _TestNode(name);
    node._udxInstance = UDX();
    final resourceManager = NullResourceManager();
    final connManager = p2p_conn_mgr.ConnectionManager();
    final eventBus = p2p_event_bus.BasicBus();

    node.nodeDetails = await createLibp2pNode(
      udxInstance: node._udxInstance,
      resourceManager: resourceManager,
      connManager: connManager,
      hostEventBus: eventBus,
      userAgentPrefix: userAgentPrefix ?? 'conn-dht-test-$name',
    );
    node.host = node.nodeDetails.host;
    node.peerId = node.nodeDetails.peerId;

    final providerStore = MemoryProviderStore();
    node.dht = IpfsDHT(
      host: node.host,
      providerStore: providerStore,
      options: const DHTOptions(mode: DHTMode.server),
    );
    return node;
  }

  Future<void> start() async {
    await dht.start();
    _logger.info('Node $name (${peerId.toBase58().substring(0,6)}) DHT started.');
  }

  Future<void> stop() async {
    _logger.info('Stopping Node $name (${peerId.toBase58().substring(0,6)})...');
    await dht.close();
    await host.close();
    // _udxInstance.dispose(); // If UDX needs explicit disposal
    _logger.info('Node $name (${peerId.toBase58().substring(0,6)}) stopped.');
  }
}

void main() {
  Logger.root.level = Level.INFO;
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
    if (record.error != null) print('ERROR: ${record.error}, ${record.stackTrace}');
  });

  group('Connection and DHT Test', () {
    _TestNode? nodeA;
    _TestNode? nodeB;

    setUpAll(() async {
      _logger.info('Setting up Connection and DHT test nodes...');
      nodeA = await _TestNode.create('A');
      nodeB = await _TestNode.create('B');

      await nodeA!.start();
      await nodeB!.start();

      _logger.info('Node A: ${nodeA!.peerId.toBase58()} @ ${nodeA!.host.addrs}');
      _logger.info('Node B: ${nodeB!.peerId.toBase58()} @ ${nodeB!.host.addrs}');

      // Add peer info to peerstore
      if (nodeB!.host.addrs.isNotEmpty) {
        nodeA!.host.peerStore.addrBook.addAddrs(nodeB!.peerId, nodeB!.host.addrs, AddressTTL.permanentAddrTTL);
      }
      if (nodeA!.host.addrs.isNotEmpty) {
        nodeB!.host.peerStore.addrBook.addAddrs(nodeA!.peerId, nodeA!.host.addrs, AddressTTL.permanentAddrTTL);
      }

      // Connect nodes
      var connected = false;
      if (nodeA!.host.addrs.isNotEmpty && nodeB!.host.addrs.isNotEmpty) {
        final dialableBAddrs = _transformToDialable(nodeB!.host.addrs.toList());
        _logger.info('Attempting to connect Node A to Node B using addresses: $dialableBAddrs');
        try {
          await nodeA!.host.connect(AddrInfo(nodeB!.peerId, dialableBAddrs));
          if (nodeA!.host.network.connsToPeer(nodeB!.peerId).isNotEmpty) {
            _logger.info('Node A connection to Node B successful.');
            connected = true;
          } else {
            _logger.warning('Node A connect call completed but no connection found via hostA.network.connsToPeer(peerIdB).');
          }
        } catch (e, s) {
          _logger.warning('Node A failed to connect to Node B: $e', e, s);
        }
      }
      
      if (!connected && nodeA!.host.addrs.isNotEmpty && nodeB!.host.addrs.isNotEmpty) {
         _logger.warning('Node A could not connect to Node B. Attempting fallback B to A.');
         final dialableAAddrs = _transformToDialable(nodeA!.host.addrs.toList());
         _logger.info('Attempting to connect Node B to Node A as fallback using addresses: $dialableAAddrs');
         try {
           await nodeB!.host.connect(AddrInfo(nodeA!.peerId, dialableAAddrs));
           if (nodeB!.host.network.connsToPeer(nodeA!.peerId).isNotEmpty) {
             _logger.info('Node B connection to Node A successful.');
             connected = true; 
           } else {
              _logger.warning('Node B connect call completed but no connection found via hostB.network.connsToPeer(peerIdA).');
           }
         } catch (e,s) {
           _logger.warning('Node B failed to connect to Node A: $e', e, s);
         }
      }

      if (!connected) {
        _logger.severe('CRITICAL: Nodes could not connect to each other. Further tests may fail.');
      } else {
        _logger.info('Connection attempt block completed. Connected flag is: $connected');
      }

      _logger.info('Node A connected peers PRE-DELAY: ${nodeA!.host.network.peers.map((p) => p.toBase58().substring(0,6))}');
      _logger.info('Node B connected peers PRE-DELAY: ${nodeB!.host.network.peers.map((p) => p.toBase58().substring(0,6))}');
      
      _logger.info('Waiting for Identify protocol and network to settle (10 seconds)...');
      await Future.delayed(Duration(seconds: 10));

      _logger.info('Node A connected peers POST-IDENTIFY-DELAY: ${nodeA!.host.network.peers.map((p) => p.toBase58().substring(0,6))}');
      _logger.info('Node B connected peers POST-IDENTIFY-DELAY: ${nodeB!.host.network.peers.map((p) => p.toBase58().substring(0,6))}');

      // DHT Priming and Bootstrapping
      _logger.info('Priming DHT routing tables and bootstrapping...');
      if (nodeB!.host.addrs.isNotEmpty) {
        bool addedToA = await nodeA!.dht.routingTable.tryAddPeer(nodeB!.peerId, queryPeer: true);
        _logger.info('Attempted to add Node B to Node A DHT. Added: $addedToA. Node A RT size: ${await nodeA!.dht.routingTable.size()}');
      }
      if (nodeA!.host.addrs.isNotEmpty) {
        bool addedToB = await nodeB!.dht.routingTable.tryAddPeer(nodeA!.peerId, queryPeer: true);
        _logger.info('Attempted to add Node A to Node B DHT. Added: $addedToB. Node B RT size: ${await nodeB!.dht.routingTable.size()}');
      }
      
      await Future.delayed(Duration(seconds: 3)); // Allow tryAddPeer operations

      try {
        _logger.info('Node A bootstrapping DHT...');
        await nodeA!.dht.bootstrap();
        _logger.info('Node A DHT bootstrap complete.');
      } catch (e,s) { _logger.warning('Node A DHT bootstrap failed: $e', e, s); }
      try {
        _logger.info('Node B bootstrapping DHT...');
        await nodeB!.dht.bootstrap();
        _logger.info('Node B DHT bootstrap complete.');
      } catch (e,s) { _logger.warning('Node B DHT bootstrap failed: $e', e, s); }

      _logger.info('Waiting for DHTs to stabilize (15 seconds)...');
      await Future.delayed(Duration(seconds: 15));
      _logger.info('Finished setUpAll.');
    });

    tearDownAll(() async {
      _logger.info('Tearing down Connection and DHT test nodes...');
      await nodeA?.stop();
      await nodeB?.stop();
      _logger.info('Test nodes stopped.');
    });

    test('Verify Peer Connection', () {
      expect(nodeA, isNotNull, reason: 'Node A helper should exist');
      expect(nodeB, isNotNull, reason: 'Node B helper should exist');
      if (nodeA == null || nodeB == null) return;

      final nodeAPeers = nodeA!.host.network.peers;
      final nodeBPeers = nodeB!.host.network.peers;

      _logger.info('Node A connected peers (in test): ${nodeAPeers.map((p) => p.toBase58().substring(0,6))}');
      _logger.info('Node B connected peers (in test): ${nodeBPeers.map((p) => p.toBase58().substring(0,6))}');

      expect(nodeAPeers.any((p) => p == nodeB!.peerId), isTrue, reason: 'Node A should be connected to Node B.');
      expect(nodeBPeers.any((p) => p == nodeA!.peerId), isTrue, reason: 'Node B should be connected to Node A.');
    }, timeout: Timeout(Duration(seconds: 5)));


    test('DHT findPeer', () async {
      expect(nodeA, isNotNull); expect(nodeB, isNotNull);
      if (nodeA == null || nodeB == null) return;

      _logger.info('Node A (${nodeA!.peerId.toBase58().substring(0,6)}) finding Node B (${nodeB!.peerId.toBase58().substring(0,6)}) via DHT...');
      AddrInfo? foundB = await nodeA!.dht.findPeer(nodeB!.peerId);
      _logger.info('Node A dht.findPeer(Node B) completed. Found: ${foundB != null}');
      expect(foundB, isNotNull, reason: 'Node A should find Node B via DHT.');
      if (foundB != null) {
        expect(foundB.id, equals(nodeB!.peerId));
        // Optionally check addresses if needed, though they might differ (e.g. 127.0.0.1 vs 0.0.0.0)
      }

      _logger.info('Node B (${nodeB!.peerId.toBase58().substring(0,6)}) finding Node A (${nodeA!.peerId.toBase58().substring(0,6)}) via DHT...');
      AddrInfo? foundA = await nodeB!.dht.findPeer(nodeA!.peerId);
      _logger.info('Node B dht.findPeer(Node A) completed. Found: ${foundA != null}');
      expect(foundA, isNotNull, reason: 'Node B should find Node A via DHT.');
      if (foundA != null) {
        expect(foundA.id, equals(nodeA!.peerId));
      }
    }, timeout: Timeout(Duration(seconds: 20)));

    test('DHT provide and findProviders', () async {
      expect(nodeA, isNotNull); expect(nodeB, isNotNull);
      if (nodeA == null || nodeB == null) return;

      // Using a fixed, known-valid CID for testing to bypass dynamic generation issues for now.
      // This is CIDv1, dag-pb, sha2-256 for the string "test-provide-find-content"
      final cidString = 'bafybeidvst3sc3gohtxzfdxfrkdlnsaeefwc4nkqquvkslczvfdjaf45cu';
      final cid = CID.fromString(cidString); // Corrected from parse to fromString
      
      _logger.info('Node A (${nodeA!.peerId.toBase58().substring(0,6)}) providing fixed CID: $cid');
      await nodeA!.dht.provide(cid, true); // Announce
      _logger.info('Node A provide call completed for CID: $cid');

      // Allow some time for provider record to propagate, though with direct connection and bootstrapped DHT, it should be quick.
      await Future.delayed(Duration(seconds: 5));

      _logger.info('Node B (${nodeB!.peerId.toBase58().substring(0,6)}) finding providers for CID: $cid');
      final List<AddrInfo> providers = [];
      final stream = nodeB!.dht.findProvidersAsync(cid, 1); // Find at least 1 provider

      final completer = Completer<void>();
      stream.listen(
        (providerAddrInfo) {
          _logger.info('Node B found provider: ${providerAddrInfo.id.toBase58().substring(0,6)} for CID $cid');
          providers.add(providerAddrInfo);
          if (providerAddrInfo.id == nodeA!.peerId) {
            if (!completer.isCompleted) completer.complete();
          }
        },
        onDone: () {
          _logger.info('Node B findProvidersAsync stream done for CID $cid.');
          if (!completer.isCompleted) completer.complete(); // Complete if not already (e.g. if target not found)
        },
        onError: (e, s) {
          _logger.severe('Node B findProvidersAsync stream error for CID $cid: $e', e, s);
          if (!completer.isCompleted) completer.completeError(e,s);
        }
      );
      
      await completer.future.timeout(Duration(seconds: 20), onTimeout: () {
         _logger.warning('Timeout waiting for Node B to find Node A as provider for CID $cid.');
         // Don't throw, let expects below handle it.
      });

      expect(providers.any((p) => p.id == nodeA!.peerId), isTrue, 
        reason: 'Node B should find Node A as a provider for CID $cid. Providers found: ${providers.map((p) => p.id.toBase58()).toList()}');

    }, timeout: Timeout(Duration(seconds: 45)));

  });
}
