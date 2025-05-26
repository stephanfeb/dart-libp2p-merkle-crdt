import 'dart:async';

import 'package:dart_cid/dart_cid.dart';
import 'package:dart_libp2p/core/peer/peer_id.dart';
import 'package:dart_libp2p/core/peer/addr_info.dart';
import 'package:dart_libp2p/core/multiaddr.dart'; // Added for MultiAddr type
import 'package:dart_libp2p_kat_dht/src/dht/dht.dart';
import 'package:dart_libp2p_kat_dht/src/providers/provider_store.dart'; // For MemoryProviderStore
import 'package:logging/logging.dart';
import 'package:test/test.dart';

import '../real_net_stack.dart'; // For Libp2pNode and createLibp2pNode

// Imports for _NodeWithDHT helper dependencies
import 'package:dart_udx/udx.dart';
import 'package:dart_libp2p/core/network/rcmgr.dart'; // For ResourceManager and NoopResourceManager
import 'package:dart_libp2p/p2p/transport/connection_manager.dart' as p2p_conn_mgr;
import 'package:dart_libp2p/p2p/host/eventbus/basic.dart' as p2p_event_bus;
import 'package:dart_libp2p/core/host/host.dart';
import 'package:dart_libp2p/core/peerstore.dart'; // For Peerstore and AddressTTL

final Logger _logger = Logger("DHT Discovery Test");

// Helper class to encapsulate node and DHT creation for tests using real_net_stack
class _NodeWithDHT {
  late Libp2pNode nodeDetails;
  late IpfsDHT dht;
  late Host host;
  late PeerId peerId;
  late UDX _udxInstance; // Keep instance to close if necessary

  static Future<_NodeWithDHT> create() async {
    final helper = _NodeWithDHT();

    helper._udxInstance = UDX();
    // Using NullResourceManager as per user feedback
    final resourceManager = NullResourceManager(); 
    final connManager = p2p_conn_mgr.ConnectionManager();
    final eventBus = p2p_event_bus.BasicBus();

    helper.nodeDetails = await createLibp2pNode(
      udxInstance: helper._udxInstance,
      resourceManager: resourceManager,
      connManager: connManager,
      hostEventBus: eventBus,
    );
    helper.host = helper.nodeDetails.host;
    helper.peerId = helper.nodeDetails.peerId;

    final providerStore = MemoryProviderStore();
    helper.dht = IpfsDHT(
      host: helper.host,
      providerStore: providerStore,
      options: const DHTOptions(mode: DHTMode.server), // Force server mode for testing
    );
    // dht.start() will be called in the test's setUp
    return helper;
  }

  Future<void> stop() async {
    await dht.close();
    await host.close();
    // Consider if _udxInstance.dispose() is needed and available.
    // For now, assuming UDX instance might be shared or managed globally if it's a singleton.
  }
}

void main() {

  // Configure logging (ensure it's set up once, e.g. at the top or in a global setup)
  Logger.root.level = Level.ALL; // Adjust as needed, e.g. Level.ALL for more verbosity
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
    if (record.error != null) {
      print('ERROR: ${record.error}');
    }
    if (record.stackTrace != null) {
      print('STACKTRACE: ${record.stackTrace}');
    }
  });

  group('IpfsDHT Discovery Tests', () {
    late _NodeWithDHT nodeHelper1;
    late IpfsDHT dht1;
    late PeerId peerId1;

    setUp(() async {
      nodeHelper1 = await _NodeWithDHT.create();
      dht1 = nodeHelper1.dht;
      peerId1 = nodeHelper1.peerId;
      await dht1.start();
    });

    tearDown(() async {
      await nodeHelper1.stop();
    });

    test('advertise should add local peer as provider for the namespace CID', () async {
      const namespace = 'test-namespace';
      final namespaceCid = CID.fromString(namespace);

      // Advertise the namespace
      final ttl = await dht1.advertise(namespace);

      // Check that the returned TTL is the DHT's provideValidity
      expect(ttl, equals(dht1.options.provideValidity));

      // Check the provider manager to see if the local peer is registered for this CID
      final providers = await dht1.providerManager.getProviders(namespaceCid);
      
      expect(providers, isNotEmpty, reason: 'Providers list should not be empty after advertising.');
      expect(providers.length, equals(1), reason: 'Should be one provider.');
      expect(providers.first.id, equals(peerId1), reason: 'The provider should be the local peer.');
      
      bool hasMatchingAddr = false;
      if (nodeHelper1.host.addrs.isNotEmpty) {
        for (var hostAddr in nodeHelper1.host.addrs) {
          if (providers.first.addrs.any((pAddr) => pAddr.toString() == hostAddr.toString())) {
            hasMatchingAddr = true;
            break;
          }
        }
        expect(hasMatchingAddr, isTrue, reason: 'Provider addresses should contain at least one of the host listen addresses.');
      } else {
         expect(providers.first.addrs, isA<List<MultiAddr>>());
      }
    });

    test('findPeers should eventually find an advertised peer', () async {
      const namespace = 'another-service-namespace';

      final nodeHelper2 = await _NodeWithDHT.create();
      final dht2 = nodeHelper2.dht;
      await dht2.start();

      // Ensure hosts know about each other for connection
      // Using addrBook.addAddrs which is the correct way for Peerstore.
      // addAddrs returns void, so remove await.
      nodeHelper2.host.peerStore.addrBook.addAddrs(dht1.host().id, dht1.host().addrs, AddressTTL.permanentAddrTTL); // Corrected to PermanentAddrTTL
      nodeHelper1.host.peerStore.addrBook.addAddrs(dht2.host().id, dht2.host().addrs, AddressTTL.permanentAddrTTL); // Corrected to PermanentAddrTTL
      
      try {
        print('Connecting ${nodeHelper1.peerId.toBase58().substring(0,10)} and ${nodeHelper2.peerId.toBase58().substring(0,10)}');
        // Ensure there are addresses to connect to
        if (nodeHelper2.host.addrs.isNotEmpty) {
          await nodeHelper1.host.connect(AddrInfo(nodeHelper2.peerId, nodeHelper2.host.addrs));
          print('Connection from node1 to node2 attempted.');
        } else {
          print('Skipping connect from node1 to node2 as node2 has no listen addresses.');
        }
        if (nodeHelper1.host.addrs.isNotEmpty) {
          await nodeHelper2.host.connect(AddrInfo(nodeHelper1.peerId, nodeHelper1.host.addrs));
          print('Connection from node2 to node1 attempted.');
        } else {
          print('Skipping connect from node2 to node1 as node1 has no listen addresses.');
        }
      } catch (e) {
        print('Error connecting hosts: $e. This might affect peer discovery if routing tables are not populated.');
      }
      
      // Forcing dht2 to know dht1 for the sake of the test, to ensure lookup path
      if (dht1.host().addrs.isNotEmpty) {
         // queryPeer: true will make it try to query the peer, which helps populate the RT
        bool added = await dht2.routingTable.tryAddPeer(dht1.host().id, queryPeer: true); 
        print('Attempted to add ${dht1.host().id.toBase58().substring(0,6)} to dht2 routing table. Success: $added. RT size: ${await dht2.routingTable.size()}');
      } else {
        print('Skipping adding dht1 to dht2 routing table as dht1 has no listen addresses.');
      }

      // Forcing dht1 to know dht2 as well
      if (dht2.host().addrs.isNotEmpty) {
        bool added = await dht1.routingTable.tryAddPeer(dht2.host().id, queryPeer: true);
        print('Attempted to add ${dht2.host().id.toBase58().substring(0,6)} to dht1 routing table. Success: $added. RT size: ${await dht1.routingTable.size()}');
      } else {
        print('Skipping adding dht2 to dht1 routing table as dht2 has no listen addresses.');
      }
      
      // Log dht1's routing table before advertising
      final dht1PeersBeforeAdvertise = await dht1.routingTable.listPeers();
      _logger.info('dht1 (${dht1.host().id.toBase58().substring(0,6)}) routing table before advertise: ${dht1PeersBeforeAdvertise.map((p) => p.id.toBase58().substring(0,6)).toList()} (Size: ${dht1PeersBeforeAdvertise.length})');

      await dht1.advertise(namespace);
      print('DHT1 (${dht1.host().id.toBase58().substring(0,10)}) advertised $namespace');
      
      AddrInfo? foundPeerInfo;
      // Log dht2's routing table before findPeers
      final dht2PeersBeforeFind = await dht2.routingTable.listPeers();
      _logger.info('dht2 (${dht2.host().id.toBase58().substring(0,6)}) routing table before findPeers loop: ${dht2PeersBeforeFind.map((p) => p.id.toBase58().substring(0,6)).toList()} (Size: ${dht2PeersBeforeFind.length})');

      for (int i = 0; i < 15; i++) { // Increased retries and delay
        final stream = await dht2.findPeers(namespace);
        await for (final AddrInfo peerInfo in stream) {
          print('DHT2 (${dht2.host().id.toBase58().substring(0,10)}) found peer: ${peerInfo.id.toBase58().substring(0,10)} for $namespace (attempt ${i+1})');
          if (peerInfo.id == peerId1) {
            foundPeerInfo = peerInfo;
            break; 
          }
        }
        if (foundPeerInfo != null) break;
        print('Peer not found yet, delaying for 1 second before retry...');
        await Future.delayed(Duration(seconds: 1)); 
      }
      
      expect(foundPeerInfo, isNotNull, reason: 'DHT2 should find DHT1 as a provider for the namespace.');
      if (foundPeerInfo != null) {
        expect(foundPeerInfo.id, equals(peerId1));
        bool hasMatchingAddr = false;
        if (nodeHelper1.host.addrs.isNotEmpty) {
          for (var hostAddr in nodeHelper1.host.addrs) {
            if (foundPeerInfo.addrs.any((aiAddr) => aiAddr.toString() == hostAddr.toString())) {
              hasMatchingAddr = true;
              break;
            }
          }
          expect(hasMatchingAddr, isTrue, reason: 'Found peer addresses should include one of the original host listen addresses.');
        }
      }
      await nodeHelper2.stop();
    }, timeout: Timeout(Duration(seconds: 30))); // Increased timeout for network operations
  });
}
