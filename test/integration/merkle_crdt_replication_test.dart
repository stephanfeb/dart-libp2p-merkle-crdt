import 'dart:async';
import 'dart:convert'; // For utf8
import 'dart:typed_data'; // For Uint8List

import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p/core/peer/addr_info.dart' show AddrInfo;
import 'package:dart_libp2p/core/multiaddr.dart' show MultiAddr; // Corrected import
import 'package:dart_libp2p/core/network/conn.dart' show Connection; // Reverted to correct import for Connection
import 'package:dart_libp2p/p2p/host/basic/basic_host.dart' show BasicHost; // Added import for BasicHost
import 'package:dart_libp2p/core/peerstore.dart' show AddressTTL;
import 'package:dart_libp2p_kat_dht/src/providers/provider_store.dart' show MemoryProviderStore, ProviderStore; // Added for MemoryProviderStore
import 'package:dart_libp2p/core/network/rcmgr.dart' show NullResourceManager;
import 'package:dart_libp2p/p2p/host/eventbus/basic.dart' as p2p_event_bus;
import 'package:dart_libp2p/p2p/transport/connection_manager.dart' as p2p_conn_mgr;

import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:merkledag/merkledag.dart' show GSet, CRDTPayload, MerkleNode; // For GSet and CRDTPayload
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart' show PubSub, GossipSubRouter, PubSubMessage;
// Using a more specific import that is known to export DHTOptions and DHTMode from dht_discovery_test.dart
import 'package:dart_libp2p_kat_dht/src/dht/dht.dart' show IpfsDHT, DHTOptions, DHTMode; 

import 'package:dart_udx/udx.dart';
import 'package:logging/logging.dart';
import 'package:test/test.dart';

import '../real_net_stack.dart' show Libp2pNode, createLibp2pNode;

final _logger = Logger('MerkleCrdtReplicationTest');

// Configuration constants
const String crdtInstanceId = 'test-gset-replication';
const String dagSyncProtocolId = '/test-crdt-dag-sync/1.0.0';

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

// Payload factory for GSet<String>
// V = Set<String>, P = GSet<String>
GSet<String> gsetPayloadFactory(Uint8List payloadBytes) {
  try {
    final jsonString = utf8.decode(payloadBytes);
    final List<dynamic> stringList = jsonDecode(jsonString);
    final gset = GSet<String>();
    for (final item in stringList) {
      if (item is String) {
        gset.add(item);
      } else {
        // Log or handle non-string items if necessary
        _logger.warning('gsetPayloadFactory: Encountered non-string item "$item" during deserialization.');
      }
    }
    return gset;
  } catch (e, s) {
    _logger.severe('gsetPayloadFactory: Error deserializing payload.', e, s);
    // Depending on desired behavior, either rethrow or return an empty/default GSet
    // For testing, rethrowing might be better to catch serialization issues.
    // However, to match the original placeholder's "return empty on issue" style:
    return GSet<String>(); // Or rethrow;
  }
}

class _NodeWithMerkleCRDT {
  late Libp2pNode nodeDetails;
  late PubSub pubsub;
  late InMemoryBlockStore<GSet<String>, Set<String>> blockStore; // P, V
  late P2PStreamDagFetcher<GSet<String>, Set<String>> dagFetcher; // P, V
  late GossipSubAnnouncer announcer;
  late MerkleCrdtGossipManager<Set<String>, GSet<String>> manager; // V, P

  late Host host;
  late PeerId peerId;
  late UDX _udxInstance;
  IpfsDHT? dht; // Store the DHT instance

  _NodeWithMerkleCRDT(); // Private constructor

  // Removed IpfsDHT? dht from create signature, as it's sourced from nodeDetails
  static Future<_NodeWithMerkleCRDT> create({String? userAgentPrefix}) async {
    final helper = _NodeWithMerkleCRDT();

    helper._udxInstance = UDX();
    final resourceManager = NullResourceManager();
    final connManager = p2p_conn_mgr.ConnectionManager();
    final eventBus = p2p_event_bus.BasicBus();

    helper.nodeDetails = await createLibp2pNode(
      udxInstance: helper._udxInstance,
      resourceManager: resourceManager,
      connManager: connManager,
      hostEventBus: eventBus,
      userAgentPrefix: userAgentPrefix ?? 'merkle-crdt-test-node',
      // dht parameter removed from createLibp2pNode call, as it's not typically passed this way.
      // The dht instance is created in real_net_stack.dart or passed to P2PStreamDagFetcher directly.
      // real_net_stack.createLibp2pNode enables DHT by default.
    );
    helper.host = helper.nodeDetails.host;
    helper.peerId = helper.nodeDetails.peerId;
    // helper.dht = helper.nodeDetails.dht; // Removed, nodeDetails no longer has dht

    // Explicitly create DHT for this node helper
    final providerStore = MemoryProviderStore();
    helper.dht = IpfsDHT(
      host: helper.host,
      providerStore: providerStore, // Use the created provider store
      options: const DHTOptions(mode: DHTMode.server), // Or client, or auto. Server is good for tests.
    );
    // The DHT needs to be started. This could be done here or in startPubSubAndManager.
    // Let's add it to startPubSubAndManager.

    // Setup PubSub with GossipSubRouter
    final router = GossipSubRouter();
    helper.pubsub = PubSub(helper.host, router);

    // Setup MerkleCRDT components
    helper.blockStore = InMemoryBlockStore<GSet<String>, Set<String>>(); // P, V
    helper.dagFetcher = P2PStreamDagFetcher<GSet<String>, Set<String>>( // P, V
      helper.host,
      helper.blockStore,
      dagSyncProtocolId,
      gsetPayloadFactory,
      dht: helper.dht, // Pass the DHT instance obtained from nodeDetails
    );
    helper.announcer = GossipSubAnnouncer(
      helper.host,
      helper.pubsub,
      crdtInstanceId,
    );
    helper.manager = MerkleCrdtGossipManager<Set<String>, GSet<String>>( // V, P
      dagFetcher: helper.dagFetcher,
      announcer: helper.announcer,
      // Assuming MerkleCrdtGossipManager creates its own MerkleCRDT instance internally
    );

    return helper;
  }

  Future<void> startPubSubAndManager() async {
    await pubsub.start();
    if (dht != null) {
      await dht!.start(); // Start the DHT
      _logger.info('Peer ${peerId.toBase58().substring(0, 10)} DHT started.');
    }
    // MerkleCrdtGossipManager might not have an explicit start(), 
    // it becomes active when its components (especially announcer via pubsub) are active.
    // If it had manager.start(), it would be called here.
    _logger.info('Peer ${peerId.toBase58().substring(0, 10)} PubSub started.');
  }

  Future<void> stop() async {
    _logger.info('Stopping Peer ${peerId.toBase58().substring(0, 10)}...');
    try {
      await manager.close(); // Assuming manager has a close method
    } catch (e) {
      _logger.warning('Error closing manager for ${peerId.toBase58().substring(0,10)}: $e. May not have a close method or already closed.');
    }
    if (dht != null) {
      await dht!.close(); // Close the DHT
      _logger.info('Peer ${peerId.toBase58().substring(0, 10)} DHT closed.');
    }
    await pubsub.stop();
    await host.close();
    // _udxInstance.dispose(); // If UDX needs explicit disposal
    _logger.info('Peer ${peerId.toBase58().substring(0, 10)} stopped.');
  }
}

void main() {
  Logger.root.level = Level.INFO; // Adjust as needed
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
    if (record.error != null) print('ERROR: ${record.error}, ${record.stackTrace}');
  });

  group('MerkleCRDT Replication E2E Test', () {
    late _NodeWithMerkleCRDT nodeAHelper;
    late _NodeWithMerkleCRDT nodeBHelper;

    setUpAll(() async {
      _logger.info('Setting up MerkleCRDT replication test nodes...');
      nodeAHelper = await _NodeWithMerkleCRDT.create(userAgentPrefix: 'NodeA-CRDT');
      nodeBHelper = await _NodeWithMerkleCRDT.create(userAgentPrefix: 'NodeB-CRDT');

      await nodeAHelper.startPubSubAndManager();
      await nodeBHelper.startPubSubAndManager();

      _logger.info('Node A: ${nodeAHelper.peerId.toBase58()} @ ${nodeAHelper.host.addrs}');
      _logger.info('Node B: ${nodeBHelper.peerId.toBase58()} @ ${nodeBHelper.host.addrs}');

      // Add peer info to peerstore for successful connection
      if (nodeBHelper.host.addrs.isNotEmpty) {
        nodeAHelper.host.peerStore.addrBook.addAddrs(nodeBHelper.peerId, nodeBHelper.host.addrs, AddressTTL.permanentAddrTTL);
      } else {
        _logger.warning('Node B has no listen addresses.');
      }
      if (nodeAHelper.host.addrs.isNotEmpty) {
        nodeBHelper.host.peerStore.addrBook.addAddrs(nodeAHelper.peerId, nodeAHelper.host.addrs, AddressTTL.permanentAddrTTL);
      } else {
        _logger.warning('Node A has no listen addresses.');
      }

      // Connect nodes
      var connected = false;
      
      final hostA = nodeAHelper.host;
      final hostB = nodeBHelper.host;
      final peerIdA = nodeAHelper.peerId;
      final peerIdB = nodeBHelper.peerId;

      if (hostA.addrs.isNotEmpty && hostB.addrs.isNotEmpty) {
        final dialableBAddrs = _transformToDialable(hostB.addrs.toList());
        _logger.info('Attempting to connect Node A (${peerIdA.toBase58().substring(0,6)}) to Node B (${peerIdB.toBase58().substring(0,6)}) using addresses: $dialableBAddrs');
        try {
          await hostA.connect(AddrInfo(peerIdB, dialableBAddrs));
          // Check if connection exists using connsToPeer
          if (hostA.network.connsToPeer(peerIdB).isNotEmpty) {
            _logger.info('Node A connection to Node B successful.');
            connected = true;
          } else {
            _logger.warning('Node A connect call completed but no connection found via hostA.network.connsToPeer(peerIdB).');
          }
        } catch (e, s) {
          _logger.warning('Node A failed to connect to Node B: $e', e, s);
        }
      } else {
         _logger.warning('Skipping connect A to B due to one or both nodes having no listen addresses.');
      }
      
      if (!connected) {
         _logger.warning('Node A could not connect to Node B. Attempting fallback B to A.');
         if (hostA.addrs.isNotEmpty && hostB.addrs.isNotEmpty) {
            final dialableAAddrs = _transformToDialable(hostA.addrs.toList());
            _logger.info('Attempting to connect Node B (${peerIdB.toBase58().substring(0,6)}) to Node A (${peerIdA.toBase58().substring(0,6)}) as fallback using addresses: $dialableAAddrs');
            try {
              await hostB.connect(AddrInfo(peerIdA, dialableAAddrs));
              if (hostB.network.connsToPeer(peerIdA).isNotEmpty) {
                _logger.info('Node B connection to Node A successful.');
                connected = true; 
              } else {
                 _logger.warning('Node B connect call completed but no connection found via hostB.network.connsToPeer(peerIdA).');
              }
            } catch (e,s) {
              _logger.warning('Node B failed to connect to Node A: $e', e, s);
            }
         }
      }

      if (!connected) {
        _logger.severe('CRITICAL: Nodes could not connect to each other. Test will likely fail.');
      } else {
        _logger.info('Connection attempt block completed. Connected flag is: $connected');
        _logger.info('Node A connected peers PRE-DELAY: ${hostA.network.peers.map((p) => p.toBase58().substring(0,6))}');
        _logger.info('Node B connected peers PRE-DELAY: ${hostB.network.peers.map((p) => p.toBase58().substring(0,6))}');

        _logger.info('Connection established. Waiting a bit for identify and network to settle...');
        await Future.delayed(Duration(seconds: 8)); 

        _logger.info('Node A connected peers POST-DELAY: ${hostA.network.peers.map((p) => p.toBase58().substring(0,6))}');
        _logger.info('Node B connected peers POST-DELAY: ${hostB.network.peers.map((p) => p.toBase58().substring(0,6))}');

        _logger.info('Manually adding peers to DHT routing tables and bootstrapping DHTs...');
        if (nodeAHelper.dht != null && nodeBHelper.dht != null) {
          if (nodeBHelper.host.addrs.isNotEmpty) {
            _logger.info('Attempting to add Node B (${peerIdB.toBase58().substring(0,6)}) to Node A DHT routing table...');
            bool addedToA = await nodeAHelper.dht!.routingTable.tryAddPeer(nodeBHelper.peerId, queryPeer: true);
            _logger.info('Node B added to Node A DHT: $addedToA. Node A RT size: ${await nodeAHelper.dht!.routingTable.size()}');
          } else {
            _logger.warning('Node B has no addresses, cannot add to Node A DHT routing table.');
          }
          if (nodeAHelper.host.addrs.isNotEmpty) {
            _logger.info('Attempting to add Node A (${peerIdA.toBase58().substring(0,6)}) to Node B DHT routing table...');
            bool addedToB = await nodeBHelper.dht!.routingTable.tryAddPeer(nodeAHelper.peerId, queryPeer: true);
            _logger.info('Node A added to Node B DHT: $addedToB. Node B RT size: ${await nodeBHelper.dht!.routingTable.size()}');
          } else {
            _logger.warning('Node A has no addresses, cannot add to Node B DHT routing table.');
          }

          await Future.delayed(Duration(seconds: 3)); // Allow time for tryAddPeer operations

          try {
            _logger.info('Node A (${peerIdA.toBase58().substring(0,6)}) bootstrapping DHT...');
            await nodeAHelper.dht!.bootstrap();
            _logger.info('Node A DHT bootstrap complete.');
          } catch (e,s) {
            _logger.warning('Node A DHT bootstrap failed: $e', e, s);
          }
          try {
            _logger.info('Node B (${peerIdB.toBase58().substring(0,6)}) bootstrapping DHT...');
            await nodeBHelper.dht!.bootstrap();
            _logger.info('Node B DHT bootstrap complete.');
          } catch (e,s) {
            _logger.warning('Node B DHT bootstrap failed: $e', e, s);
          }
        } else {
          _logger.warning('One or both DHTs null, skipping manual add and bootstrap.');
        }
        
        _logger.info('DHT bootstrapping initiated/skipped. Waiting for DHT to warm up...');
        await Future.delayed(Duration(seconds: 10)); // Delay for DHT to warm up after bootstrap

        _logger.info('Attempting to ensure DHTs know each other via findPeer (PING calls removed as IpfsDHT.ping does not exist)...');
        if (nodeAHelper.dht != null && nodeBHelper.dht != null) {
          try {
            _logger.info('Node A (${peerIdA.toBase58().substring(0,6)}) dht.findPeer(Node B ${peerIdB.toBase58().substring(0,6)})');
            final foundB = await nodeAHelper.dht!.findPeer(peerIdB);
            _logger.info('Node A dht.findPeer(Node B) completed. Found: ${foundB != null}');
          } catch (e,s) {
            _logger.warning('Error during Node A dht.findPeer(Node B): $e', e, s);
          }
          try {
            _logger.info('Node B (${peerIdB.toBase58().substring(0,6)}) dht.findPeer(Node A ${peerIdA.toBase58().substring(0,6)})');
            final foundA = await nodeBHelper.dht!.findPeer(peerIdA);
            _logger.info('Node B dht.findPeer(Node A) completed. Found: ${foundA != null}');
          } catch (e,s) {
            _logger.warning('Error during Node B dht.findPeer(Node A): $e', e, s);
          }
        } else {
          _logger.warning('One or both DHT instances are null, skipping findPeer calls.');
        }
        _logger.info('DHT findPeer calls initiated/skipped.');
      }
      
      // Allow time for GossipSub peers to connect and exchange control messages
      _logger.info('Waiting for GossipSub overlay to form and final DHT stabilization...');
      await Future.delayed(Duration(seconds: 15)); // Original delay for overall stabilization
      _logger.info('Finished waiting for overlay formation and DHT stabilization.');
    });

    tearDownAll(() async {
      _logger.info('Tearing down MerkleCRDT replication test nodes...');
      await nodeAHelper.stop();
      await nodeBHelper.stop();
      _logger.info('Test nodes stopped.');
    });

    test('CRDT state replicates from Node A to Node B', () async {
      final managerA = nodeAHelper.manager;
      final managerB = nodeBHelper.manager;
      final peerIdA = nodeAHelper.peerId;
      final peerIdB = nodeBHelper.peerId;

      final uniqueValue = 'test_value_${DateTime.now().millisecondsSinceEpoch}';
      final updateCompleter = Completer<Set<String>?>();

      final subscriptionB = managerB.onStateChanged.listen((Set<String>? newState) {
        _logger.info('Node B (${peerIdB.toBase58().substring(0,6)}) received state update: $newState');
        if (newState != null && newState.contains(uniqueValue)) {
          if (!updateCompleter.isCompleted) {
            updateCompleter.complete(newState);
          }
        }
      });

      _logger.info('Node A (${peerIdA.toBase58().substring(0,6)}) applying local update with value: $uniqueValue');
      final gsetUpdate = GSet<String>();
      gsetUpdate.add(uniqueValue);
      await managerA.applyLocalUpdate(gsetUpdate);
      _logger.info('Node A (${peerIdA.toBase58().substring(0,6)}) local update applied. Current state: ${await managerA.getState()}');

      try {
        _logger.info('Waiting for Node B (${peerIdB.toBase58().substring(0,6)}) to receive the update...');
        final receivedStateB = await updateCompleter.future.timeout(Duration(seconds: 30), // Increased timeout 
          onTimeout: () {
            _logger.warning('Timeout waiting for Node B to receive update.');
            return null;
          });

        expect(receivedStateB, isNotNull, reason: 'Node B should have received a state update.');
        expect(receivedStateB, contains(uniqueValue), reason: 'Node B\'s state should contain the value from Node A.');
        
        // Also check managerB.getState() directly
        final directStateB = await managerB.getState();
        expect(directStateB, isNotNull, reason: 'Node B direct state check: should not be null.');
        expect(directStateB, contains(uniqueValue), reason: 'Node B\'s direct state should contain the value from Node A.');

        _logger.info('Test PASSED: CRDT state successfully replicated from Node A to Node B.');

      } catch (e, s) {
        _logger.severe('Test FAILED: Error during replication or assertion.', e, s);
        _logger.info('Diagnostic Info:');
        _logger.info('Node A (${peerIdA.toBase58().substring(0,6)}) connected peers: ${nodeAHelper.host.network.peers.map((p) => p.toBase58().substring(0,6))}');
        _logger.info('Node B (${peerIdB.toBase58().substring(0,6)}) connected peers: ${nodeBHelper.host.network.peers.map((p) => p.toBase58().substring(0,6))}');
        
        final fullTopic = 'merkledag-crdt/heads/1.0.0/$crdtInstanceId';
        final currentAMesh = (nodeAHelper.pubsub.router as GossipSubRouter).mesh[fullTopic]?.map((p) => p.toBase58().substring(0,6)).toList() ?? [];
        final currentBMesh = (nodeBHelper.pubsub.router as GossipSubRouter).mesh[fullTopic]?.map((p) => p.toBase58().substring(0,6)).toList() ?? [];
        _logger.info('Node A Mesh for topic $fullTopic (at failure): $currentAMesh');
        _logger.info('Node B Mesh for topic $fullTopic (at failure): $currentBMesh');
        fail('CRDT replication test failed: $e');
      } finally {
        await subscriptionB.cancel();
      }
    }, timeout: Timeout(Duration(seconds: 60))); // Increased test timeout
  });
}
