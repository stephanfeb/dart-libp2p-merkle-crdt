import 'dart:async';
import 'dart:convert'; // For utf8
import 'dart:typed_data'; // For Uint8List

import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p/core/peer/addr_info.dart' show AddrInfo;
import 'package:dart_libp2p/core/multiaddr.dart' show MultiAddr; // Corrected import
import 'package:dart_libp2p/core/peerstore.dart' show AddressTTL;
import 'package:dart_libp2p/core/network/rcmgr.dart' show NullResourceManager;
import 'package:dart_libp2p/p2p/host/eventbus/basic.dart' as p2p_event_bus;
import 'package:dart_libp2p/p2p/transport/connection_manager.dart' as p2p_conn_mgr;

import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:merkledag/merkledag.dart' show GSet, CRDTPayload, MerkleNode; // For GSet and CRDTPayload
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart' show PubSub, GossipSubRouter, PubSubMessage;
import 'package:dart_libp2p_kat_dht/dart_libp2p_kat_dht.dart' show IpfsDHT;

import 'package:dart_udx/udx.dart';
import 'package:logging/logging.dart';
import 'package:test/test.dart';

import '../real_net_stack.dart' show Libp2pNode, createLibp2pNode;

final _logger = Logger('MerkleCrdtReplicationTest');

// Configuration constants
const String crdtInstanceId = 'test-gset-replication';
const String dagSyncProtocolId = '/test-crdt-dag-sync/1.0.0';

// Payload factory for GSet<String>
// V = Set<String>, P = GSet<String>
GSet<String> gsetPayloadFactory(Uint8List payloadBytes) {
  // This is a simplified example, as GSet might not have a direct fromBytes.
  // In a real scenario, GSet would need a proper deserialization method.
  _logger.warning('gsetPayloadFactory: Deserialization from Uint8List is a placeholder. Returning empty GSet.');
  return GSet<String>(); // Placeholder: returns an empty GSet
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

  _NodeWithMerkleCRDT(); // Private constructor

  static Future<_NodeWithMerkleCRDT> create({String? userAgentPrefix, IpfsDHT? dht}) async {
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
    );
    helper.host = helper.nodeDetails.host;
    helper.peerId = helper.nodeDetails.peerId;

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
      dht: dht, // Pass DHT to DagFetcher if provided
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
      var connectedAtoB = false;
      if (nodeAHelper.host.addrs.isNotEmpty && nodeBHelper.host.addrs.isNotEmpty) {
        _logger.info('Attempting to connect Node A to Node B...');
        try {
          await nodeAHelper.host.connect(AddrInfo(nodeBHelper.peerId, nodeBHelper.host.addrs));
          _logger.info('Node A connection attempt to Node B successful.');
          connectedAtoB = true;
        } catch (e, s) {
          _logger.warning('Node A failed to connect to Node B: $e', e, s);
        }
      } else {
         _logger.warning('Skipping connect A to B due to one or both nodes having no listen addresses.');
      }
      
      if (!connectedAtoB) {
         _logger.severe('CRITICAL: Node A could not connect to Node B. Test will likely fail.');
         // Optionally, try connecting B to A as a fallback
         if (nodeAHelper.host.addrs.isNotEmpty && nodeBHelper.host.addrs.isNotEmpty) {
            _logger.info('Attempting to connect Node B to Node A as fallback...');
            try {
              await nodeBHelper.host.connect(AddrInfo(nodeAHelper.peerId, nodeAHelper.host.addrs));
              _logger.info('Node B connection attempt to Node A successful.');
            } catch (e,s) {
              _logger.warning('Node B failed to connect to Node A: $e', e, s);
            }
         }
      }
      
      // Allow time for GossipSub peers to connect and exchange control messages
      _logger.info('Waiting for GossipSub overlay to form...');
      await Future.delayed(Duration(seconds: 8)); // Adjust as needed
      _logger.info('Finished waiting for overlay formation.');
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
        final receivedStateB = await updateCompleter.future.timeout(Duration(seconds: 20), 
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
        
        final currentAMesh = (nodeAHelper.pubsub.router as GossipSubRouter).mesh[crdtInstanceId]?.map((p) => p.toBase58().substring(0,6)).toList() ?? [];
        final currentBMesh = (nodeBHelper.pubsub.router as GossipSubRouter).mesh[crdtInstanceId]?.map((p) => p.toBase58().substring(0,6)).toList() ?? [];
        _logger.info('Node A Mesh for topic $crdtInstanceId (at failure): $currentAMesh');
        _logger.info('Node B Mesh for topic $crdtInstanceId (at failure): $currentBMesh');
        fail('CRDT replication test failed: $e');
      } finally {
        await subscriptionB.cancel();
      }
    }, timeout: Timeout(Duration(seconds: 45)));
  });
}
