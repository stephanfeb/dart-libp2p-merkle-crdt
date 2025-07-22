import 'dart:async';
import 'dart:convert'; // For utf8
import 'dart:typed_data'; // For Uint8List

import 'package:crypto/crypto.dart' as crypto;
import 'package:dart_cid/dart_cid.dart';
import 'package:dart_libp2p/core/host/host.dart' show Host;
import 'package:dart_libp2p/core/peer/peer_id.dart' show PeerId;
import 'package:dart_libp2p/core/peer/addr_info.dart' show AddrInfo;
import 'package:dart_libp2p/core/multiaddr.dart' show MultiAddr;
import 'package:dart_libp2p/core/network/conn.dart' show Connection;
import 'package:dart_libp2p/p2p/host/basic/basic_host.dart' show BasicHost;
import 'package:dart_libp2p/core/peerstore.dart' show AddressTTL;
import 'package:dart_libp2p_kat_dht/dart_libp2p_kat_dht.dart';
import 'package:dart_libp2p_kat_dht/src/providers/provider_store.dart' show MemoryProviderStore, ProviderStore;
import 'package:dart_libp2p/core/network/rcmgr.dart' show NullResourceManager;
import 'package:dart_libp2p/p2p/host/eventbus/basic.dart' as p2p_event_bus;
import 'package:dart_libp2p/p2p/transport/connection_manager.dart' as p2p_conn_mgr;

import 'package:dart_libp2p_merkle_crdt/dart_libp2p_merkle_crdt.dart';
import 'package:dart_multihash/dart_multihash.dart';
import 'package:merkledag/merkledag.dart' show GSet, CRDTPayload, MerkleNode;
import 'package:dart_libp2p_pubsub/dart_libp2p_pubsub.dart' show PubSub, GossipSubRouter, PubSubMessage;

import 'package:dart_udx/udx.dart';
import 'package:logging/logging.dart';
import 'package:test/test.dart';

import '../real_net_stack.dart' show Libp2pNode, createLibp2pNode;

final _logger = Logger('SyncFailureScenariosTest');

// Configuration constants
const String crdtInstanceId = 'test-sync-failure-scenarios';
const String dagSyncProtocolId = '/test-sync-failure-dag-sync/1.0.0';

// Wrapper for GSet to control serialization
class TestGSet implements CRDTPayload<Set<String>> {
  final GSet<String> _gset;

  TestGSet() : _gset = GSet<String>();
  TestGSet.fromSet(Set<String> initialSet) : _gset = GSet<String>() {
    for (final item in initialSet) {
      _gset.add(item);
    }
  }

  @override
  Set<String> get value => _gset.value;

  void add(String item) => _gset.add(item);

  @override
  bool equals(covariant CRDTPayload<Set<String>> other) {
    if (other is! TestGSet) return false;
    if (value.length != other.value.length) return false;
    for (final item in value) {
      if (!other.value.contains(item)) return false;
    }
    return true;
  }

  @override
  TestGSet merge(covariant CRDTPayload<Set<String>> other) {
    if (other is! TestGSet) {
      _logger.warning('TestGSet.merge: other is not TestGSet, returning current.');
      return this;
    }
    final newSetInitialValues = GSet<String>();
    for (final item in value) {
      newSetInitialValues.add(item);
    }
    for (final item in other.value) {
      newSetInitialValues.add(item);
    }
    return TestGSet.fromSet(newSetInitialValues.value);
  }

  @override
  Uint8List toCanonicalBytes() {
    final jsonString = jsonEncode(value.toList());
    return utf8.encode(jsonString);
  }

  @override
  String toString() => 'TestGSet(${_gset.value})';
}

// Payload factory for TestGSet
TestGSet testGSetFactory(Uint8List payloadBytes) {
  final jsonString = utf8.decode(payloadBytes);
  final List<dynamic> items = jsonDecode(jsonString);
  final stringItems = items.cast<String>();
  return TestGSet.fromSet(stringItems.toSet());
}

// Test node wrapper with enhanced monitoring capabilities
class _SyncFailureTestNode {
  late Libp2pNode nodeDetails;
  late PubSub pubsub;
  late InMemoryBlockStore<TestGSet, Set<String>> blockStore;
  late P2PStreamDagFetcher<TestGSet, Set<String>> dagFetcher;
  late GossipSubAnnouncer announcer;
  late MerkleCrdtGossipManager<Set<String>, TestGSet> manager;

  late Host host;
  late PeerId peerId;
  late UDX _udxInstance;
  IpfsDHTv2? dht;

  // Enhanced monitoring
  final List<String> receivedAnnouncements = [];
  final List<String> sentAnnouncements = [];
  final List<String> failedSyncAttempts = [];
  final Map<String, DateTime> syncAttemptTimes = {};

  _SyncFailureTestNode();

  static Future<_SyncFailureTestNode> create({String? userAgentPrefix}) async {
    final helper = _SyncFailureTestNode();

    helper._udxInstance = UDX();
    final resourceManager = NullResourceManager();
    final connManager = p2p_conn_mgr.ConnectionManager();
    final eventBus = p2p_event_bus.BasicBus();

    helper.nodeDetails = await createLibp2pNode(
      udxInstance: helper._udxInstance,
      resourceManager: resourceManager,
      connManager: connManager,
      hostEventBus: eventBus,
      userAgentPrefix: userAgentPrefix ?? 'sync-failure-test-node',
    );
    helper.host = helper.nodeDetails.host;
    helper.peerId = helper.nodeDetails.peerId;

    final providerStore = MemoryProviderStore();
    helper.dht = IpfsDHTv2(
      host: helper.host,
      providerStore: providerStore,
      options: const DHTOptions(mode: DHTMode.server),
    );

    helper.blockStore = InMemoryBlockStore<TestGSet, Set<String>>();
    helper.dagFetcher = P2PStreamDagFetcher<TestGSet, Set<String>>(
      helper.host,
      helper.blockStore,
      dagSyncProtocolId,
      testGSetFactory,
      dht: helper.dht,
    );

    final router = GossipSubRouter();
    helper.pubsub = PubSub(helper.host, router);

    helper.announcer = GossipSubAnnouncer(helper.host, helper.pubsub, crdtInstanceId);
    helper.manager = MerkleCrdtGossipManager<Set<String>, TestGSet>(
      dagFetcher: helper.dagFetcher,
      announcer: helper.announcer,
      loggerName: '${userAgentPrefix ?? 'TestNode'}-Manager',
    );

    return helper;
  }

  Future<void> startPubSubAndManager() async {
    await pubsub.start();
    await dht!.start();
    
    // Set up monitoring for announcements
    _setupAnnouncementMonitoring();
  }

  void _setupAnnouncementMonitoring() {
    // Monitor incoming announcements
    announcer.subscribe().listen((cid) {
      receivedAnnouncements.add(cid);
      _logger.info('Node ${peerId.toBase58().substring(0, 6)}: Received announcement for CID: $cid');
    });

    // Monitor outgoing announcements by intercepting the broadcast method
    // This would require modifying the announcer or using a wrapper
  }

  /// Artificially create a state where this node is missing intermediate updates
  Future<void> simulatePartialSync(List<String> missingUpdates) async {
    _logger.info('Node ${peerId.toBase58().substring(0, 6)}: Simulating partial sync by removing ${missingUpdates.length} updates');
    
    // This simulates the scenario where a node missed some updates
    // We'll track what updates this node "should have missed"
    for (final update in missingUpdates) {
      failedSyncAttempts.add('SIMULATED_MISS:$update');
    }
  }

  /// Force disconnect from other nodes to simulate network partition
  Future<void> simulateNetworkPartition() async {
    _logger.info('Node ${peerId.toBase58().substring(0, 6)}: Simulating network partition');
    
    // Close all connections
    final peers = host.network.peers;
    for (final peerId in peers) {
      final connections = host.network.connsToPeer(peerId);
      for (final conn in connections) {
        try {
          await conn.close();
        } catch (e) {
          // Ignore close errors
        }
      }
    }
  }

  /// Reconnect after partition
  Future<void> reconnectAfterPartition(List<_SyncFailureTestNode> otherNodes) async {
    _logger.info('Node ${peerId.toBase58().substring(0, 6)}: Reconnecting after partition to ${otherNodes.length} nodes');
    
    for (final otherNode in otherNodes) {
      if (otherNode.peerId == peerId) continue;
      
      try {
        if (otherNode.host.addrs.isNotEmpty) {
          host.peerStore.addrBook.addAddrs(otherNode.peerId, otherNode.host.addrs, AddressTTL.permanentAddrTTL);
          await host.connect(AddrInfo(otherNode.peerId, otherNode.host.addrs.toList()));
        }
      } catch (e) {
        _logger.warning('Failed to reconnect to ${otherNode.peerId.toBase58().substring(0, 6)}: $e');
      }
    }
    
    // Wait for connections to stabilize
    await Future.delayed(Duration(seconds: 2));
  }

  Future<void> stop() async {
    _logger.info('Stopping sync failure test node ${peerId.toBase58().substring(0, 6)}...');
    await manager.close();
    await pubsub.stop();
    await dht!.close();
    await host.close();
    // UDX doesn't have a close method, it will be cleaned up automatically
    _logger.info('Sync failure test node ${peerId.toBase58().substring(0, 6)} stopped.');
  }

  /// Get current CRDT state for debugging
  Future<Set<String>?> getCurrentState() async {
    return await manager.getState();
  }

  /// Apply a local update and track it
  Future<CID> applyUpdate(String value) async {
    _logger.info('Node ${peerId.toBase58().substring(0, 6)}: Applying update: $value');
    
    final gset = TestGSet();
    gset.add(value);
    
    final cid = await manager.applyLocalUpdate(gset);
    sentAnnouncements.add(cid.toString());
    
    return cid;
  }
}

void main() {
  Logger.root.level = Level.INFO;
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${record.time}: ${record.loggerName}: ${record.message}');
    if (record.error != null) print('ERROR: ${record.error}, STACK: ${record.stackTrace}');
  });

  group('CRDT Sync Failure Scenarios', () {
    late _SyncFailureTestNode nodeA;
    late _SyncFailureTestNode nodeB;
    late _SyncFailureTestNode nodeC;

    setUpAll(() async {
      _logger.info('Setting up sync failure scenario test nodes...');
      nodeA = await _SyncFailureTestNode.create(userAgentPrefix: 'NodeA-SyncTest');
      nodeB = await _SyncFailureTestNode.create(userAgentPrefix: 'NodeB-SyncTest');
      nodeC = await _SyncFailureTestNode.create(userAgentPrefix: 'NodeC-SyncTest');

      await nodeA.startPubSubAndManager();
      await nodeB.startPubSubAndManager();
      await nodeC.startPubSubAndManager();

      _logger.info('Node A: ${nodeA.peerId.toBase58().substring(0, 6)} @ ${nodeA.host.addrs}');
      _logger.info('Node B: ${nodeB.peerId.toBase58().substring(0, 6)} @ ${nodeB.host.addrs}');
      _logger.info('Node C: ${nodeC.peerId.toBase58().substring(0, 6)} @ ${nodeC.host.addrs}');

      // Connect all nodes initially
      await _connectNodes([nodeA, nodeB, nodeC]);
      
      _logger.info('All nodes connected and ready for sync failure tests');
    });

    tearDownAll(() async {
      _logger.info('Tearing down sync failure scenario test nodes...');
      await nodeA.stop();
      await nodeB.stop();
      await nodeC.stop();
      _logger.info('Sync failure test nodes stopped.');
    });

    test('Scenario 0: Simple demonstration of sync failure', () async {
      _logger.info('=== SCENARIO 0: Simple sync failure demonstration ===');
      
      // Start with a clean slate
      await _resetNodes([nodeA, nodeB, nodeC]);
      
      // All nodes start connected and synced
      final baseUpdate = 'base-${DateTime.now().millisecondsSinceEpoch}';
      await nodeA.applyUpdate(baseUpdate);
      await Future.delayed(Duration(seconds: 3));
      
      // Verify sync
      final initialStateA = await nodeA.getCurrentState();
      final initialStateB = await nodeB.getCurrentState();
      _logger.info('Initial states - A: $initialStateA, B: $initialStateB');
      
      // Partition Node B
      await nodeB.simulateNetworkPartition();
      
      // A makes an update while B is offline
      final missedUpdate = 'missed-${DateTime.now().millisecondsSinceEpoch}';
      await nodeA.applyUpdate(missedUpdate);
      await Future.delayed(Duration(seconds: 2));
      
      // Reconnect B
      await nodeB.reconnectAfterPartition([nodeA, nodeC]);
      await Future.delayed(Duration(seconds: 2));
      
      // A makes another update - B should receive announcement but fail to sync
      final newUpdate = 'new-${DateTime.now().millisecondsSinceEpoch}';
      await nodeA.applyUpdate(newUpdate);
      await Future.delayed(Duration(seconds: 5));
      
      // Check results
      final finalStateA = await nodeA.getCurrentState();
      final finalStateB = await nodeB.getCurrentState();
      
      _logger.info('Final states - A: $finalStateA, B: $finalStateB');
      
      // A should have all updates
      expect(finalStateA, contains(baseUpdate));
      expect(finalStateA, contains(missedUpdate));
      expect(finalStateA, contains(newUpdate));
      
      // B should be missing both the missed update AND the new update
      expect(finalStateB, contains(baseUpdate));
      expect(finalStateB, isNot(contains(missedUpdate)));
      expect(finalStateB, isNot(contains(newUpdate)));
      
      _logger.info('✅ SYNC FAILURE DEMONSTRATED: Node B cannot sync due to missing intermediate state');
    }, timeout: Timeout(Duration(seconds: 30)));

    test('Scenario 1: Node misses intermediate updates and ignores later announcements', () async {
      _logger.info('=== SCENARIO 1: Testing missed intermediate updates ===');
      
      // Initial state - all nodes start with the same base
      final baseUpdate = 'base-${DateTime.now().millisecondsSinceEpoch}';
      await nodeA.applyUpdate(baseUpdate);
      
      // Wait for replication
      await Future.delayed(Duration(seconds: 3));
      
      // Verify all nodes have the base update
      final stateA1 = await nodeA.getCurrentState();
      final stateB1 = await nodeB.getCurrentState();
      final stateC1 = await nodeC.getCurrentState();
      
      _logger.info('After base update - A: $stateA1, B: $stateB1, C: $stateC1');
      expect(stateA1, contains(baseUpdate));
      expect(stateB1, contains(baseUpdate));
      expect(stateC1, contains(baseUpdate));
      
      // Simulate Node B going offline/partitioned
      _logger.info('Simulating Node B partition...');
      await nodeB.simulateNetworkPartition();
      await Future.delayed(Duration(seconds: 2));
      
      // Node A makes several updates while B is offline
      final missedUpdates = <String>[];
      for (int i = 1; i <= 3; i++) {
        final update = 'missed-update-$i-${DateTime.now().millisecondsSinceEpoch}';
        missedUpdates.add(update);
        await nodeA.applyUpdate(update);
        await Future.delayed(Duration(seconds: 1));
      }
      
      // Verify Node C got the updates but B didn't
      final stateA2 = await nodeA.getCurrentState();
      final stateC2 = await nodeC.getCurrentState();
      
      _logger.info('After missed updates - A: $stateA2, C: $stateC2');
      for (final update in missedUpdates) {
        expect(stateA2, contains(update), reason: 'Node A should have update: $update');
        expect(stateC2, contains(update), reason: 'Node C should have update: $update');
      }
      
      // Node B comes back online
      _logger.info('Reconnecting Node B after partition...');
      await nodeB.reconnectAfterPartition([nodeA, nodeC]);
      await Future.delayed(Duration(seconds: 2));
      
      // Node A makes a new update - this should be announced to B
      final newUpdate = 'post-partition-update-${DateTime.now().millisecondsSinceEpoch}';
      await nodeA.applyUpdate(newUpdate);
      
      // Wait longer for network stabilization and potential sync attempts
      _logger.info('Waiting for sync attempts...');
      await Future.delayed(Duration(seconds: 10));
      
      final stateB2 = await nodeB.getCurrentState();
      _logger.info('Node B state after reconnection: $stateB2');
      _logger.info('Node B received announcements: ${nodeB.receivedAnnouncements}');
      
      // The EXPECTED FAILURE - Node B should be missing the new update
      // This demonstrates the core synchronization problem
      expect(stateB2, isNot(contains(newUpdate)), 
        reason: 'EXPECTED FAILURE: Node B should miss the new update due to missing intermediate states');
      
      // B should also be missing the intermediate updates
      for (final update in missedUpdates) {
        expect(stateB2, isNot(contains(update)), 
          reason: 'EXPECTED: Node B should be missing intermediate update: $update');
      }
      
      // However, B should still have the base update
      expect(stateB2, contains(baseUpdate), 
        reason: 'Node B should still have the base update from before partition');
      
      _logger.info('✅ Scenario 1 SUCCESSFULLY demonstrates the sync failure:');
      _logger.info('   - Node B missed ${missedUpdates.length} intermediate updates during partition');
      _logger.info('   - Node B cannot process new announcements due to missing DAG dependencies');
      _logger.info('   - Current system has NO recovery mechanism for out-of-sync scenarios');
    }, timeout: Timeout(Duration(seconds: 60)));

    test('Scenario 2: Multiple nodes with different missing states create divergent views', () async {
      _logger.info('=== SCENARIO 2: Testing divergent views from different missing states ===');
      
      // Reset all nodes to clean state
      await _resetNodes([nodeA, nodeB, nodeC]);
      
      // Create a common base
      final baseUpdate = 'common-base-${DateTime.now().millisecondsSinceEpoch}';
      await nodeA.applyUpdate(baseUpdate);
      await Future.delayed(Duration(seconds: 2));
      
      // Partition the network into A-B and A-C (A is the bridge)
      _logger.info('Creating partial network partition...');
      
      // Disconnect B and C from each other, but keep A connected to both
      final connectionsBC = nodeB.host.network.connsToPeer(nodeC.peerId);
      for (final conn in connectionsBC) {
        await conn.close();
      }
      
      // Node A makes updates that only B sees
      final updatesOnlyB = <String>[];
      for (int i = 1; i <= 2; i++) {
        final update = 'b-only-update-$i-${DateTime.now().millisecondsSinceEpoch}';
        updatesOnlyB.add(update);
        await nodeA.applyUpdate(update);
        await Future.delayed(Duration(seconds: 1));
      }
      
      // Simulate B going offline
      await nodeB.simulateNetworkPartition();
      await Future.delayed(Duration(seconds: 1));
      
      // Node A makes updates that only C sees
      final updatesOnlyC = <String>[];
      for (int i = 1; i <= 2; i++) {
        final update = 'c-only-update-$i-${DateTime.now().millisecondsSinceEpoch}';
        updatesOnlyC.add(update);
        await nodeA.applyUpdate(update);
        await Future.delayed(Duration(seconds: 1));
      }
      
      // Reconnect B
      await nodeB.reconnectAfterPartition([nodeA]);
      await Future.delayed(Duration(seconds: 2));
      
      // Now make a final update that should go to both B and C
      final finalUpdate = 'final-update-${DateTime.now().millisecondsSinceEpoch}';
      await nodeA.applyUpdate(finalUpdate);
      await Future.delayed(Duration(seconds: 3));
      
      // Check the states
      final stateA = await nodeA.getCurrentState();
      final stateB = await nodeB.getCurrentState();
      final stateC = await nodeC.getCurrentState();
      
      _logger.info('Final states - A: $stateA');
      _logger.info('Final states - B: $stateB');
      _logger.info('Final states - C: $stateC');
      
      // A should have everything
      expect(stateA, contains(baseUpdate));
      for (final update in updatesOnlyB) {
        expect(stateA, contains(update));
      }
      for (final update in updatesOnlyC) {
        expect(stateA, contains(update));
      }
      expect(stateA, contains(finalUpdate));
      
      // B should be missing C-only updates and possibly the final update
      expect(stateB, contains(baseUpdate));
      for (final update in updatesOnlyB) {
        expect(stateB, contains(update));
      }
      // This demonstrates the sync failure
      for (final update in updatesOnlyC) {
        expect(stateB, isNot(contains(update)), 
          reason: 'EXPECTED FAILURE: B should miss C-only updates');
      }
      
      // C should be missing B-only updates and possibly the final update  
      expect(stateC, contains(baseUpdate));
      for (final update in updatesOnlyB) {
        expect(stateC, isNot(contains(update)), 
          reason: 'EXPECTED FAILURE: C should miss B-only updates');
      }
      for (final update in updatesOnlyC) {
        expect(stateC, contains(update));
      }
      
      _logger.info('✅ Scenario 2 demonstrates divergent views as expected');
    }, timeout: Timeout(Duration(seconds: 90)));

    test('Scenario 3: Rapid updates during intermittent connectivity', () async {
      _logger.info('=== SCENARIO 3: Testing rapid updates with intermittent connectivity ===');
      
      await _resetNodes([nodeA, nodeB, nodeC]);
      
      // Create base state
      final baseUpdate = 'rapid-base-${DateTime.now().millisecondsSinceEpoch}';
      await nodeA.applyUpdate(baseUpdate);
      await Future.delayed(Duration(seconds: 2));
      
      // Simulate rapid updates with intermittent connectivity
      final allUpdates = <String>[];
      
      for (int round = 1; round <= 3; round++) {
        _logger.info('Round $round: Making rapid updates...');
        
        // Make several rapid updates
        for (int i = 1; i <= 3; i++) {
          final update = 'rapid-r${round}-u${i}-${DateTime.now().millisecondsSinceEpoch}';
          allUpdates.add(update);
          await nodeA.applyUpdate(update);
          await Future.delayed(Duration(milliseconds: 500)); // Rapid updates
        }
        
        // Simulate network instability - disconnect B randomly
        if (round == 2) {
          _logger.info('Simulating network instability for Node B...');
          await nodeB.simulateNetworkPartition();
          await Future.delayed(Duration(seconds: 2));
          await nodeB.reconnectAfterPartition([nodeA, nodeC]);
        }
        
        await Future.delayed(Duration(seconds: 1));
      }
      
      // Wait for any final synchronization attempts
      await Future.delayed(Duration(seconds: 5));
      
      // Check final states
      final stateA = await nodeA.getCurrentState();
      final stateB = await nodeB.getCurrentState();
      final stateC = await nodeC.getCurrentState();
      
      _logger.info('Rapid update results:');
      _logger.info('Node A state size: ${stateA?.length ?? 0}');
      _logger.info('Node B state size: ${stateB?.length ?? 0}');
      _logger.info('Node C state size: ${stateC?.length ?? 0}');
      
      // A should have all updates
      expect(stateA?.length, greaterThan(allUpdates.length), 
        reason: 'Node A should have all updates');
      
      // B and C should be missing some updates due to network instability
      final bMissing = allUpdates.where((update) => !(stateB?.contains(update) ?? false)).toList();
      final cMissing = allUpdates.where((update) => !(stateC?.contains(update) ?? false)).toList();
      
      _logger.info('Node B missing ${bMissing.length} updates: $bMissing');
      _logger.info('Node C missing ${cMissing.length} updates: $cMissing');
      
      // This demonstrates that rapid updates with network issues cause sync problems
      expect(bMissing.isNotEmpty || cMissing.isNotEmpty, isTrue,
        reason: 'EXPECTED FAILURE: Some nodes should miss updates during network instability');
      
      _logger.info('✅ Scenario 3 demonstrates rapid update sync issues as expected');
    }, timeout: Timeout(Duration(seconds: 120)));
  });
}

/// Helper function to connect all nodes to each other
Future<void> _connectNodes(List<_SyncFailureTestNode> nodes) async {
  for (int i = 0; i < nodes.length; i++) {
    for (int j = i + 1; j < nodes.length; j++) {
      final nodeA = nodes[i];
      final nodeB = nodes[j];
      
      try {
        if (nodeB.host.addrs.isNotEmpty) {
          nodeA.host.peerStore.addrBook.addAddrs(nodeB.peerId, nodeB.host.addrs, AddressTTL.permanentAddrTTL);
          await nodeA.host.connect(AddrInfo(nodeB.peerId, nodeB.host.addrs.toList()));
        }
        if (nodeA.host.addrs.isNotEmpty) {
          nodeB.host.peerStore.addrBook.addAddrs(nodeA.peerId, nodeA.host.addrs, AddressTTL.permanentAddrTTL);
          await nodeB.host.connect(AddrInfo(nodeA.peerId, nodeA.host.addrs.toList()));
        }
        
        // DHT routing table setup
        await nodeA.dht!.routingTable.tryAddPeer(nodeB.peerId, queryPeer: true);
        await nodeB.dht!.routingTable.tryAddPeer(nodeA.peerId, queryPeer: true);
        
      } catch (e) {
        _logger.warning('Failed to connect ${nodeA.peerId.toBase58().substring(0, 6)} to ${nodeB.peerId.toBase58().substring(0, 6)}: $e');
      }
    }
  }
  
  // Bootstrap DHTs
  for (final node in nodes) {
    try {
      await node.dht!.bootstrap();
    } catch (e) {
      _logger.warning('DHT bootstrap failed for ${node.peerId.toBase58().substring(0, 6)}: $e');
    }
  }
  
  // Wait for network to stabilize
  await Future.delayed(Duration(seconds: 5));
}

/// Helper function to reset nodes to clean state (if needed)
Future<void> _resetNodes(List<_SyncFailureTestNode> nodes) async {
  for (final node in nodes) {
    node.receivedAnnouncements.clear();
    node.sentAnnouncements.clear();
    node.failedSyncAttempts.clear();
    node.syncAttemptTimes.clear();
  }
} 